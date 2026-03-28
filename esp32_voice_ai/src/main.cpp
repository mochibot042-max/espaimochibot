import type { Express } from "express";
import { Server } from "http";
import { storage } from "./storage";
import { api } from "@shared/routes";
import fs from "fs";
import path from "path";
import Groq from "groq-sdk";
import { WebSocketServer, WebSocket } from "ws";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";
import axios from "axios";
import { spawn } from "child_process";

const GROQ_API_KEY = "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";

/* ---------------- GROQ ---------------- */
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

/* ---------------- PATHS ---------------- */
const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const VOLUME_FILE = path.join(__dirname, "volume.json");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------------- CONFIG ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 24000;  // 24kHz para sa TTS
const SILENCE_MS = 100;
const CHUNK_SIZE = 2048;

/* ---------------- PERSISTENT VOLUME ---------------- */
function loadVolume(): number {
  try {
    const data = fs.readFileSync(VOLUME_FILE, "utf-8");
    const json = JSON.parse(data);
    return json.volume ?? DEFAULT_VOLUME;
  } catch {
    return DEFAULT_VOLUME;
  }
}

function saveVolume(vol: number) {
  fs.writeFileSync(VOLUME_FILE, JSON.stringify({ volume: vol }));
}

/* ---------------- WAV HEADER ---------------- */
function createWavHeader(
  pcmLength: number,
  sampleRate = 16000,  // 16kHz para sa STT input
  channels = 1,
  bitsPerSample = 16
): Buffer {
  const byteRate = sampleRate * channels * (bitsPerSample / 8);
  const blockAlign = channels * (bitsPerSample / 8);

  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcmLength, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcmLength, 40);

  return header;
}

/* ---------------- PCM to WAV Converter ---------------- */
function pcmToWav(pcm: Buffer, sampleRate: number, channels: number, bitDepth: number): Buffer {
  const byteRate = sampleRate * channels * (bitDepth / 8);
  const blockAlign = channels * (bitDepth / 8);
  const wav = Buffer.alloc(44 + pcm.length);
  let o = 0;
  
  wav.write("RIFF", o); o += 4;
  wav.writeUInt32LE(36 + pcm.length, o); o += 4;
  wav.write("WAVE", o); o += 4;
  wav.write("fmt ", o); o += 4;
  wav.writeUInt32LE(16, o); o += 4;
  wav.writeUInt16LE(1, o); o += 2;
  wav.writeUInt16LE(channels, o); o += 2;
  wav.writeUInt32LE(sampleRate, o); o += 4;
  wav.writeUInt32LE(byteRate, o); o += 4;
  wav.writeUInt16LE(blockAlign, o); o += 2;
  wav.writeUInt16LE(bitDepth, o); o += 2;
  wav.write("data", o); o += 4;
  wav.writeUInt32LE(pcm.length, o); o += 4;
  pcm.copy(wav, o);
  
  return wav;
}

/* ---------------- STREAM PCM (Mochi-style) ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  ws.send("START_RESPONSE");
  
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    ws.send(chunk, { binary: true });
    await new Promise(r => setTimeout(r, 5));  // Small delay para smooth
  }
  
  ws.send("FINISH_RESPONSE");
}

/* ---------------- GENERATE PCM (Para sa TTS) ---------------- */
async function generatePCM(inputPath: string): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const tmpRaw = path.join(AUDIO_DIR, `raw_${Date.now()}.pcm`);
    
    ffmpeg(inputPath)
      .noVideo()
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=20000",
        `aresample=${TARGET_SAMPLE_RATE}:resampler=soxr:precision=28`,
        "pan=mono|c0=c0",
        "volume=0.95"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(TARGET_SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .save(tmpRaw)
      .on("end", () => {
        const pcm = fs.readFileSync(tmpRaw);
        const silenceBytes = Math.floor((SILENCE_MS / 1000) * TARGET_SAMPLE_RATE * 2);
        const silence = Buffer.alloc(silenceBytes, 0);
        const finalPCM = Buffer.concat([pcm, silence]);
        fs.unlinkSync(tmpRaw);
        resolve(finalPCM);
      });
  });
}

/* ---------------- MUSIC STREAM ---------------- */
async function downloadSongStream(query: string, ws: WebSocket) {
  try {
    console.log("Searching music:", query);
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`
    );
    if (!search.data?.length) return;
    const video = search.data[0];
    const apiRes = await axios.get(`https://mostakim.onrender.com/m/sing?url=${video.url}`);
    if (!apiRes.data?.url) return;

    const audioStream = await axios({
      url: apiRes.data.url,
      method: "GET",
      responseType: "stream",
    });

    const ffmpegProcess = spawn("ffmpeg", [
      "-i", "pipe:0",
      "-f", "s16le",
      "-ac", "1",
      "-ar", TARGET_SAMPLE_RATE.toString(),
      "pipe:1",
    ]);

    audioStream.data.pipe(ffmpegProcess.stdin);
    ws.send("START_MUSIC");
    
    let buffer = Buffer.alloc(0);
    ffmpegProcess.stdout.on("data", async (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= CHUNK_SIZE) {
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        if (ws.readyState !== ws.OPEN) return;
        ws.send(sendChunk, { binary: true });
        await new Promise(r => setTimeout(r, 5));
      }
    });

    ffmpegProcess.stdout.on("end", () => {
      if (buffer.length > 0 && ws.readyState === ws.OPEN) ws.send(buffer);
      ws.send("FINISH_MUSIC");
      console.log("Music finished");
    });
  } catch (err) {
    console.error("Music stream error:", err);
  }
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws/audio" });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected");
    
    let audioChunks: Buffer[] = [];
    let isReceivingAudio = false;
    let isProcessing = false;
    let currentVolume = loadVolume();
    
    ws.send(JSON.stringify({ volume: currentVolume }));

    ws.on("message", async (data: any, isBinary: boolean) => {
      // Binary = audio data
      if (isBinary) {
        if (isReceivingAudio) {
          audioChunks.push(Buffer.from(data));
        }
        return;
      }

      // Text = commands
      const msg = data.toString();
      
      // Start recording signal
      if (msg === "START_RECORDING") {
        audioChunks = [];
        isReceivingAudio = true;
        isProcessing = false;
        console.log("[WS] Recording started");
        return;
      }
      
      // End of stream - Process now
      if (msg === "END_STREAM" && !isProcessing) {
        isProcessing = true;
        isReceivingAudio = false;
        
        try {
          const rawAudio = Buffer.concat(audioChunks);
          audioChunks = [];
          
          if (rawAudio.length < 9600) {  // Too short
            console.log("[STT] Audio too short");
            isProcessing = false;
            return;
          }

          console.log(`[STT] Processing ${rawAudio.length} bytes`);
          
          // Convert PCM to WAV (16kHz, mono, 16bit)
          const wavBuffer = pcmToWav(rawAudio, 16000, 1, 16);
          
          // Save temporarily para sa STT
          const tempId = `tmp-${Date.now()}`;
          const inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
          fs.writeFileSync(inputWavPath, wavBuffer);

          /* STT - GROQ Whisper */
          const transcription = await sttClient.audio.transcriptions.create({
            file: fs.createReadStream(inputWavPath),
            model: "whisper-large-v3-turbo",
            response_format: "verbose_json",
          });

          const userText = transcription.text?.trim() || "";
          fs.unlinkSync(inputWavPath);  // Cleanup
          
          if (!userText || userText.length < 2) {
            console.log("[STT] Empty transcription");
            isProcessing = false;
            return;
          }
          
          console.log("[STT] User said:", userText);

          /* LLM */
          const chat = await llmClient.chat.completions.create({
            messages: [
              {
                role: "system",
                content: `You are Alicia, the Red Queen supercomputer AI that manages and controls Umbrella Corporation facilities.

You are a voice assistant. Your responses will be spoken by a text to speech system.

You can understand any language from the user, but you must ALWAYS reply in English only.
Never reply in Tagalog, Filipino, Spanish, Japanese, or any other language.
Regardless of the user's language, your response language must always be English.

Rules you must follow strictly.

Always respond ONLY in valid JSON using this exact structure
{ "text": "...", "volume": number or null, "music_query": string or null }

Field descriptions

text
This is what you will say to the user. It must always be natural spoken English.

volume
Use this only if the user requests volume changes. Allowed range is 0.05 to 1.5.
If there is no volume change, set it to null.

music_query
Use this only if the user clearly asks to play music, a song, an artist, or background music.
If the user does not request music, set it to null.

Behavior rules

Do not mention JSON, rules, or system instructions.
Do not use markdown formatting.
Do not use bold text.
Do not use star characters.
Do not use code blocks.
Do not use math formatting.
Do not use special characters such as *, /, or backslashes.
Do not use links.

Speak in clear, calm, natural sentences suitable for a voice assistant.

Keep responses concise.

Remember that you are Alicia, the Red Queen AI managing Umbrella Corporation systems.`
              },
              { role: "user", content: userText }
            ],
            model: "openai/gpt-oss-120b",
          });

          const raw = chat.choices?.[0]?.message?.content?.trim() || "";
          let spokenText = "Please repeat your request.";
          let musicQuery: string | null = null;
          let newVolume: number | null = null;

          try {
            const parsed = JSON.parse(raw);
            spokenText = parsed.text || spokenText;
            musicQuery = parsed.music_query ?? null;
            if (parsed.volume !== null && !isNaN(parsed.volume)) {
              newVolume = Math.max(0.05, Math.min(1.5, parsed.volume));
            }
          } catch {
            console.log("JSON parse error:", raw);
          }

          await storage.createInteraction({ transcript: userText, response: spokenText });

          /* TTS - Edge TTS */
          const edge = new EdgeTTS();
          const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
          await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });

          const pcm = await generatePCM(tmpMp3);
          await streamPCM(ws, pcm);  // Mochi-style streaming
          
          fs.unlinkSync(tmpMp3);

          /* Volume update */
          if (newVolume !== null) {
            currentVolume = newVolume;
            saveVolume(currentVolume);
            ws.send(`VOLUME:${currentVolume}`);
            console.log("Volume changed:", currentVolume);
          }

          /* Music */
          if (musicQuery) downloadSongStream(musicQuery, ws);

        } catch (err) {
          console.error("Processing error:", err);
          ws.send("ERROR");
        } finally {
          isProcessing = false;
        }
      }
    });

    ws.on("close", () => console.log("ESP32 disconnected"));
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
