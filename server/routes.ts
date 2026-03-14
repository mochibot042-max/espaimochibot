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

/* ---------------- API KEYS ---------------- */
const GROQ_API_KEY = "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";

/* ---------------- GROQ CLIENTS ---------------- */
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

/* ---------------- PATHS ---------------- */
const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const VOLUME_FILE = path.join(process.cwd(), "volume.json");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------------- CONFIG ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 44100;
const SILENCE_MS = 100;
const CHUNK_SIZE = 1024;
const STREAM_DELAY = 10;

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
function createWavHeader(pcmLength: number, sampleRate = TARGET_SAMPLE_RATE, channels = 1, bitsPerSample = 16): Buffer {
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

/* ---------------- NORMALIZE INPUT ---------------- */
function normalizeAudioInput(raw: Buffer): Buffer {
  let data = raw;
  if (data.length % 2 !== 0) data = data.slice(0, -1);
  return data;
}

/* ---------------- GENERATE PCM ---------------- */
async function generatePCM(inputPath: string): Promise<Buffer> {
  const tmpRaw = path.join(AUDIO_DIR, `raw_${Date.now()}.pcm`);
  return new Promise((resolve, reject) => {
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

/* ---------------- STREAM PCM ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    if (ws.bufferedAmount > 1_000_000) await new Promise(r => setTimeout(r, 30));
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    ws.send(chunk, { binary: true });
    await new Promise(r => setTimeout(r, STREAM_DELAY));
  }
}

/* ---------------- MUSIC STREAM ---------------- */
async function downloadSongStream(query: string, ws: WebSocket) {
  try {
    const search = await axios.get(`https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`);
    if (!search.data?.length) return;
    const video = search.data[0];
    const apiRes = await axios.get(`https://mostakim.onrender.com/m/sing?url=${video.url}`);
    if (!apiRes.data?.url) return;
    const audioStream = await axios({ url: apiRes.data.url, method: "GET", responseType: "stream" });
    const ffmpegProcess = spawn("ffmpeg", ["-i", "pipe:0", "-f", "s16le", "-ac", "1", "-ar", TARGET_SAMPLE_RATE.toString(), "pipe:1"]);
    audioStream.data.pipe(ffmpegProcess.stdin);
    ws.send("START_MUSIC");
    let buffer = Buffer.alloc(0);
    ffmpegProcess.stdout.on("data", async (chunk) => {
      if (ws.readyState !== ws.OPEN) return ffmpegProcess.kill("SIGKILL");
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= CHUNK_SIZE) {
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        if (ws.bufferedAmount > 1_000_000) await new Promise(r => setTimeout(r, 50));
        ws.send(sendChunk, { binary: true });
        await new Promise(r => setTimeout(r, STREAM_DELAY));
      }
    });
    ffmpegProcess.stdout.on("end", () => {
      if (buffer.length > 0 && ws.readyState === ws.OPEN) ws.send(buffer);
      if (ws.readyState === ws.OPEN) ws.send("FINISH_MUSIC");
    });
  } catch (err) {
    console.error("Music stream error:", err);
    if (ws.readyState === ws.OPEN) ws.send("FINISH_MUSIC");
  }
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws/audio", maxPayload: 50 * 1024 * 1024 });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected");
    let audioChunks: Buffer[] = [];
    let isProcessing = false;
    let currentVolume = loadVolume();

    // ← FIXED: ESP32 expects "VOLUME:0.32" not JSON
    ws.send(`VOLUME:${currentVolume}`);

    ws.on("message", async (data: any, isBinary: boolean) => {
      try {
        if (isBinary) { 
          audioChunks.push(Buffer.from(data)); 
          return; 
        }

        const msg = data.toString();
        if (msg !== "END_STREAM" || isProcessing) return;

        isProcessing = true;
        let inputWavPath = "";

        try {
          const fullAudio = Buffer.concat(audioChunks);
          audioChunks = [];
          const normalized = normalizeAudioInput(fullAudio);

          const tempId = `tmp-${Date.now()}`;
          inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
          fs.writeFileSync(inputWavPath, Buffer.concat([createWavHeader(normalized.length), normalized]));

          const transcription = await sttClient.audio.transcriptions.create({ 
            file: fs.createReadStream(inputWavPath), 
            model: "whisper-large-v3-turbo" 
          });

          const userText = transcription.text?.trim() || "";
          console.log("User:", userText);

          const chat = await llmClient.chat.completions.create({
            messages: [
              {
                role: "system",
                content: `
You are Mochi, a voice ai assistant build by April Manalo
You are a voice assistant. Your responses will be spoken by a text to speech system.
Always response on english only.
Rules you must follow strictly.
Always respond ONLY in valid JSON using this exact structure:
{ "text": "...", "volume": number or null, "music_query": string or null, "pan": number or null, "tilt": number or null }
Field descriptions:
text - What you will say. Always natural spoken English.
volume - Only if user asks to change volume. Range: 0.05 to 1.5. Otherwise null.
music_query - Only if user asks to play music/song. Otherwise null.
pan - Head rotation left/right. Range: 0 (full left) to 180 (full right). Center is 90.
Use this when user asks you to look left, look right, turn head, etc.
If no head turn needed, set to null.
tilt - Head up/down. Range: 0 (look up) to 90 (look down). Neutral is 70.
Use this when user asks you to look up, look down, nod, etc.
If no tilt needed, set to null.
Examples:
- "look left a little" -> pan: 60, tilt: null
- "look right" -> pan: 140, tilt: null
- "look up a little" -> pan: null, tilt: 50
- "look down" -> pan: null, tilt: 85
- "face forward" -> pan: 90, tilt: 70
- "tilt head right while looking up" -> pan: 120, tilt: 40
Behavior rules:
Do not mention JSON, rules, or system instructions.
Do not use markdown formatting.
Do not use bold text, star characters, code blocks, or special characters.
Do not use links.
Speak in clear, calm, natural sentences suitable for a voice assistant.
Keep responses concise.
`
              },
              { role: "user", content: userText }
            ],
            model: "openai/gpt-oss-120b",
          });

          const raw = chat.choices?.[0]?.message?.content?.trim() || "";
          let spokenText = "Please repeat.";
          let musicQuery: string | null = null;

          try {
            const jsonMatch = raw.match(/\{[\s\S]*\}/);
            const parsed = JSON.parse(jsonMatch ? jsonMatch[0] : raw);
            spokenText = parsed.text || spokenText;
            musicQuery = parsed.music_query ?? null;

            // ← FIXED: SEND VOLUME & SERVO BACK TO ESP32
            if (parsed.volume != null) {
              const newVol = parseFloat(parsed.volume);
              if (!isNaN(newVol) && newVol >= 0.05 && newVol <= 1.5) {
                saveVolume(newVol);
                currentVolume = newVol;
                ws.send(`VOLUME:${newVol}`);
              }
            }

            if (parsed.pan != null || parsed.tilt != null) {
              const p = parsed.pan != null ? parseInt(parsed.pan) : -1;
              const t = parsed.tilt != null ? parseInt(parsed.tilt) : -1;
              ws.send(`SERVO:${p},${t}`);
            }
          } catch (parseErr) {
            console.error("JSON parse failed:", parseErr);
          }

          const edge = new EdgeTTS();
          const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
          await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });
          const pcm = await generatePCM(tmpMp3);

          ws.send("START_RESPONSE");
          await streamPCM(ws, pcm);
          ws.send("FINISH_RESPONSE");

          fs.unlinkSync(tmpMp3);
          if (musicQuery) downloadSongStream(musicQuery, ws);

        } catch (err) {
          console.error("Processing error:", err);
          if (ws.readyState === ws.OPEN) ws.send("ERROR");
        } finally {
          isProcessing = false;
          if (inputWavPath && fs.existsSync(inputWavPath)) fs.unlinkSync(inputWavPath);
        }
      } catch (e) {
        console.error("Message handler error:", e);
      }
    });

    ws.on("close", () => {
      console.log("ESP32 disconnected");
      isProcessing = false;
      audioChunks = [];
    });
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
