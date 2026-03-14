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

/* ---------------- API KEY ---------------- */
const GROQ_API_KEY = "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";

/* ---------------- GROQ ---------------- */
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
  sampleRate = TARGET_SAMPLE_RATE,
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

/* ---------------- STREAM PCM ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    ws.send(chunk, { binary: true });
    await new Promise(r => setTimeout(r, 5));
  }
}

/* ---------------- MUSIC STREAM ---------------- */
async function downloadSongStream(query: string, ws: WebSocket) {
  let ffmpegProcess: any = null;
  
  try {
    console.log("Searching music:", query);
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`
    );
    if (!search.data?.length) {
      ws.send("ERROR_MUSIC_NOT_FOUND");
      return;
    }
    const video = search.data[0];
    const apiRes = await axios.get(`https://mostakim.onrender.com/m/sing?url=${video.url}`);
    if (!apiRes.data?.url) {
      ws.send("ERROR_MUSIC_NO_URL");
      return;
    }

    const audioStream = await axios({
      url: apiRes.data.url,
      method: "GET",
      responseType: "stream",
      timeout: 30000,
    });

    ffmpegProcess = spawn("ffmpeg", [
      "-i", "pipe:0",
      "-f", "s16le",
      "-ac", "1",
      "-ar", TARGET_SAMPLE_RATE.toString(),
      "-af", "volume=0.9",
      "pipe:1",
    ]);

    audioStream.data.pipe(ffmpegProcess.stdin);

    ws.send("START_MUSIC");

    let buffer = Buffer.alloc(0);
    let isActive = true;

    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isActive || ws.readyState !== ws.OPEN) return;
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= CHUNK_SIZE && isActive) {
        if (ws.readyState !== ws.OPEN) {
          isActive = false;
          break;
        }
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        ws.send(sendChunk, { binary: true });
        await new Promise(r => setTimeout(r, 4));
      }
    });

    ffmpegProcess.stdout.on("end", () => {
      isActive = false;
      if (buffer.length > 0 && ws.readyState === ws.OPEN) {
        ws.send(buffer);
      }
      if (ws.readyState === ws.OPEN) {
        ws.send("FINISH_MUSIC");
      }
      console.log("Music finished");
    });

    ffmpegProcess.stderr.on("data", (data: Buffer) => {
      console.log("FFmpeg:", data.toString());
    });

    // Store reference to kill if needed
    (ws as any).musicProcess = ffmpegProcess;

  } catch (err) {
    console.error("Music stream error:", err);
    if (ws.readyState === ws.OPEN) {
      ws.send("ERROR_MUSIC");
    }
    if (ffmpegProcess) ffmpegProcess.kill();
  }
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    maxPayload: 50 * 1024 * 1024,
    perMessageDeflate: false
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected");
    
    // Heartbeat to prevent timeout
    let heartbeatInterval: NodeJS.Timeout;
    let isProcessing = false;

    ws.on("pong", () => {
      (ws as any).isAlive = true;
    });

    let audioChunks: Buffer[] = [];
    let currentVolume = loadVolume();

    // Send initial volume
    ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));

    ws.on("message", async (data: any, isBinary: boolean) => {
      try {
        if (isBinary) {
          audioChunks.push(Buffer.from(data));
          return;
        }

        const msg = data.toString();
        console.log("Received:", msg);

        // Handle ping/pong manually if needed
        if (msg === "ping") {
          ws.send("pong");
          return;
        }

        if (msg !== "END_STREAM" || isProcessing) return;

        isProcessing = true;
        let inputWavPath = "";

        try {
          const fullAudio = Buffer.concat(audioChunks);
          audioChunks = [];
          const normalized = normalizeAudioInput(fullAudio);
          const tempId = `tmp-${Date.now()}`;
          inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);

          fs.writeFileSync(
            inputWavPath,
            Buffer.concat([createWavHeader(normalized.length), normalized])
          );

          /* STT */
          const transcription = await sttClient.audio.transcriptions.create({
            file: fs.createReadStream(inputWavPath),
            model: "whisper-large-v3-turbo",
          });

          const userText = transcription.text?.trim() || "";
          console.log("User:", userText);

          /* LLM */
          const chat = await llmClient.chat.completions.create({
            messages: [
              {
                role: "system",
                content: `You are Alicia, the Red Queen supercomputer AI managing Umbrella Corporation facilities.

You are a voice assistant. Responses will be spoken by text-to-speech.

You understand any language but MUST ALWAYS reply in English only.

Respond ONLY in valid JSON with this exact structure:
{ "text": "...", "volume": number or null, "music_query": string or null, "servo_pan": number or null, "servo_tilt": number or null }

Field descriptions:
- text: What you say to user. Natural spoken English only.
- volume: Use ONLY if user requests volume changes. Range 0.05 to 1.5. Null if no change.
- music_query: Use ONLY if user asks for music/song. Null if no music.
- servo_pan: Use ONLY if user asks to look left/right/turn head. Range 0-180 (0=left, 90=center, 180=right). Null if no movement.
- servo_tilt: Use ONLY if user asks to look up/down. Range 0-90 (0=down, 90=up). Null if no movement.

Examples:
User: "look left" -> servo_pan: 0
User: "look right" -> servo_pan: 180  
User: "look up" or "tumingala" -> servo_tilt: 90
User: "look down" -> servo_tilt: 0
User: "center your head" -> servo_pan: 90, servo_tilt: 45

Behavior rules:
- No markdown, no bold, no stars, no code blocks
- Speak in clear, calm, natural sentences
- Keep responses concise
- Remember you are Alicia, Red Queen AI`
              },
              { role: "user", content: userText }
            ],
            model: "llama-3.1-8b-instant",
          });

          const raw = chat.choices?.[0]?.message?.content?.trim() || "";

          let spokenText = "Please repeat your request.";
          let musicQuery: string | null = null;
          let newVolume: number | null = null;
          let servoPan: number | null = null;
          let servoTilt: number | null = null;

          try {
            const parsed = JSON.parse(raw);
            spokenText = parsed.text || spokenText;
            musicQuery = parsed.music_query ?? null;
            servoPan = parsed.servo_pan ?? null;
            servoTilt = parsed.servo_tilt ?? null;
            
            if (parsed.volume !== null && !isNaN(parsed.volume)) {
              newVolume = Math.max(0.05, Math.min(1.5, parsed.volume));
            }
          } catch (e) {
            console.log("JSON parse error:", raw);
            // Fallback: use raw text if JSON fails
            spokenText = raw.replace(/[{}"]/g, '').substring(0, 200) || "I didn't understand that.";
          }

          await storage.createInteraction({ transcript: userText, response: spokenText });

          /* Send servo commands FIRST (before TTS) */
          if (servoPan !== null || servoTilt !== null) {
            const panVal = servoPan ?? -1;
            const tiltVal = servoTilt ?? -1;
            ws.send(JSON.stringify({ 
              type: "servo", 
              pan: panVal, 
              tilt: tiltVal 
            }));
            console.log("Servo command:", panVal, tiltVal);
          }

          /* TTS */
          const edge = new EdgeTTS();
          const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
          await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });

          const pcm = await generatePCM(tmpMp3);
          ws.send("START_RESPONSE");
          await streamPCM(ws, pcm);
          ws.send("FINISH_RESPONSE");

          fs.unlinkSync(tmpMp3);

          /* Volume update */
          if (newVolume !== null) {
            currentVolume = newVolume;
            saveVolume(currentVolume);
            ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));
            console.log("Volume changed:", currentVolume);
          }

          /* Music */
          if (musicQuery) {
            downloadSongStream(musicQuery, ws);
          }

        } catch (err) {
          console.error("Processing error:", err);
          if (ws.readyState === ws.OPEN) {
            ws.send("ERROR");
          }
        } finally {
          isProcessing = false;
          if (inputWavPath && fs.existsSync(inputWavPath)) {
            fs.unlinkSync(inputWavPath);
          }
        }
      } catch (e) {
        console.error("Message handler error:", e);
        isProcessing = false;
      }
    });

    // Setup heartbeat
    (ws as any).isAlive = true;
    heartbeatInterval = setInterval(() => {
      if ((ws as any).isAlive === false) {
        console.log("Terminating inactive connection");
        if ((ws as any).musicProcess) {
          (ws as any).musicProcess.kill();
        }
        return ws.terminate();
      }
      (ws as any).isAlive = false;
      ws.ping();
    }, 30000);

    ws.on("error", (error) => {
      console.error("WebSocket error:", error);
      isProcessing = false;
      audioChunks = [];
    });

    ws.on("close", () => {
      console.log("ESP32 disconnected");
      clearInterval(heartbeatInterval);
      isProcessing = false;
      audioChunks = [];
      if ((ws as any).musicProcess) {
        (ws as any).musicProcess.kill();
      }
    });
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
