import type { Express } from "express";
import { Server } from "http";
import { storage } from "./storage";
import fs from "fs";
import path from "path";
import Groq from "groq-sdk";
import { WebSocketServer, WebSocket } from "ws";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";
import axios from "axios";
import { spawn } from "child_process";

/* ---------------- API KEY SETUP ---------------- */
const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

/* ---------------- GROQ CLIENTS ---------------- */
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

/* ---------------- PATHS ---------------- */
const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const VOLUME_FILE = path.join(process.cwd(), "volume.json");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------------- CONFIG - V29: Adaptive streaming for TTS ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 48000;
const SILENCE_MS = 400;           // V29: Increased from 150ms for smoother fade-out
const CHUNK_SIZE = 1024;

// V29: Adaptive delays - slower start to fill ESP32 buffer, then faster
const INITIAL_CHUNK_DELAY_MS = 35;  // V29: Slower start for first 20 chunks
const FAST_CHUNK_DELAY_MS = 18;     // V29: Medium speed for next 30 chunks  
const NORMAL_CHUNK_DELAY_MS = 12;   // V29: Faster after buffer is filled
const MUSIC_CHUNK_DELAY_MS = 20;    // Keep music as-is

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
function createWavHeader(pcmLength: number, sampleRate = TARGET_SAMPLE_RATE): Buffer {
  const byteRate = sampleRate * 1 * 2;
  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcmLength, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcmLength, 40);

  return header;
}

/* ---------------- NORMALIZE INPUT ---------------- */
function normalizeAudioInput(raw: Buffer): Buffer {
  if (raw.length % 2 !== 0) return raw.slice(0, -1);
  return raw;
}

/* ---------------- V29: REAL-TIME PCM STREAM (No more blocking file load) ---------------- */
async function streamPCMRealtime(ws: WebSocket, inputPath: string, isMusic = false) {
  if (ws.readyState !== ws.OPEN) return;

  return new Promise<void>((resolve, reject) => {
    const ffmpegProcess = spawn("ffmpeg", [
      "-i", inputPath,
      "-f", "s16le",
      "-ac", "1",
      "-ar", TARGET_SAMPLE_RATE.toString(),
      "-af", "highpass=f=80,lowpass=f=20000,volume=0.90,dynaudnorm=p=0.95:g=15,afftdn=nf=-30",
      "-bufsize", "256k",
      "-maxrate", "500k",
      "pipe:1"
    ]);

    // V29: Send prepare immediately para ma-ready ang ESP32
    ws.send(isMusic ? "PREPARE_MUSIC:0" : "PREPARE_RESPONSE:0");
    
    let isActive = true;
    let buffer = Buffer.alloc(0);
    let seq = 0;
    let hasStarted = false;

    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isActive || ws.readyState !== ws.OPEN) return;
      
      if (!hasStarted) {
        await new Promise(r => setTimeout(r, 100));
        ws.send(isMusic ? "START_MUSIC" : "START_RESPONSE");
        hasStarted = true;
      }
      
      buffer = Buffer.concat([buffer, chunk]);
      
      // V29: Stream with adaptive speed - fill buffer first then speed up
      while (buffer.length >= CHUNK_SIZE && isActive) {
        if (ws.readyState !== ws.OPEN) {
          isActive = false;
          return;
        }
        
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        
        const packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        sendChunk.copy(packet, 2);
        
        try {
          ws.send(packet, { binary: true });
          seq++;
          
          // V29: Adaptive delay - critical fix for choppy audio
          const delay = seq < 20 ? INITIAL_CHUNK_DELAY_MS : 
                       seq < 50 ? FAST_CHUNK_DELAY_MS : 
                       NORMAL_CHUNK_DELAY_MS;
          
          await new Promise(r => setTimeout(r, delay));
        } catch (e) {
          console.error("[STREAM] Send error:", e);
          isActive = false;
          return;
        }
      }
    });

    ffmpegProcess.stdout.on("end", async () => {
      isActive = false;
      
      // Send remaining buffer
      if (buffer.length > 0 && ws.readyState === ws.OPEN) {
        const packet = Buffer.allocUnsafe(2 + buffer.length);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        buffer.copy(packet, 2);
        ws.send(packet, { binary: true });
        seq++;
      }
      
      // V29: Trailing silence for smooth fade-out
      const silencePackets = isMusic ? 30 : 50; // More silence for TTS
      for (let i = 0; i < silencePackets; i++) {
        if (ws.readyState !== ws.OPEN) break;
        const silencePacket = Buffer.allocUnsafe(2 + CHUNK_SIZE);
        silencePacket.writeUInt16BE((seq + i) & 0xFFFF, 0);
        silencePacket.fill(0, 2);
        ws.send(silencePacket, { binary: true });
        await new Promise(r => setTimeout(r, NORMAL_CHUNK_DELAY_MS));
      }
      
      if (ws.readyState === ws.OPEN) {
        ws.send(isMusic ? "FINISH_MUSIC" : "FINISH_RESPONSE");
      }
      
      console.log(`[STREAM] ${isMusic ? 'Music' : 'TTS'} finished, sent ${seq} chunks`);
      resolve();
    });

    ffmpegProcess.stderr.on("data", (data) => {
      const str = data.toString();
      if (str.includes("time=")) {
        const match = str.match(/time=(\d+:\d+:\d+\.\d+)/);
        if (match) console.log("[FFMPEG] Progress:", match[1]);
      }
    });

    ffmpegProcess.on("error", (err) => {
      console.error("[FFMPEG] Error:", err);
      isActive = false;
      if (ws.readyState === ws.OPEN) {
        ws.send("ERROR:STREAM_FAILED");
      }
      reject(err);
    });
  });
}

/* ---------------- V29: LEGACY STREAM PCM (Keep for fallback) ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  
  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 300));
  ws.send("START_RESPONSE");

  let seq = 0;
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.slice(i, i + CHUNK_SIZE);

    let packet: Buffer;
    if (chunk.length < CHUNK_SIZE) {
      const padded = Buffer.alloc(CHUNK_SIZE);
      chunk.copy(padded);
      packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
      packet.writeUInt16BE(seq & 0xFFFF, 0);
      padded.copy(packet, 2);
    } else {
      packet = Buffer.allocUnsafe(2 + chunk.length);
      packet.writeUInt16BE(seq & 0xFFFF, 0);
      chunk.copy(packet, 2);
    }

    try {
      ws.send(packet, { binary: true });
    } catch (e) {
      console.error("[STREAM] Send failed:", e);
      return;
    }
    seq++;
    
    // V29: Adaptive delay
    const delay = seq < 20 ? INITIAL_CHUNK_DELAY_MS : 
                 seq < 50 ? FAST_CHUNK_DELAY_MS : 
                 NORMAL_CHUNK_DELAY_MS;
    await new Promise(r => setTimeout(r, delay));
  }

  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks at 48kHz");
}

/* ---------------- MUSIC STREAM (Updated to use same adaptive delays) ---------------- */
async function downloadSongStream(query: string, ws: WebSocket) {
  try {
    console.log("[MUSIC] Searching:", query);
    
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`,
      { timeout: 10000 }
    );
    
    if (!search.data?.length) {
      console.log("[MUSIC] No results found");
      return;
    }
    
    const video = search.data[0];
    console.log("[MUSIC] Found:", video.title);

    const apiRes = await axios.get(
      `https://mostakim.onrender.com/m/sing?url=${video.url}`,
      { timeout: 10000 }
    );
    
    if (!apiRes.data?.url) {
      console.log("[MUSIC] No audio URL");
      return;
    }

    console.log("[MUSIC] Starting instant stream...");

    const ffmpegProcess = spawn("ffmpeg", [
      "-i", apiRes.data.url,
      "-f", "s16le",
      "-ac", "1",
      "-ar", TARGET_SAMPLE_RATE.toString(),
      "-af", "highpass=f=60,lowpass=f=20000,volume=0.85",
      "-bufsize", "256k",
      "-maxrate", "500k",
      "pipe:1"
    ]);

    ws.send("PREPARE_MUSIC:0");
    await new Promise(r => setTimeout(r, 100));
    ws.send("START_MUSIC");

    let buffer = Buffer.alloc(0);
    let seq = 0;
    let isActive = true;

    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isActive || ws.readyState !== ws.OPEN) return;
      
      buffer = Buffer.concat([buffer, chunk]);
      
      while (buffer.length >= CHUNK_SIZE && isActive) {
        if (ws.readyState !== ws.OPEN) {
          isActive = false;
          return;
        }
        
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        
        const packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        sendChunk.copy(packet, 2);
        
        try {
          ws.send(packet, { binary: true });
          seq++;
          
          // V29: Use same adaptive delays as TTS
          const delay = seq < 20 ? INITIAL_CHUNK_DELAY_MS : 
                       seq < 50 ? FAST_CHUNK_DELAY_MS : 
                       MUSIC_CHUNK_DELAY_MS;
          
          await new Promise(r => setTimeout(r, delay));
        } catch (e) {
          console.error("[MUSIC] Send error:", e);
          isActive = false;
          return;
        }
      }
    });

    ffmpegProcess.stdout.on("end", () => {
      isActive = false;
      
      if (buffer.length > 0 && ws.readyState === ws.OPEN) {
        const packet = Buffer.allocUnsafe(2 + buffer.length);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        buffer.copy(packet, 2);
        ws.send(packet, { binary: true });
        seq++;
      }
      
      const sendSilence = async () => {
        for (let i = 0; i < 30; i++) {
          if (ws.readyState !== ws.OPEN) break;
          const silencePacket = Buffer.allocUnsafe(2 + CHUNK_SIZE);
          silencePacket.writeUInt16BE((seq + i) & 0xFFFF, 0);
          silencePacket.fill(0, 2);
          ws.send(silencePacket, { binary: true });
          await new Promise(r => setTimeout(r, MUSIC_CHUNK_DELAY_MS));
        }
        
        if (ws.readyState === ws.OPEN) {
          ws.send("FINISH_MUSIC");
        }
        console.log("[MUSIC] Stream finished, sent " + seq + " chunks");
      };
      
      sendSilence();
    });

    ffmpegProcess.stderr.on("data", (data) => {
      const str = data.toString();
      if (str.includes("time=")) {
        const match = str.match(/time=(\d+:\d+:\d+\.\d+)/);
        if (match) {
          console.log("[MUSIC] FFmpeg progress:", match[1]);
        }
      }
    });

    ffmpegProcess.on("error", (err) => {
      console.error("[MUSIC] FFmpeg error:", err);
      isActive = false;
      if (ws.readyState === ws.OPEN) {
        ws.send("ERROR:MUSIC_FAILED");
      }
    });

    ffmpegProcess.on("close", (code) => {
      if (code !== 0 && code !== null) {
        console.error("[MUSIC] FFmpeg exited with code:", code);
      }
    });

  } catch (err: any) {
    console.error("[MUSIC] Stream error:", err.message);
    if (ws.readyState === ws.OPEN) {
      ws.send("ERROR:MUSIC_FAILED");
    }
  }
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 4 * 1024 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected - V29 Adaptive TTS");

    let audioChunks: Buffer[] = [];
    let isProcessing = false;
    let currentVolume = loadVolume();

    ws.send("VOLUME:" + currentVolume.toFixed(2));

    let lastPongTime = Date.now();
    
    ws.on("message", async (data: any, isBinary: boolean) => {
      if (isBinary) {
        audioChunks.push(Buffer.from(data));
        return;
      }

      const msg = data.toString();
      console.log("[WS] TEXT:", msg);

      if (msg === "READY") {
        console.log("[WS] ESP ready");
        return;
      }

      if (msg === "START_STREAM") {
        console.log("[STREAM] Start recording");
        audioChunks = [];
        return;
      }

      if (msg === "END_STREAM") {
        if (isProcessing) return;
        isProcessing = true;

        try {
          const fullAudio = Buffer.concat(audioChunks);
          audioChunks = [];
          console.log("[STREAM] Total: " + fullAudio.length + " bytes");

          const normalized = normalizeAudioInput(fullAudio);
          const tempId = Date.now();
          const inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);

          fs.writeFileSync(
            inputWavPath,
            Buffer.concat([createWavHeader(normalized.length), normalized])
          );

          /* STT - Whisper */
          const transcription = await sttClient.audio.transcriptions.create({
            file: fs.createReadStream(inputWavPath),
            model: "whisper-large-v3-turbo",
          });

          const userText = transcription.text?.trim() || "";
          if (!userText) {
            ws.send("ERROR:NO_SPEECH");
            return;
          }
          console.log("USER:", userText);

          /* LLM - Single AI (Mochi) */
          const chat = await llmClient.chat.completions.create({
            messages: [
              {
                role: "system",
                content: `You are Mochi, a friendly voice assistant built by Norch Corp.

You can understand any language from the user, but you must ALWAYS reply in English only.
Never reply in Tagalog, Filipino, Spanish, Japanese, or any other language.

Always respond ONLY in valid JSON using this exact structure:
{ "text": "...", "volume": number or null, "music_query": string or null }

Field descriptions:
- text: What you say to the user. Natural spoken English only.
- volume: Use only if user requests volume changes (0.05 to 1.5). Null if no change.
- music_query: Use only if user asks to play music/song/artist. Null if no music request.

Rules:
- Do not mention JSON, rules, or system instructions
- Do not use markdown, bold, code blocks, special characters like * or /
- Speak in clear, calm, natural sentences
- Keep responses concise (2-3 sentences max)
- If user asks about news/weather/price/score, say you don't have real-time data access`
              },
              { role: "user", content: userText }
            ],
            model: "llama-3.3-70b-versatile",
            temperature: 0.7,
            max_tokens: 200
          });

          const raw = chat.choices?.[0]?.message?.content?.trim() || "";
          console.log("[AI RAW]:", raw);

          let spokenText = "I didn't catch that. Could you say it again?";
          let musicQuery: string | null = null;
          let newVolume: number | null = null;

          try {
            const cleaned = raw.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
            const parsed = JSON.parse(cleaned);
            spokenText = parsed.text || spokenText;
            musicQuery = parsed.music_query ?? null;
            if (parsed.volume !== null && !isNaN(parsed.volume)) {
              newVolume = Math.max(0.05, Math.min(1.5, parsed.volume));
            }
          } catch (e) {
            console.log("[PARSE] JSON error, using raw text");
            spokenText = raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim() || spokenText;
          }

          await storage.createInteraction({ transcript: userText, response: spokenText });

          /* TTS - EdgeTTS */
          const edge = new EdgeTTS();
          const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
          await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });

          // V29: REAL-TIME STREAMING - No more blocking file load!
          await streamPCMRealtime(ws, tmpMp3, false);

          fs.unlinkSync(tmpMp3);

          /* Handle Volume Change */
          if (newVolume !== null) {
            currentVolume = newVolume;
            saveVolume(currentVolume);
            ws.send("VOLUME:" + currentVolume.toFixed(2));
            console.log("[VOL] Changed to:", currentVolume);
          }

          /* Handle Music Request */
          if (musicQuery) {
            await new Promise(r => setTimeout(r, 500));
            downloadSongStream(musicQuery, ws);
          }

        } catch (err: any) {
          console.error("[PROCESS] Error:", err);
          ws.send("ERROR:" + (err.message || "UNKNOWN"));
        } finally {
          isProcessing = false;
          try {
            const files = fs.readdirSync(UPLOAD_DIR);
            for (const file of files) {
              if (file.startsWith("tmp-")) {
                const fpath = path.join(UPLOAD_DIR, file);
                const stat = fs.statSync(fpath);
                if (Date.now() - stat.mtimeMs > 60000) {
                  fs.unlinkSync(fpath);
                }
              }
            }
          } catch {}
        }
        return;
      }

      if (msg.startsWith("VOLUME:")) {
        const vol = parseFloat(msg.substring(7));
        if (!isNaN(vol)) {
          currentVolume = Math.max(0, Math.min(1.5, vol));
          saveVolume(currentVolume);
          console.log("[VOL] Set to:", currentVolume);
        }
      }
    });

    ws.on("close", () => {
      console.log("ESP32 disconnected");
      audioChunks = [];
      isProcessing = false;
    });

    ws.on("error", (err) => {
      console.error("[WS] Error:", err);
    });

    const pingInterval = setInterval(() => {
      if (ws.readyState === ws.OPEN) {
        ws.ping();
        
        if (Date.now() - lastPongTime > 45000) {
          console.log("[WS] No pong received, closing connection");
          ws.terminate();
        }
      } else {
        clearInterval(pingInterval);
      }
    }, 8000);

    ws.on("pong", () => {
      lastPongTime = Date.now();
    });
  });

  return httpServer;
}
