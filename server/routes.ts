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

/* ---------------- CONFIG - V29: 48kHz ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 48000;
const SILENCE_MS = 150;
const CHUNK_SIZE = 1024;
const CHUNK_DELAY_MS = 20;
const INITIAL_CHUNK_DELAY_MS = 30;
const FAST_CHUNK_DELAY_MS = 15;

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
        "volume=0.90",
        "dynaudnorm=p=0.95:g=15",
        "afftdn=nf=-30"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(TARGET_SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => {
        let pcm = fs.readFileSync(tmpRaw);
        fs.unlinkSync(tmpRaw);

        const samples = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.length / 2);
        let sum = 0;
        for (let i = 0; i < samples.length; i++) sum += samples[i];
        const mean = Math.round(sum / samples.length);
        if (Math.abs(mean) > 10) {
          for (let i = 0; i < samples.length; i++) samples[i] -= mean;
        }

        const silenceBytes = Math.floor((SILENCE_MS / 1000) * TARGET_SAMPLE_RATE * 2);
        const silence = Buffer.alloc(silenceBytes, 0);
        const finalPCM = Buffer.concat([pcm, silence]);

        resolve(finalPCM);
      })
      .save(tmpRaw);
  });
}

/* ---------------- STREAM PCM TO ESP32 ---------------- */
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
    await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
  }

  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks at 48kHz");
}

/* ---------------- MUSIC STREAM ---------------- */
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
          
          const delay = seq < 20 ? INITIAL_CHUNK_DELAY_MS : 
                       seq < 50 ? FAST_CHUNK_DELAY_MS : 
                       CHUNK_DELAY_MS;
          
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
          await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
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

/* ============================================================================
   V29: SERVER - REAL-TIME AUDIO HANDLING
   ============================================================================ */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 4 * 1024 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected - V29 Real-time Voice");

    // V29: Real-time audio buffer - accumulates chunks until END_STREAM
    let audioChunks: Buffer[] = [];
    let isProcessing = false;
    let isRecording = false;  // V29: Track recording state
    let currentVolume = loadVolume();

    ws.send("VOLUME:" + currentVolume.toFixed(2));

    let lastPongTime = Date.now();
    
    ws.on("message", async (data: any, isBinary: boolean) => {
      // V29: Handle binary audio chunks in real-time
      if (isBinary) {
        // V29: If we're recording, accumulate chunks
        if (isRecording) {
          audioChunks.push(Buffer.from(data));
        }
        return;
      }

      const msg = data.toString();
      console.log("[WS] TEXT:", msg);

      if (msg === "READY") {
        console.log("[WS] ESP ready");
        return;
      }

      // V29: START_STREAM - Begin real-time accumulation
      if (msg === "START_STREAM") {
        console.log("[STREAM] Start real-time recording");
        audioChunks = [];
        isRecording = true;
        return;
      }

      // V29: END_STREAM - Process accumulated audio
      if (msg === "END_STREAM") {
        if (isProcessing) return;
        isProcessing = true;
        isRecording = false;  // Stop accumulating

        try {
          const fullAudio = Buffer.concat(audioChunks);
          audioChunks = [];
          console.log("[STREAM] Total received: " + fullAudio.length + " bytes");

          // V29: Save for debugging if needed
          const tempId = Date.now();
          const inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);

          const normalized = normalizeAudioInput(fullAudio);
          fs.writeFileSync(
            inputWavPath,
            Buffer.concat([createWavHeader(normalized.length), normalized])
          );

          /* STT - Whisper */
          console.log("[STT] Starting transcription...");
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

          /* LLM - Mochi */
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

          // Save interaction
          await storage.createInteraction({ transcript: userText, response: spokenText });

          /* TTS - EdgeTTS */
          console.log("[TTS] Generating voice...");
          const edge = new EdgeTTS();
          const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
          await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });

          const pcm = await generatePCM(tmpMp3);
          await streamPCM(ws, pcm);

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
          // Cleanup
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

      // Handle other text messages
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
      isRecording = false;
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
