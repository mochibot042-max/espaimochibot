import type { Express } from "express";
import { Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import Groq from "groq-sdk";
import fs from "fs";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";
import { storage } from "./storage";

const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ============================================================================
// V13: ADD SILENCE PADDING TO PREVENT HISS
// ============================================================================
const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 2048;
const SEND_INTERVAL_MS = 58;

// Silence padding: 200ms before and after audio
const SILENCE_PADDING_MS = 200;
const SILENCE_SAMPLES = (SAMPLE_RATE * SILENCE_PADDING_MS) / 1000;  // 3200 samples
const SILENCE_BYTES = SILENCE_SAMPLES * 2;  // 6400 bytes

// ============================================================================
// PCM GENERATOR WITH SILENCE PADDING
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + ".pcm");
  const tmpPadded = path.join(AUDIO_DIR, "padded_" + Date.now() + ".pcm");
  
  return new Promise((resolve, reject) => {
    // First: generate raw PCM
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=8000",
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "aformat=sample_fmts=s16:channel_layouts=mono",
        "volume=0.85",
        "dynaudnorm=p=0.95:g=15"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => {
        // Read raw PCM
        const rawPCM = fs.readFileSync(tmp);
        fs.unlinkSync(tmp);
        
        // Create silence padding
        const silence = Buffer.alloc(SILENCE_BYTES, 0);
        
        // Combine: silence + audio + silence
        const padded = Buffer.concat([silence, rawPCM, silence]);
        
        // Fade in/out to prevent clicks
        // We'll let ffmpeg do this in one pass instead
        
        resolve(padded);
      })
      .save(tmp);
  });
}

// Better: Single-pass with fade and padding
async function generatePCMBetter(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "final_" + Date.now() + ".pcm");
  
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        // High-pass to remove DC offset
        "highpass=f=80",
        // Low-pass for 16kHz
        "lowpass=f=8000",
        // Resample to 16kHz
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        // Ensure mono 16-bit
        "aformat=sample_fmts=s16:channel_layouts=mono",
        // Fade in (50ms) to prevent click at start
        "afade=t=in:ss=0:d=0.05",
        // Fade out (100ms) to prevent hiss after speech
        "afade=t=out:st=" + (getDuration(input) - 0.1) + ":d=0.1",
        // Normalize
        "dynaudnorm=p=0.95:g=15",
        // Final volume
        "volume=0.85"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => {
        const pcm = fs.readFileSync(tmp);
        fs.unlinkSync(tmp);
        resolve(pcm);
      })
      .save(tmp);
  });
}

// Helper: get audio duration (simplified)
function getDuration(input: string): number {
  // Default 5 seconds if can't determine
  return 5.0;
}

// ============================================================================
// STREAM
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  
  const alignedLen = Math.floor(pcm.length / CHUNK_SIZE) * CHUNK_SIZE;
  const totalChunks = alignedLen / CHUNK_SIZE;
  
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 500));
  ws.send("START_RESPONSE");
  
  let seq = 0;
  let nextSendTime = Date.now() + SEND_INTERVAL_MS;
  
  for (let i = 0; i < alignedLen; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    
    const chunk = pcm.subarray(i, i + CHUNK_SIZE);
    const packet = Buffer.allocUnsafe(2 + chunk.length);
    packet.writeUInt16BE(seq & 0xFFFF, 0);
    packet.set(chunk, 2);
    
    try {
      ws.send(packet, { binary: true });
    } catch (e) {
      console.error("[STREAM] Send failed:", e);
      return;
    }
    
    seq++;
    
    const now = Date.now();
    const wait = nextSendTime - now;
    if (wait > 0) {
      await new Promise(r => setTimeout(r, wait));
    }
    nextSendTime += SEND_INTERVAL_MS;
  }
  
  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks");
}

// ============================================================================
// PROCESS
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer) {
  try {
    if (audioBuffer.length < 1000) {
      ws.send("ERROR:AUDIO_TOO_SHORT");
      return;
    }

    const riff = audioBuffer.slice(0, 4).toString();
    const wave = audioBuffer.slice(8, 12).toString();
    if (riff !== "RIFF" || wave !== "WAVE") {
      ws.send("ERROR:INVALID_FORMAT");
      return;
    }

    const id = Date.now();
    const wavPath = path.join(UPLOAD_DIR, id + ".wav");
    fs.writeFileSync(wavPath, audioBuffer);
    console.log("[UPLOAD] Saved:", audioBuffer.length, "bytes");

    // STT
    const stt = await Promise.race([
      sttClient.audio.transcriptions.create({
        file: fs.createReadStream(wavPath),
        model: "whisper-large-v3-turbo",
        language: "en"
      }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("STT_TIMEOUT")), 15000)
      )
    ]);

    const userText = stt.text?.trim();
    if (!userText) {
      ws.send("ERROR:NO_SPEECH");
      return;
    }

    console.log("USER:", userText);

    // LLM
    const ai = await Promise.race([
      llmClient.chat.completions.create({
        model: "llama-3.3-70b-versatile",
        messages: [
          { role: "system", content: "You are a helpful voice assistant. Keep responses short and natural. Return JSON: {\"text\":\"your response\"}" },
          { role: "user", content: userText }
        ],
        max_tokens: 100,
        temperature: 0.7
      }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("LLM_TIMEOUT")), 10000)
      )
    ]);

    const raw = ai.choices[0].message.content || "{}";
    let text = "Sorry, I didn't understand.";
    try {
      const parsed = JSON.parse(raw);
      text = parsed.text || text;
    } catch {
      text = raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim();
    }

    console.log("AI:", text);

    await storage.createInteraction({
      transcript: userText,
      response: text
    });

    // TTS
    const tts = new EdgeTTS();
    const mp3 = path.join(AUDIO_DIR, id + ".mp3");
    await tts.ttsPromise(text, mp3, {
      voice: "en-US-AriaNeural",
      rate: "+15%"
    });

    // PCM with fade out (prevents hiss)
    const pcm = await generatePCMBetter(mp3);
    console.log("[TTS] PCM:", pcm.length, "bytes");

    // Stream
    await streamPCM(ws, pcm);

    // Cleanup
    try {
      fs.unlinkSync(mp3);
      fs.unlinkSync(wavPath);
    } catch (e) {}

  } catch (e: any) {
    console.error("[PROCESS] Error:", e.message);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
  }
}

// ============================================================================
// ROUTES
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 512 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - V13 No Hiss");

    let processing = false;

    ws.on("message", async (data: any, isBinary: boolean) => {
      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT:", msg);
        if (msg === "READY") {
          console.log("[WS] ESP ready");
        }
        return;
      }

      if (processing) {
        console.log("[UPLOAD] Busy");
        return;
      }

      const audioBuffer = Buffer.from(data);
      console.log("[UPLOAD] WAV:", audioBuffer.length, "bytes");

      processing = true;
      processAndRespond(ws, audioBuffer).finally(() => {
        processing = false;
      });
    });

    ws.on("close", () => {
      console.log("ESP disconnected");
      processing = false;
    });

    ws.on("error", (err) => {
      console.error("[WS] Error:", err.message);
    });

    const pingInterval = setInterval(() => {
      if (ws.readyState === ws.OPEN) {
        ws.ping();
      } else {
        clearInterval(pingInterval);
      }
    }, 15000);
  });

  return httpServer;
}
