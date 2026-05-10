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
// V9: HOTSPOT SURVIVAL - BURST MODE + SLOW STEADY STREAM
// ============================================================================
const SAMPLE_RATE = 16000;

// BURST MODE: Send 4x chunks together, then longer gap
// This handles hotspot jitter better than steady small chunks
const CHUNK_SIZE = 1024;        // 64ms of audio @ 16kHz
const BURST_COUNT = 4;          // 4 chunks per burst = 256ms
const BURST_INTERVAL_MS = 350;  // Send burst every 350ms (90ms safety margin)

// Total: 256ms audio every 350ms = 73% duty cycle = plenty of headroom

// ============================================================================
// PCM GENERATOR
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=8000",
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "volume=0.85",          // Conservative to prevent clipping
        "dynaudnorm=p=0.95:g=15"
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

// ============================================================================
// BURST STREAM: Send groups of chunks for hotspot stability
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  
  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);
  const totalBursts = Math.ceil(totalChunks / BURST_COUNT);
  
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  
  // VERY LONG prep time: let ESP build huge buffer
  await new Promise(r => setTimeout(r, 1000));
  ws.send("START_RESPONSE");
  
  let seq = 0;
  
  for (let burst = 0; burst < totalBursts; burst++) {
    if (ws.readyState !== ws.OPEN) return;
    
    // Send BURST_COUNT chunks immediately (back-to-back)
    for (let c = 0; c < BURST_COUNT; c++) {
      const idx = burst * BURST_COUNT + c;
      if (idx >= totalChunks) break;
      
      const offset = idx * CHUNK_SIZE;
      const chunk = pcm.subarray(offset, offset + CHUNK_SIZE);
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
    }
    
    // Wait before next burst
    await new Promise(r => setTimeout(r, BURST_INTERVAL_MS));
  }
  
  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks in " + totalBursts + " bursts");
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
      console.error("[UPLOAD] Bad WAV:", riff, wave);
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
        max_tokens: 100,        // SHORTER for faster response
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
      rate: "+15%"            // Faster for hotspot
    });

    // PCM
    const pcm = await generatePCM(mp3);
    console.log("[TTS] PCM:", pcm.length, "bytes @", SAMPLE_RATE, "Hz");

    // Burst stream
    await streamPCM(ws, pcm);

    // Cleanup
    try {
      fs.unlinkSync(mp3);
      fs.unlinkSync(wavPath);
    } catch (e) {
      // ignore
    }

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
    console.log("ESP connected - V9 Hotspot");

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
