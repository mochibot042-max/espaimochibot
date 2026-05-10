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
// V14: 500ms SILENCE TAIL + FADE OUT
// ============================================================================
const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 2048;
const SEND_INTERVAL_MS = 58;

// 500ms silence tail = 8000 samples = 16000 bytes
const SILENCE_TAIL_MS = 500;
const SILENCE_TAIL_BYTES = (SAMPLE_RATE * 2 * SILENCE_TAIL_MS) / 1000;  // 16000

// ============================================================================
// PCM WITH SILENCE TAIL AND FADE
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "proc_" + Date.now() + ".pcm");
  
  return new Promise((resolve, reject) => {
    // Get duration first
    ffmpeg.ffprobe(input, (err, metadata) => {
      if (err) {
        reject(err);
        return;
      }
      
      const duration = metadata.format.duration || 5.0;
      const fadeStart = Math.max(0, duration - 0.3);  // Fade out last 300ms
      
      ffmpeg(input)
        .audioFilters([
          "highpass=f=80",
          "lowpass=f=8000",
          "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
          "aformat=sample_fmts=s16:channel_layouts=mono",
          // Fade in 50ms
          "afade=t=in:ss=0:d=0.05",
          // Fade out 300ms (smooth ending)
          "afade=t=out:st=" + fadeStart + ":d=0.3",
          "dynaudnorm=p=0.95:g=15",
          "volume=0.85"
        ])
        .audioCodec("pcm_s16le")
        .audioChannels(1)
        .audioFrequency(SAMPLE_RATE)
        .format("s16le")
        .on("error", reject)
        .on("end", () => {
          const rawPCM = fs.readFileSync(tmp);
          fs.unlinkSync(tmp);
          
          // Add 500ms silence tail (prevents cut-off hiss)
          const silence = Buffer.alloc(SILENCE_TAIL_BYTES, 0);
          const finalPCM = Buffer.concat([rawPCM, silence]);
          
          console.log("[PCM] Raw: " + rawPCM.length + " + tail: " + silence.length + " = " + finalPCM.length);
          resolve(finalPCM);
        })
        .save(tmp);
    });
  });
}

// ============================================================================
// STREAM
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  
  const alignedLen = Math.floor(pcm.length / CHUNK_SIZE) * CHUNK_SIZE;
  const totalChunks = alignedLen / CHUNK_SIZE;
  
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 600));  // Longer prep
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
  
  // Wait before sending FINISH - ensure all data played
  await new Promise(r => setTimeout(r, 1000));
  
  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks + tail");
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

    // PCM with fade + silence tail
    const pcm = await generatePCM(mp3);
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
    console.log("ESP connected - V14 No Hiss");

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
