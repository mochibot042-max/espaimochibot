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
// V11: SETTINGS - 16kHz ESP32
// ============================================================================
const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 1024;
const CHUNK_DELAY_MS = 46;

// ============================================================================
// WAV HEADER
// ============================================================================
function wavHeader(len: number): Buffer {
  const b = Buffer.alloc(44);
  b.write("RIFF", 0);
  b.writeUInt32LE(36 + len, 4);
  b.write("WAVE", 8);
  b.write("fmt ", 12);
  b.writeUInt32LE(16, 16);
  b.writeUInt16LE(1, 20);
  b.writeUInt16LE(1, 22);
  b.writeUInt32LE(SAMPLE_RATE, 24);
  b.writeUInt32LE(SAMPLE_RATE * 2, 28);
  b.writeUInt16LE(2, 32);
  b.writeUInt16LE(16, 34);
  b.write("data", 36);
  b.writeUInt32LE(len, 40);
  return b;
}

// ============================================================================
// PCM GENERATOR
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=16000",
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "volume=1.2",
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
// STREAM PCM BACK TO ESP
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 300));
  ws.send("START_RESPONSE");
  let seq = 0;
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.subarray(i, i + CHUNK_SIZE);
    const packet = Buffer.allocUnsafe(2 + chunk.length);
    packet.writeUInt16BE(seq & 0xFFFF, 0);
    chunk.copy(packet, 2);
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
  console.log("[STREAM] Sent " + seq + " chunks, total " + pcm.length + " bytes");
}

// ============================================================================
// V11: PROCESS AUDIO AND RESPOND
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer) {
  try {
    if (audioBuffer.length < 800) {
      ws.send("ERROR:AUDIO_TOO_SHORT");
      return;
    }

    const id = Date.now();
    const wavPath = path.join(UPLOAD_DIR, id + ".wav");

    // Write WAV with header
    fs.writeFileSync(wavPath, Buffer.concat([
      wavHeader(audioBuffer.length),
      audioBuffer
    ]));

    console.log("[PROCESS] Received audio: " + audioBuffer.length + " bytes");

    // Already 16kHz from ESP32, but resample just in case
    const resampled = path.join(UPLOAD_DIR, id + "_16k.wav");
    await new Promise<void>((res, rej) => {
      ffmpeg(wavPath)
        .audioFrequency(16000)
        .audioChannels(1)
        .format("wav")
        .on("end", res)
        .on("error", rej)
        .save(resampled);
    });

    const stt = await Promise.race([
      sttClient.audio.transcriptions.create({
        file: fs.createReadStream(resampled),
        model: "whisper-large-v3-turbo"
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

    const ai = await Promise.race([
      llmClient.chat.completions.create({
        model: "llama-3.3-70b-versatile",
        messages: [
          { role: "system", content: "Return JSON only: {text}" },
          { role: "user", content: userText }
        ],
        max_tokens: 150,
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
      text = raw;
    }

    await storage.createInteraction({
      transcript: userText,
      response: text
    });

    const tts = new EdgeTTS();
    const mp3 = path.join(AUDIO_DIR, id + ".mp3");
    await tts.ttsPromise(text, mp3, {
      voice: "en-US-AriaNeural"
    });

    const pcm = await generatePCM(mp3);
    console.log("[TTS] Generated PCM: " + pcm.length + " bytes");
    await streamPCM(ws, pcm);

    try {
      fs.unlinkSync(mp3);
      fs.unlinkSync(wavPath);
      fs.unlinkSync(resampled);
    } catch (e) {
      console.error("[CLEANUP] Error:", e);
    }

  } catch (e: any) {
    console.error("[PROCESS] Error:", e);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
  }
}

// ============================================================================
// V11: ROUTES - FULL BINARY AUDIO RECEIVE
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 2 * 1024 * 1024  // 2MB max payload for full audio
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - V11 Full Audio Upload");

    let audioBuffer: Buffer | null = null;
    let processing = false;

    ws.on("message", async (data: any, isBinary: boolean) => {
      // Handle text messages
      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT:", msg);

        if (msg === "READY") {
          console.log("[WS] ESP ready for streaming");
          return;
        }

        return;
      }

      // V11: Binary data = FULL AUDIO from ESP
      if (isBinary && !processing) {
        processing = true;
        console.log("[UPLOAD] Received full audio: " + data.length + " bytes");

        // Process the full audio buffer
        await processAndRespond(ws, Buffer.from(data));
        processing = false;
      }
    });

    ws.on("close", () => {
      console.log("ESP disconnected");
      audioBuffer = null;
      processing = false;
    });

    ws.on("error", (err) => {
      console.error("[WS] Error:", err);
    });

    const pingInterval = setInterval(() => {
      if (ws.readyState === ws.OPEN) {
        ws.ping();
      } else {
        clearInterval(pingInterval);
      }
    }, 10000);
  });

  return httpServer;
}
