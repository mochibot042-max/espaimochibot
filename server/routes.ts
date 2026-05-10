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
// V6: FILE UPLOAD SETTINGS
// ============================================================================
const SAMPLE_RATE = 44100;
const CHUNK_SIZE = 2048;
const CHUNK_DELAY_MS = 23;

// ============================================================================
// STREAM PCM BACK TO ESP (unchanged - still streaming for playback)
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 200));
  ws.send("START_RESPONSE");
  let seq = 0;
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
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
    await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
  }
  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks, total " + pcm.length + " bytes");
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
// V6: PROCESS UPLOADED WAV FILE
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer) {
  try {
    // Check minimum size (WAV header + some data)
    if (audioBuffer.length < 1000) {
      ws.send("ERROR:AUDIO_TOO_SHORT");
      return;
    }

    // Validate WAV header
    const riff = audioBuffer.slice(0, 4).toString();
    const wave = audioBuffer.slice(8, 12).toString();
    if (riff !== "RIFF" || wave !== "WAVE") {
      console.error("[UPLOAD] Invalid WAV header:", riff, wave);
      ws.send("ERROR:INVALID_AUDIO_FORMAT");
      return;
    }

    const id = Date.now();
    const wavPath = path.join(UPLOAD_DIR, id + ".wav");

    // Save the uploaded WAV directly (no need to rebuild)
    fs.writeFileSync(wavPath, audioBuffer);
    console.log("[UPLOAD] Saved WAV:", audioBuffer.length, "bytes");

    // Resample to 16kHz for STT
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

    // STT
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

    // LLM
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

    // TTS
    const tts = new EdgeTTS();
    const mp3 = path.join(AUDIO_DIR, id + ".mp3");
    await tts.ttsPromise(text, mp3, {
      voice: "en-US-AriaNeural"
    });

    // Generate PCM and stream back
    const pcm = await generatePCM(mp3);
    console.log("[TTS] Generated PCM: " + pcm.length + " bytes");
    await streamPCM(ws, pcm);

    // Cleanup
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
// V6: ROUTES WITH FILE UPLOAD SUPPORT
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 1024 * 1024  // 1MB max for file upload
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - V6 File Upload Mode");

    let processing = false;

    ws.on("message", async (data: any, isBinary: boolean) => {
      // Handle text messages
      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT:", msg);

        if (msg === "READY") {
          console.log("[WS] ESP ready");
          return;
        }

        return;
      }

      // V6: Binary data = complete WAV file upload
      if (processing) {
        console.log("[UPLOAD] Already processing, ignoring");
        return;
      }

      const audioBuffer = Buffer.from(data);
      console.log("[UPLOAD] Received WAV file:", audioBuffer.length, "bytes");

      processing = true;

      // Process in background
      processAndRespond(ws, audioBuffer).finally(() => {
        processing = false;
      });
    });

    ws.on("close", () => {
      console.log("ESP disconnected");
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
