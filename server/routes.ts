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
// HOTSPOT-OPTIMIZED SETTINGS
// ============================================================================
const SAMPLE_RATE = 44100;
const CHUNK_SIZE = 4096;           // ↑ MUCH larger: ~46ms audio per chunk
const CHUNK_DELAY_MS = 50;         // ↑ Slower pacing: slightly slower than real-time
                                   // This gives ESP32 buffer time to build up
const PREBUFFER_CHUNKS = 12;       // ↑ Wait for 12 chunks (~550ms) before play

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
// STREAM PCM - HOTSPOT OPTIMIZED
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;

  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);

  // Step 1: Tell ESP to prepare (prebuffer)
  ws.send("PREPARE_RESPONSE:" + totalChunks);

  // Step 2: Wait a bit for ESP to clear buffer and get ready
  await new Promise(r => setTimeout(r, 200));

  // Step 3: Send START signal
  ws.send("START_RESPONSE");

  // Step 4: Stream chunks with conservative pacing
  let seq = 0;
  let startTime = Date.now();

  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) {
      console.log("[STREAM] WebSocket closed mid-stream");
      return;
    }

    const chunk = pcm.subarray(i, i + CHUNK_SIZE);

    // 2-byte sequence header + audio data
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

    // HOTSPOT FIX: Slightly slower than real-time to let buffer build up
    // 4096 bytes / 2 = 2048 samples / 44100 = ~46.4ms
    // We send every 50ms = ~3.6ms slower per chunk
    // Over 100 chunks = 360ms extra buffer time
    await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
  }

  const duration = Date.now() - startTime;
  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks in " + duration + "ms, total " + pcm.length + " bytes");
}

// ============================================================================
// ROUTES
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 1024 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected");

    let chunks: Buffer[] = [];
    let processing = false;

    ws.on("message", async (data: any, isBinary: boolean) => {
      if (!isBinary) {
        const msg = data.toString();

        if (msg === "READY") {
          console.log("[WS] ESP ready for streaming");
          return;
        }

        if (msg !== "END_STREAM" || processing) return;
        processing = true;

        try {
          const audio = Buffer.concat(chunks);
          chunks = [];

          if (audio.length < 800) {
            ws.send("ERROR:AUDIO_TOO_SHORT");
            return;
          }

          const id = Date.now();
          const wavPath = path.join(UPLOAD_DIR, id + ".wav");

          fs.writeFileSync(wavPath, Buffer.concat([
            wavHeader(audio.length),
            audio
          ]));

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
            processing = false;
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
        } finally {
          processing = false;
        }

        return;
      }

      // Binary data = audio chunks from ESP
      chunks.push(Buffer.from(data));
    });

    ws.on("close", () => {
      console.log("ESP disconnected");
      chunks = [];
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
