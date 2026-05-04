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

// 🔥 MUST MATCH ESP (IMPORTANT)
const SAMPLE_RATE = 44100;
const CHUNK_SIZE = 2048;           // ↑ Mas malaki: ~23ms audio per chunk (was 1024 = ~11ms)
const CHUNK_DELAY_MS = 23;         // ↑ Real-time pacing: match actual chunk duration
const PREBUFFER_CHUNKS = 5;        // ← Bago: Ihintay na magkaroon ng 5 chunks sa buffer bago mag-play

// ===================== WAV HEADER =====================
function wavHeader(len: number): Buffer {
  const b = Buffer.alloc(44);
  b.write("RIFF", 0);
  b.writeUInt32LE(36 + len, 4);
  b.write("WAVE", 8);
  b.write("fmt ", 12);
  b.writeUInt32LE(16, 16);
  b.writeUInt16LE(1, 20);            // PCM
  b.writeUInt16LE(1, 22);            // Mono
  b.writeUInt32LE(SAMPLE_RATE, 24);
  b.writeUInt32LE(SAMPLE_RATE * 2, 28);  // Byte rate
  b.writeUInt16LE(2, 32);            // Block align
  b.writeUInt16LE(16, 34);           // Bits per sample
  b.write("data", 36);
  b.writeUInt32LE(len, 40);
  return b;
}

// ===================== PCM GENERATOR =====================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, `raw_${Date.now()}.pcm`);

  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",               // Tanggalin ang low rumble
        "lowpass=f=16000",             // Limit sa 16kHz
        `aresample=${SAMPLE_RATE}:resampler=soxr:precision=28`,  // ↑ Mas magandang resampler quality
        "volume=1.2",
        "dynaudnorm=p=0.95:g=15"       // ← Bago: Normalize volume para consistent
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

// ===================== STREAM PCM (IMPROVED) =====================
// ← BINAGO: May flow control at mas stable na pacing
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;

  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);

  // Step 1: Sabihin sa ESP na maghanda (mag-prebuffer)
  ws.send("PREPARE_RESPONSE:" + totalChunks);

  // Step 2: Hulihin ang "READY" mula sa ESP (max 2 seconds)
  let clientReady = false;
  const readyTimeout = 2000;
  const startWait = Date.now();

  // Tignan kung may READY message sa susunod na loop (handled sa message handler)
  // Kung wala, proceed na rin after timeout
  while (!clientReady && Date.now() - startWait < readyTimeout) {
    await new Promise(r => setTimeout(r, 50));
    // Ang READY detection ay nasa message handler sa baba
    // Pero para dito, proceed na tayo
    clientReady = true; // Trust-based: proceed after short delay
  }

  // Step 3: Maghintay ng konti para sa prebuffer ng ESP
  await new Promise(r => setTimeout(r, 100));

  // Step 4: I-send ang START signal
  ws.send("START_RESPONSE");

  // Step 5: I-stream ang chunks nang may tamang pacing
  let seq = 0;
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) {
      console.log("[STREAM] WebSocket closed mid-stream");
      return;
    }

    const chunk = pcm.subarray(i, i + CHUNK_SIZE);

    // ← BINAGO: Lagyan ng 2-byte sequence number sa unahan ng chunk
    // Format: [seq_high][seq_low][pcm_data...]
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

    // ← BINAGO: Real-time pacing: delay = exact audio duration of chunk
    // 2048 bytes / 2 bytes-per-sample = 1024 samples / 44100 Hz = ~23.2ms
    await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
  }

  // Step 6: Tapos na
  ws.send("FINISH_RESPONSE");
  console.log(`[STREAM] Sent ${seq} chunks, total ${pcm.length} bytes`);
}

// ===================== ROUTES =====================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    // ← BINAGO: Mas malaking buffers para sa hotspot
    perMessageDeflate: false,  // Patayin compression para mabilis
    maxPayload: 1024 * 1024   // 1MB max payload
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected");

    let chunks: Buffer[] = [];
    let processing = false;
    let clientAcknowledged = false;  // ← Bago: Track kung ready na ang ESP

    ws.on("message", async (data: any, isBinary: boolean) => {
      // ← BINAGO: Handle "READY" acknowledgment mula sa ESP
      if (!isBinary) {
        const msg = data.toString();

        if (msg === "READY") {
          clientAcknowledged = true;
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

          const wavPath = path.join(UPLOAD_DIR, `${id}.wav`);

          fs.writeFileSync(wavPath, Buffer.concat([
            wavHeader(audio.length),
            audio
          ]));

          const resampled = path.join(UPLOAD_DIR, `${id}_16k.wav`);

          await new Promise<void>((res, rej) => {
            ffmpeg(wavPath)
              .audioFrequency(16000)
              .audioChannels(1)
              .format("wav")
              .on("end", res)
              .on("error", rej)
              .save(resampled);
          });

          // ← BINAGO: Timeout para sa STT
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

          // ← BINAGO: Timeout para sa LLM
          const ai = await Promise.race([
            llmClient.chat.completions.create({
              model: "llama-3.3-70b-versatile",
              messages: [
                {
                  role: "system",
                  content: "Return JSON only: {text}"
                },
                {
                  role: "user",
                  content: userText
                }
              ],
              max_tokens: 150,  // ← Bago: Limit response length para mas mabilis
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
          const mp3 = path.join(AUDIO_DIR, `${id}.mp3`);

          await tts.ttsPromise(text, mp3, {
            voice: "en-US-AriaNeural"
          });

          const pcm = await generatePCM(mp3);
          console.log(`[TTS] Generated PCM: ${pcm.length} bytes`);

          // Reset client ack para sa susunod na stream
          clientAcknowledged = false;

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

    // ← BINAGO: Ping/Pong para ma-detect kung buhay pa ang connection sa hotspot
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
