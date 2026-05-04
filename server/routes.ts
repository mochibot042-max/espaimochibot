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
const CHUNK_SIZE = 1024;
const CHUNK_DELAY_MS = 8;

// ===================== WAV HEADER =====================
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

// ===================== PCM GENERATOR =====================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, `raw_${Date.now()}.pcm`);

  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=16000",
        `aresample=${SAMPLE_RATE}:resampler=soxr`,
        "volume=1.2"
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

// ===================== STREAM PCM =====================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;

  ws.send("START_RESPONSE");

  await new Promise(r => setTimeout(r, 60));

  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;

    ws.send(pcm.subarray(i, i + CHUNK_SIZE), { binary: true });

    await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
  }

  ws.send("FINISH_RESPONSE");
}

// ===================== ROUTES =====================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio"
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected");

    let chunks: Buffer[] = [];
    let processing = false;

    ws.on("message", async (data: any, isBinary: boolean) => {
      if (isBinary) {
        chunks.push(Buffer.from(data));
        return;
      }

      const msg = data.toString();

      if (msg !== "END_STREAM" || processing) return;
      processing = true;

      try {
        const audio = Buffer.concat(chunks);
        chunks = [];

        if (audio.length < 800) {
          ws.send("ERROR");
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

        const stt = await sttClient.audio.transcriptions.create({
          file: fs.createReadStream(resampled),
          model: "whisper-large-v3-turbo"
        });

        const userText = stt.text?.trim();
        if (!userText) return;

        console.log("USER:", userText);

        const ai = await llmClient.chat.completions.create({
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
          ]
        });

        const raw = ai.choices[0].message.content || "{}";

        let text = "Sorry.";
        try {
          text = JSON.parse(raw).text || text;
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

        await streamPCM(ws, pcm);

        fs.unlinkSync(mp3);
        fs.unlinkSync(wavPath);
        fs.unlinkSync(resampled);

      } catch (e) {
        console.error(e);
        ws.send("ERROR");
      } finally {
        processing = false;
      }
    });

    ws.on("close", () => console.log("ESP disconnected"));
  });

  return httpServer;
}
