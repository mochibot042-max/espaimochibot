import type { Express } from "express";
import { Server } from "http";
import { storage } from "./storage";
import { api } from "@shared/routes";
import fs from "fs";
import path from "path";
import Groq from "groq-sdk";
import { WebSocketServer, WebSocket } from "ws";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";

const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 44100;
const CHUNK_SIZE = 1024;
const CHUNK_DELAY_MS = 8;

function createWavHeader(pcmLength: number, sampleRate = 44100): Buffer {
  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcmLength, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcmLength, 40);

  return header;
}

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
        "volume=0.95"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(TARGET_SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .save(tmpRaw)
      .on("end", () => {
        const pcm = fs.readFileSync(tmpRaw);
        fs.unlinkSync(tmpRaw);
        resolve(pcm);
      });
  });
}

async function streamPCM(
  ws: WebSocket,
  pcm: Buffer,
  startMsg: string,
  endMsg: string
) {
  ws.send(startMsg);

  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    const chunk = pcm.slice(i, i + CHUNK_SIZE);

    if (ws.readyState !== ws.OPEN) return;

    ws.send(chunk, { binary: true });
    await new Promise((r) => setTimeout(r, CHUNK_DELAY_MS));
  }

  ws.send(endMsg);
}

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected");

    let audioChunks: Buffer[] = [];
    let isProcessing = false;
    let currentVolume = DEFAULT_VOLUME;

    ws.send(`VOLUME:${currentVolume.toFixed(2)}`);

    ws.on("message", async (data: any, isBinary: boolean) => {
      if (isBinary) {
        audioChunks.push(Buffer.from(data));
        return;
      }

      const msg = data.toString();

      if (msg !== "END_STREAM" || isProcessing) return;

      isProcessing = true;

      let inputWavPath = "";

      try {
        const fullAudio = Buffer.concat(audioChunks);
        audioChunks = [];

        if (fullAudio.length < 800) {
          ws.send("ERROR: too short");
          return;
        }

        const tempId = `tmp-${Date.now()}`;

        inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);

        fs.writeFileSync(
          inputWavPath,
          Buffer.concat([
            createWavHeader(fullAudio.length, 44100),
            fullAudio
          ])
        );

        const resampledPath = path.join(
          UPLOAD_DIR,
          `${tempId}_16k.wav`
        );

        await new Promise<void>((resolve, reject) => {
          ffmpeg(inputWavPath)
            .noVideo()
            .audioFilters([
              `aresample=16000:resampler=soxr:precision=28`
            ])
            .audioCodec("pcm_s16le")
            .audioChannels(1)
            .audioFrequency(16000)
            .format("wav")
            .on("error", reject)
            .on("end", resolve)
            .save(resampledPath);
        });

        // =========================
        // Speech-to-Text
        // =========================
        const transcription =
          await sttClient.audio.transcriptions.create({
            file: fs.createReadStream(resampledPath),
            model: "whisper-large-v3-turbo"
          });

        if (fs.existsSync(resampledPath)) {
          fs.unlinkSync(resampledPath);
        }

        const userText = transcription.text?.trim() || "";

        if (!userText) {
          ws.send("ERROR: no text");
          return;
        }

        console.log("User:", userText);

        /// FIXES:
/// 1. Groq browser_search tool often breaks JSON formatting
/// 2. GPT-OSS may return tool-call structures instead of plain content
/// 3. ESP expects immediate START_RESPONSE + binary stream
/// 4. Safer fallback added
/// 5. Removed unstable tool mode for real-time voice
/// 6. Faster model for voice assistant

// =========================
// REPLACE ONLY LLM SECTION
// =========================

const chatCompletion =
  await llmClient.chat.completions.create({
    messages: [
      {
        role: "system",
        content:
          'You are Alicia, a highly intelligent quantum AI voice assistant. Respond ONLY with raw JSON in this exact format: {"text":"your response","volume":1.0}. No markdown, no code block, no explanations.'
      },
      {
        role: "user",
        content: userText
      }
    ],
    model: "llama-3.3-70b-versatile",
    temperature: 0.6,
    max_tokens: 250,
    top_p: 1,
    stream: false
  });

let raw =
  chatCompletion.choices?.[0]?.message?.content?.trim() || "";

let spokenText =
  "I'm sorry, I couldn't process that properly.";

try {
  // Clean accidental markdown/codeblocks
  raw = raw
    .replace(/```json/gi, "")
    .replace(/```/g, "")
    .trim();

  const parsed = JSON.parse(raw);

  spokenText = parsed.text || spokenText;

  if (parsed.volume !== undefined) {
    currentVolume = parseFloat(parsed.volume);

    if (!isNaN(currentVolume)) {
      ws.send(`VOLUME:${currentVolume.toFixed(2)}`);
    }
  }
} catch (err) {
  console.log("JSON parse failed, fallback raw text:", raw);

  // If AI ignored JSON, use plain text directly
  spokenText = raw || spokenText;
}

console.log("AI:", spokenText);

        // =========================
        // Save interaction
        // =========================
        await storage.createInteraction({
          transcript: userText,
          response: spokenText
        });

        // =========================
        // Text-to-Speech
        // =========================
        const edge = new EdgeTTS();

        const tmpMp3 = path.join(
          AUDIO_DIR,
          `tts_${Date.now()}.mp3`
        );

        await edge.ttsPromise(spokenText, tmpMp3, {
          voice: "en-US-AriaNeural"
        });

        const pcm = await generatePCM(tmpMp3);

        await streamPCM(
          ws,
          pcm,
          "START_RESPONSE",
          "FINISH_RESPONSE"
        );

        if (fs.existsSync(tmpMp3)) {
          fs.unlinkSync(tmpMp3);
        }
      } catch (err) {
        console.error("Processing Error:", err);
        ws.send("ERROR");
      } finally {
        isProcessing = false;

        if (
          inputWavPath &&
          fs.existsSync(inputWavPath)
        ) {
          fs.unlinkSync(inputWavPath);
        }
      }
    });

    ws.on("close", () => {
      console.log("Disconnected");
    });
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
