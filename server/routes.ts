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
import axios from "axios";
import { spawn } from "child_process";

const GROQ_API_KEY = "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";

/* ---------------- GROQ ---------------- */
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

/* ---------------- PATHS ---------------- */
const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const VOLUME_FILE = path.join(__dirname, "volume.json");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------------- CONFIG ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 44100;
const CHUNK_SIZE = 2048;

/* ---------------- VOLUME ---------------- */
function loadVolume(): number {
  try {
    return JSON.parse(fs.readFileSync(VOLUME_FILE, "utf-8")).volume ?? DEFAULT_VOLUME;
  } catch {
    return DEFAULT_VOLUME;
  }
}

function saveVolume(vol: number) {
  fs.writeFileSync(VOLUME_FILE, JSON.stringify({ volume: vol }));
}

/* ---------------- WAV HEADER ---------------- */
function createWavHeader(pcmLength: number, sampleRate = 44100) {
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

/* ---------------- IMPROVED STT PREPROCESS ---------------- */
async function convertTo16k(inputPath: string): Promise<string> {
  const output = path.join(UPLOAD_DIR, `stt_${Date.now()}.wav`);

  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=8000",
        "aresample=16000"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(16000)
      .format("wav")
      .on("error", reject)
      .on("end", () => resolve(output))
      .save(output);
  });
}

/* ---------------- STREAM PCM ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    ws.send(pcm.slice(i, i + CHUNK_SIZE), { binary: true });
    await new Promise(r => setTimeout(r, 5));
  }
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {

  const wss = new WebSocketServer({ server: httpServer, path: "/ws/audio" });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected");

    let audioChunks: Buffer[] = [];
    let isProcessing = false;
    let currentVolume = loadVolume();

    ws.send(JSON.stringify({ volume: currentVolume }));

    ws.on("message", async (data: any, isBinary: boolean) => {

      if (isBinary) {
        audioChunks.push(Buffer.from(data));
        return;
      }

      if (data.toString() !== "END_STREAM" || isProcessing) return;

      isProcessing = true;

      try {
        /* ---------------- BUILD AUDIO ---------------- */
        const raw = Buffer.concat(audioChunks);
        audioChunks = [];

        const wavPath = path.join(UPLOAD_DIR, `input_${Date.now()}.wav`);
        fs.writeFileSync(wavPath, Buffer.concat([createWavHeader(raw.length), raw]));

        /* ---------------- 🔥 NEW STT ---------------- */
        const wav16 = await convertTo16k(wavPath);

        const transcription = await sttClient.audio.transcriptions.create({
          file: fs.createReadStream(wav16),
          model: "whisper-large-v3-turbo",
          response_format: "verbose_json",
          temperature: 0,
        });

        fs.unlinkSync(wavPath);
        fs.unlinkSync(wav16);

        const userText = transcription.text?.trim() || "";
        console.log("User:", userText);

        /* ---------------- LLM ---------------- */
        const chat = await llmClient.chat.completions.create({
          model: "openai/gpt-oss-120b",
          messages: [
            { role: "system", content: "You are a helpful voice assistant." },
            { role: "user", content: userText }
          ],
        });

        const reply = chat.choices?.[0]?.message?.content || "Sorry.";

        await storage.createInteraction({ transcript: userText, response: reply });

        /* ---------------- TTS ---------------- */
        const edge = new EdgeTTS();
        const mp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
        await edge.ttsPromise(reply, mp3, { voice: "en-US-JennyNeural" });

        const pcm = fs.readFileSync(mp3); // simple (pwede mo pa i ffmpeg kung gusto mo)

        ws.send("START_RESPONSE");
        await streamPCM(ws, pcm);
        ws.send("FINISH_RESPONSE");

        fs.unlinkSync(mp3);

      } catch (err) {
        console.error(err);
        ws.send("ERROR");
      }

      isProcessing = false;
    });

    ws.on("close", () => console.log("Disconnected"));
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
