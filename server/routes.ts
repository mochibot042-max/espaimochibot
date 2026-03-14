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
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const VOLUME_FILE = path.join(process.cwd(), "volume.json");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 44100;
const SILENCE_MS = 100;
const CHUNK_SIZE = 2048;

/* VOLUME */
function loadVolume() {
  try {
    const data = fs.readFileSync(VOLUME_FILE, "utf-8");
    const json = JSON.parse(data);
    return json.volume ?? DEFAULT_VOLUME;
  } catch { return DEFAULT_VOLUME; }
}
function saveVolume(vol) { fs.writeFileSync(VOLUME_FILE, JSON.stringify({ volume: vol })); }

/* WAV HEADER, NORMALIZE, GENERATE PCM, STREAM PCM, MUSIC STREAM — same as dati mo (hindi ko binago) */
function createWavHeader(pcmLength, sampleRate = TARGET_SAMPLE_RATE, channels = 1, bitsPerSample = 16) { /* same */ }
function normalizeAudioInput(raw) { /* same */ }
async function generatePCM(inputPath) { /* same as your pasted */ }
async function streamPCM(ws, pcm) { /* same */ }
async function downloadSongStream(query, ws) { /* same */ }

/* SERVER */
export async function registerRoutes(httpServer, app) {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws/audio" });

  wss.on("connection", (ws) => {
    console.log("ESP32 connected");
    let audioChunks = [];
    let isProcessing = false;
    let currentVolume = loadVolume();

    ws.send(`VOLUME:${currentVolume}`);   // ← FIXED (string)

    ws.on("message", async (data, isBinary) => {
      if (isBinary) { audioChunks.push(Buffer.from(data)); return; }

      const msg = data.toString();
      if (msg !== "END_STREAM" || isProcessing) return;
      isProcessing = true;

      try {
        const fullAudio = Buffer.concat(audioChunks);
        audioChunks = [];
        const normalized = normalizeAudioInput(fullAudio);
        const tempId = `tmp-${Date.now()}`;
        const inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
        fs.writeFileSync(inputWavPath, Buffer.concat([createWavHeader(normalized.length), normalized]));

        const transcription = await sttClient.audio.transcriptions.create({ file: fs.createReadStream(inputWavPath), model: "whisper-large-v3-turbo" });
        const userText = transcription.text?.trim() || "";
        console.log("User:", userText);

        const chat = await llmClient.chat.completions.create({ /* same messages + Alicia prompt mo */ });
        const raw = chat.choices?.[0]?.message?.content?.trim() || "";
        let spokenText = "Please repeat.";
        let musicQuery = null;
        let parsedVolume = null;

        try {
          const parsed = JSON.parse(raw);
          spokenText = parsed.text || spokenText;
          musicQuery = parsed.music_query ?? null;
          if (parsed.volume !== null) parsedVolume = parseFloat(parsed.volume);
        } catch {}

        // TTS + STREAM
        const edge = new EdgeTTS();
        const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
        await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });
        const pcm = await generatePCM(tmpMp3);

        ws.send("START_RESPONSE");
        await streamPCM(ws, pcm);
        ws.send("FINISH_RESPONSE");
        fs.unlinkSync(tmpMp3);

        if (parsedVolume !== null) {
          currentVolume = Math.max(0.05, Math.min(1.5, parsedVolume));
          saveVolume(currentVolume);
          ws.send(`VOLUME:${currentVolume}`);
        }

        // SERVO (added para sa atin)
        if (parsed.pan !== undefined || parsed.tilt !== undefined) {
          const p = parsed.pan ?? -1;
          const t = parsed.tilt ?? -1;
          ws.send(`SERVO:${p},${t}`);
        }

        if (musicQuery) downloadSongStream(musicQuery, ws);

      } catch (err) {
        console.error(err);
        ws.send("ERROR");
      } finally {
        isProcessing = false;
        // cleanup
      }
    });

    ws.on("close", () => console.log("ESP32 disconnected"));
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
