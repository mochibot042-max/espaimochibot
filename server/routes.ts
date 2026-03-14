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

function loadVolume() {
  try {
    const data = fs.readFileSync(VOLUME_FILE, "utf-8");
    const json = JSON.parse(data);
    return json.volume ?? DEFAULT_VOLUME;
  } catch {
    return DEFAULT_VOLUME;
  }
}
function saveVolume(vol) {
  fs.writeFileSync(VOLUME_FILE, JSON.stringify({ volume: vol }));
}

function createWavHeader(pcmLength, sampleRate = TARGET_SAMPLE_RATE, channels = 1, bitsPerSample = 16) {
  const byteRate = sampleRate * channels * (bitsPerSample / 8);
  const blockAlign = channels * (bitsPerSample / 8);
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcmLength, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcmLength, 40);
  return header;
}

function normalizeAudioInput(raw) {
  let data = raw;
  if (data.length % 2 !== 0) data = data.slice(0, -1);
  return data;
}

async function generatePCM(inputPath) {
  const tmpRaw = path.join(AUDIO_DIR, `raw_${Date.now()}.pcm`);
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .noVideo()
      .audioFilters(["highpass=f=80", "lowpass=f=20000", `aresample=${TARGET_SAMPLE_RATE}:resampler=soxr:precision=28`, "pan=mono|c0=c0", "volume=0.95"])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(TARGET_SAMPLE_RATE)
      .on("error", reject)
      .save(tmpRaw)
      .on("end", () => {
        const pcm = fs.readFileSync(tmpRaw);
        const silence = Buffer.alloc(Math.floor((SILENCE_MS / 1000) * TARGET_SAMPLE_RATE * 2), 0);
        resolve(Buffer.concat([pcm, silence]));
        fs.unlinkSync(tmpRaw);
      });
  });
}

async function streamPCM(ws, pcm) {
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    ws.send(pcm.slice(i, i + CHUNK_SIZE), { binary: true });
    await new Promise(r => setTimeout(r, 5));
  }
}

async function downloadSongStream(query, ws) {
  // same as your old code (hindi ko binago)
  try {
    const search = await axios.get(`https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`);
    if (!search.data?.length) return;
    const video = search.data[0];
    const apiRes = await axios.get(`https://mostakim.onrender.com/m/sing?url=${video.url}`);
    if (!apiRes.data?.url) return;
    const audioStream = await axios({ url: apiRes.data.url, method: "GET", responseType: "stream" });
    const ffmpegProcess = spawn("ffmpeg", ["-i", "pipe:0", "-f", "s16le", "-ac", "1", "-ar", TARGET_SAMPLE_RATE.toString(), "pipe:1"]);
    audioStream.data.pipe(ffmpegProcess.stdin);
    ws.send("START_MUSIC");
    let buffer = Buffer.alloc(0);
    ffmpegProcess.stdout.on("data", async (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= CHUNK_SIZE) {
        ws.send(buffer.slice(0, CHUNK_SIZE), { binary: true });
        buffer = buffer.slice(CHUNK_SIZE);
        await new Promise(r => setTimeout(r, 5));
      }
    });
    ffmpegProcess.stdout.on("end", () => {
      if (buffer.length > 0) ws.send(buffer);
      ws.send("FINISH_MUSIC");
    });
  } catch (err) {
    console.error(err);
  }
}

export async function registerRoutes(httpServer, app) {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws/audio" });

  wss.on("connection", (ws) => {
    console.log("ESP32 CONNECTED");
    let audioChunks = [];
    let isProcessing = false;
    let currentVolume = loadVolume();

    ws.send(`VOLUME:${currentVolume}`);

    ws.on("message", async (data, isBinary) => {
      if (isBinary) {
        audioChunks.push(Buffer.from(data));
        return;
      }

      const msg = data.toString();
      if (msg !== "END_STREAM" || isProcessing) return;

      console.log(">>> END_STREAM RECEIVED - START PROCESSING");
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

        const chat = await llmClient.chat.completions.create({
          messages: [ /* ALICIA PROMPT MO DITO - same as your last message */ ],
          model: "openai/gpt-oss-120b",
        });

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

        if (musicQuery) downloadSongStream(musicQuery, ws);

      } catch (err) {
        console.error("ERROR:", err);
        ws.send("ERROR");
      } finally {
        isProcessing = false;
        audioChunks = [];
        console.log(">>> PROCESSING DONE");
      }
    });
  });

  return httpServer;
}
