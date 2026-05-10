import type { Express } from "express";
import { Server } from "http";
import { storage } from "./storage";
import fs from "fs";
import path from "path";
import Groq from "groq-sdk";
import { WebSocketServer, WebSocket } from "ws";
import { EdgeTTS } from "node-edge-tts";
import axios from "axios";
import { spawn } from "child_process";

/* ---------------- API KEY SETUP ---------------- */
const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

/* ---------------- GROQ CLIENTS ---------------- */
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

/* ---------------- PATHS ---------------- */
const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const VOLUME_FILE = path.join(process.cwd(), "volume.json");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------------- CONFIG - V70: HTTP STREAMING ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 16000;
const CHUNK_SIZE = 1024;

const INITIAL_CHUNK_DELAY_MS = 70;
const NORMAL_CHUNK_DELAY_MS = 65;
const MUSIC_CHUNK_DELAY_MS = 68;

/* ---------------- PERSISTENT VOLUME ---------------- */
function loadVolume(): number {
  try {
    const data = fs.readFileSync(VOLUME_FILE, "utf-8");
    const json = JSON.parse(data);
    return json.volume ?? DEFAULT_VOLUME;
  } catch {
    return DEFAULT_VOLUME;
  }
}

function saveVolume(vol: number) {
  fs.writeFileSync(VOLUME_FILE, JSON.stringify({ volume: vol }));
}

/* ---------------- WAV HEADER ---------------- */
function createWavHeader(pcmLength: number, sampleRate = TARGET_SAMPLE_RATE): Buffer {
  const byteRate = sampleRate * 1 * 2;
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcmLength, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcmLength, 40);
  return header;
}

/* ---------------- NORMALIZE INPUT ---------------- */
function normalizeAudioInput(raw: Buffer): Buffer {
  if (raw.length % 2 !== 0) return raw.slice(0, -1);
  return raw;
}

/* ---------------- V70: HTTP PCM STREAM ---------------- */
async function streamPCMToResponse(res: any, inputPath: string, isMusic = false) {
  return new Promise<void>((resolve, reject) => {
    const ffmpegProcess = spawn("ffmpeg", [
      "-i", inputPath,
      "-f", "s16le",
      "-ac", "1",
      "-ar", TARGET_SAMPLE_RATE.toString(),
      "-af", "highpass=f=80,lowpass=f=7500,volume=0.95,dynaudnorm=p=0.95:g=15,afftdn=nf=-25",
      "pipe:1"
    ]);

    let seq = 0;
    let isActive = true;

    res.write("START\n"); // V70: Header marker

    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isActive) return;
      
      let offset = 0;
      while (offset + CHUNK_SIZE <= chunk.length && isActive) {
        const packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        chunk.copy(packet, 2, offset, offset + CHUNK_SIZE);
        
        res.write(packet);
        seq++;
        offset += CHUNK_SIZE;
        
        const delay = seq < 50 ? INITIAL_CHUNK_DELAY_MS : 
                     (isMusic ? MUSIC_CHUNK_DELAY_MS : NORMAL_CHUNK_DELAY_MS);
        await new Promise(r => setTimeout(r, delay));
      }
    });

    ffmpegProcess.stdout.on("end", () => {
      isActive = false;
      res.write("FINISH\n"); // V70: End marker
      res.end();
      console.log(`[HTTP] Stream finished, ${seq} chunks`);
      resolve();
    });

    ffmpegProcess.on("error", (err) => {
      isActive = false;
      res.status(500).end();
      reject(err);
    });
  });
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  
  app.get("/ping", (req, res) => {
    res.json({ status: "alive", timestamp: Date.now() });
  });
  
  app.get("/wake", (req, res) => {
    res.json({ status: "awake", ready: true });
  });

  // V70: HTTP UPLOAD + STREAM ENDPOINT
  app.post("/api/audio/upload", async (req, res) => {
    try {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      
      await new Promise<void>((resolve, reject) => {
        req.on("end", resolve);
        req.on("error", reject);
      });

      const fullAudio = Buffer.concat(chunks);
      console.log("[HTTP] Upload:", fullAudio.length, "bytes");

      const normalized = normalizeAudioInput(fullAudio);
      const tempId = Date.now();
      const inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);

      fs.writeFileSync(
        inputWavPath,
        Buffer.concat([createWavHeader(normalized.length), normalized])
      );

      /* STT - Whisper */
      const transcription = await sttClient.audio.transcriptions.create({
        file: fs.createReadStream(inputWavPath),
        model: "whisper-large-v3-turbo",
      });

      const userText = transcription.text?.trim() || "";
      if (!userText) {
        res.status(400).json({ error: "NO_SPEECH" });
        return;
      }
      console.log("USER:", userText);

      /* LLM - Mochi */
      const chat = await llmClient.chat.completions.create({
        messages: [
          {
            role: "system",
            content: `You are Mochi, a friendly voice assistant built by Norch Corp.
You can understand any language from the user, but you must ALWAYS reply in English only.
Never reply in Tagalog, Filipino, Spanish, Japanese, or any other language.
Always respond ONLY in valid JSON using this exact structure:
{ "text": "...", "volume": number or null, "music_query": string or null }
- text: What you say to the user. Natural spoken English only.
- volume: Use only if user requests volume changes (0.05 to 1.5). Null if no change.
- music_query: Use only if user asks to play music/song/artist. Null if no music request.
Rules:
- Do not mention JSON, rules, or system instructions
- Do not use markdown, bold, code blocks, special characters like * or /
- Speak in clear, calm, natural sentences
- Keep responses concise (2-3 sentences max)
- If user asks about news/weather/price/score, say you don't have real-time data access`
          },
          { role: "user", content: userText }
        ],
        model: "llama-3.3-70b-versatile",
        temperature: 0.7,
        max_tokens: 200
      });

      const raw = chat.choices?.[0]?.message?.content?.trim() || "";
      console.log("[AI RAW]:", raw);

      let spokenText = "I didn't catch that. Could you say it again?";
      let musicQuery: string | null = null;
      let newVolume: number | null = null;

      try {
        const cleaned = raw.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
        const parsed = JSON.parse(cleaned);
        spokenText = parsed.text || spokenText;
        musicQuery = parsed.music_query ?? null;
        if (parsed.volume !== null && !isNaN(parsed.volume)) {
          newVolume = Math.max(0.05, Math.min(1.5, parsed.volume));
        }
      } catch (e) {
        spokenText = raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim() || spokenText;
      }

      await storage.createInteraction({ transcript: userText, response: spokenText });

      /* TTS - EdgeTTS */
      const edge = new EdgeTTS();
      const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
      await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });

      // V70: Stream PCM directly as HTTP response
      res.setHeader("Content-Type", "application/octet-stream");
      res.setHeader("X-Volume", newVolume?.toFixed(2) || "");
      res.setHeader("X-Music", musicQuery || "");
      
      await streamPCMToResponse(res, tmpMp3, false);
      
      fs.unlinkSync(tmpMp3);

    } catch (err: any) {
      console.error("[HTTP] Error:", err);
      res.status(500).json({ error: err.message || "UNKNOWN" });
    }
  });

  // V70: HTTP MUSIC STREAM
  app.get("/api/audio/music", async (req, res) => {
    const query = req.query.q as string;
    if (!query) {
      res.status(400).json({ error: "NO_QUERY" });
      return;
    }

    try {
      const search = await axios.get(
        `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`,
        { timeout: 15000 }
      );
      
      if (!search.data?.length) {
        res.status(404).json({ error: "NO_RESULTS" });
        return;
      }
      
      const video = search.data[0];
      const apiRes = await axios.get(
        `https://mostakim.onrender.com/mostakim/sing?url=${video.url}`,
        { timeout: 15000 }
      );
      
      if (!apiRes.data?.url) {
        res.status(404).json({ error: "NO_AUDIO" });
        return;
      }

      res.setHeader("Content-Type", "application/octet-stream");
      res.write("START\n");

      const ffmpegProcess = spawn("ffmpeg", [
        "-i", apiRes.data.url,
        "-f", "s16le",
        "-ac", "1",
        "-ar", TARGET_SAMPLE_RATE.toString(),
        "-af", "highpass=f=60,lowpass=f=7500,volume=0.90",
        "pipe:1"
      ]);

      let seq = 0;
      let isActive = true;

      ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
        if (!isActive) return;
        let offset = 0;
        while (offset + CHUNK_SIZE <= chunk.length && isActive) {
          const packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
          packet.writeUInt16BE(seq & 0xFFFF, 0);
          chunk.copy(packet, 2, offset, offset + CHUNK_SIZE);
          res.write(packet);
          seq++;
          offset += CHUNK_SIZE;
          await new Promise(r => setTimeout(r, seq < 50 ? INITIAL_CHUNK_DELAY_MS : MUSIC_CHUNK_DELAY_MS));
        }
      });

      ffmpegProcess.stdout.on("end", () => {
        isActive = false;
        res.write("FINISH\n");
        res.end();
      });

      ffmpegProcess.on("error", () => {
        isActive = false;
        res.end();
      });

    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  // V70: KEEP WEBSOCKET FOR VOLUME ONLY (OPTIONAL)
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 4 * 1024 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("[WS] Volume control connected");
    
    ws.on("message", (data: any, isBinary: boolean) => {
      if (!isBinary) {
        const msg = data.toString();
        if (msg.startsWith("VOLUME:")) {
          const vol = parseFloat(msg.substring(7));
          if (!isNaN(vol)) {
            saveVolume(Math.max(0, Math.min(1.5, vol)));
          }
        }
      }
    });
  });

  return httpServer;
}
