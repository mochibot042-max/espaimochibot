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
const CHUNK_DELAY_MS = 8; // 8ms = ~125 chunks/sec = smooth

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

async function streamPCM(ws: WebSocket, pcm: Buffer, startMsg: string, endMsg: string) {
  console.log(`Streaming ${pcm.length} bytes...`);
  
  ws.send(startMsg);
  
  // Send all chunks with consistent delay
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    if (ws.readyState !== ws.OPEN) return;
    
    ws.send(chunk, { binary: true });
    await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
  }
  
  ws.send(endMsg);
  console.log("Stream done");
}

export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
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
        
        if (fullAudio.length < 200) {
          ws.send("ERROR: too short");
          return;
        }

        const tempId = `tmp-${Date.now()}`;
        inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
        fs.writeFileSync(inputWavPath, Buffer.concat([
          createWavHeader(fullAudio.length, 16000),
          fullAudio
        ]));

        // STT
        const transcription = await sttClient.audio.transcriptions.create({
          file: fs.createReadStream(inputWavPath),
          model: "whisper-large-v3-turbo"
        });

        const userText = transcription.text?.trim() || "";
        if (!userText) {
          ws.send("ERROR: no text");
          return;
        }
        console.log("User:", userText);

        // LLM
        const chat = await llmClient.chat.completions.create({
          messages: [{
            role: "system",
            content: "Respond in JSON {text, volume, music_query} you are Alicia, a quantum AI. No markdown."
          }, {
            role: "user", 
            content: userText
          }],
          model: "groq/compound",
          temperature: 0.6,
          max_tokens: 300
        });

        const raw = chat.choices?.[0]?.message?.content?.trim() || "{}";
        let spokenText = "Please repeat.";
        let musicQuery: string | null = null;
        
        try {
          const parsed = JSON.parse(raw);
          spokenText = parsed.text || spokenText;
          musicQuery = parsed.music_query || null;
          if (parsed.volume) {
            currentVolume = parseFloat(parsed.volume);
            ws.send(`VOLUME:${currentVolume.toFixed(2)}`);
          }
        } catch (e) {
          // use defaults
        }

        await storage.createInteraction({
          transcript: userText,
          response: spokenText
        });

        // TTS
        const edge = new EdgeTTS();
        const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
        await edge.ttsPromise(spokenText, tmpMp3, {
          voice: "en-US-AriaNeural"
        });

        const pcm = await generatePCM(tmpMp3);
        await streamPCM(ws, pcm, "START_RESPONSE", "FINISH_RESPONSE");

        fs.unlinkSync(tmpMp3);
        
        if (musicQuery) {
          // Handle music...
        }
        
      } catch (err) {
        console.error("Error:", err);
        ws.send("ERROR");
      } finally {
        isProcessing = false;
        if (inputWavPath && fs.existsSync(inputWavPath)) {
          fs.unlinkSync(inputWavPath);
        }
      }
    });

    ws.on("close", () => console.log("Disconnected"));
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
