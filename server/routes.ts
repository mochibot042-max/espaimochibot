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

/* ---------------- API KEY ---------------- */
const GROQ_API_KEY = "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";

/* ---------------- GROQ ---------------- */
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

/* ---------------- PATHS ---------------- */
const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const VOLUME_FILE = path.join(process.cwd(), "volume.json");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------------- CONFIG ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 16000;  // CHANGED: 16kHz para sa Whisper
const SILENCE_MS = 100;
const CHUNK_SIZE = 2048;

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

/* ---------------- WAV HEADER - FIXED ---------------- */
function createWavHeader(
  pcmLength: number,
  sampleRate = TARGET_SAMPLE_RATE,
  channels = 1,
  bitsPerSample = 16
): Buffer {
  const byteRate = sampleRate * channels * (bitsPerSample / 8);
  const blockAlign = channels * (bitsPerSample / 8);
  const dataChunkSize = pcmLength;
  const fileSize = 36 + dataChunkSize;

  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(fileSize, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);        // Subchunk1Size (16 for PCM)
  header.writeUInt16LE(1, 20);         // AudioFormat (1 = PCM)
  header.writeUInt16LE(channels, 22);  // NumChannels
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write("data", 36);
  header.writeUInt32LE(dataChunkSize, 40);

  return header;
}

/* ---------------- NORMALIZE INPUT ---------------- */
function normalizeAudioInput(raw: Buffer): Buffer {
  if (raw.length % 2 !== 0) {
    raw = raw.slice(0, -1);
  }
  return raw;
}

/* ---------------- CONVERT TO 16kHz MONO ---------------- */
async function convertTo16kHzMono(inputPath: string, outputPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .noVideo()
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=8000",
        "aresample=16000:resampler=soxr:precision=28",
        "pan=mono|c0=c0",
        "volume=1.0"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(16000)
      .format("s16le")
      .on("error", (err) => {
        console.error("FFmpeg error:", err);
        reject(err);
      })
      .on("end", () => {
        console.log("FFmpeg conversion done");
        resolve();
      })
      .save(outputPath);
  });
}

/* ---------------- GENERATE PCM FOR OUTPUT (44.1kHz) ---------------- */
async function generatePCM(inputPath: string): Promise<Buffer> {
  const tmpRaw = path.join(AUDIO_DIR, `raw_${Date.now()}.pcm`);

  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .noVideo()
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=20000",
        "aresample=44100:resampler=soxr:precision=28",
        "pan=mono|c0=c0",
        "volume=0.95"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(44100)
      .format("s16le")
      .on("error", reject)
      .save(tmpRaw)
      .on("end", () => {
        const pcm = fs.readFileSync(tmpRaw);
        const silenceBytes = Math.floor((SILENCE_MS / 1000) * 44100 * 2);
        const silence = Buffer.alloc(silenceBytes, 0);
        const finalPCM = Buffer.concat([pcm, silence]);
        fs.unlinkSync(tmpRaw);
        resolve(finalPCM);
      });
  });
}

/* ---------------- STREAM PCM ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);
  console.log(`Streaming ${totalChunks} chunks...`);

  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== WebSocket.OPEN) {
      console.log("WS closed during stream");
      return;
    }
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    ws.send(chunk, { binary: true });

    // Smaller delay para hindi mag-timeout
    if (i % (CHUNK_SIZE * 4) === 0) {
      await new Promise(r => setTimeout(r, 2));
    }
  }
  console.log("Stream complete");
}

/* ---------------- MUSIC STREAM ---------------- */
async function downloadSongStream(query: string, ws: WebSocket) {
  try {
    console.log("Searching music:", query);
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`
    );
    if (!search.data?.length) {
      ws.send("ERROR: No results");
      return;
    }
    const video = search.data[0];
    console.log("Found:", video.title);

    const apiRes = await axios.get(`https://mostakim.onrender.com/m/sing?url=${video.url}`);
    if (!apiRes.data?.url) {
      ws.send("ERROR: No stream URL");
      return;
    }

    const audioStream = await axios({
      url: apiRes.data.url,
      method: "GET",
      responseType: "stream",
      timeout: 10000,
    });

    const ffmpegProcess = spawn("ffmpeg", [
      "-i", "pipe:0",
      "-f", "s16le",
      "-ac", "1",
      "-ar", "44100",
      "-af", "volume=0.8",
      "pipe:1"
    ]);

    audioStream.data.pipe(ffmpegProcess.stdin);

    let hasStarted = false;
    let buffer = Buffer.alloc(0);

    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);

      if (!hasStarted && buffer.length >= CHUNK_SIZE) {
        hasStarted = true;
        ws.send("START_MUSIC");
        console.log("Music started");
      }

      while (buffer.length >= CHUNK_SIZE) {
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(sendChunk, { binary: true });
        }
      }
    });

    ffmpegProcess.stdout.on("end", () => {
      if (buffer.length > 0 && ws.readyState === WebSocket.OPEN) {
        ws.send(buffer);
      }
      ws.send("FINISH_MUSIC");
      console.log("Music finished");
    });

    ffmpegProcess.stderr.on("data", (data) => {
      // console.log("FFmpeg:", data.toString());
    });

  } catch (err) {
    console.error("Music stream error:", err);
    ws.send("ERROR: Music failed");
  }
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    perMessageDeflate: false,  // Disable compression para sa binary
    maxPayload: 50 * 1024 * 1024  // Allow 50MB frames para sa audio data
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected");

    let audioChunks: Buffer[] = [];
    let isProcessing = false;
    let currentVolume = loadVolume();

    // Send initial volume
    ws.send(JSON.stringify({ volume: currentVolume }));
    console.log("Sent volume:", currentVolume);

    ws.on("message", async (data: any, isBinary: boolean) => {
      // Handle binary audio data
      if (isBinary) {
        audioChunks.push(Buffer.from(data));
        return;
      }

      // Handle text commands
      const msg = data.toString().trim();
      console.log("Received:", msg);

      if (msg === "END_STREAM") {
        if (isProcessing) {
          console.log("Already processing, ignoring");
          return;
        }
        isProcessing = true;

        let inputWavPath = "";
        let convertedPath = "";

        try {
          // Combine all chunks
          const fullAudio = Buffer.concat(audioChunks);
          audioChunks = [];

          console.log(`Received audio: ${fullAudio.length} bytes`);

          if (fullAudio.length < 1000) {
            console.log("Audio too short");
            ws.send("ERROR: Audio too short");
            isProcessing = false;
            return;
          }

          // Normalize
          const normalized = normalizeAudioInput(fullAudio);
          const tempId = `tmp-${Date.now()}`;
          inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
          convertedPath = path.join(UPLOAD_DIR, `${tempId}_16k.wav`);

          // Create WAV at 44.1kHz (what ESP32 sends)
          const wavBuffer = Buffer.concat([
            createWavHeader(normalized.length, 44100),  // ESP32 sends 44.1kHz
            normalized
          ]);

          fs.writeFileSync(inputWavPath, wavBuffer);
          console.log("Saved WAV:", inputWavPath);

          // Convert to 16kHz for Whisper
          await convertTo16kHzMono(inputWavPath, convertedPath);
          console.log("Converted to 16kHz:", convertedPath);

          /* STT with Groq */
          console.log("Sending to Groq STT...");
          const transcription = await sttClient.audio.transcriptions.create({
            file: fs.createReadStream(convertedPath),
            model: "whisper-large-v3-turbo",
            language: "en",  // Optional: auto-detect if removed
            response_format: "json"
          });

          const userText = transcription.text?.trim() || "";
          console.log("Transcription:", userText);

          if (!userText) {
            ws.send("ERROR: No speech detected");
            isProcessing = false;
            return;
          }

          /* LLM */
          console.log("Sending to LLM...");
          const chat = await llmClient.chat.completions.create({
            messages: [
              {
                role: "system",
                content: `You are Alicia, the Red Queen AI from Umbrella Corporation. You are a voice assistant.

CRITICAL RULES:
1. ALWAYS respond in valid JSON format: {"text": "...", "volume": number or null, "music_query": string or null}
2. "text" field must be natural spoken English (what you will say)
3. "volume" only if user asks to change volume (0.05 to 1.5), otherwise null
4. "music_query" only if user asks for music/song, otherwise null
5. Never use markdown, special characters, or formatting
6. Keep responses concise (1-2 sentences)
7. Speak clearly for text-to-speech

Example: {"text": "Hello, I am Alicia. How may I assist you?", "volume": null, "music_query": null}`
              },
              { role: "user", content: userText }
            ],
            model: "llama-3.1-8b-instant",  // Faster model
            temperature: 0.7,
            max_tokens: 150
          });

          const raw = chat.choices?.[0]?.message?.content?.trim() || "";
          console.log("LLM raw:", raw);

          let spokenText = "I apologize, I could not process that.";
          let musicQuery: string | null = null;
          let newVolume: number | null = null;

          try {
            // Try to extract JSON
            const jsonMatch = raw.match(/\{[\s\S]*\}/);
            const jsonStr = jsonMatch ? jsonMatch[0] : raw;
            const parsed = JSON.parse(jsonStr);

            spokenText = parsed.text || spokenText;
            musicQuery = parsed.music_query || null;

            if (parsed.volume !== null && !isNaN(parsed.volume)) {
              newVolume = Math.max(0.05, Math.min(1.5, parsed.volume));
            }
          } catch (e) {
            console.log("JSON parse failed, using raw text");
            spokenText = raw.replace(/[{}"]/g, "").substring(0, 100);
          }

          console.log("Response:", spokenText);
          await storage.createInteraction({ transcript: userText, response: spokenText });

          /* TTS */
          console.log("Generating TTS...");
          const edge = new EdgeTTS();
          const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);

          await edge.ttsPromise(spokenText, tmpMp3, { 
            voice: "en-US-JennyNeural",
            rate: "0%",      // Normal speed
            volume: "100%"
          });

          /* Convert to PCM */
          console.log("Converting to PCM...");
          const pcm = await generatePCM(tmpMp3);
          console.log(`PCM size: ${pcm.length} bytes`);

          /* Stream to ESP32 */
          ws.send("START_RESPONSE");
          await streamPCM(ws, pcm);
          ws.send("FINISH_RESPONSE");
          console.log("Response sent");

          /* Cleanup */
          fs.unlinkSync(tmpMp3);

          /* Handle Volume */
          if (newVolume !== null) {
            currentVolume = newVolume;
            saveVolume(currentVolume);
            ws.send(JSON.stringify({ volume: currentVolume }));
            console.log("Volume updated:", currentVolume);
          }

          /* Handle Music */
          if (musicQuery) {
            console.log("Playing music:", musicQuery);
            downloadSongStream(musicQuery, ws);
          }

        } catch (err: any) {
          console.error("Processing error:", err);
          ws.send(`ERROR: ${err.message || "Processing failed"}`);
        } finally {
          isProcessing = false;
          // Cleanup files
          try {
            if (inputWavPath && fs.existsSync(inputWavPath)) fs.unlinkSync(inputWavPath);
            if (convertedPath && fs.existsSync(convertedPath)) fs.unlinkSync(convertedPath);
          } catch (e) {
            // Ignore cleanup errors
          }
        }
      }
    });

    ws.on("close", () => {
      console.log("ESP32 disconnected");
      audioChunks = [];
    });

    ws.on("error", (err) => {
      console.error("WS error:", err);
    });
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}