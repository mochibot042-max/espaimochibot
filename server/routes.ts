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

function createWavHeader(pcmLength: number, sampleRate = TARGET_SAMPLE_RATE, channels = 1, bitsPerSample = 16): Buffer {
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

function normalizeAudioInput(raw: Buffer): Buffer {
  let data = raw;
  if (data.length % 2 !== 0) data = data.slice(0, -1);
  return data;
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
        const silenceBytes = Math.floor((SILENCE_MS / 1000) * TARGET_SAMPLE_RATE * 2);
        const silence = Buffer.alloc(silenceBytes, 0);
        const finalPCM = Buffer.concat([pcm, silence]);
        fs.unlinkSync(tmpRaw);
        resolve(finalPCM);
      });
  });
}

async function streamPCM(ws: WebSocket, pcm: Buffer) {
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    ws.send(chunk, { binary: true });
    await new Promise(r => setTimeout(r, 5));
  }
}

// FIXED: Better music streaming with proper end detection
async function downloadSongStream(query: string, ws: WebSocket) {
  let ffmpegProcess: any = null;
  let isStreamActive = true;
  
  try {
    console.log("[MUSIC] Searching:", query);
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`,
      { timeout: 15000 }
    );
    
    if (!search.data?.length) {
      ws.send("ERROR_MUSIC_NOT_FOUND");
      return;
    }
    
    const video = search.data[0];
    console.log("[MUSIC] Found:", video.title, "Duration:", video.duration);
    
    // Calculate expected duration in ms (approximate)
    let expectedDuration = 180000; // default 3 minutes
    if (video.duration) {
      const parts = video.duration.split(':').map(Number);
      if (parts.length === 2) expectedDuration = (parts[0] * 60 + parts[1]) * 1000;
      if (parts.length === 3) expectedDuration = (parts[0] * 3600 + parts[1] * 60 + parts[2]) * 1000;
    }
    console.log("[MUSIC] Expected duration:", expectedDuration, "ms");

    const apiRes = await axios.get(
      `https://mostakim.onrender.com/m/sing?url=${video.url}`,
      { timeout: 15000 }
    );
    
    if (!apiRes.data?.url) {
      ws.send("ERROR_MUSIC_NO_URL");
      return;
    }

    // Start dance mode if requested
    if ((ws as any).danceMode) {
      ws.send(JSON.stringify({ type: "dance", action: "start" }));
    }

    const audioStream = await axios({
      url: apiRes.data.url,
      method: "GET",
      responseType: "stream",
      timeout: 120000, // 2 minutes timeout for connection
    });

    ffmpegProcess = spawn("ffmpeg", [
      "-i", "pipe:0",
      "-f", "s16le",
      "-ac", "1",
      "-ar", TARGET_SAMPLE_RATE.toString(),
      "-af", "volume=0.9",
      "-buffer_size", "1024k",
      "pipe:1",
    ]);

    let stderrData = "";
    ffmpegProcess.stderr.on("data", (data: Buffer) => {
      stderrData += data.toString();
      // Parse duration from ffmpeg output
      if (data.toString().includes("Duration:")) {
        console.log("[FFMPEG]", data.toString().trim());
      }
    });

    audioStream.data.pipe(ffmpegProcess.stdin);
    
    // Handle ffmpeg errors
    ffmpegProcess.stdin.on("error", (e: any) => {
      console.log("[FFMPEG] stdin error:", e.message);
    });

    ws.send("START_MUSIC");

    let buffer = Buffer.alloc(0);
    let totalBytesSent = 0;
    let lastSendTime = Date.now();
    let streamTimeout: NodeJS.Timeout;

    // Set up stream timeout watchdog
    const resetStreamTimeout = () => {
      if (streamTimeout) clearTimeout(streamTimeout);
      streamTimeout = setTimeout(() => {
        console.log("[MUSIC] Stream timeout - no data for 10 seconds");
        isStreamActive = false;
        if (ffmpegProcess) ffmpegProcess.kill();
      }, 10000);
    };

    resetStreamTimeout();

    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isStreamActive || ws.readyState !== ws.OPEN) {
        isStreamActive = false;
        return;
      }
      
      resetStreamTimeout();
      lastSendTime = Date.now();
      buffer = Buffer.concat([buffer, chunk]);
      totalBytesSent += chunk.length;
      
      while (buffer.length >= CHUNK_SIZE && isStreamActive && ws.readyState === ws.OPEN) {
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        
        try {
          ws.send(sendChunk, { binary: true });
        } catch (e) {
          console.log("[MUSIC] Send error:", e);
          isStreamActive = false;
          break;
        }
        
        // Shorter delay for smoother streaming
        await new Promise(r => setTimeout(r, 2));
      }
    });

    // FIXED: Proper end handling
    ffmpegProcess.stdout.on("end", () => {
      console.log("[MUSIC] stdout ended, bytes sent:", totalBytesSent);
      isStreamActive = false;
      if (streamTimeout) clearTimeout(streamTimeout);
      
      // Send remaining buffer
      if (buffer.length > 0 && ws.readyState === ws.OPEN) {
        try {
          ws.send(buffer);
        } catch (e) {
          console.log("[MUSIC] Final send error");
        }
      }
      
      // Delay before sending FINISH to ensure all data received
      setTimeout(() => {
        if (ws.readyState === ws.OPEN) {
          ws.send("FINISH_MUSIC");
          if ((ws as any).danceMode) {
            ws.send(JSON.stringify({ type: "dance", action: "stop" }));
            (ws as any).danceMode = false;
          }
          console.log("[MUSIC] Finished properly");
        }
      }, 1000); // 1 second delay
    });

    ffmpegProcess.on("close", (code: number) => {
      console.log("[MUSIC] FFmpeg closed with code:", code);
      isStreamActive = false;
      if (streamTimeout) clearTimeout(streamTimeout);
      
      // If still open and not already finished, send finish
      if (ws.readyState === ws.OPEN && stderrData.includes("Conversion failed") === false) {
        setTimeout(() => {
          if (ws.readyState === ws.OPEN) {
            ws.send("FINISH_MUSIC");
          }
        }, 500);
      }
    });

    ffmpegProcess.on("error", (err: any) => {
      console.error("[MUSIC] FFmpeg error:", err);
      isStreamActive = false;
    });

    // Store references
    (ws as any).musicProcess = ffmpegProcess;
    (ws as any).musicStreamActive = true;

    // Auto-kill after expected duration + buffer
    setTimeout(() => {
      if (ffmpegProcess && !ffmpegProcess.killed) {
        console.log("[MUSIC] Auto-killing after duration limit");
        ffmpegProcess.kill();
      }
    }, expectedDuration + 10000);

  } catch (err) {
    console.error("[MUSIC] Stream error:", err);
    isStreamActive = false;
    if (ws.readyState === ws.OPEN) {
      ws.send("ERROR_MUSIC");
    }
    if (ffmpegProcess) ffmpegProcess.kill();
    if ((ws as any).danceMode) {
      (ws as any).danceMode = false;
    }
  }
}

export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    maxPayload: 50 * 1024 * 1024,
    perMessageDeflate: false
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("[WS] ESP32 connected");
    
    let heartbeatInterval: NodeJS.Timeout;
    let isProcessing = false;
    (ws as any).danceMode = false;

    ws.on("pong", () => {
      (ws as any).isAlive = true;
      console.log("[WS] Pong received");
    });

    let audioChunks: Buffer[] = [];
    let currentVolume = loadVolume();

    // Send initial volume
    ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));

    ws.on("message", async (data: any, isBinary: boolean) => {
      try {
        if (isBinary) {
          audioChunks.push(Buffer.from(data));
          return;
        }

        const msg = data.toString();
        console.log("[WS] Received:", msg.substring(0, 100));

        if (msg === "ping") {
          ws.send("pong");
          return;
        }

        if (msg !== "END_STREAM" || isProcessing) return;

        isProcessing = true;
        let inputWavPath = "";

        try {
          const fullAudio = Buffer.concat(audioChunks);
          audioChunks = [];
          const normalized = normalizeAudioInput(fullAudio);
          const tempId = `tmp-${Date.now()}`;
          inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);

          fs.writeFileSync(
            inputWavPath,
            Buffer.concat([createWavHeader(normalized.length), normalized])
          );

          const transcription = await sttClient.audio.transcriptions.create({
            file: fs.createReadStream(inputWavPath),
            model: "whisper-large-v3-turbo",
          });

          const userText = transcription.text?.trim() || "";
          console.log("[STT] User:", userText);

          const isDanceCommand = /dance|sayaw|sumayaw|boogie|groove/i.test(userText);
          
          const chat = await llmClient.chat.completions.create({
            messages: [
              {
                role: "system",
                content: `You are Mochi an voice ai assistant made by April Manalo. Reply in JSON:
{ "text": "...", "volume": number|null, "music_query": string|null, "servo_pan": number|null, "servo_tilt": number|null, "dance": boolean }

- servo_pan: 0=left, 90=center, 180=right
- servo_tilt: 0=down, 90=up
- dance: true if user wants to dance

Examples:
"dance" -> dance: true, music_query: "upbeat dance music"
"look up" -> servo_tilt: 90
"look left" -> servo_pan: 0`
              },
              { role: "user", content: userText }
            ],
            model: "openai/gpt-oss-120b",
          });

          const raw = chat.choices?.[0]?.message?.content?.trim() || "";
          console.log("[LLM] Raw:", raw);

          let spokenText = "Please repeat.";
          let musicQuery: string | null = null;
          let newVolume: number | null = null;
          let servoPan: number | null = null;
          let servoTilt: number | null = null;
          let danceMode = false;

          try {
            const parsed = JSON.parse(raw);
            spokenText = parsed.text || spokenText;
            musicQuery = parsed.music_query ?? null;
            servoPan = parsed.servo_pan ?? null;
            servoTilt = parsed.servo_tilt ?? null;
            danceMode = parsed.dance || isDanceCommand;
            
            if (parsed.volume !== null && !isNaN(parsed.volume)) {
              newVolume = Math.max(0.05, Math.min(1.5, parsed.volume));
            }
            
            if (danceMode && !musicQuery) {
              musicQuery = "upbeat dance music";
            }
          } catch (e) {
            console.log("[LLM] Parse error:", e);
            spokenText = raw.replace(/[{}"]/g, '').substring(0, 200) || "I didn't understand.";
            if (isDanceCommand) {
              danceMode = true;
              musicQuery = "upbeat dance music";
            }
          }

          (ws as any).danceMode = danceMode;
          await storage.createInteraction({ transcript: userText, response: spokenText });

          // Send servo
          if (servoPan !== null || servoTilt !== null) {
            ws.send(JSON.stringify({ 
              type: "servo", 
              pan: servoPan ?? -1, 
              tilt: servoTilt ?? -1 
            }));
          }

          // TTS
          const edge = new EdgeTTS();
          const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
          await edge.ttsPromise(spokenText, tmpMp3, { voice: "en-US-JennyNeural" });

          const pcm = await generatePCM(tmpMp3);
          ws.send("START_RESPONSE");
          await streamPCM(ws, pcm);
          ws.send("FINISH_RESPONSE");
          fs.unlinkSync(tmpMp3);

          // Volume
          if (newVolume !== null) {
            currentVolume = newVolume;
            saveVolume(currentVolume);
            ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));
          }

          // Music
          if (musicQuery) {
            downloadSongStream(musicQuery, ws);
          }

        } catch (err) {
          console.error("[PROC] Error:", err);
          if (ws.readyState === ws.OPEN) ws.send("ERROR");
        } finally {
          isProcessing = false;
          if (inputWavPath && fs.existsSync(inputWavPath)) {
            fs.unlinkSync(inputWavPath);
          }
        }
      } catch (e) {
        console.error("[MSG] Handler error:", e);
        isProcessing = false;
      }
    });

    // FIXED: Longer heartbeat interval (60 seconds) for music streaming
    (ws as any).isAlive = true;
    heartbeatInterval = setInterval(() => {
      if ((ws as any).isAlive === false) {
        console.log("[WS] Connection dead, terminating");
        if ((ws as any).musicProcess) {
          (ws as any).musicProcess.kill();
        }
        return ws.terminate();
      }
      
      // Don't ping if music is playing (ESP32 is busy)
      if ((ws as any).musicStreamActive) {
        console.log("[WS] Music active, skipping ping");
        (ws as any).isAlive = true; // Assume alive
        return;
      }
      
      (ws as any).isAlive = false;
      ws.ping();
      console.log("[WS] Ping sent");
    }, 60000); // 60 seconds instead of 30

    ws.on("error", (error) => {
      console.error("[WS] Error:", error);
      isProcessing = false;
    });

    ws.on("close", () => {
      console.log("[WS] Disconnected");
      clearInterval(heartbeatInterval);
      if ((ws as any).musicProcess) {
        (ws as any).musicProcess.kill();
      }
      isProcessing = false;
    });
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
