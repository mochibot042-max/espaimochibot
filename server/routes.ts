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
const SAMPLE_RATE = 16000;  // Match ESP32

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

function createWavHeader(pcmLength: number, sampleRate: number): Buffer {
  const byteRate = sampleRate * 2;  // 16-bit mono
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcmLength, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);  // mono
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(2, 32);  // block align
  header.writeUInt16LE(16, 34); // bits per sample
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
        fs.unlinkSync(tmpRaw);
        resolve(pcm);
      });
  });
}

async function streamPCM(ws: WebSocket, pcm: Buffer) {
  const CHUNK = 2048;
  for (let i = 0; i < pcm.length; i += CHUNK) {
    if (ws.readyState !== ws.OPEN) return;
    ws.send(pcm.slice(i, i + CHUNK), { binary: true });
    await new Promise(r => setTimeout(r, 5));
  }
}

async function downloadSongStream(query: string, ws: WebSocket) {
  let ffmpegProcess: any = null;
  
  try {
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`,
      { timeout: 15000 }
    );
    
    if (!search.data?.length) {
      ws.send("ERROR_MUSIC_NOT_FOUND");
      return;
    }
    
    const video = search.data[0];
    const apiRes = await axios.get(
      `https://mostakim.onrender.com/m/sing?url=${video.url}`,
      { timeout: 15000 }
    );
    
    if (!apiRes.data?.url) {
      ws.send("ERROR_MUSIC_NO_URL");
      return;
    }

    if ((ws as any).danceMode) {
      ws.send(JSON.stringify({ type: "dance", action: "start" }));
    }

    const audioStream = await axios({
      url: apiRes.data.url,
      method: "GET",
      responseType: "stream",
      timeout: 120000,
    });

    ffmpegProcess = spawn("ffmpeg", [
      "-i", "pipe:0",
      "-f", "s16le",
      "-ac", "1",
      "-ar", "44100",
      "-af", "volume=0.9",
      "pipe:1",
    ]);

    audioStream.data.pipe(ffmpegProcess.stdin);
    ws.send("START_MUSIC");

    let buffer = Buffer.alloc(0);
    let isActive = true;
    
    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isActive || ws.readyState !== ws.OPEN) return;
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 2048 && isActive && ws.readyState === ws.OPEN) {
        ws.send(buffer.slice(0, 2048), { binary: true });
        buffer = buffer.slice(2048);
        await new Promise(r => setTimeout(r, 3));
      }
    });

    ffmpegProcess.stdout.on("end", () => {
      isActive = false;
      if (buffer.length > 0 && ws.readyState === ws.OPEN) ws.send(buffer);
      setTimeout(() => {
        if (ws.readyState === ws.OPEN) {
          ws.send("FINISH_MUSIC");
          if ((ws as any).danceMode) {
            ws.send(JSON.stringify({ type: "dance", action: "stop" }));
            (ws as any).danceMode = false;
          }
        }
      }, 500);
    });

    (ws as any).musicProcess = ffmpegProcess;

  } catch (err) {
    console.error("[MUSIC] Error:", err);
    if (ws.readyState === ws.OPEN) ws.send("ERROR_MUSIC");
    if (ffmpegProcess) ffmpegProcess.kill();
  }
}

export async function registerRoutes(httpServer: Server, app: Express): Promise<Server> {
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    maxPayload: 2 * 1024 * 1024,  // 2MB max para sa buong recording
    perMessageDeflate: false
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("[WS] ESP32 connected");
    
    let heartbeatInterval: NodeJS.Timeout;
    let isProcessing = false;
    (ws as any).danceMode = false;
    
    // CRITICAL: Accumulate complete recording here
    let audioAccumulator: Buffer[] = [];
    let isReceivingAudio = false;
    let currentVolume = loadVolume();

    ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));

    // Heartbeat every 15 seconds
    (ws as any).isAlive = true;
    heartbeatInterval = setInterval(() => {
      if ((ws as any).isAlive === false) {
        console.log("[WS] Dead connection");
        if ((ws as any).musicProcess) (ws as any).musicProcess.kill();
        return ws.terminate();
      }
      (ws as any).isAlive = false;
      ws.ping();
    }, 15000);

    ws.on("pong", () => {
      (ws as any).isAlive = true;
    });

    ws.on("message", async (data: any, isBinary: boolean) => {
      try {
        // CRITICAL: Binary data = part of complete recording
        if (isBinary) {
          if (!isReceivingAudio) {
            isReceivingAudio = true;
            audioAccumulator = [];
            console.log("[AUDIO] Receiving complete recording...");
          }
          
          // Accumulate
          audioAccumulator.push(Buffer.from(data));
          return;
        }

        // Text commands
        const msg = data.toString();
        console.log("[WS] Text:", msg);

        if (msg === "ping") {
          ws.send("pong");
          return;
        }

        // CRITICAL: END_SPEECH = process the complete accumulated audio
        if (msg === "END_SPEECH" && isReceivingAudio && !isProcessing) {
          isProcessing = true;
          isReceivingAudio = false;
          
          ws.send("PROCESSING");
          
          try {
            // Combine ALL audio into single buffer
            const completeAudio = Buffer.concat(audioAccumulator);
            audioAccumulator = [];
            
            console.log(`[AUDIO] Complete recording: ${completeAudio.length} bytes`);
            
            if (completeAudio.length === 0) {
              ws.send("ERROR_NO_AUDIO");
              isProcessing = false;
              return;
            }

            // Ensure 16-bit alignment
            let audioData = completeAudio;
            if (audioData.length % 2 !== 0) {
              audioData = audioData.slice(0, -1);
            }

            // Create WAV file
            const tempId = `rec_${Date.now()}`;
            const inputPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
            
            const wavBuffer = Buffer.concat([
              createWavHeader(audioData.length, SAMPLE_RATE),
              audioData
            ]);
            
            fs.writeFileSync(inputPath, wavBuffer);
            console.log(`[WAV] Saved ${wavBuffer.length} bytes to ${tempId}.wav`);

            // Transcribe with Whisper
            const transcription = await sttClient.audio.transcriptions.create({
              file: fs.createReadStream(inputPath),
              model: "whisper-large-v3-turbo",
              language: "en",
              prompt: "Mochi, how are you today, hello, hi, what, when, where, who, why",
              temperature: 0.0,
              response_format: "json"
            });

            fs.unlinkSync(inputPath);
            
            const userText = transcription.text?.trim() || "";
            console.log("[STT] Result:", userText);

            if (userText.length === 0) {
              ws.send("NO_SPEECH");
              isProcessing = false;
              return;
            }

            const isDanceCommand = /dance|sayaw|sumayaw|boogie|groove/i.test(userText);

            // LLM
            const chat = await llmClient.chat.completions.create({
              messages: [
                {
                  role: "system",
                  content: `You are Mochi, a voice AI assistant made by April Manalo. Reply in JSON:
{ "text": "...", "volume": number|null, "music_query": string|null, "servo_pan": number|null, "servo_tilt": number|null, "dance": boolean }`
                },
                { role: "user", content: userText }
              ],
              model: "llama-3.1-8b-instant",
              temperature: 0.7
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
            } catch (e) {
              console.log("[LLM] Parse error:", raw);
              spokenText = raw.replace(/[{}"]/g, '').substring(0, 200);
            }

            await storage.createInteraction({ 
              transcript: userText, 
              response: spokenText 
            });

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
            await edge.ttsPromise(spokenText, tmpMp3, { 
              voice: "en-US-JennyNeural" 
            });

            const pcm = await generatePCM(tmpMp3);
            
            ws.send("START_RESPONSE");
            await streamPCM(ws, pcm);
            ws.send("FINISH_RESPONSE");

            fs.unlinkSync(tmpMp3);

            if (newVolume !== null) {
              saveVolume(newVolume);
              ws.send(JSON.stringify({ type: "volume", volume: newVolume }));
            }

            if (musicQuery) {
              downloadSongStream(musicQuery, ws);
            }

          } catch (err) {
            console.error("[PROCESS] Error:", err);
            if (ws.readyState === ws.OPEN) ws.send("ERROR_PROCESSING");
          } finally {
            isProcessing = false;
          }
          return;
        }

        // Handle other commands
        if (msg.includes("dance") || msg.includes('"type":"dance"')) {
          if (msg.includes("start")) {
            (ws as any).danceMode = true;
            ws.send(JSON.stringify({ type: "dance", action: "start" }));
          } else if (msg.includes("stop")) {
            (ws as any).danceMode = false;
            ws.send(JSON.stringify({ type: "dance", action: "stop" }));
          }
        }

      } catch (e) {
        console.error("[MSG] Error:", e);
        isProcessing = false;
        isReceivingAudio = false;
      }
    });

    ws.on("error", (error) => {
      console.error("[WS] Error:", error);
    });

    ws.on("close", () => {
      console.log("[WS] Disconnected");
      clearInterval(heartbeatInterval);
      if ((ws as any).musicProcess) (ws as any).musicProcess.kill();
    });
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
