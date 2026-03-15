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
    console.log("[MUSIC] Found:", video.title);

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
      "-ar", TARGET_SAMPLE_RATE.toString(),
      "-af", "volume=0.9",
      "pipe:1",
    ]);

    audioStream.data.pipe(ffmpegProcess.stdin);
    ws.send("START_MUSIC");

    let buffer = Buffer.alloc(0);
    
    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isStreamActive || ws.readyState !== ws.OPEN) return;
      buffer = Buffer.concat([buffer, chunk]);
      
      while (buffer.length >= CHUNK_SIZE && isStreamActive && ws.readyState === ws.OPEN) {
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        ws.send(sendChunk, { binary: true });
        await new Promise(r => setTimeout(r, 3));
      }
    });

    ffmpegProcess.stdout.on("end", () => {
      isStreamActive = false;
      if (buffer.length > 0 && ws.readyState === ws.OPEN) {
        ws.send(buffer);
      }
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

    ffmpegProcess.on("close", () => {
      isStreamActive = false;
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
    });

    // STREAMING STATE: Accumulate audio chunks here
    let audioChunks: Buffer[] = [];
    let isRecording = false;
    let currentVolume = loadVolume();

    ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));

    ws.on("message", async (data: any, isBinary: boolean) => {
      try {
        if (isBinary) {
          // STREAMING: Accumulate audio chunk
          if (!isRecording) {
            isRecording = true;
            audioChunks = [];
            console.log("[STREAM] Started receiving chunks");
          }
          
          audioChunks.push(Buffer.from(data));
          return;
        }

        // Handle text commands
        const msg = data.toString();
        console.log("[WS] Command:", msg);

        if (msg === "ping") {
          ws.send("pong");
          return;
        }

        // END_SPEECH: Process accumulated audio
        if (msg === "END_SPEECH" && isRecording && !isProcessing) {
          isProcessing = true;
          isRecording = false;
          
          console.log("[STREAM] Finalizing transcription...");
          ws.send("PROCESSING");

          try {
            // Combine all chunks
            const fullAudio = Buffer.concat(audioChunks);
            audioChunks = [];

            if (fullAudio.length === 0) {
              ws.send("ERROR_NO_AUDIO");
              isProcessing = false;
              return;
            }

            // Create WAV file for Groq
            const tempId = `stream_${Date.now()}`;
            const inputPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
            
            // Ensure 16-bit alignment
            let wavData = fullAudio;
            if (wavData.length % 2 !== 0) wavData = wavData.slice(0, -1);
            
            fs.writeFileSync(inputPath, Buffer.concat([createWavHeader(wavData.length, TARGET_SAMPLE_RATE), wavData]));

            // Transcribe with Groq
            const transcription = await sttClient.audio.transcriptions.create({
              file: fs.createReadStream(inputPath),
              model: "whisper-large-v3-turbo",
              language: "en",
            });

            fs.unlinkSync(inputPath);
            
            const userText = transcription.text?.trim() || "";
            console.log("[STT] Result:", userText);

            if (userText.length === 0) {
              ws.send("NO_SPEECH");
              isProcessing = false;
              return;
            }

            // Check for dance command
            const isDanceCommand = /dance|sayaw|sumayaw|boogie|groove/i.test(userText);

            // LLM Processing
            const chat = await llmClient.chat.completions.create({
              messages: [
                {
                  role: "system",
                  content: `You are Mochi, a voice AI assistant made by April Manalo. Reply in JSON:
{ "text": "...", "volume": number|null, "music_query": string|null, "servo_pan": number|null, "servo_tilt": number|null, "dance": boolean }

- servo_pan: 0=left, 90=center, 180=right
- servo_tilt: 0=down, 90=up
- dance: true if user wants to dance`
                },
                { role: "user", content: userText }
              ],
              model: "llama-3.1-8b-instant",
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
              spokenText = raw.replace(/[{}"]/g, '').substring(0, 200) || "I didn't understand.";
            }

            // Save interaction
            await storage.createInteraction({ 
              transcript: userText, 
              response: spokenText 
            });

            // Send servo commands
            if (servoPan !== null || servoTilt !== null) {
              ws.send(JSON.stringify({ 
                type: "servo", 
                pan: servoPan ?? -1, 
                tilt: servoTilt ?? -1 
              }));
            }

            // TTS Generation - THIS MUST WORK!
            const edge = new EdgeTTS();
            const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
            await edge.ttsPromise(spokenText, tmpMp3, { 
              voice: "en-US-JennyNeural" 
            });

            const pcm = await generatePCM(tmpMp3);
            
            // Send TTS to ESP32
            ws.send("START_RESPONSE");
            await streamPCM(ws, pcm);
            ws.send("FINISH_RESPONSE");

            fs.unlinkSync(tmpMp3);

            // Update volume if requested
            if (newVolume !== null) {
              currentVolume = newVolume;
              saveVolume(currentVolume);
              ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));
            }

            // Handle music request
            if (musicQuery) {
              downloadSongStream(musicQuery, ws);
            }

          } catch (err) {
            console.error("[STREAM] Error:", err);
            if (ws.readyState === ws.OPEN) ws.send("ERROR");
          } finally {
            isProcessing = false;
          }
          return;
        }

        // Legacy support
        if (msg === "END_STREAM") {
          ws.send("ERROR_USE_END_SPEECH");
        }

      } catch (e) {
        console.error("[MSG] Handler error:", e);
        isProcessing = false;
        isRecording = false;
      }
    });

    // Heartbeat
    (ws as any).isAlive = true;
    heartbeatInterval = setInterval(() => {
      if ((ws as any).isAlive === false) {
        console.log("[WS] Dead connection, terminating");
        if ((ws as any).musicProcess) (ws as any).musicProcess.kill();
        return ws.terminate();
      }
      (ws as any).isAlive = false;
      ws.ping();
    }, 60000);

    ws.on("error", (error) => {
      console.error("[WS] Error:", error);
      isProcessing = false;
      isRecording = false;
    });

    ws.on("close", () => {
      console.log("[WS] Disconnected");
      clearInterval(heartbeatInterval);
      isProcessing = false;
      isRecording = false;
      if ((ws as any).musicProcess) (ws as any).musicProcess.kill();
    });
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
