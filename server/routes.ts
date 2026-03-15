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
const TARGET_SAMPLE_RATE = 16000;
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

// FIX: Simplified and more robust audio preprocessing
async function preprocessForSTT(inputPath: string, outputPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .noVideo()
      .audioFilters([
        "aresample=16000",           // Simple resample to 16kHz
        "pan=mono|c0=c0",            // Force mono
        "volume=2.0"                 // Boost volume
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(16000)
      .format("s16le")
      .on("start", (cmd) => {
        console.log("[FFMPEG] Preprocess:", cmd);
      })
      .on("error", (err, stdout, stderr) => {
        console.error("[FFMPEG] Preprocess error:", err.message);
        console.error("[FFMPEG] stderr:", stderr);
        reject(err);
      })
      .on("end", () => {
        console.log("[FFMPEG] Preprocess complete");
        resolve();
      })
      .save(outputPath);
  });
}

async function generatePCM(inputPath: string): Promise<Buffer> {
  const tmpRaw = path.join(AUDIO_DIR, `raw_${Date.now()}.pcm`);
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .noVideo()
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=20000",
        "aresample=44100",
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

async function streamPCM(ws: WebSocket, pcm: Buffer) {
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    ws.send(chunk, { binary: true });
    await new Promise(r => setTimeout(r, 3));
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

    ws.send(JSON.stringify({ type: "dance", action: "start" }));

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
    
    ffmpegProcess.stdout.on("data", async (chunk: Buffer) => {
      if (!isStreamActive || ws.readyState !== ws.OPEN) return;
      buffer = Buffer.concat([buffer, chunk]);
      
      while (buffer.length >= CHUNK_SIZE && isStreamActive && ws.readyState === ws.OPEN) {
        const sendChunk = buffer.slice(0, CHUNK_SIZE);
        buffer = buffer.slice(CHUNK_SIZE);
        ws.send(sendChunk, { binary: true });
        await new Promise(r => setTimeout(r, 2));
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
          ws.send(JSON.stringify({ type: "dance", action: "stop" }));
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

    ws.on("pong", () => {
      (ws as any).isAlive = true;
    });

    let audioChunks: Buffer[] = [];
    let isRecording = false;
    let currentVolume = loadVolume();

    ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));

    ws.on("message", async (data: any, isBinary: boolean) => {
      try {
        if (isBinary) {
          if (!isRecording) {
            isRecording = true;
            audioChunks = [];
            console.log("[STREAM] Started receiving chunks");
          }
          
          audioChunks.push(Buffer.from(data));
          return;
        }

        const msg = data.toString();
        console.log("[WS] Command:", msg);

        if (msg === "ping") {
          ws.send("pong");
          return;
        }

        if (msg === "END_SPEECH" && isRecording && !isProcessing) {
          isProcessing = true;
          isRecording = false;
          
          console.log("[STREAM] Finalizing transcription...");
          ws.send("PROCESSING");

          try {
            const fullAudio = Buffer.concat(audioChunks);
            audioChunks = [];

            if (fullAudio.length === 0) {
              console.log("[STT] No audio received");
              ws.send("NO_SPEECH");
              isProcessing = false;
              return;
            }

            console.log(`[STT] Audio size: ${fullAudio.length} bytes`);

            // FIX: Simplified audio processing - direct to Whisper without complex preprocessing
            const tempId = `stream_${Date.now()}`;
            const wavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
            
            // FIX: Ensure even byte length for 16-bit audio
            let audioData = fullAudio;
            if (audioData.length % 2 !== 0) {
              audioData = audioData.slice(0, -1);
            }

            // FIX: Create proper WAV file with 16kHz (Whisper optimal)
            const wavHeader = createWavHeader(audioData.length, 16000, 1, 16);
            fs.writeFileSync(wavPath, Buffer.concat([wavHeader, audioData]));

            console.log("[STT] Sending to Whisper...");

            // FIX: Use whisper-large-v3-turbo with optimal settings
            const transcription = await sttClient.audio.transcriptions.create({
              file: fs.createReadStream(wavPath),
              model: "whisper-large-v3-turbo",
              language: "en",
              prompt: "The user is speaking to a voice assistant named Mochi.",
              response_format: "text",
              temperature: 0.0,
            });

            // Cleanup
            try { fs.unlinkSync(wavPath); } catch(e) {}

            const userText = transcription.text?.trim() || "";
            console.log("[STT] Result:", userText);

            if (userText.length === 0) {
              console.log("[STT] Empty transcription");
              ws.send("NO_SPEECH");
              isProcessing = false;
              return;
            }

            // Simple text corrections
            let correctedText = userText
              .toLowerCase()
              .replace(/wait for the/gi, "how are you")
              .replace(/weight for the/gi, "how are you")
              .trim();

            console.log("[STT] Corrected:", correctedText);

            const isDanceCommand = /dance|sayaw|sumayaw|boogie|groove/i.test(correctedText);

            // FIX: Better LLM prompt
            const chat = await llmClient.chat.completions.create({
              messages: [
                {
                  role: "system",
                  content: `You are Mochi, a helpful voice AI assistant. 
Respond naturally in English. 
If the user asks for music, include a music_query field with the song name.
If the user wants to dance, set dance to true.
Reply in this exact JSON format:
{"text": "your response here", "volume": null, "music_query": null, "servo_pan": null, "servo_tilt": null, "dance": false}`
                },
                { role: "user", content: correctedText }
              ],
              model: "llama-3.1-8b-instant",
              temperature: 0.7,
              max_tokens: 150
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
              // FIX: Better JSON parsing with fallback
              let parsed;
              try {
                parsed = JSON.parse(raw);
              } catch (e) {
                // Try to extract JSON from text if wrapped in other content
                const jsonMatch = raw.match(/\{[\s\S]*\}/);
                if (jsonMatch) {
                  parsed = JSON.parse(jsonMatch[0]);
                } else {
                  throw e;
                }
              }
              
              spokenText = parsed.text || raw.substring(0, 200) || "I didn't understand.";
              musicQuery = parsed.music_query ?? null;
              servoPan = parsed.servo_pan ?? null;
              servoTilt = parsed.servo_tilt ?? null;
              danceMode = parsed.dance || isDanceCommand;
              
              if (parsed.volume !== null && !isNaN(parsed.volume)) {
                newVolume = Math.max(0.05, Math.min(2.0, parsed.volume));
              }
            } catch (e) {
              console.log("[LLM] Parse error, using raw:", raw);
              spokenText = raw.replace(/[{}"]/g, '').substring(0, 200) || "I didn't understand.";
              if (isDanceCommand) danceMode = true;
            }

            // Save interaction
            await storage.createInteraction({ 
              transcript: correctedText, 
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

            // Send dance command
            if (danceMode && !musicQuery) {
              ws.send(JSON.stringify({ type: "dance", action: "start" }));
            }

            // Send volume update
            if (newVolume !== null) {
              currentVolume = newVolume;
              saveVolume(currentVolume);
              ws.send(JSON.stringify({ type: "volume", volume: currentVolume }));
            }

            // Generate TTS
            console.log("[TTS] Generating:", spokenText);
            const edge = new EdgeTTS();
            const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
            
            await edge.ttsPromise(spokenText, tmpMp3, { 
              voice: "en-US-JennyNeural",
              rate: "0%",
              pitch: "0Hz"
            });

            const pcm = await generatePCM(tmpMp3);
            
            ws.send("START_RESPONSE");
            await streamPCM(ws, pcm);
            ws.send("FINISH_RESPONSE");

            // Cleanup
            try { fs.unlinkSync(tmpMp3); } catch(e) {}

            // Start music if requested
            if (musicQuery) {
              downloadSongStream(musicQuery, ws);
            }

          } catch (err: any) {
            console.error("[STREAM] Error:", err.message || err);
            console.error("[STREAM] Stack:", err.stack);
            if (ws.readyState === ws.OPEN) ws.send("ERROR");
          } finally {
            isProcessing = false;
          }
          return;
        }

      } catch (e: any) {
        console.error("[MSG] Handler error:", e.message || e);
        isProcessing = false;
        isRecording = false;
      }
    });

    (ws as any).isAlive = true;
    heartbeatInterval = setInterval(() => {
      if ((ws as any).isAlive === false) {
        console.log("[WS] Dead connection, terminating");
        if ((ws as any).musicProcess) (ws as any).musicProcess.kill();
        return ws.terminate();
      }
      (ws as any).isAlive = false;
      ws.ping();
    }, 30000);

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
