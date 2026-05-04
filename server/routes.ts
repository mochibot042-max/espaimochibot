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

// =============================
// CONFIG
// =============================
const GROQ_API_KEY =
  process.env.GROQ_API_KEY || "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";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(
  process.cwd(),
  "generated_audio"
);

const UPLOAD_DIR = path.join(
  process.cwd(),
  "uploads"
);

if (!fs.existsSync(AUDIO_DIR)) {
  fs.mkdirSync(AUDIO_DIR, { recursive: true });
}

if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

// =============================
// AUDIO SETTINGS
// =============================
const DEFAULT_VOLUME = 1.0;

// Lower bitrate for hotspot stability
const TARGET_SAMPLE_RATE = 22050;

// Larger packets
const CHUNK_SIZE = 4096;

// More stable pacing
const CHUNK_DELAY_MS = 15;

// =============================
// WAV HEADER
// =============================
function createWavHeader(
  pcmLength: number,
  sampleRate = TARGET_SAMPLE_RATE
): Buffer {
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

// =============================
// GENERATE PCM
// =============================
async function generatePCM(
  inputPath: string
): Promise<Buffer> {
  const tmpRaw = path.join(
    AUDIO_DIR,
    `raw_${Date.now()}.pcm`
  );

  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .noVideo()
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=12000",
        `aresample=${TARGET_SAMPLE_RATE}:resampler=soxr:precision=28`,
        "pan=mono|c0=c0",
        "volume=1.0"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(TARGET_SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => {
        const pcm = fs.readFileSync(tmpRaw);

        if (fs.existsSync(tmpRaw)) {
          fs.unlinkSync(tmpRaw);
        }

        resolve(pcm);
      })
      .save(tmpRaw);
  });
}

// =============================
// STREAM AUDIO
// =============================
async function streamPCM(
  ws: WebSocket,
  pcm: Buffer,
  startMsg: string,
  endMsg: string
) {
  if (ws.readyState !== ws.OPEN) return;

  ws.send(startMsg);

  // Small startup buffer
  await new Promise((r) => setTimeout(r, 40));

  for (
    let i = 0;
    i < pcm.length;
    i += CHUNK_SIZE
  ) {
    if (ws.readyState !== ws.OPEN) return;

    const chunk = pcm.slice(i, i + CHUNK_SIZE);

    ws.send(chunk, { binary: true });

    await new Promise((r) =>
      setTimeout(r, CHUNK_DELAY_MS)
    );
  }

  ws.send(endMsg);
}

// =============================
// SAFE JSON PARSER
// =============================
function parseAIResponse(raw: string) {
  let spokenText =
    "I'm sorry, I couldn't process that properly.";

  let volume = DEFAULT_VOLUME;

  try {
    raw = raw
      .replace(/```json/gi, "")
      .replace(/```/g, "")
      .trim();

    const parsed = JSON.parse(raw);

    spokenText = parsed.text || spokenText;

    if (
      parsed.volume !== undefined &&
      !isNaN(parseFloat(parsed.volume))
    ) {
      volume = parseFloat(parsed.volume);
    }
  } catch {
    spokenText = raw || spokenText;
  }

  return {
    spokenText,
    volume
  };
}

// =============================
// MAIN ROUTES
// =============================
export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
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

    ws.send(
      `VOLUME:${currentVolume.toFixed(2)}`
    );

    ws.on(
      "message",
      async (data: any, isBinary: boolean) => {
        if (isBinary) {
          audioChunks.push(Buffer.from(data));
          return;
        }

        const msg = data.toString();

        if (
          msg !== "END_STREAM" ||
          isProcessing
        ) {
          return;
        }

        isProcessing = true;

        let inputWavPath = "";
        let resampledPath = "";

        try {
          const fullAudio = Buffer.concat(
            audioChunks
          );

          audioChunks = [];

          if (fullAudio.length < 800) {
            ws.send("ERROR: too short");
            return;
          }

          const tempId = `tmp-${Date.now()}`;

          inputWavPath = path.join(
            UPLOAD_DIR,
            `${tempId}.wav`
          );

          fs.writeFileSync(
            inputWavPath,
            Buffer.concat([
              createWavHeader(
                fullAudio.length,
                TARGET_SAMPLE_RATE
              ),
              fullAudio
            ])
          );

          // =============================
          // RESAMPLE FOR WHISPER
          // =============================
          resampledPath = path.join(
            UPLOAD_DIR,
            `${tempId}_16k.wav`
          );

          await new Promise<void>(
            (resolve, reject) => {
              ffmpeg(inputWavPath)
                .noVideo()
                .audioFilters([
                  `aresample=16000:resampler=soxr:precision=28`
                ])
                .audioCodec("pcm_s16le")
                .audioChannels(1)
                .audioFrequency(16000)
                .format("wav")
                .on("error", reject)
                .on("end", resolve)
                .save(resampledPath);
            }
          );

          // =============================
          // STT
          // =============================
          const transcription =
            await sttClient.audio.transcriptions.create(
              {
                file: fs.createReadStream(
                  resampledPath
                ),
                model:
                  "whisper-large-v3-turbo"
              }
            );

          const userText =
            transcription.text?.trim() || "";

          if (!userText) {
            ws.send("ERROR: no text");
            return;
          }

          console.log(
            "User:",
            userText
          );

          // =============================
          // AI
          // =============================
          const chat =
            await llmClient.chat.completions.create(
              {
                messages: [
                  {
                    role: "system",
                    content:
                      'You are Alicia, an advanced AI assistant. Respond ONLY with raw JSON: {"text":"response","volume":1.0}. No markdown.'
                  },
                  {
                    role: "user",
                    content: userText
                  }
                ],
                model:
                  "llama-3.3-70b-versatile",
                temperature: 0.6,
                max_tokens: 250,
                top_p: 1,
                stream: false
              }
            );

          const raw =
            chat.choices?.[0]?.message?.content?.trim() ||
            "";

          const {
            spokenText,
            volume
          } = parseAIResponse(raw);

          currentVolume = volume;

          ws.send(
            `VOLUME:${currentVolume.toFixed(
              2
            )}`
          );

          console.log(
            "AI:",
            spokenText
          );

          // =============================
          // SAVE HISTORY
          // =============================
          await storage.createInteraction({
            transcript: userText,
            response: spokenText
          });

          // =============================
          // TTS
          // =============================
          const edge = new EdgeTTS();

          const tmpMp3 = path.join(
            AUDIO_DIR,
            `tts_${Date.now()}.mp3`
          );

          await edge.ttsPromise(
            spokenText,
            tmpMp3,
            {
              voice: "en-US-AriaNeural"
            }
          );

          // =============================
          // CONVERT + STREAM
          // =============================
          const pcm =
            await generatePCM(tmpMp3);

          await streamPCM(
            ws,
            pcm,
            "START_RESPONSE",
            "FINISH_RESPONSE"
          );

          // =============================
          // CLEANUP
          // =============================
          if (fs.existsSync(tmpMp3)) {
            fs.unlinkSync(tmpMp3);
          }
        } catch (err) {
          console.error(
            "Processing Error:",
            err
          );

          ws.send("ERROR");
        } finally {
          isProcessing = false;

          [
            inputWavPath,
            resampledPath
          ].forEach((file) => {
            if (
              file &&
              fs.existsSync(file)
            ) {
              fs.unlinkSync(file);
            }
          });
        }
      }
    );

    ws.on("close", () => {
      console.log("Disconnected");
    });
  });

  // =============================
  // API ROUTES
  // =============================
  app.get(
    api.interactions.list.path,
    async (req, res) => {
      res.json(
        await storage.getInteractions()
      );
    }
  );

  return httpServer;
}
