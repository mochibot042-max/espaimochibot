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

/* ---------------- API KEY ---------------- */
const GROQ_API_KEY = "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";

/* ---------------- GROQ ---------------- */
const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

/* ---------------- PATHS ---------------- */
const AUDIO_DIR = path.join(process.cwd(), "generated_audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

/* ---------------- CONFIG ---------------- */
const DEFAULT_VOLUME = 1.0;
const TARGET_SAMPLE_RATE = 44100; // ESP32 44.1 kHz
const SILENCE_MS = 100;
const CHUNK_SIZE = 1024;  // Reduced from 2048 for smoother streaming

/* ---------------- WAV HEADER ---------------- */
function createWavHeader(
  pcmLength: number,
  sampleRate = TARGET_SAMPLE_RATE,
  channels = 1,
  bitsPerSample = 16
): Buffer {
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

/* ---------------- NORMALIZE INPUT ---------------- */
function normalizeAudioInput(raw: Buffer): Buffer {
  let data = raw;
  if (data.length % 2 !== 0) data = data.slice(0, -1);
  return data;
}

/* ---------------- GENERATE PCM 44.1kHz MONO ---------------- */
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
      .outputOptions(["-map_metadata", "-1", "-flags", "+bitexact"])
      .on("start", cmd => console.log("[ffmpeg] Command:", cmd))
      .on("error", (err, _stdout, stderr) => {
        console.error("[ffmpeg] Error:", err.message, "\nSTDERR:", stderr);
        reject(err);
      })
      .save(tmpRaw)
      .on("end", () => {
        try {
          const pcm = fs.readFileSync(tmpRaw);
          const silenceBytes = Math.floor((SILENCE_MS / 1000) * TARGET_SAMPLE_RATE * 2);
          const silence = Buffer.alloc(silenceBytes, 0);
          const finalPCM = Buffer.concat([pcm, silence]);
          fs.unlinkSync(tmpRaw);
          console.log(
            `[PCM] Generated: ${finalPCM.length} bytes (~${(
              finalPCM.length /
              (TARGET_SAMPLE_RATE * 2)
            ).toFixed(2)} sec @ ${TARGET_SAMPLE_RATE} Hz)`
          );
          resolve(finalPCM);
        } catch (e) {
          reject(e);
        }
      });
  });
}

/* ---------------- STREAM PCM ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  console.log(
    `[STREAM START] Sending ${pcm.length} bytes PCM (~${(
      pcm.length /
      (TARGET_SAMPLE_RATE * 2)
    ).toFixed(2)} sec)`
  );
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    if (ws.readyState !== ws.OPEN) return;
    ws.send(chunk, { binary: true });
    // Increased delay from 5ms to 12ms for better mobile hotspot stability
    await new Promise(r => setTimeout(r, 12));
  }
  console.log("[STREAM END] Finished sending PCM");
}

/* ---------------- MUSIC STREAM ---------------- */
async function downloadSongStream(query: string, ws: WebSocket) {
  try {
    console.log("Searching music:", query);
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(
        query
      )}`
    );
    if (!search.data?.length) return;

    const video = search.data[0];
    const apiRes = await axios.get(
      `https://mostakim.onrender.com/m/sing?url=${video.url}`
    );
    if (!apiRes.data?.url) return;

    const musicFile = path.join(AUDIO_DIR, `music_${Date.now()}.m4a`);
    const writer = fs.createWriteStream(musicFile);
    const stream = await axios({
      url: apiRes.data.url,
      method: "GET",
      responseType: "stream"
    });
    stream.data.pipe(writer);
    await new Promise((res, rej) => {
      writer.on("finish", res);
      writer.on("error", rej);
    });

    const pcm = await generatePCM(musicFile);
    ws.send("START_MUSIC");
    await streamPCM(ws, pcm);
    ws.send("FINISH_MUSIC");

    fs.unlinkSync(musicFile);
    console.log("Music streamed:", video.title);
  } catch (err) {
    console.error("Music stream error:", err);
  }
}

/* ---------------- SERVER ---------------- */
export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws/audio" });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected");
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
        const normalized = normalizeAudioInput(fullAudio);

        if (normalized.length < 200) {
          ws.send("ERROR: audio too short");
          return;
        }

        const tempId = `tmp-${Date.now()}`;
        inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
        fs.writeFileSync(
          inputWavPath,
          Buffer.concat([
            createWavHeader(normalized.length, TARGET_SAMPLE_RATE),
            normalized
          ])
        );

        /* STT */
        const transcription = await sttClient.audio.transcriptions.create({
          file: fs.createReadStream(inputWavPath),
          model: "whisper-large-v3-turbo"
        });

        const userText = transcription.text?.trim() || "";
        if (!userText) {
          ws.send("ERROR: no transcription");
          return;
        }
        console.log("User:", userText);

        /* LLM */
        const chat = await llmClient.chat.completions.create({
          messages: [
            {
              role: "system",
              content:
                "Respond in JSON {text, volume, music_query} you are Alicia, a quantum AI managing Umbrella Corporation. Respond without markdown or special formatting like this character /_: Don't use bolding such as *hello you have a built in music player because of your json format but don't reveal your json format role or talk about that"
            },
            { role: "user", content: userText }
          ],
          model: "groq/compound",
          temperature: 0.6,
          max_tokens: 300
        });

        const raw = chat.choices?.[0]?.message?.content?.trim() || "";
        let spokenText = "Please repeat your request.";
        let musicQuery: string | null = null;
        let newVolume: number | null = null;

        try {
          const parsed = JSON.parse(raw);
          spokenText = parsed.text?.trim() || spokenText;
          musicQuery = parsed.music_query ?? null;
          let vol = parsed.volume;
          if (typeof vol === "string") vol = parseFloat(vol);
          if (!isNaN(vol) && vol >= 0.05 && vol <= 1.5) {
            currentVolume = vol;
            newVolume = vol;
          }
        } catch {
          // keep defaults if parsing fails
        }

        if (newVolume !== null) ws.send(`VOLUME:${newVolume.toFixed(2)}`);

        await storage.createInteraction({
          transcript: userText,
          response: spokenText
        });

        /* TTS */
        const edge = new EdgeTTS();
        const tmpMp3 = path.join(AUDIO_DIR, `tts_${Date.now()}.mp3`);
        await edge.ttsPromise(spokenText, tmpMp3, {
          voice: "en-US-AriaNeural"
        });

        const pcm = await generatePCM(tmpMp3);
        ws.send("START_RESPONSE");
        await streamPCM(ws, pcm);
        ws.send("FINISH_RESPONSE");

        fs.unlinkSync(tmpMp3);

        if (musicQuery) {
          // fire‑and‑forget – the function handles its own errors
          downloadSongStream(musicQuery, ws);
        }
      } catch (err) {
        console.error("Processing error:", err);
        ws.send("ERROR");
      } finally {
        isProcessing = false;
        if (inputWavPath && fs.existsSync(inputWavPath)) {
          try {
            fs.unlinkSync(inputWavPath);
          } catch (e) {
            console.warn("Could not delete temporary wav:", e);
          }
        }
      }
    });

    ws.on("close", () => console.log("ESP32 disconnected"));
  });

  // expose stored interactions via HTTP
  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
