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
const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

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
const TARGET_SAMPLE_RATE = 44100;  // Playback rate
const RECORD_SAMPLE_RATE = 16000;  // Recording rate (matches ESP32)
const SILENCE_MS = 100;
const CHUNK_SIZE = 512;           // Small chunks for smooth streaming
const CHUNK_DELAY_MS = 12;        // ~85 chunks/sec = smooth for hotspot

/* ---------------- ADPCM Decoder ---------------- */
class ADPCMDecoder {
  private lastSample = 0;
  private index = 0;
  
  private readonly stepTable: number[] = [
    7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45,
    50, 55, 60, 66, 73, 80, 88, 97, 107, 118, 130, 143, 157, 173, 190, 209, 230, 253,
    279, 307, 337, 371, 408, 449, 494, 544, 598, 658, 724, 796, 876, 963, 1060, 1166,
    1282, 1411, 1552, 1707, 1878, 2066, 2272, 2499, 2749, 3024, 3327, 3660, 4026,
    4428, 4871, 5358, 5894, 6484, 7132, 7845, 8630, 9493, 10442, 11487, 12635, 13899,
    15289, 16818, 18500, 20350, 22385, 24623, 27086, 29794, 32767
  ];
  
  private readonly indexTable: number[] = [-1, -1, -1, -1, 2, 4, 6, 8, -1, -1, -1, -1, 2, 4, 6, 8];

  reset() {
    this.lastSample = 0;
    this.index = 0;
  }

  decode(input: Buffer, output: Int16Array): number {
    let outPos = 0;
    
    for (let i = 0; i < input.length && outPos < output.length - 1; i++) {
      const byte = input[i];
      
      // Decode low nibble
      this.decodeNibble(byte & 0x0F, output, outPos++);
      
      // Decode high nibble
      this.decodeNibble((byte >> 4) & 0x0F, output, outPos++);
    }
    
    return outPos;
  }

  private decodeNibble(code: number, output: Int16Array, outPos: number) {
    const step = this.stepTable[this.index];
    let diffq = step >> 3;
    
    if (code & 4) diffq += step;
    if (code & 2) diffq += step >> 1;
    if (code & 1) diffq += step >> 2;
    if (code & 8) diffq = -diffq;
    
    this.lastSample += diffq;
    if (this.lastSample > 32767) this.lastSample = 32767;
    if (this.lastSample < -32768) this.lastSample = -32768;
    
    this.index += this.indexTable[code & 7];
    if (this.index < 0) this.index = 0;
    if (this.index > 88) this.index = 88;
    
    output[outPos] = this.lastSample;
  }
}

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

/* ---------------- STREAM PCM (Hotspot-Optimized) ---------------- */
async function streamPCM(ws: WebSocket, pcm: Buffer, startText: string, endText: string) {
  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);
  console.log(
    `[STREAM START] ${startText}: ${pcm.length} bytes, ${totalChunks} chunks (~${(
      pcm.length /
      (TARGET_SAMPLE_RATE * 2)
    ).toFixed(2)} sec)`
  );
  
  ws.send(startText);
  
  // Initial burst: mas mabagal para mag-build up ang buffer ng ESP32
  let initialBurst = Math.min(20, totalChunks);
  
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    const chunk = pcm.slice(i, i + CHUNK_SIZE);
    if (ws.readyState !== ws.OPEN) {
      console.log("[STREAM ABORTED] WebSocket closed mid-stream");
      return;
    }
    
    ws.send(chunk, { binary: true });
    
    // Adaptive pacing: mas mabagal sa simula, normal pagkatapos
    const delay = i < (initialBurst * CHUNK_SIZE) ? CHUNK_DELAY_MS + 8 : CHUNK_DELAY_MS;
    await new Promise(r => setTimeout(r, delay));
  }
  
  ws.send(endText);
  console.log(`[STREAM END] Finished ${endText}`);
}

/* ---------------- MUSIC STREAM ---------------- */
async function downloadSongStream(query: string, ws: WebSocket) {
  try {
    console.log("Searching music:", query);
    const search = await axios.get(
      `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`,
      { timeout: 10000 }
    );
    if (!search.data?.length) return;

    const video = search.data[0];
    const apiRes = await axios.get(
      `https://mostakim.onrender.com/m/sing?url=${video.url}`,
      { timeout: 10000 }
    );
    if (!apiRes.data?.url) return;

    const musicFile = path.join(AUDIO_DIR, `music_${Date.now()}.m4a`);
    const writer = fs.createWriteStream(musicFile);
    const stream = await axios({
      url: apiRes.data.url,
      method: "GET",
      responseType: "stream",
      timeout: 30000
    });
    stream.data.pipe(writer);
    await new Promise((res, rej) => {
      writer.on("finish", res);
      writer.on("error", rej);
    });

    const pcm = await generatePCM(musicFile);
    await streamPCM(ws, pcm, "START_MUSIC", "FINISH_MUSIC");

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
  const wss = new WebSocketServer({ 
    server: httpServer, 
    path: "/ws/audio",
    perMessageDeflate: false,  // Disable compression for binary audio
    maxPayload: 1024 * 1024    // 1MB max payload
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP32 connected (Hotspot Edition)");
    let audioChunks: Buffer[] = [];
    let isProcessing = false;
    let currentVolume = DEFAULT_VOLUME;
    let isADPCM = false;
    const adpcmDecoder = new ADPCMDecoder();

    ws.send(`VOLUME:${currentVolume.toFixed(2)}`);

    ws.on("message", async (data: any, isBinary: boolean) => {
      if (isBinary) {
        if (isADPCM) {
          // Decode ADPCM on-the-fly
          const compressed = Buffer.from(data);
          const decompressed = new Int16Array(compressed.length * 2);
          const samplesDecoded = adpcmDecoder.decode(compressed, decompressed);
          // Convert to buffer
          const pcmBuffer = Buffer.from(decompressed.buffer).slice(0, samplesDecoded * 2);
          audioChunks.push(pcmBuffer);
        } else {
          audioChunks.push(Buffer.from(data));
        }
        return;
      }

      const msg = data.toString();
      
      // Handle ADPCM start
      if (msg === "START_ADPCM") {
        isADPCM = true;
        adpcmDecoder.reset();
        console.log("ADPCM compression enabled");
        return;
      }
      
      if (msg !== "END_STREAM" && msg !== "END_STREAM_ADPCM" || isProcessing) return;

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

        console.log(`Received audio: ${normalized.length} bytes (${isADPCM ? 'ADPCM' : 'PCM'})`);

        const tempId = `tmp-${Date.now()}`;
        inputWavPath = path.join(UPLOAD_DIR, `${tempId}.wav`);
        
        // Create WAV at 16kHz (input rate)
        fs.writeFileSync(
          inputWavPath,
          Buffer.concat([
            createWavHeader(normalized.length, RECORD_SAMPLE_RATE),
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
        await streamPCM(ws, pcm, "START_RESPONSE", "FINISH_RESPONSE");

        fs.unlinkSync(tmpMp3);

        if (musicQuery) {
          downloadSongStream(musicQuery, ws);
        }
      } catch (err) {
        console.error("Processing error:", err);
        ws.send("ERROR");
      } finally {
        isProcessing = false;
        isADPCM = false;
        adpcmDecoder.reset();
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
    ws.on("error", (err) => console.error("WebSocket error:", err));
  });

  app.get(api.interactions.list.path, async (req, res) => {
    res.json(await storage.getInteractions());
  });

  return httpServer;
}
