import type { Express } from "express";
import { Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import Groq from "groq-sdk";
import fs from "fs";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import { MsEdgeTTS, OUTPUT_FORMAT } from "msedge-tts";
import { spawn } from "child_process";
import { storage, pushSchema, verifySchema } from "./storage.js";

const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";
const OPENWEATHER_API_KEY = process.env.OPENWEATHER_API_KEY || "0925ec58684006d349a4c0556d20e802";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ============================================================================
// AUDIO CONFIG — AI RESPONSE (16kHz MONO)
// ============================================================================
const AI_SAMPLE_RATE = 16000;
const AI_CHUNK_SIZE_MONO = 1024;
const SEND_INTERVAL_MS_AI = 28;
const PREBUFFER_CHUNKS_AI = 24;

const MUSIC_SAMPLE_RATE = 44100;
const MUSIC_CHUNK_SIZE_MONO = 2048;
const SEND_INTERVAL_MS_MUSIC = 20;
const PREBUFFER_CHUNKS_MUSIC = 32;

// ============================================================================
// STREAMING STT CONFIG
// ============================================================================
const STT_STREAM_SAMPLE_RATE = 16000;
const STT_MAX_AUDIO_SECONDS = 12;

// ============================================================================
// WEATHER CONFIG — OpenWeatherMap (Requires API Key)
// ============================================================================
const WEATHER_CITY = "Alfonso,PH";
const WEATHER_LAT = 14.1239;
const WEATHER_LON = 120.8547;

// ============================================================================
// DEBOUNCE
// ============================================================================
const RECENT_REQUESTS = new Map<number, number>();
const DEBOUNCE_MS = 3000;

function isDuplicate(userId: number): boolean {
  const now = Date.now();
  const last = RECENT_REQUESTS.get(userId);
  if (last && (now - last) < DEBOUNCE_MS) {
    console.log("[DEBOUNCE] Duplicate request blocked for user:", userId);
    return true;
  }
  RECENT_REQUESTS.set(userId, now);
  return false;
}

// ============================================================================
// NAME DETECTION
// ============================================================================
function extractName(text: string): { action: "save" | "delete" | "none"; name: string | null } {
  const lower = text.toLowerCase();
  const deletePatterns = [
    /delete\s+(?:my\s+)?name/i, /remove\s+(?:my\s+)?name/i,
    /forget\s+(?:my\s+)?name/i, /clear\s+(?:my\s+)?name/i,
    /wala\s+na\s+ang\s+pangalan\s+ko/i,
    /burahin\s+(?:ang\s+)?pangalan\s+ko/i,
    /alisin\s+(?:ang\s+)?pangalan\s+ko/i,
  ];
  for (const pattern of deletePatterns) {
    if (pattern.test(lower)) return { action: "delete", name: null };
  }
  const savePatterns = [
    /(?:my\s+name\s+is|i\s+am|call\s+me|i'm)\s+([a-zA-Z\s]+)/i,
    /pangalan\s+ko\s+ay\s+([a-zA-Z\s]+)/i,
    /ako\s+si\s+([a-zA-Z\s]+)/i,
    /tawagin\s+mo\s+akong\s+([a-zA-Z\s]+)/i,
  ];
  for (const pattern of savePatterns) {
    const match = lower.match(pattern);
    if (match && match[1]) {
      const name = match[1].trim().split(/\s+/)[0];
      if (name.length > 1) return { action: "save", name: name.charAt(0).toUpperCase() + name.slice(1) };
    }
  }
  return { action: "none", name: null };
}

// ============================================================================
// LANGUAGE DETECTION
// ============================================================================
function detectLanguage(text: string): "en" | "fil" {
  const lower = text.toLowerCase();
  const filWords = [
    "ano", "ang", "bakit", "paano", "sino", "saan", "kailan", "kumusta", "salamat",
    "opo", "hindi", "oo", "mga", "ng", "sa", "na", "ay", "ko", "mo", "po", "lang",
    "naman", "talaga", "siguro", "pala", "din", "rin", "dito", "diyan", "doon",
    "panahon", "ulan", "mainit", "malamig", "bagyo", "araw", "gabi", "umaga",
    "tumugtog", "kanta", "musika", "tawagin", "pangalan", "tawag", "gusto", "ayaw",
    "maganda", "masaya", "malungkot", "pagod", "gutom", "uhaw", "lamig"
  ];
  const words = lower.split(/\s+/);
  let filCount = 0;
  for (const word of words) {
    if (filWords.includes(word)) filCount++;
  }
  return filCount / words.length > 0.3 ? "fil" : "en";
}

// ============================================================================
// OPENWEATHERMAP API (Requires API Key)
// ============================================================================
interface WeatherData {
  temperature: number;
  feelsLike: number;
  humidity: number;
  windSpeed: number;
  condition: string;
  description: string;
  city: string;
  country: string;
  isDay: boolean;
}

async function fetchWeather(): Promise<WeatherData | null> {
  try {
    const url = `https://api.openweathermap.org/data/2.5/weather?lat=${WEATHER_LAT}&lon=${WEATHER_LON}&appid=${OPENWEATHER_API_KEY}&units=metric`;
    const response = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" }
    });
    if (!response.ok) {
      console.error("[WEATHER] API error:", response.status, await response.text());
      return null;
    }
    const data = await response.json();
    
    return {
      temperature: Math.round(data.main.temp),
      feelsLike: Math.round(data.main.feels_like),
      humidity: data.main.humidity,
      windSpeed: Math.round(data.wind.speed * 3.6), // m/s to km/h
      condition: data.weather[0]?.main || "Unknown",
      description: data.weather[0]?.description || "unknown",
      city: data.name || "Alfonso",
      country: data.sys?.country || "PH",
      isDay: data.weather[0]?.icon?.includes("d") || true
    };
  } catch (e: any) {
    console.error("[WEATHER] Fetch error:", e.message);
    return null;
  }
}

function formatWeatherResponse(weather: WeatherData, lang: "en" | "fil"): string {
  if (lang === "fil") {
    const conditionFil: Record<string, string> = {
      "Clear": "Malinis na langit",
      "Clouds": "Maulap",
      "Rain": "Umuulan",
      "Drizzle": "Mahinang ulan",
      "Thunderstorm": "May bagyo",
      "Snow": "Nagsisnow",
      "Mist": "Mahamog",
      "Fog": "Makapal na hamog",
      "Haze": "Mabahong hangin",
      "Dust": "Alikabok",
      "Sand": "Buhangin"
    };
    const cond = conditionFil[weather.condition] || weather.condition;
    const timeDesc = weather.isDay ? "ngayon" : "ngayong gabi";
    return `Ang temperatura ${timeDesc} sa ${weather.city}, ${weather.country} ay ${weather.temperature}°C. ${cond}. Ang humidity ay ${weather.humidity}% at ang bilis ng hangin ay ${weather.windSpeed} km/h.`;
  } else {
    return `The temperature right now in ${weather.city}, ${weather.country} is ${weather.temperature}°C with ${weather.description}. It feels like ${weather.feelsLike}°C. Humidity is ${weather.humidity}% and wind speed is ${weather.windSpeed} km/h.`;
  }
}

// ============================================================================
// EDGE TTS — DYNAMIC LANGUAGE
// ============================================================================
async function generateEdgeTTS(text: string, outputPath: string, lang: "en" | "fil"): Promise<void> {
  return new Promise(async (resolve, reject) => {
    try {
      const tts = new MsEdgeTTS();
      const voice = lang === "en" ? "en-US-AriaNeural" : "fil-PH-BlessicaNeural";
      await tts.setMetadata(
        voice,
        OUTPUT_FORMAT.AUDIO_24KHZ_96KBITRATE_MONO_MP3
      );
      const { audioStream } = tts.toStream(text);
      const chunks: Buffer[] = [];
      audioStream.on("data", (chunk: Buffer) => chunks.push(chunk));
      audioStream.on("end", () => {
        const buf = Buffer.concat(chunks);
        if (buf.length < 100) return reject(new Error("Edge TTS returned empty audio"));
        fs.writeFileSync(outputPath, buf);
        console.log("[TTS]", voice, "success, size:", buf.length);
        resolve();
      });
      audioStream.on("error", reject);
    } catch (err: any) {
      reject(err);
    }
  });
}

// ============================================================================
// PCM GENERATION (AI)
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8) + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80", "lowpass=f=8000",
        "aresample=" + AI_SAMPLE_RATE + ":resampler=soxr:precision=28",
        "aformat=sample_fmts=s16:channel_layouts=mono",
        "volume=0.85", "dynaudnorm=p=0.95:g=15"
      ])
      .audioCodec("pcm_s16le").audioChannels(1).audioFrequency(AI_SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => { const pcm = fs.readFileSync(tmp); fs.unlinkSync(tmp); resolve(pcm); })
      .save(tmp);
  });
}

// ============================================================================
// MOSTAKIM MUSIC API
// ============================================================================
interface YTSearchResult { title: string; thumbnail: string; timestamp: string; url: string; }
interface YTDownloadResult { status: boolean; title: string; url: string; author: string; }

async function searchYouTube(query: string): Promise<YTSearchResult | null> {
  try {
    const searchUrl = `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`;
    console.log("[MUSIC] Searching:", query);
    const response = await fetch(searchUrl, { headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" } });
    if (!response.ok) return null;
    const results: YTSearchResult[] = await response.json();
    if (!Array.isArray(results) || results.length === 0) return null;
    console.log("[MUSIC] Found:", results[0].title);
    return results[0];
  } catch (e: any) { console.error("[MUSIC] Search error:", e.message); return null; }
}

async function getDownloadUrl(youtubeUrl: string): Promise<string | null> {
  try {
    const dlUrl = `https://mostakim.onrender.com/m/ytDl?url=${encodeURIComponent(youtubeUrl)}`;
    const response = await fetch(dlUrl, { headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" } });
    if (!response.ok) return null;
    const data: YTDownloadResult = await response.json();
    if (!data.status || !data.url) return null;
    return data.url;
  } catch (e: any) { console.error("[MUSIC] DL error:", e.message); return null; }
}

async function fetchMusicUrl(query: string): Promise<string | null> {
  const searchResult = await searchYouTube(query);
  if (!searchResult) return null;
  return await getDownloadUrl(searchResult.url);
}

// ============================================================================
// STREAM AI RESPONSE PCM (16kHz MONO)
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer, sessionId: string) {
  if (ws.readyState !== ws.OPEN) return;

  const alignedLen = Math.floor(pcm.length / AI_CHUNK_SIZE_MONO) * AI_CHUNK_SIZE_MONO;
  const totalChunks = alignedLen / AI_CHUNK_SIZE_MONO;
  console.log("[STREAM] AI:", sessionId, "chunks:", totalChunks, "chunkSize:", AI_CHUNK_SIZE_MONO, "interval:", SEND_INTERVAL_MS_AI, "ms", "prebuffer:", PREBUFFER_CHUNKS_AI);

  ws.send("SESSION:" + sessionId);
  await delay(100);
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await delay(300);

  let seq = 0;
  const prebufferLimit = Math.min(PREBUFFER_CHUNKS_AI, totalChunks);

  for (let i = 0; i < prebufferLimit * AI_CHUNK_SIZE_MONO; i += AI_CHUNK_SIZE_MONO) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.subarray(i, i + AI_CHUNK_SIZE_MONO);
    const packet = Buffer.allocUnsafe(2 + chunk.length);
    packet.writeUInt16BE(seq & 0xFFFF, 0);
    packet.set(chunk, 2);
    ws.send(packet, { binary: true });
    seq++;
    await delay(SEND_INTERVAL_MS_AI);
  }

  ws.send("START_RESPONSE");
  await delay(100);
  console.log("[STREAM] AI prebuffer done (", prebufferLimit, "chunks = ~", prebufferLimit * 32, "ms), started playback");

  try {
    for (let i = prebufferLimit * AI_CHUNK_SIZE_MONO; i < alignedLen; i += AI_CHUNK_SIZE_MONO) {
      if (ws.readyState !== ws.OPEN) return;
      const chunk = pcm.subarray(i, i + AI_CHUNK_SIZE_MONO);
      const packet = Buffer.allocUnsafe(2 + chunk.length);
      packet.writeUInt16BE(seq & 0xFFFF, 0);
      packet.set(chunk, 2);
      ws.send(packet, { binary: true });
      seq++;
      await delay(SEND_INTERVAL_MS_AI);
    }

    await delay(500);
    for (let retry = 0; retry < 3; retry++) {
      if (ws.readyState === ws.OPEN) { 
        ws.send("FINISH_RESPONSE:" + sessionId); 
        await delay(200); 
      }
    }
    console.log("[STREAM] AI done:", seq, "chunks");
  } catch (e: any) {
    console.error("[STREAM] Error:", e.message);
    try { ws.send("FINISH_RESPONSE:ERROR"); } catch {}
  }
}

// ============================================================================
// REAL-TIME MUSIC STREAMING
// ============================================================================
async function streamMusicRealtime(ws: WebSocket, musicUrl: string, sessionId: string) {
  if (ws.readyState !== ws.OPEN) { console.log("[MUSIC] WS not open"); return; }

  console.log("[MUSIC] Starting stream:", sessionId, "chunkSize:", MUSIC_CHUNK_SIZE_MONO, "interval:", SEND_INTERVAL_MS_MUSIC, "ms", "prebuffer:", PREBUFFER_CHUNKS_MUSIC);

  return new Promise<void>((resolve, reject) => {
    const ffmpegArgs = [
      "-re",
      "-i", musicUrl,
      "-vn",
      "-af", "highpass=f=60,lowpass=f=18000,aresample=44100:resampler=soxr:precision=28,aformat=sample_fmts=s16:channel_layouts=mono,volume=0.65,loudnorm=I=-16:TP=-1.5:LRA=11,equalizer=f=100:t=h:width=200:g=-2,equalizer=f=8000:t=h:width=2000:g=2",
      "-acodec", "pcm_s16le",
      "-ac", "1",
      "-ar", "44100",
      "-f", "s16le",
      "pipe:1"
    ];

    const ffmpegProc = spawn("ffmpeg", ffmpegArgs, {
      stdio: ["ignore", "pipe", "pipe"]
    });

    let buffer = Buffer.alloc(0);
    let seq = 0;
    let started = false;
    let finished = false;
    let chunkCount = 0;
    let prebufferChunks: Buffer[] = [];

    ffmpegProc.stderr.on("data", (data) => {
      const line = data.toString().trim();
      if (line.includes("Error") || line.includes("error")) {
        console.log("[FFMPEG]", line.substring(0, 100));
      }
    });

    ffmpegProc.on("error", (err) => {
      console.error("[MUSIC] FFmpeg spawn error:", err.message);
      if (!finished) {
        finished = true;
        try { ws.send("ERROR:MUSIC_FAILED"); ws.send("FINISH_MUSIC:ERROR"); } catch {}
        reject(err);
      }
    });

    ffmpegProc.on("close", (code) => {
      console.log("[MUSIC] FFmpeg exited:", code);
      if (!finished) {
        finished = true;
        if (buffer.length >= MUSIC_CHUNK_SIZE_MONO && ws.readyState === ws.OPEN) {
          const alignedLen = Math.floor(buffer.length / MUSIC_CHUNK_SIZE_MONO) * MUSIC_CHUNK_SIZE_MONO;
          for (let i = 0; i < alignedLen; i += MUSIC_CHUNK_SIZE_MONO) {
            const chunk = buffer.subarray(i, i + MUSIC_CHUNK_SIZE_MONO);
            const packet = Buffer.allocUnsafe(2 + chunk.length);
            packet.writeUInt16BE(seq & 0xFFFF, 0);
            packet.set(chunk, 2);
            try { ws.send(packet, { binary: true }); seq++; } catch {}
          }
        }
        for (let retry = 0; retry < 3; retry++) {
          if (ws.readyState === ws.OPEN) {
            try { ws.send("FINISH_MUSIC:" + sessionId); } catch {}
          }
        }
        console.log("[MUSIC] Stream done:", chunkCount, "chunks");
        resolve();
      }
    });

    ffmpegProc.stdout.on("data", async (data: Buffer) => {
      buffer = Buffer.concat([buffer, data]);

      if (!started) {
        while (buffer.length >= MUSIC_CHUNK_SIZE_MONO && prebufferChunks.length < PREBUFFER_CHUNKS_MUSIC) {
          const chunk = buffer.subarray(0, MUSIC_CHUNK_SIZE_MONO);
          buffer = buffer.subarray(MUSIC_CHUNK_SIZE_MONO);
          prebufferChunks.push(Buffer.from(chunk));
        }

        if (prebufferChunks.length >= PREBUFFER_CHUNKS_MUSIC) {
          started = true;
          console.log("[MUSIC] Prebuffer ready (", prebufferChunks.length, "chunks = ~", Math.round(prebufferChunks.length * 23.2), "ms), starting stream...");

          ws.send("SESSION:" + sessionId);
          await delay(100);
          ws.send("PREPARE_MUSIC:0");
          await delay(300);

          for (const chunk of prebufferChunks) {
            if (ws.readyState !== ws.OPEN) {
              finished = true;
              ffmpegProc.kill("SIGKILL");
              return;
            }
            const packet = Buffer.allocUnsafe(2 + chunk.length);
            packet.writeUInt16BE(seq & 0xFFFF, 0);
            packet.set(chunk, 2);
            ws.send(packet, { binary: true });
            seq++;
            chunkCount++;
            await delay(SEND_INTERVAL_MS_MUSIC);
          }

          ws.send("START_MUSIC");
          await delay(100);
          console.log("[MUSIC] Playback started after prebuffer");
        }
        return;
      }

      while (buffer.length >= MUSIC_CHUNK_SIZE_MONO && !finished) {
        if (ws.readyState !== ws.OPEN) {
          console.log("[MUSIC] WS closed, killing FFmpeg");
          finished = true;
          ffmpegProc.kill("SIGKILL");
          return;
        }

        const chunk = buffer.subarray(0, MUSIC_CHUNK_SIZE_MONO);
        buffer = buffer.subarray(MUSIC_CHUNK_SIZE_MONO);

        const packet = Buffer.allocUnsafe(2 + chunk.length);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        packet.set(chunk, 2);

        ws.send(packet, { binary: true });
        seq++;
        chunkCount++;

        await delay(SEND_INTERVAL_MS_MUSIC);
      }
    });

    setTimeout(() => {
      if (!finished) {
        console.log("[MUSIC] Timeout, stopping stream");
        finished = true;
        ffmpegProc.kill("SIGKILL");
        try { ws.send("FINISH_MUSIC:" + sessionId); } catch {}
        resolve();
      }
    }, 600000);
  });
}

// ============================================================================
// DELAY HELPER
// ============================================================================
function delay(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

// ============================================================================
// STREAMING STT PROCESSOR
// ============================================================================
interface StreamingSTTSession {
  userId: number;
  ws: WebSocket;
  audioBuffer: Buffer;
  isRecording: boolean;
  sessionId: string;
}

function createSTTSession(ws: WebSocket, userId: number): StreamingSTTSession {
  return {
    userId,
    ws,
    audioBuffer: Buffer.alloc(0),
    isRecording: false,
    sessionId: Date.now() + "_" + Math.random().toString(36).slice(2, 8),
  };
}

async function processFinalSTT(session: StreamingSTTSession): Promise<string | null> {
  if (session.audioBuffer.length < 1600) {
    console.log("[STT] Audio too short, ignoring");
    return null;
  }
  
  const tmpWav = path.join(UPLOAD_DIR, session.sessionId + "_stt.wav");
  const tmpClean = path.join(UPLOAD_DIR, session.sessionId + "_clean.wav");
  const dataLen = session.audioBuffer.length;
  
  try {
    const wavBuffer = Buffer.alloc(44 + dataLen);
    wavBuffer.write("RIFF", 0);
    wavBuffer.writeUInt32LE(36 + dataLen, 4);
    wavBuffer.write("WAVE", 8);
    wavBuffer.write("fmt ", 12);
    wavBuffer.writeUInt32LE(16, 16);
    wavBuffer.writeUInt16LE(1, 20);
    wavBuffer.writeUInt16LE(1, 22);
    wavBuffer.writeUInt32LE(STT_STREAM_SAMPLE_RATE, 24);
    wavBuffer.writeUInt32LE(STT_STREAM_SAMPLE_RATE * 2, 28);
    wavBuffer.writeUInt16LE(2, 32);
    wavBuffer.writeUInt16LE(16, 34);
    wavBuffer.write("data", 36);
    wavBuffer.writeUInt32LE(dataLen, 40);
    session.audioBuffer.copy(wavBuffer, 44);
    
    fs.writeFileSync(tmpWav, wavBuffer);
    console.log("[STT] WAV saved:", dataLen, "bytes (~", (dataLen/2/16000).toFixed(2), "seconds)");

    await new Promise<void>((resolve, reject) => {
      ffmpeg(tmpWav)
        .audioFilters([
          "highpass=f=80",
          "lowpass=f=8000",
          "dynaudnorm=p=0.95:g=15",
          "afftdn=nf=-25"
        ])
        .audioCodec("pcm_s16le")
        .audioChannels(1)
        .audioFrequency(16000)
        .on("error", (err) => {
          console.log("[STT] FFmpeg normalize failed, using raw:", err.message);
          fs.copyFileSync(tmpWav, tmpClean);
          resolve();
        })
        .on("end", resolve)
        .save(tmpClean);
    });

    const stt = await Promise.race([
      sttClient.audio.transcriptions.create({ 
        file: fs.createReadStream(tmpClean), 
        model: "whisper-large-v3-turbo", 
        language: "en",
        prompt: "The user speaks English or Tagalog (Filipino). Common words: ano, ang, photosynthesis, bakit, paano, sino, saan, kailan, tumugtog, music, pangalan, weather, panahon, ulan, mainit, lamig."
      }),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("STT_TIMEOUT")), 15000))
    ]);
    
    const text = stt.text?.trim();
    
    try { fs.unlinkSync(tmpWav); } catch {}
    try { fs.unlinkSync(tmpClean); } catch {}
    
    if (!text) {
      console.log("[STT] No speech detected");
      return null;
    }
    
    console.log("[STT] RESULT:", text);
    return text;
  } catch (e: any) {
    console.error("[STT] Error:", e.message);
    try { fs.unlinkSync(tmpWav); } catch {}
    try { fs.unlinkSync(tmpClean); } catch {}
    return null;
  }
}

// ============================================================================
// AI-DRIVEN PROCESSOR — LLM DECIDES EVERYTHING
// ============================================================================
interface AIAction {
  type: "chat" | "weather" | "music";
  text: string;
  music?: string | null;
  weather?: boolean;
  lang: "en" | "fil";
}

async function getAIDecision(userText: string, history: any[], savedName: string | null): Promise<AIAction> {
  const systemPrompt = `You are Mochi, a helpful voice assistant. The user is in Alfonso, Cavite, Philippines (coordinates: 14.1239, 120.8547).

CRITICAL RULES:
1. You MUST respond in valid JSON ONLY. No extra text, no markdown.
2. Detect the user's language (English or Filipino/Tagalog) and respond in that same language.
3. If the user asks about weather, temperature, rain, forecast, or anything related to current conditions, set "type": "weather" and "weather": true.
4. If the user wants to play music or a song, set "type": "music" and provide the "music" search query.
5. For normal conversation, set "type": "chat".
6. Keep responses natural, concise, and conversational (1-2 sentences max for voice).
7. If the user says something vague like "play music" without specifying a song, ask what song they want.

JSON FORMAT:
{
  "type": "chat" | "weather" | "music",
  "text": "your response text",
  "music": "song search query or null",
  "weather": true or false,
  "lang": "en" or "fil"
}

Examples:
- "What's the weather?" -> {"type":"weather","text":"Let me check the weather for you.","weather":true,"music":null,"lang":"en"}
- "Kumusta ang panahon?" -> {"type":"weather","text":"Tingnan ko ang panahon ngayon.","weather":true,"music":null,"lang":"fil"}
- "Play Tibok" -> {"type":"music","text":"Playing Tibok by Earl Agustin","music":"Tibok by Earl Agustin","weather":false,"lang":"en"}
- "Tumugtog ka ng music" -> {"type":"music","text":"Anong kanta gusto mo?","music":null,"weather":false,"lang":"fil"}
- "Hello" -> {"type":"chat","text":"Hello! How can I help you today?","music":null,"weather":false,"lang":"en"}
- "Kumusta" -> {"type":"chat","text":"Mabuti naman! Paano kita matutulungan?","music":null,"weather":false,"lang":"fil"}

Return ONLY the JSON object.`;

  const messages: any[] = [
    { role: "system", content: systemPrompt },
    ...history.map(h => ({ role: h.role, content: h.content })),
    { role: "user", content: userText }
  ];

  const ai = await Promise.race([
    llmClient.chat.completions.create({ 
      model: "llama-3.3-70b-versatile", 
      messages, 
      max_tokens: 200, 
      temperature: 0.3 
    }),
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("LLM_TIMEOUT")), 10000))
  ]);

  const raw = ai.choices[0].message.content || "{}";
  console.log("[AI RAW]", raw);

  try {
    const parsed = JSON.parse(raw);
    return {
      type: parsed.type || "chat",
      text: parsed.text || "Sorry, I didn't understand.",
      music: parsed.music || null,
      weather: parsed.weather === true,
      lang: parsed.lang || detectLanguage(userText)
    };
  } catch {
    // Fallback: try to extract intent from raw text
    const lower = raw.toLowerCase();
    const lang = detectLanguage(userText);
    
    if (lower.includes("weather") || lower.includes("panahon") || lower.includes("temperature")) {
      return { type: "weather", text: raw, music: null, weather: true, lang };
    }
    if (lower.includes("music") || lower.includes("song") || lower.includes("kanta") || lower.includes("tumugtog")) {
      return { type: "music", text: raw, music: null, weather: false, lang };
    }
    return { type: "chat", text: raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim(), music: null, weather: false, lang };
  }
}

// ============================================================================
// PROCESS AI RESPONSE
// ============================================================================
async function processAIResponse(ws: WebSocket, userText: string, userId: number, sessionId: string) {
  if (isDuplicate(userId)) { ws.send("ERROR:PROCESSING_BUSY"); return; }
  
  const filesToCleanup: string[] = [];
  
  try {
    const nameAction = extractName(userText);
    let nameResponse = "";
    if (nameAction.action === "save" && nameAction.name) {
      await storage.saveName(userId, nameAction.name);
      nameResponse = `Nice to meet you, ${nameAction.name}!`;
    } else if (nameAction.action === "delete") {
      await storage.deleteSavedName(userId);
      nameResponse = "Name deleted.";
    }

    const history = await storage.getConversationHistory(userId);
    const savedName = await storage.getSavedName(userId);

    // AI decides what to do
    const action = await getAIDecision(userText, history, savedName);
    console.log("[AI DECISION]", action.type, "lang:", action.lang, "weather:", action.weather, "music:", action.music);

    let finalText = action.text;
    let musicQuery = action.music;

    if (nameResponse) {
      finalText = nameResponse;
      musicQuery = null;
    }

    // Handle Weather (AI decided)
    if (action.type === "weather" || action.weather) {
      const weather = await fetchWeather();
      if (weather) {
        const weatherText = formatWeatherResponse(weather, action.lang);
        console.log("[WEATHER] Response:", weatherText);
        
        const mp3 = path.join(AUDIO_DIR, sessionId + "_weather.mp3");
        filesToCleanup.push(mp3);
        
        await generateEdgeTTS(weatherText, mp3, action.lang);
        const pcm = await generatePCM(mp3);
        await streamPCM(ws, pcm, sessionId);
        
        await storage.addMessage(userId, "user", userText);
        await storage.addMessage(userId, "assistant", weatherText);
        return;
      } else {
        const failText = action.lang === "fil" 
          ? "Pasensya na, hindi ko makuha ang impormasyon ng panahon ngayon. Siguraduhin mo na may OpenWeatherMap API key." 
          : "Sorry, I couldn't fetch the weather right now. Please make sure your OpenWeatherMap API key is set.";
        const mp3 = path.join(AUDIO_DIR, sessionId + "_weather_fail.mp3");
        filesToCleanup.push(mp3);
        await generateEdgeTTS(failText, mp3, action.lang);
        const pcm = await generatePCM(mp3);
        await streamPCM(ws, pcm, sessionId);
        return;
      }
    }

    // Save to history
    await storage.addMessage(userId, "user", userText);
    await storage.addMessage(userId, "assistant", finalText);

    // Handle Music
    if (action.type === "music" && musicQuery) {
      const musicUrl = await fetchMusicUrl(musicQuery);
      if (!musicUrl) { 
        const notFound = action.lang === "fil" 
          ? "Hindi ko mahanap ang kanta. Subukan mo ulit." 
          : "I couldn't find that song. Please try again.";
        const mp3 = path.join(AUDIO_DIR, sessionId + "_notfound.mp3");
        filesToCleanup.push(mp3);
        await generateEdgeTTS(notFound, mp3, action.lang);
        const pcm = await generatePCM(mp3);
        await streamPCM(ws, pcm, sessionId);
        return; 
      }

      if (finalText && !nameResponse) {
        const introId = sessionId + "_intro";
        const introMp3 = path.join(AUDIO_DIR, introId + ".mp3");

        await generateEdgeTTS(finalText, introMp3, action.lang);
        filesToCleanup.push(introMp3);

        const introPcm = await generatePCM(introMp3);
        await streamPCM(ws, introPcm, introId);
        await delay(1000);
      }

      await streamMusicRealtime(ws, musicUrl, sessionId + "_music");
    } 
    // Handle Chat (default)
    else {
      const mp3 = path.join(AUDIO_DIR, sessionId + ".mp3");
      await generateEdgeTTS(finalText, mp3, action.lang);
      filesToCleanup.push(mp3);
      const pcm = await generatePCM(mp3);
      await streamPCM(ws, pcm, sessionId);
    }
  } catch (e: any) {
    console.error("[PROCESS] Error:", e.message);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
    try { ws.send("FINISH_RESPONSE:ERROR"); } catch {}
  } finally {
    for (const f of filesToCleanup) { try { fs.unlinkSync(f); } catch {} }
    setTimeout(() => RECENT_REQUESTS.delete(userId), DEBOUNCE_MS);
  }
}

// ============================================================================
// ROUTES
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  try {
    await pushSchema();
    const isValid = await verifySchema();
    if (!isValid) throw new Error("Schema verification failed");
    console.log("[SERVER] Database ready");
  } catch (e: any) { console.error("[SERVER] DB init failed:", e.message); }

  const wss = new WebSocketServer({
    server: httpServer, path: "/ws/audio",
    perMessageDeflate: false, maxPayload: 64 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - AI-DRIVEN WEATHER + LANG_SWITCH");
    let currentUserId: number | null = null;
    let messageCount = 0;
    let sttSession: StreamingSTTSession | null = null;

    ws.on("message", async (data: any, isBinary: boolean) => {
      messageCount++;
      const currentMsgNum = messageCount;
      
      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT #" + currentMsgNum + ":", msg);
        
        if (msg === "READY") { 
          ws.send("STATE:IDLE"); 
        }
        else if (msg === "STREAM_START") {
          if (!currentUserId) {
            try { 
              const anon = await storage.getOrCreateUser("anon_" + Date.now()); 
              currentUserId = anon.id; 
            } catch (e: any) { 
              ws.send("ERROR:DB_FAILED"); 
              return; 
            }
          }
          sttSession = createSTTSession(ws, currentUserId);
          sttSession.isRecording = true;
          ws.send("STREAM_READY");
          console.log("[STT] Session started for user:", currentUserId);
        }
        else if (msg === "STREAM_END") {
          if (sttSession && sttSession.isRecording) {
            sttSession.isRecording = false;
            console.log("[STT] Finalizing, buffer size:", sttSession.audioBuffer.length);
            
            const text = await processFinalSTT(sttSession);
            if (text) {
              ws.send("STT_RESULT:" + text);
              await processAIResponse(ws, text, sttSession.userId, sttSession.sessionId);
            } else {
              ws.send("ERROR:NO_SPEECH");
              ws.send("STATE:IDLE");
            }
            sttSession = null;
          }
        }
        else if (msg.startsWith("USER:")) {
          const userName = msg.replace("USER:", "").trim();
          try {
            const user = await storage.getOrCreateUser(userName);
            currentUserId = user.id;
            ws.send("USER_CONFIRMED:" + user.name);
          } catch (e: any) { ws.send("ERROR:USER_FAILED"); }
        }
        return;
      }
      
      const chunkLen = Buffer.from(data).length;
      console.log("[WS] BINARY #" + currentMsgNum + ":", chunkLen, "bytes");
      
      if (!sttSession || !sttSession.isRecording) {
        return;
      }
      
      const chunk = Buffer.from(data);
      
      const maxBytes = STT_STREAM_SAMPLE_RATE * 2 * STT_MAX_AUDIO_SECONDS;
      if (sttSession.audioBuffer.length + chunk.length > maxBytes) {
        console.log("[STT] Buffer full, forcing finalize");
        sttSession.isRecording = false;
        
        const text = await processFinalSTT(sttSession);
        if (text) {
          ws.send("STT_RESULT:" + text);
          await processAIResponse(ws, text, sttSession.userId, sttSession.sessionId);
        } else {
          ws.send("ERROR:AUDIO_TOO_LONG");
          ws.send("STATE:IDLE");
        }
        sttSession = null;
        return;
      }
      
      sttSession.audioBuffer = Buffer.concat([sttSession.audioBuffer, chunk]);
    });

    ws.on("close", () => { 
      console.log("ESP disconnected"); 
      if (sttSession) {
        sttSession = null;
      }
      currentUserId = null; 
    });
    
    ws.on("error", (err) => console.error("[WS] Error:", err.message));

    const pingInterval = setInterval(() => {
      if (ws.readyState === ws.OPEN) ws.ping();
      else clearInterval(pingInterval);
    }, 15000);
  });

  return httpServer;
}
