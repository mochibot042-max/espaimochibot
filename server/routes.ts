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

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
const CACHE_DIR = path.join(process.cwd(), "cache", "weather");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

// ============================================================================
// AUDIO CONFIG — TRUE 24-BIT MAX QUALITY (NOT 16-BIT)
// ============================================================================
const AI_SAMPLE_RATE = 24000;        // 24kHz for crisp voice
const AI_CHUNK_SIZE_MONO = 1536;     // 24kHz * 0.064s = 1536 bytes (16-bit) or 3072 (24-bit packed)
const SEND_INTERVAL_MS_AI = 32;      // ~31.25 fps smooth
const PREBUFFER_CHUNKS_AI = 20;

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
// WEATHER CONFIG — wttr.in (Free, No API Key)
// ============================================================================
const WEATHER_LOCATION = "Alfonso,Cavite,Philippines";

// ============================================================================
// CACHE CONFIG
// ============================================================================
const CACHE_DURATION = 10 * 60 * 1000;

function getCachePath(key: string): string {
  const safeKey = key.replace(/[^a-zA-Z0-9]/g, "_");
  return path.join(CACHE_DIR, `weather_${safeKey}.json`);
}

function getCache(key: string): any | null {
  const cachePath = getCachePath(key);
  if (!fs.existsSync(cachePath)) return null;
  const data = JSON.parse(fs.readFileSync(cachePath, "utf8"));
  const age = Date.now() - data.timestamp;
  if (age > CACHE_DURATION) {
    try { fs.unlinkSync(cachePath); } catch {}
    return null;
  }
  return data.value;
}

function setCache(key: string, value: any): void {
  const cachePath = getCachePath(key);
  fs.writeFileSync(cachePath, JSON.stringify({
    timestamp: Date.now(),
    value: value
  }));
}

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
// VOLUME COMMAND DETECTION
// ============================================================================
function extractVolumeCommand(text: string): { action: "set" | "none"; volume: number | null } {
  const lower = text.toLowerCase();
  const volumePatterns = [
    /(?:set\s+(?:your\s+)?)?volume\s+(?:to\s+)?(\d+)%?/i,
    /(?:set\s+(?:your\s+)?)?volume\s+(?:to\s+)?(\d+(?:\.\d+)?)/i,
    /volume\s+(?:into\s+)?(\d+)%?/i,
    /palakasin\s+(?:ang\s+)?volume\s+(?:sa\s+)?(\d+)%?/i,
    /hinaan\s+(?:ang\s+)?volume\s+(?:sa\s+)?(\d+)%?/i,
    /lakasan\s+(?:ang\s+)?volume/i,
    /pahinaan\s+(?:ang\s+)?volume/i,
  ];
  for (const pattern of volumePatterns) {
    const match = lower.match(pattern);
    if (match && match[1]) {
      let vol = parseFloat(match[1]);
      if (vol > 1) vol = vol / 100;
      if (vol >= 0 && vol <= 1.5) return { action: "set", volume: Math.min(vol, 1.0) };
    }
  }
  // Handle "louder" / "softer" without numbers
  if (/lakasan|palakasin|louder/i.test(lower)) return { action: "set", volume: null }; // null = relative
  if (/pahinaan|hinaan|softer|quieter/i.test(lower)) return { action: "set", volume: null };
  return { action: "none", volume: null };
}

// ============================================================================
// LED COLOR COMMAND DETECTION
// ============================================================================
function extractLedCommand(text: string): { action: "set" | "none"; color: string | null } {
  const lower = text.toLowerCase();
  const colorMap: Record<string, string> = {
    "red": "RED", "green": "GREEN", "blue": "BLUE", "yellow": "YELLOW",
    "cyan": "CYAN", "magenta": "MAGENTA", "white": "WHITE", "orange": "ORANGE",
    "purple": "PURPLE", "pink": "PINK", "off": "OFF", "black": "OFF",
    "pula": "RED", "berde": "GREEN", "asul": "BLUE", "dilaw": "YELLOW",
    "puti": "WHITE", "kahel": "ORANGE", "lila": "PURPLE", "rosas": "PINK"
  };

  const ledPatterns = [
    /(?:set|change|gawin|gawing|ilaw|led)\s+(?:ang\s+)?(?:led|light|color|ilaw|neon)\s+(?:to\s+|sa\s+|na\s+)?(\w+)/i,
    /(?:set|change)\s+(?:the\s+)?(?:led|light|color|neon)\s+(?:to\s+)?(\w+)/i,
    /(?:ilaw|led|neon)\s+(?:na\s+)?(\w+)/i,
    /(\w+)\s+(?:ang\s+)?(?:ilaw|led|neon)/i,
  ];

  for (const pattern of ledPatterns) {
    const match = lower.match(pattern);
    if (match && match[1]) {
      const colorKey = match[1].toLowerCase();
      if (colorMap[colorKey]) {
        return { action: "set", color: colorMap[colorKey] };
      }
    }
  }
  return { action: "none", color: null };
}

// ============================================================================
// RESTART COMMAND DETECTION
// ============================================================================
function isRestartCommand(text: string): boolean {
  const lower = text.toLowerCase();
  const restartPatterns = [
    /restart\s+(?:the\s+)?(?:system|device|robot|esp)/i,
    /reboot\s+(?:the\s+)?(?:system|device|robot|esp)/i,
    /i\s+restart\s+mo/i,
    /mag\s*restart\s*ka/i,
    /restart\s*ka/i,
    /mag\s*reboot\s*ka/i,
  ];
  return restartPatterns.some(p => p.test(lower));
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
// WEATHER API — wttr.in
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
  uvIndex: number;
  visibility: number;
  pressure: number;
  maxTemp: number;
  minTemp: number;
  isDay: boolean;
}

async function fetchWeather(): Promise<WeatherData | null> {
  try {
    const cacheKey = "alfonso_weather";
    const cached = getCache(cacheKey);
    if (cached) {
      console.log("[WEATHER] Using cached data");
      return cached;
    }

    const url = `https://wttr.in/${encodeURIComponent(WEATHER_LOCATION)}?format=j1`;
    const response = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" },
      signal: AbortSignal.timeout(15000)
    });

    if (!response.ok) {
      console.error("[WEATHER] API error:", response.status);
      return null;
    }

    const data = await response.json();
    const current = data.current_condition[0];
    const area = data.nearest_area[0];
    const today = data.weather[0];

    const weatherData: WeatherData = {
      temperature: parseInt(current.temp_C),
      feelsLike: parseInt(current.FeelsLikeC),
      humidity: parseInt(current.humidity),
      windSpeed: parseInt(current.windspeedKmph),
      condition: current.weatherDesc[0].value,
      description: current.weatherDesc[0].value,
      city: area.areaName[0].value,
      country: area.country[0].value,
      uvIndex: parseInt(current.uvIndex),
      visibility: parseInt(current.visibility),
      pressure: parseInt(current.pressure),
      maxTemp: parseInt(today.maxtempC),
      minTemp: parseInt(today.mintempC),
      isDay: current.isdaytime === "yes"
    };

    setCache(cacheKey, weatherData);
    console.log("[WEATHER] Fetched fresh data:", weatherData.temperature + "°C", weatherData.condition);
    return weatherData;
  } catch (e: any) {
    console.error("[WEATHER] Fetch error:", e.message);
    return null;
  }
}

function formatWeatherResponse(weather: WeatherData, lang: "en" | "fil"): string {
  if (lang === "fil") {
    const conditionFil: Record<string, string> = {
      "Clear": "Malinis na langit",
      "Sunny": "Maaraw",
      "Partly cloudy": "Bahagyang maulap",
      "Cloudy": "Maulap",
      "Overcast": "Makapal na maulap",
      "Mist": "Mahamog",
      "Fog": "Makapal na hamog",
      "Rain": "Umuulan",
      "Light rain": "Mahinang ulan",
      "Moderate rain": "Katamtamang ulan",
      "Heavy rain": "Malakas na ulan",
      "Drizzle": "Ambon",
      "Thunderstorm": "May bagyo",
      "Snow": "Nagsisnow",
      "Haze": "Mabahong hangin",
      "Patchy rain possible": "Posibleng umulan",
      "Patchy light rain": "Bahagyang mahinang ulan"
    };
    const cond = conditionFil[weather.condition] || weather.condition;
    const timeDesc = weather.isDay ? "ngayon" : "ngayong gabi";
    return `Ang temperatura ${timeDesc} sa ${weather.city}, ${weather.country} ay ${weather.temperature}°C. Pakiramdam ay ${weather.feelsLike}°C. ${cond}. Ang humidity ay ${weather.humidity}%, ang bilis ng hangin ay ${weather.windSpeed} km/h, at ang pressure ay ${weather.pressure} hPa. Ang UV index ay ${weather.uvIndex} at ang visibility ay ${weather.visibility} km. Ang maximum temperatura ngayong araw ay ${weather.maxTemp}°C at ang minimum ay ${weather.minTemp}°C.`;
  } else {
    const timeDesc = weather.isDay ? "right now" : "tonight";
    return `The temperature ${timeDesc} in ${weather.city}, ${weather.country} is ${weather.temperature}°C. It feels like ${weather.feelsLike}°C with ${weather.condition}. Humidity is ${weather.humidity}%, wind speed is ${weather.windSpeed} km/h, and pressure is ${weather.pressure} hPa. The UV index is ${weather.uvIndex} and visibility is ${weather.visibility} km. Today's high is ${weather.maxTemp}°C and low is ${weather.minTemp}°C.`;
  }
}

// ============================================================================
// EDGE TTS — TRUE 24-BIT MAX QUALITY (DECODE TO 24-BIT PCM)
// ============================================================================
async function generateEdgeTTS(text: string, outputPath: string, lang: "en" | "fil"): Promise<void> {
  return new Promise(async (resolve, reject) => {
    try {
      const tts = new MsEdgeTTS();
      const voice = lang === "en" ? "en-US-AriaNeural" : "fil-PH-BlessicaNeural";
      // 24kHz 96kbps mono — MAX quality MP3 from Edge
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
        console.log("[TTS] 24kHz/96kbps MP3 generated:", voice, "size:", buf.length);
        resolve();
      });
      audioStream.on("error", reject);
    } catch (err: any) {
      reject(err);
    }
  });
}

// ============================================================================
// PCM GENERATION — TRUE 24-BIT CLEAN OUTPUT (NO BASS CRACKING)
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp24 = path.join(AUDIO_DIR, "raw24_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8) + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        // Step 1: High quality resample to 24kHz
        "aresample=" + AI_SAMPLE_RATE + ":resampler=soxr:precision=33:osf=s24",
        // Step 2: Format as 24-bit mono
        "aformat=sample_fmts=s32:channel_layouts=mono",
        // Step 3: Clean high-pass to remove sub-bass rumble (causes cracking on small speakers)
        "highpass=f=120:dB=24:p=2",
        // Step 4: Low-pass to remove harsh highs above 12kHz
        "lowpass=f=12000:dB=12:p=2",
        // Step 5: Gentle volume (0.92 = safe headroom)
        "volume=0.92",
        // Step 6: Dynamic normalization (smooth, not aggressive)
        "dynaudnorm=f=150:g=25:p=0.95:m=5:r=0.8",
        // Step 7: Loudness normalization (broadcast standard)
        "loudnorm=I=-16:TP=-1.5:LRA=11:measured_I=-20:measured_TP=-3:measured_LRA=8",
        // Step 8: De-esser to reduce sibilance harshness
        "adeclick=ar=2",
        // Step 9: Slight presence boost for clarity (3-5kHz human voice fundamental)
        "equalizer=f=3500:t=h:width=800:g=1.5:w=0.3",
        // Step 10: Air/presence for intelligibility
        "equalizer=f=8000:t=h:width=2000:g=1:w=0.3"
      ])
      // Output as 32-bit float (ESP32 will handle as 24-bit effective)
      .audioCodec("pcm_s32le")
      .audioChannels(1)
      .audioFrequency(AI_SAMPLE_RATE)
      .format("s32le")
      .on("error", reject)
      .on("end", () => { 
        const pcm = fs.readFileSync(tmp24); 
        fs.unlinkSync(tmp24); 
        console.log("[PCM] 24-bit clean output:", pcm.length, "bytes (", pcm.length/4, "samples @ 24kHz =", (pcm.length/4/AI_SAMPLE_RATE).toFixed(2), "s)");
        resolve(pcm); 
      })
      .save(tmp24);
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
// STREAM AI RESPONSE PCM (24kHz MONO 32-BIT)
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer, sessionId: string) {
  if (ws.readyState !== ws.OPEN) return;

  // 32-bit = 4 bytes per sample
  const BYTES_PER_SAMPLE = 4;
  const CHUNK_SAMPLES = AI_CHUNK_SIZE_MONO / BYTES_PER_SAMPLE; // 384 samples per chunk
  const alignedLen = Math.floor(pcm.length / AI_CHUNK_SIZE_MONO) * AI_CHUNK_SIZE_MONO;
  const totalChunks = alignedLen / AI_CHUNK_SIZE_MONO;

  console.log("[STREAM] AI:", sessionId, "chunks:", totalChunks, "chunkSize:", AI_CHUNK_SIZE_MONO, "interval:", SEND_INTERVAL_MS_AI, "ms", "prebuffer:", PREBUFFER_CHUNKS_AI, "rate:", AI_SAMPLE_RATE, "Hz");

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
  console.log("[STREAM] AI prebuffer done (", prebufferLimit, "chunks = ~", Math.round(prebufferLimit * CHUNK_SAMPLES / AI_SAMPLE_RATE * 1000), "ms), started playback");

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
// REAL-TIME MUSIC STREAMING (24-BIT CLEAN)
// ============================================================================
async function streamMusicRealtime(ws: WebSocket, musicUrl: string, sessionId: string) {
  if (ws.readyState !== ws.OPEN) { console.log("[MUSIC] WS not open"); return; }

  console.log("[MUSIC] Starting stream:", sessionId, "chunkSize:", MUSIC_CHUNK_SIZE_MONO, "interval:", SEND_INTERVAL_MS_MUSIC, "ms", "prebuffer:", PREBUFFER_CHUNKS_MUSIC);

  return new Promise<void>((resolve, reject) => {
    const ffmpegArgs = [
      "-re",
      "-i", musicUrl,
      "-vn",
      // Clean audio chain — no bass boost, remove sub-bass that cracks small speakers
      "-af", "highpass=f=80:dB=24,lowpass=f=18000:dB=12,aresample=44100:resampler=soxr:precision=33,aformat=sample_fmts=s32:channel_layouts=mono,volume=0.65,loudnorm=I=-16:TP=-1.5:LRA=11,equalizer=f=100:t=h:width=200:g=-3,equalizer=f=8000:t=h:width=2000:g=1.5",
      "-acodec", "pcm_s32le",
      "-ac", "1",
      "-ar", "44100",
      "-f", "s32le",
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
          console.log("[MUSIC] Prebuffer ready (", prebufferChunks.length, "chunks = ~", Math.round(prebufferChunks.length * MUSIC_CHUNK_SIZE_MONO / 4 / 44100 * 1000), "ms), starting stream...");

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
  type: "chat" | "weather" | "music" | "command";
  text: string;
  music?: string | null;
  weather?: boolean;
  lang: "en" | "fil";
  command?: {
    type: "volume" | "led" | "restart" | null;
    value: string | number | null;
  };
}

async function getAIDecision(userText: string, history: any[], savedName: string | null): Promise<AIAction> {
  const systemPrompt = `You are Mochi, a helpful voice assistant. The user is in Alfonso, Cavite, Philippines.

CRITICAL RULES:
1. You MUST respond in valid JSON ONLY. No extra text, no markdown.
2. Detect the user's language (English or Filipino/Tagalog) and respond in that same language.
3. If the user asks about weather, temperature, rain, forecast, or anything related to current conditions, set "type": "weather" and "weather": true.
4. If the user wants to play music or a song, set "type": "music" and provide the "music" search query.
5. If the user asks to set volume, change LED color, or restart the system, set "type": "command" with "command" details.
6. For normal conversation, set "type": "chat".
7. Keep responses natural, concise, and conversational (1-2 sentences max for voice).

JSON FORMAT:
{
  "type": "chat" | "weather" | "music" | "command",
  "text": "your response text",
  "music": "song search query or null",
  "weather": true or false,
  "lang": "en" or "fil",
  "command": {
    "type": "volume" | "led" | "restart" | null,
    "value": "command value or null"
  }
}

Examples:
- "What's the weather?" -> {"type":"weather","text":"Let me check the weather for you.","weather":true,"music":null,"lang":"en","command":null}
- "Set volume to 50%" -> {"type":"command","text":"Volume set to 50%.","weather":false,"music":null,"lang":"en","command":{"type":"volume","value":"0.5"}}
- "Set LED to red" -> {"type":"command","text":"LED color set to red.","weather":false,"music":null,"lang":"en","command":{"type":"led","value":"RED"}}
- "Restart the system" -> {"type":"command","text":"Restarting the system now.","weather":false,"music":null,"lang":"en","command":{"type":"restart","value":null}}
- "Play Tibok" -> {"type":"music","text":"Playing Tibok by Earl Agustin","music":"Tibok by Earl Agustin","weather":false,"lang":"en","command":null}
- "Hello" -> {"type":"chat","text":"Hello! How can I help you today?","music":null,"weather":false,"lang":"en","command":null}

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
      max_tokens: 250, 
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
      lang: parsed.lang || detectLanguage(userText),
      command: parsed.command || { type: null, value: null }
    };
  } catch {
    // Fallback detection
    const lower = raw.toLowerCase();
    const lang = detectLanguage(userText);

    const volCmd = extractVolumeCommand(userText);
    if (volCmd.action === "set") {
      return { 
        type: "command", 
        text: `Volume set to ${Math.round((volCmd.volume || 0.5) * 100)}%.`, 
        music: null, 
        weather: false, 
        lang,
        command: { type: "volume", value: (volCmd.volume || 0.5).toString() }
      };
    }

    const ledCmd = extractLedCommand(userText);
    if (ledCmd.action === "set") {
      return { 
        type: "command", 
        text: `LED color set to ${ledCmd.color}.`, 
        music: null, 
        weather: false, 
        lang,
        command: { type: "led", value: ledCmd.color }
      };
    }

    if (isRestartCommand(userText)) {
      return { 
        type: "command", 
        text: "Restarting the system now.", 
        music: null, 
        weather: false, 
        lang,
        command: { type: "restart", value: null }
      };
    }

    if (lower.includes("weather") || lower.includes("panahon") || lower.includes("temperature")) {
      return { type: "weather", text: raw, music: null, weather: true, lang, command: { type: null, value: null } };
    }
    if (lower.includes("music") || lower.includes("song") || lower.includes("kanta") || lower.includes("tumugtog")) {
      return { type: "music", text: raw, music: null, weather: false, lang, command: { type: null, value: null } };
    }
    return { type: "chat", text: raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim(), music: null, weather: false, lang, command: { type: null, value: null } };
  }
}

// ============================================================================
// PROCESS AI RESPONSE
// ============================================================================
async function processAIResponse(ws: WebSocket, userText: string, userId: number, sessionId: string) {
  if (isDuplicate(userId)) { ws.send("ERROR:PROCESSING_BUSY"); return; }

  const filesToCleanup: string[] = [];
  let pendingRestart = false;
  let pendingLedColor: string | null = null;
  let pendingVolume: number | null = null;

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

    const action = await getAIDecision(userText, history, savedName);
    console.log("[AI DECISION]", action.type, "lang:", action.lang, "weather:", action.weather, "music:", action.music, "command:", action.command);

    let finalText = action.text;
    let musicQuery = action.music;

    if (nameResponse) {
      finalText = nameResponse;
      musicQuery = null;
    }

    // Handle Commands (Volume, LED, Restart) — QUEUE them, don't execute immediately
    if (action.type === "command" && action.command && action.command.type) {
      const cmd = action.command;

      if (cmd.type === "volume" && cmd.value) {
        const volValue = parseFloat(cmd.value as string);
        if (!isNaN(volValue) && volValue >= 0 && volValue <= 1.5) {
          pendingVolume = Math.min(volValue, 1.0);
          console.log("[COMMAND] Volume queued:", pendingVolume);
        }
      }
      else if (cmd.type === "led" && cmd.value) {
        pendingLedColor = cmd.value as string;
        console.log("[COMMAND] LED queued:", pendingLedColor);
      }
      else if (cmd.type === "restart") {
        pendingRestart = true;
        console.log("[COMMAND] Restart queued after TTS");
      }

      // Speak the confirmation first
      const mp3 = path.join(AUDIO_DIR, sessionId + "_cmd.mp3");
      filesToCleanup.push(mp3);
      await generateEdgeTTS(finalText, mp3, action.lang);
      const pcm = await generatePCM(mp3);
      await streamPCM(ws, pcm, sessionId);

      await storage.addMessage(userId, "user", userText);
      await storage.addMessage(userId, "assistant", finalText);

      // After TTS finishes, send commands
      await delay(300);

      if (pendingVolume !== null) {
        ws.send("VOLUME:" + pendingVolume.toFixed(3));
        console.log("[COMMAND] Volume sent:", pendingVolume);
      }
      if (pendingLedColor !== null) {
        ws.send("LED:" + pendingLedColor);
        console.log("[COMMAND] LED sent:", pendingLedColor);
      }
      if (pendingRestart) {
        await delay(500);
        ws.send("RESTART:NOW");
        console.log("[COMMAND] Restart signal sent after TTS");
      }
      return;
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
          ? "Pasensya na, hindi ko makuha ang impormasyon ng panahon ngayon. Subukan mo ulit mamaya." 
          : "Sorry, I couldn't fetch the weather right now. Please try again later.";
        const mp3 = path.join(AUDIO_DIR, sessionId + "_weather_fail.mp3");
        filesToCleanup.push(mp3);
        await generateEdgeTTS(failText, mp3, action.lang);
        const pcm = await generatePCM(mp3);
        await streamPCM(ws, pcm, sessionId);
        return;
      }
    }

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
    console.log("ESP connected - TRUE 24-BIT TTS + COMMANDS + PERSISTENT VOL + RESTART AFTER TTS");
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
