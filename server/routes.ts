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

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ============================================================================
// AUDIO CONFIG — ULTRA: Synced with ESP V30-ULTRA for weak hotspot
// ============================================================================
// AI: 16kHz mono, 1024 bytes = 32ms audio → ESP tick every 64ms (plays 2x)
// Music: 44.1kHz mono, 2048 bytes = 23.2ms audio → ESP tick every 46ms
// Server sends slightly faster than ESP consumes to keep buffer full
const AI_SAMPLE_RATE = 16000;
const AI_CHUNK_SIZE_MONO = 1024;     // 32ms @ 16kHz mono
const SEND_INTERVAL_MS_AI = 60;       // Slower than real-time for weak hotspot
const PREBUFFER_CHUNKS_AI = 24;       // ~1.5 seconds prebuffer

const MUSIC_SAMPLE_RATE = 44100;
const MUSIC_CHUNK_SIZE_MONO = 2048;  // 23.2ms @ 44.1kHz mono
const SEND_INTERVAL_MS_MUSIC = 44;    // Slightly slower than 23ms x 2
const PREBUFFER_CHUNKS_MUSIC = 32;    // ~1.5 seconds prebuffer

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
// EDGE TTS — FILIPINO (fil-PH-AngeloNeural)
// ============================================================================
async function generateEdgeTTS(text: string, outputPath: string): Promise<void> {
  return new Promise(async (resolve, reject) => {
    try {
      const tts = new MsEdgeTTS();
      await tts.setMetadata(
        "fil-PH-AngeloNeural",
        OUTPUT_FORMAT.AUDIO_24KHZ_96KBITRATE_MONO_MP3
      );
      const { audioStream } = tts.toStream(text);
      const chunks: Buffer[] = [];
      audioStream.on("data", (chunk: Buffer) => chunks.push(chunk));
      audioStream.on("end", () => {
        const buf = Buffer.concat(chunks);
        if (buf.length < 100) return reject(new Error("Edge TTS returned empty audio"));
        fs.writeFileSync(outputPath, buf);
        console.log("[TTS] fil-PH-AngeloNeural success, size:", buf.length);
        resolve();
      });
      audioStream.on("error", reject);
    } catch (err: any) {
      reject(err);
    }
  });
}

// ============================================================================
// PCM GENERATION (AI) — Optimized for streaming
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
// STREAM AI RESPONSE PCM (16kHz MONO) — ULTRA for weak hotspot
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

  // === ULTRA PRE-BUFFER: Send first N chunks silently ===
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

  // Now tell ESP to start — jitter buffer already has 1.5s of data
  ws.send("START_RESPONSE");
  await delay(100);
  console.log("[STREAM] AI prebuffer done (", prebufferLimit, "chunks = ~", prebufferLimit * 32, "ms), started playback");

  // Continue rest of chunks — slower than real-time to let buffer grow
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
// REAL-TIME MUSIC STREAMING — ULTRA for weak hotspot
// ============================================================================
async function streamMusicRealtime(ws: WebSocket, musicUrl: string, sessionId: string) {
  if (ws.readyState !== ws.OPEN) { console.log("[MUSIC] WS not open"); return; }

  console.log("[MUSIC] Starting ULTRA stream:", sessionId, "chunkSize:", MUSIC_CHUNK_SIZE_MONO, "interval:", SEND_INTERVAL_MS_MUSIC, "ms", "prebuffer:", PREBUFFER_CHUNKS_MUSIC);

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
        // Send remaining buffer
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

      // Collect ULTRA prebuffer first
      if (!started) {
        while (buffer.length >= MUSIC_CHUNK_SIZE_MONO && prebufferChunks.length < PREBUFFER_CHUNKS_MUSIC) {
          const chunk = buffer.subarray(0, MUSIC_CHUNK_SIZE_MONO);
          buffer = buffer.subarray(MUSIC_CHUNK_SIZE_MONO);
          prebufferChunks.push(Buffer.from(chunk));
        }

        if (prebufferChunks.length >= PREBUFFER_CHUNKS_MUSIC) {
          started = true;
          console.log("[MUSIC] ULTRA prebuffer ready (", prebufferChunks.length, "chunks = ~", Math.round(prebufferChunks.length * 23.2), "ms), starting stream...");

          ws.send("SESSION:" + sessionId);
          await delay(100);
          ws.send("PREPARE_MUSIC:0");
          await delay(300);

          // Send prebuffer chunks first
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
          console.log("[MUSIC] Playback started after ULTRA prebuffer");
        }
        return;
      }

      // Normal streaming after prebuffer — slower than real-time
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

    // Timeout safety
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
// PROCESS WITH MEMORY, DEDUPLICATION & MUSIC
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer, userId: number) {
  if (isDuplicate(userId)) { ws.send("ERROR:PROCESSING_BUSY"); return; }

  let processingComplete = false;
  const uniqueId = Date.now() + "_" + Math.random().toString(36).slice(2, 8);
  const wavPath = path.join(UPLOAD_DIR, uniqueId + ".wav");
  const filesToCleanup: string[] = [];

  try {
    if (audioBuffer.length < 1000) { ws.send("ERROR:AUDIO_TOO_SHORT"); return; }
    const riff = audioBuffer.slice(0, 4).toString();
    const wave = audioBuffer.slice(8, 12).toString();
    if (riff !== "RIFF" || wave !== "WAVE") { ws.send("ERROR:INVALID_FORMAT"); return; }

    fs.writeFileSync(wavPath, audioBuffer);
    filesToCleanup.push(wavPath);
    console.log("[UPLOAD] Saved:", audioBuffer.length, "bytes");

    const stt = await Promise.race([
      sttClient.audio.transcriptions.create({ file: fs.createReadStream(wavPath), model: "whisper-large-v3-turbo", language: "en" }),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("STT_TIMEOUT")), 15000))
    ]);

    const userText = stt.text?.trim();
    if (!userText) { ws.send("ERROR:NO_SPEECH"); return; }
    console.log("USER:", userText);

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

    const systemPrompt = `You are a helpful voice assistant. Keep responses short and natural.
${savedName ? `The user's name is ${savedName}. Address them by name.` : ""}
If the user wants to play music or a song, return JSON with "music" field:
{"text":"short acknowledgment","music":"song search query"}
Examples:
- "Play Tibok" -> {"text":"Playing Tibok by Earl Agustin","music":"Tibok by Earl Agustin"}
- "Tumugtog ka ng music" -> {"text":"Anong kanta gusto mo?","music":null}
Otherwise return: {"text":"your response"}
Return ONLY the JSON.`;

    const messages: any[] = [
      { role: "system", content: systemPrompt },
      ...history.map(h => ({ role: h.role, content: h.content })),
      { role: "user", content: userText }
    ];

    const ai = await Promise.race([
      llmClient.chat.completions.create({ model: "llama-3.3-70b-versatile", messages, max_tokens: 150, temperature: 0.7 }),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("LLM_TIMEOUT")), 10000))
    ]);

    const raw = ai.choices[0].message.content || "{}";
    let text = "Sorry, I didn't understand.";
    let musicQuery: string | null = null;
    try { const parsed = JSON.parse(raw); text = parsed.text || text; musicQuery = parsed.music || null; }
    catch { text = raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim(); }

    if (nameResponse) { text = nameResponse; musicQuery = null; }
    console.log("AI:", text, musicQuery ? `(Music: ${musicQuery})` : "");

    await storage.addMessage(userId, "user", userText);
    await storage.addMessage(userId, "assistant", text);

    if (musicQuery) {
      const musicUrl = await fetchMusicUrl(musicQuery);
      if (!musicUrl) { ws.send("ERROR:MUSIC_NOT_FOUND"); return; }

      if (text && !nameResponse) {
        const introId = uniqueId + "_intro";
        const introMp3 = path.join(AUDIO_DIR, introId + ".mp3");

        // Use Filipino TTS for intro
        await generateEdgeTTS(text, introMp3);
        filesToCleanup.push(introMp3);

        const introPcm = await generatePCM(introMp3);
        await streamPCM(ws, introPcm, introId);
        await delay(1000);
      }

      await streamMusicRealtime(ws, musicUrl, uniqueId + "_music");
    } else {
      const mp3 = path.join(AUDIO_DIR, uniqueId + ".mp3");

      // Use Filipino TTS
      await generateEdgeTTS(text, mp3);
      filesToCleanup.push(mp3);

      const pcm = await generatePCM(mp3);
      await streamPCM(ws, pcm, uniqueId);
    }
    processingComplete = true;
  } catch (e: any) {
    console.error("[PROCESS] Error:", e.message);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
    try { ws.send("FINISH_RESPONSE:ERROR"); } catch {}
  } finally {
    for (const f of filesToCleanup) { try { fs.unlinkSync(f); } catch {} }
    if (processingComplete) setTimeout(() => RECENT_REQUESTS.delete(userId), DEBOUNCE_MS);
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
    perMessageDeflate: false, maxPayload: 512 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - V30-ULTRA WEAK HOTSPOT [fil-PH-AngeloNeural TTS]");
    let processing = false;
    let currentUserId: number | null = null;
    let messageCount = 0;

    ws.on("message", async (data: any, isBinary: boolean) => {
      messageCount++;
      const currentMsgNum = messageCount;
      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT #" + currentMsgNum + ":", msg);
        if (msg === "READY") { ws.send("STATE:IDLE"); }
        if (msg.startsWith("USER:")) {
          const userName = msg.replace("USER:", "").trim();
          try {
            const user = await storage.getOrCreateUser(userName);
            currentUserId = user.id;
            ws.send("USER_CONFIRMED:" + user.name);
          } catch (e: any) { ws.send("ERROR:USER_FAILED"); }
        }
        return;
      }
      console.log("[WS] BINARY #" + currentMsgNum + ":", Buffer.from(data).length, "bytes");
      if (processing) { ws.send("ERROR:PROCESSING_BUSY"); return; }
      processing = true;
      if (!currentUserId) {
        try { const anon = await storage.getOrCreateUser("anon_" + Date.now()); currentUserId = anon.id; }
        catch (e: any) { ws.send("ERROR:DB_FAILED"); processing = false; return; }
      }
      const audioBuffer = Buffer.from(data);
      try { await processAndRespond(ws, audioBuffer, currentUserId); }
      finally { processing = false; }
    });

    ws.on("close", () => { console.log("ESP disconnected"); processing = false; currentUserId = null; });
    ws.on("error", (err) => console.error("[WS] Error:", err.message));

    const pingInterval = setInterval(() => {
      if (ws.readyState === ws.OPEN) ws.ping();
      else clearInterval(pingInterval);
    }, 15000);
  });

  return httpServer;
}
