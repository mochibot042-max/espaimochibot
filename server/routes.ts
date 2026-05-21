import type { Express } from "express";
import { Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import Groq from "groq-sdk";
import fs from "fs";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";
import { storage, pushSchema, verifySchema } from "./storage.js";

const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ============================================================================
// AUDIO CONFIG
// ============================================================================
const AI_SAMPLE_RATE = 16000;
const AI_CHUNK_SIZE_MONO = 512;       // 16kHz mono

const MUSIC_SAMPLE_RATE = 44100;      // FIXED: 41kHz (CD quality)
const MUSIC_CHUNK_SIZE_MONO = 1024;   // Smaller chunks for smoother streaming

const SEND_INTERVAL_MS = 20;          // Slightly slower for stability
const WS_KEEPALIVE_MS = 5000;         // Send ping every 5s during music

// ============================================================================
// DEBOUNCE & DEDUPLICATION
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
// PCM GENERATION (AI — 16kHz MONO)
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8) + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=8000",
        "aresample=" + AI_SAMPLE_RATE + ":resampler=soxr:precision=28",
        "aformat=sample_fmts=s16:channel_layouts=mono",
        "volume=0.85",
        "dynaudnorm=p=0.95:g=15"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(AI_SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => {
        const pcm = fs.readFileSync(tmp);
        fs.unlinkSync(tmp);
        resolve(pcm);
      })
      .save(tmp);
  });
}

// ============================================================================
// MOSTAKIM MUSIC API
// ============================================================================
interface YTSearchResult {
  title: string;
  thumbnail: string;
  timestamp: string;
  url: string;
}

interface YTDownloadResult {
  status: boolean;
  title: string;
  url: string;
  author: string;
}

async function searchYouTube(query: string): Promise<YTSearchResult | null> {
  try {
    const searchUrl = `https://mostakim.onrender.com/mostakim/ytSearch?search=${encodeURIComponent(query)}`;
    console.log("[MUSIC] Searching:", query);
    const response = await fetch(searchUrl, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" }
    });
    if (!response.ok) { console.log("[MUSIC] Search error:", response.status); return null; }
    const results: YTSearchResult[] = await response.json();
    if (!Array.isArray(results) || results.length === 0) { console.log("[MUSIC] No results"); return null; }
    console.log("[MUSIC] Found:", results[0].title);
    return results[0];
  } catch (e: any) { console.error("[MUSIC] Search error:", e.message); return null; }
}

async function getDownloadUrl(youtubeUrl: string): Promise<string | null> {
  try {
    const dlUrl = `https://mostakim.onrender.com/m/ytDl?url=${encodeURIComponent(youtubeUrl)}`;
    const response = await fetch(dlUrl, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" }
    });
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
// DOWNLOAD MUSIC FILE
// ============================================================================
async function downloadMusicFile(musicUrl: string, outputPath: string): Promise<void> {
  console.log("[DOWNLOAD] Starting:", outputPath);
  const response = await fetch(musicUrl, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "Accept": "*/*", "Accept-Encoding": "identity",
      "Referer": "https://mostakim.onrender.com/",
    },
    redirect: "follow",
  });
  if (!response.ok) throw new Error(`Download failed: ${response.status}`);
  const buffer = Buffer.from(await response.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
  console.log("[DOWNLOAD] Saved", buffer.length, "bytes");
}

// ============================================================================
// STREAM AI RESPONSE PCM (16kHz MONO)
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer, sessionId: string) {
  if (ws.readyState !== ws.OPEN) return;
  const alignedLen = Math.floor(pcm.length / AI_CHUNK_SIZE_MONO) * AI_CHUNK_SIZE_MONO;
  const totalChunks = alignedLen / AI_CHUNK_SIZE_MONO;
  console.log("[STREAM] AI:", sessionId, "chunks:", totalChunks);

  ws.send("SESSION:" + sessionId);
  await new Promise(r => setTimeout(r, 100));
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 300));
  ws.send("START_RESPONSE");
  await new Promise(r => setTimeout(r, 100));

  let seq = 0;
  try {
    for (let i = 0; i < alignedLen; i += AI_CHUNK_SIZE_MONO) {
      if (ws.readyState !== ws.OPEN) return;
      const chunk = pcm.subarray(i, i + AI_CHUNK_SIZE_MONO);
      const packet = Buffer.allocUnsafe(2 + chunk.length);
      packet.writeUInt16BE(seq & 0xFFFF, 0);
      packet.set(chunk, 2);
      ws.send(packet, { binary: true });
      seq++;
      await new Promise(r => setTimeout(r, SEND_INTERVAL_MS));
    }
    await new Promise(r => setTimeout(r, 500));
    for (let retry = 0; retry < 3; retry++) {
      if (ws.readyState === ws.OPEN) {
        ws.send("FINISH_RESPONSE:" + sessionId);
        await new Promise(r => setTimeout(r, 200));
      }
    }
    console.log("[STREAM] AI done:", seq, "chunks");
  } catch (e: any) {
    console.error("[STREAM] Error:", e.message);
    try { ws.send("FINISH_RESPONSE:ERROR"); } catch {}
  }
}

// ============================================================================
// STREAM MUSIC PCM (41kHz MONO) — CHUNKED TO PREVENT DISCONNECT
// ============================================================================
async function streamMusic(ws: WebSocket, mp3Url: string, sessionId: string) {
  if (ws.readyState !== ws.OPEN) { console.log("[MUSIC] WS not open"); return; }

  const tmpFile = path.join(AUDIO_DIR, "tmp_music_" + sessionId + ".mp4");
  const pcmPath = path.join(AUDIO_DIR, "music_" + sessionId + ".pcm");

  try {
    await downloadMusicFile(mp3Url, tmpFile);

    console.log("[MUSIC] Converting to 41kHz mono PCM...");
    await new Promise<void>((resolve, reject) => {
      ffmpeg(tmpFile)
        .audioFilters([
          "highpass=f=60",           // Lower highpass para hindi mawala bass
          "lowpass=f=18000",         // Higher lowpass para full range
          "aresample=" + MUSIC_SAMPLE_RATE + ":resampler=soxr:precision=28",
          "aformat=sample_fmts=s16:channel_layouts=mono",
          "volume=0.65",             // Lower volume para hindi clip
          "loudnorm=I=-16:TP=-1.5:LRA=11",  // Better than dynaudnorm para music
          "equalizer=f=100:t=h:width=200:g=-2",  // Reduce muddiness
          "equalizer=f=8000:t=h:width=2000:g=2"  // Slight treble boost
        ])
        .audioCodec("pcm_s16le")
        .audioChannels(1)
        .audioFrequency(MUSIC_SAMPLE_RATE)
        .format("s16le")
        .on("error", (err) => { console.error("[MUSIC] FFmpeg:", err.message); reject(err); })
        .on("end", () => { console.log("[MUSIC] Conversion done"); resolve(); })
        .save(pcmPath);
    });

    try { fs.unlinkSync(tmpFile); } catch {}
    if (!fs.existsSync(pcmPath)) throw new Error("PCM conversion failed");

    const pcm = fs.readFileSync(pcmPath);
    try { fs.unlinkSync(pcmPath); } catch {}

    console.log("[MUSIC] PCM:", pcm.length, "bytes @ 41kHz");
    if (ws.readyState !== ws.OPEN) return;

    const alignedLen = Math.floor(pcm.length / MUSIC_CHUNK_SIZE_MONO) * MUSIC_CHUNK_SIZE_MONO;
    const totalChunks = alignedLen / MUSIC_CHUNK_SIZE_MONO;
    console.log("[MUSIC] Streaming", totalChunks, "chunks");

    ws.send("SESSION:" + sessionId);
    await new Promise(r => setTimeout(r, 100));
    ws.send("PREPARE_MUSIC:" + totalChunks);
    await new Promise(r => setTimeout(r, 300));
    ws.send("START_MUSIC");
    await new Promise(r => setTimeout(r, 100));

    let seq = 0;
    let lastKeepalive = Date.now();

    for (let i = 0; i < alignedLen; i += MUSIC_CHUNK_SIZE_MONO) {
      if (ws.readyState !== ws.OPEN) { console.log("[MUSIC] WS closed mid-stream"); return; }

      const chunk = pcm.subarray(i, i + MUSIC_CHUNK_SIZE_MONO);
      const packet = Buffer.allocUnsafe(2 + chunk.length);
      packet.writeUInt16BE(seq & 0xFFFF, 0);
      packet.set(chunk, 2);
      ws.send(packet, { binary: true });
      seq++;

      // Send keepalive during long music to prevent timeout
      if (Date.now() - lastKeepalive > WS_KEEPALIVE_MS) {
        try { ws.send("PING:" + seq); } catch {}
        lastKeepalive = Date.now();
      }

      await new Promise(r => setTimeout(r, SEND_INTERVAL_MS));
    }

    await new Promise(r => setTimeout(r, 500));
    for (let retry = 0; retry < 3; retry++) {
      if (ws.readyState === ws.OPEN) {
        ws.send("FINISH_MUSIC:" + sessionId);
        await new Promise(r => setTimeout(r, 200));
      }
    }
    console.log("[MUSIC] Done:", seq, "chunks");

  } catch (e: any) {
    console.error("[MUSIC] Error:", e.message);
    try { fs.unlinkSync(tmpFile); } catch {}
    try { fs.unlinkSync(pcmPath); } catch {}
    try { ws.send("ERROR:MUSIC_FAILED"); ws.send("FINISH_MUSIC:ERROR"); } catch {}
  }
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
      sttClient.audio.transcriptions.create({
        file: fs.createReadStream(wavPath),
        model: "whisper-large-v3-turbo", language: "en"
      }),
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
      llmClient.chat.completions.create({
        model: "llama-3.3-70b-versatile", messages, max_tokens: 150, temperature: 0.7
      }),
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
      const mp3Url = await fetchMusicUrl(musicQuery);
      if (!mp3Url) { ws.send("ERROR:MUSIC_NOT_FOUND"); return; }

      if (text && !nameResponse) {
        const introId = uniqueId + "_intro";
        const tts = new EdgeTTS();
        const introMp3 = path.join(AUDIO_DIR, introId + ".mp3");
        await tts.ttsPromise(text, introMp3, { voice: "en-US-AriaNeural", rate: "+15%" });
        filesToCleanup.push(introMp3);
        const introPcm = await generatePCM(introMp3);
        await streamPCM(ws, introPcm, introId);
        await new Promise(r => setTimeout(r, 1000));
      }
      await streamMusic(ws, mp3Url, uniqueId + "_music");
    } else {
      const tts = new EdgeTTS();
      const mp3 = path.join(AUDIO_DIR, uniqueId + ".mp3");
      await tts.ttsPromise(text, mp3, { voice: "en-US-AriaNeural", rate: "+15%" });
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
    console.log("ESP connected - V27 41kHz Stereo Music [Mostakim API]");
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
