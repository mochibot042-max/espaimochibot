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
// AUDIO CONFIG — AI RESPONSE (16kHz MONO)
// ============================================================================
const AI_SAMPLE_RATE = 16000;
const AI_CHUNK_SIZE_MONO = 1024;
const SEND_INTERVAL_MS_AI = 28;
const PREBUFFER_CHUNKS_AI = 12;  // Reduced for faster start

// FIXED: Music now uses 48000Hz to match ESP32 DAC
const MUSIC_SAMPLE_RATE = 48000;
const MUSIC_CHUNK_SIZE_MONO = 2048;
const SEND_INTERVAL_MS_MUSIC = 21;
const PREBUFFER_CHUNKS_MUSIC = 32;

// ============================================================================
// STREAMING STT CONFIG
// ============================================================================
const STT_STREAM_SAMPLE_RATE = 16000;
const STT_MAX_AUDIO_SECONDS = 12;

// ============================================================================
// STREAMING TTS CONFIG
// ============================================================================
const SENTENCE_END_CHARS = /[.!?。！？\n]+/;
const MIN_TTS_CHARS = 15;      // Minimum chars before sending to TTS
const MAX_TTS_CHARS = 120;     // Maximum chars per TTS chunk
const TTS_BUFFER_MS = 800;     // Buffer time before starting playback

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
// EDGE TTS — FILIPINO (single sentence)
// ============================================================================
async function generateEdgeTTS(text: string, outputPath: string): Promise<void> {
  return new Promise(async (resolve, reject) => {
    try {
      const tts = new MsEdgeTTS();
      await tts.setMetadata(
        "fil-PH-BlessicaNeural",
        OUTPUT_FORMAT.AUDIO_24KHZ_96KBITRATE_MONO_MP3
      );
      const { audioStream } = tts.toStream(text);
      const chunks: Buffer[] = [];
      audioStream.on("data", (chunk: Buffer) => chunks.push(chunk));
      audioStream.on("end", () => {
        const buf = Buffer.concat(chunks);
        if (buf.length < 100) return reject(new Error("Edge TTS returned empty audio"));
        fs.writeFileSync(outputPath, buf);
        console.log("[TTS] fil-PH-BlessicaNeural success, size:", buf.length, "text:", text.substring(0, 40));
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
// STREAM PCM TO ESP32
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer, sessionId: string, isFirstChunk: boolean = false) {
  if (ws.readyState !== ws.OPEN) return;

  const alignedLen = Math.floor(pcm.length / AI_CHUNK_SIZE_MONO) * AI_CHUNK_SIZE_MONO;
  const totalChunks = alignedLen / AI_CHUNK_SIZE_MONO;

  if (totalChunks === 0) return;

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
      await delay(SEND_INTERVAL_MS_AI);
    }
  } catch (e: any) {
    console.error("[STREAM] Error:", e.message);
  }
}

// ============================================================================
// REAL-TIME MUSIC STREAMING
// ============================================================================
async function streamMusicRealtime(ws: WebSocket, musicUrl: string, sessionId: string) {
  if (ws.readyState !== ws.OPEN) { console.log("[MUSIC] WS not open"); return; }

  console.log("[MUSIC] Starting stream:", sessionId);

  return new Promise<void>((resolve, reject) => {
    const ffmpegArgs = [
      "-re", "-i", musicUrl, "-vn",
      "-af", "highpass=f=60,lowpass=f=18000,aresample=48000:resampler=soxr:precision=28,aformat=sample_fmts=s16:channel_layouts=mono,volume=0.65,loudnorm=I=-16:TP=-1.5:LRA=11",
      "-acodec", "pcm_s16le", "-ac", "1", "-ar", "48000", "-f", "s16le", "pipe:1"
    ];

    const ffmpegProc = spawn("ffmpeg", ffmpegArgs, { stdio: ["ignore", "pipe", "pipe"] });
    let buffer = Buffer.alloc(0);
    let seq = 0;
    let started = false;
    let finished = false;
    let chunkCount = 0;
    let prebufferChunks: Buffer[] = [];

    ffmpegProc.stderr.on("data", (data) => {
      const line = data.toString().trim();
      if (line.includes("Error") || line.includes("error")) console.log("[FFMPEG]", line.substring(0, 100));
    });

    ffmpegProc.on("error", (err) => {
      console.error("[MUSIC] FFmpeg error:", err.message);
      if (!finished) { finished = true; try { ws.send("ERROR:MUSIC_FAILED"); } catch {}; reject(err); }
    });

    ffmpegProc.on("close", (code) => {
      console.log("[MUSIC] FFmpeg exited:", code);
      if (!finished) {
        finished = true;
        try { ws.send("FINISH_MUSIC:" + sessionId); } catch {}
        resolve();
      }
    });

    ffmpegProc.stdout.on("data", async (data: Buffer) => {
      buffer = Buffer.concat([buffer, data]);

      if (!started) {
        while (buffer.length >= MUSIC_CHUNK_SIZE_MONO && prebufferChunks.length < PREBUFFER_CHUNKS_MUSIC) {
          prebufferChunks.push(Buffer.from(buffer.subarray(0, MUSIC_CHUNK_SIZE_MONO)));
          buffer = buffer.subarray(MUSIC_CHUNK_SIZE_MONO);
        }
        if (prebufferChunks.length >= PREBUFFER_CHUNKS_MUSIC) {
          started = true;
          ws.send("SESSION:" + sessionId);
          await delay(100);
          ws.send("PREPARE_MUSIC:0");
          await delay(300);
          for (const chunk of prebufferChunks) {
            if (ws.readyState !== ws.OPEN) { finished = true; ffmpegProc.kill("SIGKILL"); return; }
            const packet = Buffer.allocUnsafe(2 + chunk.length);
            packet.writeUInt16BE(seq & 0xFFFF, 0);
            packet.set(chunk, 2);
            ws.send(packet, { binary: true });
            seq++; chunkCount++;
            await delay(SEND_INTERVAL_MS_MUSIC);
          }
          ws.send("START_MUSIC");
        }
        return;
      }

      while (buffer.length >= MUSIC_CHUNK_SIZE_MONO && !finished) {
        if (ws.readyState !== ws.OPEN) { finished = true; ffmpegProc.kill("SIGKILL"); return; }
        const chunk = buffer.subarray(0, MUSIC_CHUNK_SIZE_MONO);
        buffer = buffer.subarray(MUSIC_CHUNK_SIZE_MONO);
        const packet = Buffer.allocUnsafe(2 + chunk.length);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        packet.set(chunk, 2);
        ws.send(packet, { binary: true });
        seq++; chunkCount++;
        await delay(SEND_INTERVAL_MS_MUSIC);
      }
    });

    setTimeout(() => {
      if (!finished) { finished = true; ffmpegProc.kill("SIGKILL"); try { ws.send("FINISH_MUSIC:" + sessionId); } catch {}; resolve(); }
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
    userId, ws,
    audioBuffer: Buffer.alloc(0),
    isRecording: false,
    sessionId: Date.now() + "_" + Math.random().toString(36).slice(2, 8),
  };
}

async function processFinalSTT(session: StreamingSTTSession): Promise<string | null> {
  if (session.audioBuffer.length < 1600) {
    console.log("[STT] Audio too short");
    return null;
  }

  const tmpWav = path.join(UPLOAD_DIR, session.sessionId + "_stt.wav");
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

    const transcription = await Promise.race([
      sttClient.audio.transcriptions.create({
        file: fs.createReadStream(tmpWav),
        model: "whisper-large-v3",
        temperature: 0,
        response_format: "verbose_json",
      }),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("STT_TIMEOUT")), 15000))
    ]);

    const text = transcription.text?.trim();
    try { fs.unlinkSync(tmpWav); } catch {}

    if (!text) { console.log("[STT] No speech"); return null; }
    console.log("[STT] RESULT:", text);
    return text;
  } catch (e: any) {
    console.error("[STT] Error:", e.message);
    try { fs.unlinkSync(tmpWav); } catch {}
    return null;
  }
}

// ============================================================================
// STREAMING TTS QUEUE — Process text chunks as they arrive from LLM
// ============================================================================
interface TTSQueueItem {
  text: string;
  isLast: boolean;
}

class StreamingTTSProcessor {
  private ws: WebSocket;
  private sessionId: string;
  private textBuffer: string = "";
  private queue: TTSQueueItem[] = [];
  private isProcessing: boolean = false;
  private hasStarted: boolean = false;
  private totalChunksSent: number = 0;
  private audioChunks: Buffer[] = [];
  private finishSent: boolean = false;

  constructor(ws: WebSocket, sessionId: string) {
    this.ws = ws;
    this.sessionId = sessionId;
  }

  // Add text from LLM stream
  addText(text: string, isLast: boolean = false) {
    this.textBuffer += text;

    // Try to extract complete sentences
    while (this.textBuffer.length >= MIN_TTS_CHARS) {
      // Find sentence boundary
      let splitIndex = -1;

      // Look for sentence end within MAX_TTS_CHARS
      const searchLimit = Math.min(this.textBuffer.length, MAX_TTS_CHARS);
      for (let i = MIN_TTS_CHARS; i < searchLimit; i++) {
        if (SENTENCE_END_CHARS.test(this.textBuffer[i])) {
          splitIndex = i + 1;
          break;
        }
      }

      // If no sentence end found but buffer is getting long, force split at last space
      if (splitIndex === -1 && this.textBuffer.length >= MAX_TTS_CHARS) {
        const lastSpace = this.textBuffer.lastIndexOf(" ", MAX_TTS_CHARS);
        if (lastSpace > MIN_TTS_CHARS) {
          splitIndex = lastSpace + 1;
        } else {
          splitIndex = MAX_TTS_CHARS;
        }
      }

      // If last chunk and no more text coming, send whatever we have
      if (isLast && splitIndex === -1 && this.textBuffer.length > 0) {
        splitIndex = this.textBuffer.length;
      }

      if (splitIndex > 0) {
        const sentence = this.textBuffer.substring(0, splitIndex).trim();
        this.textBuffer = this.textBuffer.substring(splitIndex).trim();
        if (sentence.length > 0) {
          this.queue.push({ text: sentence, isLast: false });
        }
      } else {
        break;
      }
    }

    // If last chunk and still have buffer, send it
    if (isLast && this.textBuffer.length > 0) {
      this.queue.push({ text: this.textBuffer.trim(), isLast: true });
      this.textBuffer = "";
    }

    // Start processing if not already
    if (!this.isProcessing) {
      this.processQueue();
    }
  }

  private async processQueue() {
    if (this.isProcessing || this.queue.length === 0) return;
    this.isProcessing = true;

    while (this.queue.length > 0) {
      const item = this.queue.shift()!;

      if (this.ws.readyState !== this.ws.OPEN) {
        this.isProcessing = false;
        return;
      }

      try {
        // Skip if it's a JSON/music command
        if (item.text.includes('"music"') || item.text.includes('"text"')) {
          continue;
        }

        console.log("[STREAM_TTS] Processing:", item.text.substring(0, 50));

        // Generate TTS for this sentence
        const ttsPath = path.join(AUDIO_DIR, `stream_${this.sessionId}_${Date.now()}.mp3`);
        await generateEdgeTTS(item.text, ttsPath);

        // Convert to PCM
        const pcm = await generatePCM(ttsPath);

        // Clean up
        try { fs.unlinkSync(ttsPath); } catch {}

        // Send START_RESPONSE on first chunk
        if (!this.hasStarted) {
          this.ws.send("START_RESPONSE");
          this.hasStarted = true;
          console.log("[STREAM] Started playback");
          await delay(100);
        }

        // Stream PCM to ESP32
        await streamPCM(this.ws, pcm, this.sessionId);
        this.totalChunksSent += Math.floor(pcm.length / AI_CHUNK_SIZE_MONO);

      } catch (e: any) {
        console.error("[STREAM_TTS] Error:", e.message);
      }
    }

    // Send finish if last item was processed
    if (this.textBuffer.length === 0 && !this.finishSent) {
      this.finishSent = true;
      await delay(300);
      for (let retry = 0; retry < 3; retry++) {
        if (this.ws.readyState === this.ws.OPEN) {
          this.ws.send("FINISH_RESPONSE:" + this.sessionId);
          await delay(200);
        }
      }
      console.log("[STREAM] Finished, total chunks:", this.totalChunksSent);
    }

    this.isProcessing = false;
  }

  async waitForComplete(): Promise<void> {
    while (this.isProcessing || this.queue.length > 0 || this.textBuffer.length > 0) {
      await delay(100);
    }
    // Ensure finish is sent
    if (!this.finishSent && this.ws.readyState === this.ws.OPEN) {
      this.finishSent = true;
      this.ws.send("FINISH_RESPONSE:" + this.sessionId);
    }
  }
}

// ============================================================================
// PROCESS AI RESPONSE — STREAMING LLM + STREAMING TTS
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

    const systemPrompt = `You are Mochi, a helpful Filipino voice assistant.
${savedName ? `The user's name is ${savedName}. Address them by name.` : ""}
Keep responses natural, concise, and conversational — max 2-3 sentences for voice.
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

    // FIXED: Streaming LLM with llama-4-scout
    const stream = await llmClient.chat.completions.create({
      model: "meta-llama/llama-4-scout-17b-16e-instruct",
      messages,
      temperature: 1,
      max_completion_tokens: 1024,
      top_p: 1,
      stream: true,
      stop: null
    });

    let fullResponse = "";
    let isMusicRequest = false;
    let musicQuery: string | null = null;
    let jsonBuffer = "";
    let inJson = false;

    // Create streaming TTS processor
    const ttsProcessor = new StreamingTTSProcessor(ws, sessionId);

    console.log("[LLM] Streaming started...");

    for await (const chunk of stream) {
      const content = chunk.choices[0]?.delta?.content || "";
      if (!content) continue;

      fullResponse += content;

      // Check if this is a JSON response (music command)
      if (fullResponse.trim().startsWith("{") && !inJson) {
        inJson = true;
        jsonBuffer = fullResponse;
      }

      if (inJson) {
        jsonBuffer += content;
        // Try to parse complete JSON
        if (jsonBuffer.includes("}")) {
          try {
            const parsed = JSON.parse(jsonBuffer);
            if (parsed.music) {
              isMusicRequest = true;
              musicQuery = parsed.music;
              fullResponse = parsed.text || fullResponse;
            }
            break; // Stop streaming, we got the JSON
          } catch {
            // JSON not complete yet, continue
          }
        }
        continue;
      }

      // Stream text to TTS processor
      ttsProcessor.addText(content, false);
    }

    // Signal end of stream
    if (!inJson) {
      ttsProcessor.addText("", true);
      await ttsProcessor.waitForComplete();
    }

    // Handle name response override
    if (nameResponse) {
      fullResponse = nameResponse;
      isMusicRequest = false;
      musicQuery = null;
    }

    console.log("[LLM] Full response:", fullResponse.substring(0, 100));
    console.log("[LLM] Music:", musicQuery || "none");

    await storage.addMessage(userId, "user", userText);
    await storage.addMessage(userId, "assistant", fullResponse);

    // Handle music request
    if (isMusicRequest && musicQuery) {
      const musicUrl = await fetchMusicUrl(musicQuery);
      if (!musicUrl) { 
        ws.send("ERROR:MUSIC_NOT_FOUND"); 
        ws.send("STATE:IDLE");
        return; 
      }

      // Play intro text if any
      if (fullResponse && !nameResponse) {
        const introId = sessionId + "_intro";
        const introMp3 = path.join(AUDIO_DIR, introId + ".mp3");
        await generateEdgeTTS(fullResponse, introMp3);
        filesToCleanup.push(introMp3);
        const introPcm = await generatePCM(introMp3);

        ws.send("START_RESPONSE");
        await delay(100);
        await streamPCM(ws, introPcm, introId);
        await delay(500);
        ws.send("FINISH_RESPONSE:" + introId);
        await delay(1000);
      }

      await streamMusicRealtime(ws, musicUrl, sessionId + "_music");
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
    console.log("ESP connected - V34 STREAMING LLM + STREAMING TTS");
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

      if (!sttSession || !sttSession.isRecording) return;

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
      if (sttSession) sttSession = null;
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
