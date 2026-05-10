import type { Express } from "express";
import { Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import Groq from "groq-sdk";
import fs from "fs";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";
import { storage, pushSchema, verifySchema } from "./storage.js";
import axios from "axios";

const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ============================================================================
// AUDIO CONFIG — OPTIMIZED FOR MUSIC
// ============================================================================
const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 512;           // 32ms per chunk @ 16kHz
const CHUNK_DURATION_MS = 32;     // Actual duration of each chunk
const TARGET_BUFFER_MS = 500;     // Target 500ms buffer on client
const PREBUFFER_CHUNKS = 16;      // Send 16 chunks before saying START

// ============================================================================
// YOUTUBE API
// ============================================================================
const YT_SEARCH_API = "https://mostakim.onrender.com/mostakim/ytSearch?search=";
const YT_DOWNLOAD_API = "https://mostakim.onrender.com/m/sing?url=";

// ============================================================================
// DEBOUNCE
// ============================================================================
const RECENT_REQUESTS = new Map<number, number>();
const DEBOUNCE_MS = 3000;
const ACTIVE_STREAMS = new Map<string, boolean>();

function isDuplicate(userId: number): boolean {
  const now = Date.now();
  const last = RECENT_REQUESTS.get(userId);
  if (last && (now - last) < DEBOUNCE_MS) {
    console.log("[DEBOUNCE] Duplicate blocked for user:", userId);
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
    /delete\s+(?:my\s+)?name/i,
    /remove\s+(?:my\s+)?name/i,
    /forget\s+(?:my\s+)?name/i,
    /clear\s+(?:my\s+)?name/i,
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
      if (name.length > 1) {
        return { action: "save", name: name.charAt(0).toUpperCase() + name.slice(1) };
      }
    }
  }
  return { action: "none", name: null };
}

// ============================================================================
// MUSIC COMMAND DETECTION
// ============================================================================
function extractMusicCommand(text: string): { action: "play" | "stop" | "none"; query: string | null } {
  const lower = text.toLowerCase();
  
  const stopPatterns = [
    /stop\s+(?:the\s+)?music/i,
    /stop\s+(?:the\s+)?song/i,
    /pause/i,
    /tigil/i,
    /stop\s+na/i,
  ];
  for (const pattern of stopPatterns) {
    if (pattern.test(lower)) return { action: "stop", query: null };
  }
  
  const playPatterns = [
    /play\s+(?:me\s+)?(?:a\s+)?(?:song\s+)?(?:called\s+)?(.+)/i,
    /play\s+(?:the\s+)?song\s+(.+)/i,
    /play\s+music\s+(.+)/i,
    /play\s+(.+)/i,
    /patugtog\s+ng\s+(.+)/i,
    /patugtog\s+(.+)/i,
    /tugtugin\s+mo\s+ang\s+(.+)/i,
  ];
  for (const pattern of playPatterns) {
    const match = lower.match(pattern);
    if (match && match[1]) {
      return { action: "play", query: match[1].trim() };
    }
  }
  
  return { action: "none", query: null };
}

// ============================================================================
// PCM GENERATION (For TTS)
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8) + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=8000",
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "aformat=sample_fmts=s16:channel_layouts=mono",
        "volume=0.85",
        "dynaudnorm=p=0.95:g=15"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(SAMPLE_RATE)
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
// TTS STREAM (Tuloy-tuloy na streaming)
// ============================================================================
async function streamTTS(ws: WebSocket, pcm: Buffer, sessionId: string) {
  if (ws.readyState !== ws.OPEN) return;
  
  const alignedLen = Math.floor(pcm.length / CHUNK_SIZE) * CHUNK_SIZE;
  const totalChunks = alignedLen / CHUNK_SIZE;
  
  ws.send("SESSION:" + sessionId);
  await new Promise(r => setTimeout(r, 100));
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 300));
  ws.send("START_RESPONSE");
  await new Promise(r => setTimeout(r, 100));
  
  let seq = 0;
  
  for (let i = 0; i < alignedLen; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    
    const chunk = pcm.subarray(i, i + CHUNK_SIZE);
    const packet = Buffer.allocUnsafe(2 + chunk.length);
    packet.writeUInt16BE(seq & 0xFFFF, 0);
    packet.set(chunk, 2);
    
    ws.send(packet, { binary: true });
    seq++;
    await new Promise(r => setTimeout(r, CHUNK_DURATION_MS));
  }
  
  await new Promise(r => setTimeout(r, 500));
  
  for (let retry = 0; retry < 3; retry++) {
    if (ws.readyState === ws.OPEN) {
      ws.send("FINISH_RESPONSE:" + sessionId);
      await new Promise(r => setTimeout(r, 200));
    }
  }
}

// ============================================================================
// MUSIC STREAM — PRE-BUFFERED FOR SMOOTH PLAYBACK
// ============================================================================
async function streamYouTubeMusic(ws: WebSocket, query: string, sessionId: string) {
  const streamKey = ws.url || "unknown";
  ACTIVE_STREAMS.set(streamKey, true);
  
  try {
    // Step 1: Search
    console.log("[MUSIC] Searching for:", query);
    ws.send("MUSIC_STATUS:Searching...");
    
    const searchUrl = YT_SEARCH_API + encodeURIComponent(query);
    const searchRes = await axios.get(searchUrl, { timeout: 10000 });
    
    if (!searchRes.data || !Array.isArray(searchRes.data) || searchRes.data.length === 0) {
      ws.send("ERROR:NO_SONG_FOUND");
      ACTIVE_STREAMS.delete(streamKey);
      return;
    }
    
    const song = searchRes.data[0];
    console.log("[MUSIC] Found:", song.title);
    ws.send("MUSIC_STATUS:Found: " + song.title);
    
    // Step 2: Get download URL
    const downloadUrl = YT_DOWNLOAD_API + encodeURIComponent(song.url);
    console.log("[MUSIC] Getting stream URL...");
    
    const downloadRes = await axios.get(downloadUrl, { timeout: 15000 });
    
    if (!downloadRes.data || !downloadRes.data.url) {
      ws.send("ERROR:DOWNLOAD_FAILED");
      ACTIVE_STREAMS.delete(streamKey);
      return;
    }
    
    const mp3Url = downloadRes.data.url.trim();
    console.log("[MUSIC] Streaming:", song.title);
    ws.send("MUSIC_STATUS:Buffering...");
    
    // Step 3: Stream with pre-buffering
    await streamMusicWithPrebuffer(ws, mp3Url, sessionId, streamKey, song.title);
    
  } catch (e: any) {
    console.error("[MUSIC] Error:", e.message);
    ws.send("ERROR:MUSIC_FAILED");
  } finally {
    ACTIVE_STREAMS.delete(streamKey);
  }
}

// ============================================================================
// PRE-BUFFERED MUSIC STREAMING
// ============================================================================
async function streamMusicWithPrebuffer(
  ws: WebSocket, 
  url: string, 
  sessionId: string, 
  streamKey: string,
  songTitle: string
) {
  return new Promise<void>((resolve, reject) => {
    if (ws.readyState !== ws.OPEN) {
      resolve();
      return;
    }
    
    let prebuffer: Buffer[] = [];
    let prebufferComplete = false;
    let seq = 0;
    let streamEnded = false;
    let startTime = Date.now();
    
    // Send session
    ws.send("SESSION:" + sessionId);
    
    // Start ffmpeg
    const ffmpegProcess = ffmpeg(url)
      .inputOptions([
        '-re',
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        '-thread_queue_size', '512'
      ])
      .audioFilters([
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "aformat=sample_fmts=s16:channel_layouts=mono",
        "volume=0.9",
        "dynaudnorm=p=0.95:g=15"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(SAMPLE_RATE)
      .format("s16le")
      .on('error', (err) => {
        console.error("[MUSIC] FFmpeg error:", err.message);
        streamEnded = true;
        if (ws.readyState === ws.OPEN) {
          ws.send("FINISH_MUSIC");
        }
        resolve();
      })
      .on('end', () => {
        console.log("[MUSIC] FFmpeg finished");
        streamEnded = true;
      });
    
    const stream = ffmpegProcess.pipe();
    let buffer = Buffer.alloc(0);
    
    // Collect prebuffer first
    stream.on('data', (chunk: Buffer) => {
      if (!ACTIVE_STREAMS.get(streamKey)) {
        ffmpegProcess.kill('SIGKILL');
        stream.destroy();
        resolve();
        return;
      }
      
      buffer = Buffer.concat([buffer, chunk]);
      
      // Collect chunks into prebuffer
      while (buffer.length >= CHUNK_SIZE && prebuffer.length < PREBUFFER_CHUNKS) {
        const chunkData = buffer.subarray(0, CHUNK_SIZE);
        prebuffer.push(Buffer.from(chunkData));
        buffer = buffer.subarray(CHUNK_SIZE);
      }
      
      // Once prebuffer is full, start sending
      if (prebuffer.length >= PREBUFFER_CHUNKS && !prebufferComplete) {
        prebufferComplete = true;
        console.log("[MUSIC] Prebuffer ready:", prebuffer.length, "chunks");
        ws.send("PREPARE_RESPONSE:" + 0); // Unknown total for music
        ws.send("START_MUSIC");
        ws.send("MUSIC_STATUS:Playing: " + songTitle);
        
        // Start draining prebuffer
        drainPrebuffer();
      }
    });
    
    // Drain prebuffer then continue with live stream
    async function drainPrebuffer() {
      // Send all prebuffered chunks
      for (const chunk of prebuffer) {
        if (!ACTIVE_STREAMS.get(streamKey) || ws.readyState !== ws.OPEN) {
          resolve();
          return;
        }
        
        const packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        packet.set(chunk, 2);
        
        try {
          ws.send(packet, { binary: true });
        } catch (e) {
          resolve();
          return;
        }
        
        seq++;
        
        // Adaptive delay based on buffer health
        const elapsed = Date.now() - startTime;
        const expectedTime = seq * CHUNK_DURATION_MS;
        const delay = expectedTime - elapsed;
        
        if (delay > 0) {
          await new Promise(r => setTimeout(r, delay));
        }
      }
      
      prebuffer = []; // Clear prebuffer
      
      // Continue with live stream
      continueLiveStream();
    }
    
    async function continueLiveStream() {
      // Process remaining buffer
      while (buffer.length >= CHUNK_SIZE) {
        if (!ACTIVE_STREAMS.get(streamKey) || ws.readyState !== ws.OPEN) {
          resolve();
          return;
        }
        
        const chunk = buffer.subarray(0, CHUNK_SIZE);
        buffer = buffer.subarray(CHUNK_SIZE);
        
        const packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
        packet.writeUInt16BE(seq & 0xFFFF, 0);
        packet.set(chunk, 2);
        
        try {
          ws.send(packet, { binary: true });
        } catch (e) {
          resolve();
          return;
        }
        
        seq++;
        
        // Adaptive timing
        const elapsed = Date.now() - startTime;
        const expectedTime = seq * CHUNK_DURATION_MS;
        const delay = expectedTime - elapsed;
        
        if (delay > 0) {
          await new Promise(r => setTimeout(r, delay));
        }
      }
      
      // If stream ended and buffer consumed, finish
      if (streamEnded && buffer.length < CHUNK_SIZE) {
        console.log("[MUSIC] Stream complete, sent", seq, "chunks");
        if (ws.readyState === ws.OPEN) {
          ws.send("FINISH_MUSIC");
        }
        resolve();
        return;
      }
      
      // Wait for more data
      setTimeout(continueLiveStream, 10);
    }
    
    stream.on('end', () => {
      // Handled in data handler
    });
    
    stream.on('error', (err) => {
      console.error("[MUSIC] Stream error:", err.message);
      if (ws.readyState === ws.OPEN) {
        ws.send("FINISH_MUSIC");
      }
      resolve();
    });
  });
}

// ============================================================================
// PROCESS WITH MEMORY & MUSIC
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer, userId: number) {
  if (isDuplicate(userId)) {
    ws.send("ERROR:PROCESSING_BUSY");
    return;
  }

  let processingComplete = false;

  try {
    if (audioBuffer.length < 1000) {
      ws.send("ERROR:AUDIO_TOO_SHORT");
      return;
    }

    const riff = audioBuffer.slice(0, 4).toString();
    const wave = audioBuffer.slice(8, 12).toString();
    if (riff !== "RIFF" || wave !== "WAVE") {
      ws.send("ERROR:INVALID_FORMAT");
      return;
    }

    const uniqueId = Date.now() + "_" + Math.random().toString(36).slice(2, 8);
    const wavPath = path.join(UPLOAD_DIR, uniqueId + ".wav");
    fs.writeFileSync(wavPath, audioBuffer);
    console.log("[UPLOAD] Saved:", audioBuffer.length, "bytes, ID:", uniqueId);

    const stt = await Promise.race([
      sttClient.audio.transcriptions.create({
        file: fs.createReadStream(wavPath),
        model: "whisper-large-v3-turbo",
        language: "en"
      }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("STT_TIMEOUT")), 15000)
      )
    ]);

    const userText = stt.text?.trim();
    if (!userText) {
      ws.send("ERROR:NO_SPEECH");
      return;
    }

    console.log("USER:", userText);

    // Check music commands FIRST
    const musicCmd = extractMusicCommand(userText);
    if (musicCmd.action === "play" && musicCmd.query) {
      console.log("[MUSIC] Command detected:", musicCmd.query);
      fs.unlinkSync(wavPath);
      await streamYouTubeMusic(ws, musicCmd.query, "music_" + uniqueId);
      processingComplete = true;
      return;
    } else if (musicCmd.action === "stop") {
      console.log("[MUSIC] Stop command");
      const streamKey = ws.url || "unknown";
      ACTIVE_STREAMS.set(streamKey, false);
      ws.send("FINISH_MUSIC");
      fs.unlinkSync(wavPath);
      processingComplete = true;
      return;
    }

    // Check name commands
    const nameAction = extractName(userText);
    let nameResponse = "";

    if (nameAction.action === "save" && nameAction.name) {
      await storage.saveName(userId, nameAction.name);
      nameResponse = `Nice to meet you, ${nameAction.name}! I'll remember your name.`;
    } else if (nameAction.action === "delete") {
      await storage.deleteSavedName(userId);
      nameResponse = "I've deleted your name from my memory.";
    }

    // Get conversation history
    const history = await storage.getConversationHistory(userId);
    const savedName = await storage.getSavedName(userId);

    const systemPrompt = `You are a helpful voice assistant. Keep responses short and natural. 
${savedName ? `The user's name is ${savedName}. Address them by name.` : ""}
You can also play music from YouTube when asked. Just respond naturally.
Return JSON: {"text":"your response"}`;

    const messages: any[] = [
      { role: "system", content: systemPrompt },
      ...history.map(h => ({ role: h.role, content: h.content })),
      { role: "user", content: userText }
    ];

    const ai = await Promise.race([
      llmClient.chat.completions.create({
        model: "llama-3.3-70b-versatile",
        messages: messages,
        max_tokens: 150,
        temperature: 0.7
      }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("LLM_TIMEOUT")), 10000)
      )
    ]);

    const raw = ai.choices[0].message.content || "{}";
    let text = "Sorry, I didn't understand.";
    try {
      const parsed = JSON.parse(raw);
      text = parsed.text || text;
    } catch {
      text = raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim();
    }

    if (nameResponse) text = nameResponse;

    console.log("AI:", text);

    await storage.addMessage(userId, "user", userText);
    await storage.addMessage(userId, "assistant", text);

    const tts = new EdgeTTS();
    const mp3 = path.join(AUDIO_DIR, uniqueId + ".mp3");
    await tts.ttsPromise(text, mp3, {
      voice: "en-US-AriaNeural",
      rate: "+15%"
    });

    const pcm = await generatePCM(mp3);
    console.log("[TTS] PCM:", pcm.length, "bytes, ID:", uniqueId);

    await streamTTS(ws, pcm, uniqueId);

    processingComplete = true;

    try {
      fs.unlinkSync(mp3);
      fs.unlinkSync(wavPath);
    } catch (e) {}

  } catch (e: any) {
    console.error("[PROCESS] Error:", e.message);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
    try { ws.send("FINISH_RESPONSE:ERROR"); } catch {}
  } finally {
    if (processingComplete) {
      setTimeout(() => RECENT_REQUESTS.delete(userId), DEBOUNCE_MS);
    }
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
  } catch (e: any) {
    console.error("[SERVER] Database init failed:", e.message);
  }

  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 512 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - Voice AI + Smooth Music");

    let processing = false;
    let currentUserId: number | null = null;
    let messageCount = 0;

    ws.on("message", async (data: any, isBinary: boolean) => {
      messageCount++;
      const currentMsgNum = messageCount;

      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT #" + currentMsgNum + ":", msg);

        if (msg === "READY") {
          console.log("[WS] ESP ready");
          ws.send("STATE:IDLE");
        }

        if (msg.startsWith("USER:")) {
          const userName = msg.replace("USER:", "").trim();
          try {
            const user = await storage.getOrCreateUser(userName);
            currentUserId = user.id;
            console.log("[USER] Identified as:", userName, "ID:", user.id);
            ws.send("USER_CONFIRMED:" + user.name);
          } catch (e: any) {
            console.error("[USER] Error:", e.message);
            ws.send("ERROR:USER_FAILED");
          }
        }

        if (msg === "STOP_MUSIC") {
          const streamKey = ws.url || "unknown";
          ACTIVE_STREAMS.set(streamKey, false);
        }

        return;
      }

      console.log("[WS] BINARY #" + currentMsgNum + ":", Buffer.from(data).length, "bytes");

      if (processing) {
        console.log("[UPLOAD] Busy - rejecting message #" + currentMsgNum);
        ws.send("ERROR:PROCESSING_BUSY");
        return;
      }

      processing = true;

      if (!currentUserId) {
        try {
          const anon = await storage.getOrCreateUser("anon_" + Date.now());
          currentUserId = anon.id;
        } catch (e: any) {
          console.error("[USER] Anon error:", e.message);
          ws.send("ERROR:DB_FAILED");
          processing = false;
          return;
        }
      }

      const audioBuffer = Buffer.from(data);
      console.log("[UPLOAD] WAV #" + currentMsgNum + ":", audioBuffer.length, "bytes");

      try {
        await processAndRespond(ws, audioBuffer, currentUserId);
      } finally {
        processing = false;
        console.log("[UPLOAD] Done processing message #" + currentMsgNum);
      }
    });

    ws.on("close", () => {
      console.log("ESP disconnected");
      const streamKey = ws.url || "unknown";
      ACTIVE_STREAMS.delete(streamKey);
      processing = false;
      currentUserId = null;
      messageCount = 0;
    });

    ws.on("error", (err) => {
      console.error("[WS] Error:", err.message);
    });

    const pingInterval = setInterval(() => {
      if (ws.readyState === ws.OPEN) {
        ws.ping();
      } else {
        clearInterval(pingInterval);
      }
    }, 15000);
  });

  return httpServer;
}
