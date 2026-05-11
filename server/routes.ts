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
const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 512;
const SEND_INTERVAL_MS = 16;

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
    /delete\s+(?:my\s+)?name/i,
    /remove\s+(?:my\s+)?name/i,
    /forget\s+(?:my\s+)?name/i,
    /clear\s+(?:my\s+)?name/i,
    /wala\s+na\s+ang\s+pangalan\s+ko/i,
    /burahin\s+(?:ang\s+)?pangalan\s+ko/i,
    /alisin\s+(?:ang\s+)?pangalan\s+ko/i,
  ];
  
  for (const pattern of deletePatterns) {
    if (pattern.test(lower)) {
      return { action: "delete", name: null };
    }
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
// PCM GENERATION (from local file)
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
// PCM GENERATION FROM STREAM (for music — no temp MP3 file)
// ============================================================================
async function generatePCMFromStream(inputUrl: string, outputPcmPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg(inputUrl)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=8000",
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "aformat=sample_fmts=s16:channel_layouts=mono",
        "volume=0.65",
        "dynaudnorm=p=0.95:g=15"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => resolve())
      .save(outputPcmPath);
  });
}

// ============================================================================
// FETCH MUSIC URL FROM YT-DLP API
// ============================================================================
async function fetchMusicUrl(query: string): Promise<string | null> {
  try {
    const url = `https://yt-dlp-stream.onrender.com/api/v2/q?=${encodeURIComponent(query)}`;
    console.log("[MUSIC] Searching:", query);
    const response = await fetch(url);
    if (!response.ok) {
      console.log("[MUSIC] API error status:", response.status);
      return null;
    }
    const data = await response.json();
    const mp3Url = data?.media?.mp3;
    if (!mp3Url || typeof mp3Url !== "string") {
      console.log("[MUSIC] No MP3 URL in response");
      return null;
    }
    return mp3Url.trim();
  } catch (e: any) {
    console.error("[MUSIC] Fetch error:", e.message);
    return null;
  }
}

// ============================================================================
// DOWNLOAD MP3 TO TEMP FILE (bypass anti-hotlinking)
// ============================================================================
async function downloadMp3(mp3Url: string, outputPath: string): Promise<void> {
  console.log("[DOWNLOAD] Starting download to:", outputPath);
  
  const response = await fetch(mp3Url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "*/*",
      "Accept-Encoding": "identity",
      "Referer": "https://yt-dlp-stream.onrender.com/",
    },
    redirect: "follow",
  });
  
  if (!response.ok) {
    throw new Error(`Download failed: ${response.status} ${response.statusText}`);
  }
  
  const buffer = Buffer.from(await response.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
  console.log("[DOWNLOAD] Saved", buffer.length, "bytes");
}

// ============================================================================
// STREAM AI RESPONSE PCM
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer, sessionId: string) {
  if (ws.readyState !== ws.OPEN) {
    console.log("[STREAM] WebSocket not open, aborting");
    return;
  }
  
  const alignedLen = Math.floor(pcm.length / CHUNK_SIZE) * CHUNK_SIZE;
  const totalChunks = alignedLen / CHUNK_SIZE;
  
  console.log("[STREAM] Starting session:", sessionId, "chunks:", totalChunks);
  
  ws.send("SESSION:" + sessionId);
  await new Promise(r => setTimeout(r, 100));
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 300));
  ws.send("START_RESPONSE");
  await new Promise(r => setTimeout(r, 100));
  
  let seq = 0;
  
  try {
    for (let i = 0; i < alignedLen; i += CHUNK_SIZE) {
      if (ws.readyState !== ws.OPEN) {
        console.log("[STREAM] WebSocket closed mid-stream");
        return;
      }
      
      const chunk = pcm.subarray(i, i + CHUNK_SIZE);
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
        console.log("[STREAM] Sent FINISH_RESPONSE attempt", retry + 1);
        await new Promise(r => setTimeout(r, 200));
      }
    }
    
    console.log("[STREAM] Completed session:", sessionId, "sent", seq, "chunks");
    
  } catch (e: any) {
    console.error("[STREAM] Error:", e.message);
    try {
      ws.send("FINISH_RESPONSE:ERROR");
    } catch {}
  }
}

// ============================================================================
// STREAM MUSIC PCM (download first, then convert)
// ============================================================================
async function streamMusic(ws: WebSocket, mp3Url: string, sessionId: string) {
  if (ws.readyState !== ws.OPEN) {
    console.log("[MUSIC] WebSocket not open");
    return;
  }
  
  const tmpMp3 = path.join(AUDIO_DIR, "tmp_music_" + sessionId + ".mp3");
  const pcmPath = path.join(AUDIO_DIR, "music_" + sessionId + ".pcm");
  
  try {
    // STEP 1: Download MP3 with browser-like headers
    await downloadMp3(mp3Url, tmpMp3);
    
    // STEP 2: Convert downloaded MP3 to PCM
    console.log("[MUSIC] Converting MP3 -> PCM...");
    await new Promise<void>((resolve, reject) => {
      ffmpeg(tmpMp3)
        .audioFilters([
          "highpass=f=80",
          "lowpass=f=8000",
          "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
          "aformat=sample_fmts=s16:channel_layouts=mono",
          "volume=0.65",
          "dynaudnorm=p=0.95:g=15"
        ])
        .audioCodec("pcm_s16le")
        .audioChannels(1)
        .audioFrequency(SAMPLE_RATE)
        .format("s16le")
        .on("error", (err) => {
          console.error("[MUSIC] FFmpeg error:", err.message);
          reject(err);
        })
        .on("end", () => {
          console.log("[MUSIC] Conversion done");
          resolve();
        })
        .save(pcmPath);
    });
    
    // STEP 3: Delete temp MP3 immediately
    try { fs.unlinkSync(tmpMp3); } catch {}
    
    if (!fs.existsSync(pcmPath)) {
      throw new Error("PCM conversion failed");
    }
    
    const pcm = fs.readFileSync(pcmPath);
    try { fs.unlinkSync(pcmPath); } catch {}  // Delete PCM after reading
    
    console.log("[MUSIC] PCM:", pcm.length, "bytes");
    
    if (ws.readyState !== ws.OPEN) {
      console.log("[MUSIC] WebSocket closed after conversion");
      return;
    }
    
    // STEP 4: Stream PCM to ESP32
    const alignedLen = Math.floor(pcm.length / CHUNK_SIZE) * CHUNK_SIZE;
    const totalChunks = alignedLen / CHUNK_SIZE;
    
    console.log("[MUSIC] Streaming", totalChunks, "chunks");
    
    ws.send("SESSION:" + sessionId);
    await new Promise(r => setTimeout(r, 100));
    ws.send("PREPARE_MUSIC:" + totalChunks);
    await new Promise(r => setTimeout(r, 300));
    ws.send("START_MUSIC");
    await new Promise(r => setTimeout(r, 100));
    
    let seq = 0;
    
    for (let i = 0; i < alignedLen; i += CHUNK_SIZE) {
      if (ws.readyState !== ws.OPEN) {
        console.log("[MUSIC] WebSocket closed mid-stream");
        return;
      }
      
      const chunk = pcm.subarray(i, i + CHUNK_SIZE);
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
        ws.send("FINISH_MUSIC:" + sessionId);
        console.log("[MUSIC] Sent FINISH_MUSIC attempt", retry + 1);
        await new Promise(r => setTimeout(r, 200));
      }
    }
    
    console.log("[MUSIC] Completed session:", sessionId, "sent", seq, "chunks");
    
  } catch (e: any) {
    console.error("[MUSIC] Error:", e.message);
    // Cleanup on error
    try { fs.unlinkSync(tmpMp3); } catch {}
    try { fs.unlinkSync(pcmPath); } catch {}
    try {
      ws.send("ERROR:MUSIC_FAILED");
      ws.send("FINISH_MUSIC:ERROR");
    } catch {}
  }
}

// ============================================================================
// PROCESS WITH MEMORY, DEDUPLICATION & MUSIC
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer, userId: number) {
  if (isDuplicate(userId)) {
    ws.send("ERROR:PROCESSING_BUSY");
    return;
  }

  let processingComplete = false;
  const uniqueId = Date.now() + "_" + Math.random().toString(36).slice(2, 8);
  const wavPath = path.join(UPLOAD_DIR, uniqueId + ".wav");
  const filesToCleanup: string[] = [];

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

    fs.writeFileSync(wavPath, audioBuffer);
    filesToCleanup.push(wavPath);
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

    // Build system prompt with music capability
    const systemPrompt = `You are a helpful voice assistant. Keep responses short and natural.
${savedName ? `The user's name is ${savedName}. Address them by name.` : ""}
If the user wants to play music or a song, return JSON with "music" field:
{"text":"short acknowledgment","music":"song search query"}
Examples:
- "Play Tibok" -> {"text":"Playing Tibok by Earl Agustin","music":"Tibok by Earl Agustin"}
- "Tumugtog ka ng music" -> {"text":"Anong kanta gusto mo?","music":null}
- "Play rock music" -> {"text":"Playing rock music","music":"best rock songs"}
Otherwise return: {"text":"your response"}
Return ONLY the JSON.`;

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
    let musicQuery: string | null = null;

    try {
      const parsed = JSON.parse(raw);
      text = parsed.text || text;
      musicQuery = parsed.music || null;
    } catch {
      text = raw.replace(/[{}"]/g, "").replace(/text:/g, "").trim();
    }

    if (nameResponse) {
      text = nameResponse;
      musicQuery = null;
    }

    console.log("AI:", text, musicQuery ? `(Music: ${musicQuery})` : "");

    // Save to DB (FIFO, max 10)
    await storage.addMessage(userId, "user", userText);
    await storage.addMessage(userId, "assistant", text);

    // ==================== MUSIC FLOW ====================
    if (musicQuery) {
      const mp3Url = await fetchMusicUrl(musicQuery);
      if (!mp3Url) {
        ws.send("ERROR:MUSIC_NOT_FOUND");
        return;
      }

      // Optional: TTS intro before music
      if (text && !nameResponse) {
        const introId = uniqueId + "_intro";
        const tts = new EdgeTTS();
        const introMp3 = path.join(AUDIO_DIR, introId + ".mp3");
        await tts.ttsPromise(text, introMp3, {
          voice: "en-US-AriaNeural",
          rate: "+15%"
        });
        filesToCleanup.push(introMp3);
        const introPcm = await generatePCM(introMp3);
        await streamPCM(ws, introPcm, introId);
        
        await new Promise(r => setTimeout(r, 1000));
      }

      // Stream the actual music
      await streamMusic(ws, mp3Url, uniqueId + "_music");
    }
    // ==================== NORMAL TTS FLOW ====================
    else {
      const tts = new EdgeTTS();
      const mp3 = path.join(AUDIO_DIR, uniqueId + ".mp3");
      await tts.ttsPromise(text, mp3, {
        voice: "en-US-AriaNeural",
        rate: "+15%"
      });
      filesToCleanup.push(mp3);
      const pcm = await generatePCM(mp3);
      await streamPCM(ws, pcm, uniqueId);
    }

    processingComplete = true;

  } catch (e: any) {
    console.error("[PROCESS] Error:", e.message);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
    try {
      ws.send("FINISH_RESPONSE:ERROR");
    } catch {}
  } finally {
    for (const f of filesToCleanup) {
      try { fs.unlinkSync(f); } catch {}
    }
    if (processingComplete) {
      setTimeout(() => {
        RECENT_REQUESTS.delete(userId);
      }, DEBOUNCE_MS);
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
    if (!isValid) {
      throw new Error("Schema verification failed");
    }
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
    console.log("ESP connected - Voice AI with Music");

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
