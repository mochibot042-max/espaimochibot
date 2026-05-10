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
// PCM GENERATION
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + ".pcm");
  
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
// STREAM PCM
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  
  const alignedLen = Math.floor(pcm.length / CHUNK_SIZE) * CHUNK_SIZE;
  const totalChunks = alignedLen / CHUNK_SIZE;
  
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 200));
  ws.send("START_RESPONSE");
  
  let seq = 0;
  
  for (let i = 0; i < alignedLen; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    
    const chunk = pcm.subarray(i, i + CHUNK_SIZE);
    const packet = Buffer.allocUnsafe(2 + chunk.length);
    packet.writeUInt16BE(seq & 0xFFFF, 0);
    packet.set(chunk, 2);
    
    try {
      ws.send(packet, { binary: true });
    } catch (e) {
      console.error("[STREAM] Send failed:", e);
      return;
    }
    
    seq++;
    await new Promise(r => setTimeout(r, SEND_INTERVAL_MS));
  }
  
  await new Promise(r => setTimeout(r, 200));
  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks");
}

// ============================================================================
// PROCESS WITH MEMORY
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer, userId: number) {
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

    const id = Date.now();
    const wavPath = path.join(UPLOAD_DIR, id + ".wav");
    fs.writeFileSync(wavPath, audioBuffer);
    console.log("[UPLOAD] Saved:", audioBuffer.length, "bytes");

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

    // Build messages
    const systemPrompt = `You are a helpful voice assistant. Keep responses short and natural. 
${savedName ? `The user's name is ${savedName}. Address them by name.` : ""}
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

    if (nameResponse) {
      text = nameResponse;
    }

    console.log("AI:", text);

    // Save to DB (FIFO, max 10)
    await storage.addMessage(userId, "user", userText);
    await storage.addMessage(userId, "assistant", text);

    // TTS
    const tts = new EdgeTTS();
    const mp3 = path.join(AUDIO_DIR, id + ".mp3");
    await tts.ttsPromise(text, mp3, {
      voice: "en-US-AriaNeural",
      rate: "+15%"
    });

    const pcm = await generatePCM(mp3);
    console.log("[TTS] PCM:", pcm.length, "bytes");

    await streamPCM(ws, pcm);

    // Cleanup
    try {
      fs.unlinkSync(mp3);
      fs.unlinkSync(wavPath);
    } catch (e) {}

  } catch (e: any) {
    console.error("[PROCESS] Error:", e.message);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
  }
}

// ============================================================================
// ROUTES
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  // AUTO PUSH SCHEMA ON STARTUP
  try {
    await pushSchema();
    
    // Verify schema was created
    const isValid = await verifySchema();
    if (!isValid) {
      throw new Error("Schema verification failed");
    }
    
    console.log("[SERVER] Database ready");
  } catch (e: any) {
    console.error("[SERVER] Database init failed:", e.message);
    // Continue running - baka may existing tables na
  }

  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 512 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - Voice AI with Memory");

    let processing = false;
    let currentUserId: number | null = null;

    ws.on("message", async (data: any, isBinary: boolean) => {
      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT:", msg);
        
        if (msg === "READY") {
          console.log("[WS] ESP ready");
        }
        
        // User identification
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

      if (processing) {
        console.log("[UPLOAD] Busy");
        return;
      }

      // Auto-create anonymous user if none
      if (!currentUserId) {
        try {
          const anon = await storage.getOrCreateUser("anon_" + Date.now());
          currentUserId = anon.id;
        } catch (e: any) {
          console.error("[USER] Anon error:", e.message);
          ws.send("ERROR:DB_FAILED");
          return;
        }
      }

      const audioBuffer = Buffer.from(data);
      console.log("[UPLOAD] WAV:", audioBuffer.length, "bytes");

      processing = true;
      processAndRespond(ws, audioBuffer, currentUserId).finally(() => {
        processing = false;
      });
    });

    ws.on("close", () => {
      console.log("ESP disconnected");
      processing = false;
      currentUserId = null;
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
