import type { Express } from "express";
import { Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import Groq from "groq-sdk";
import fs from "fs";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";
import { storage } from "./storage";

// ============================================================================
// PIPER TTS SETUP - WASM BASED, NO SYSTEM INSTALL NEEDED
// ============================================================================
// Piper WASM works in Node.js without any system installation
let piperTTS: any = null;

async function initPiper() {
  try {
    // Dynamic import para hindi mag-crash kung wala pa
    const { Piper } = await import("piper-wasm");
    piperTTS = new Piper();
    
    // Download voices on first run (auto-cached)
    // English voice
    await piperTTS.loadVoice(
      "https://huggingface.co/rhasspy/piper-voices/resolve/v1.0.0/en/en_US/amy/medium/en_US-amy-medium.onnx",
      "https://huggingface.co/rhasspy/piper-voices/resolve/v1.0.0/en/en_US/amy/medium/en_US-amy-medium.onnx.json"
    );
    
    // Tagalog/Filipino - use en for now, or find tl voice
    // Note: Piper has limited Tagalog support, fallback to English
    
    console.log("[PIPER] WASM TTS initialized successfully!");
    return true;
  } catch (e: any) {
    console.error("[PIPER] Failed to initialize:", e.message);
    console.log("[PIPER] Will fallback to EdgeTTS only");
    piperTTS = null;
    return false;
  }
}

const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 512;
const CHUNK_DELAY_MS = 20;

const EDGE_TTS_VOICES: Record<string, string> = {
  "en": "en-US-AriaNeural",
  "tl": "fil-PH-BlessicaNeural",
  "fil": "fil-PH-BlessicaNeural",
  "tagalog": "fil-PH-BlessicaNeural",
  "english": "en-US-AriaNeural"
};

function wavHeader(len: number): Buffer {
  const b = Buffer.alloc(44);
  b.write("RIFF", 0);
  b.writeUInt32LE(36 + len, 4);
  b.write("WAVE", 8);
  b.write("fmt ", 12);
  b.writeUInt32LE(16, 16);
  b.writeUInt16LE(1, 20);
  b.writeUInt16LE(1, 22);
  b.writeUInt32LE(SAMPLE_RATE, 24);
  b.writeUInt32LE(SAMPLE_RATE * 2, 28);
  b.writeUInt16LE(2, 32);
  b.writeUInt16LE(16, 34);
  b.write("data", 36);
  b.writeUInt32LE(len, 40);
  return b;
}

// ============================================================================
// PIPER TTS FUNCTION - WASM BASED
// ============================================================================
async function piperTTSGenerate(text: string, outputPath: string, lang: string): Promise<string | null> {
  try {
    if (!piperTTS) {
      console.log("[PIPER] Not initialized, skipping...");
      return null;
    }
    
    console.log("[PIPER] Generating TTS for:", text.substring(0, 50) + "...");
    
    // Use appropriate voice based on language
    const voiceUrl = lang === "tl" || lang === "fil" || lang === "tagalog"
      ? "https://huggingface.co/rhasspy/piper-voices/resolve/v1.0.0/en/en_US/amy/medium/en_US-amy-medium.onnx"  // Fallback to English for Tagalog
      : "https://huggingface.co/rhasspy/piper-voices/resolve/v1.0.0/en/en_US/amy/medium/en_US-amy-medium.onnx";
    
    // Generate raw PCM using Piper WASM
    const pcmBuffer = await piperTTS.synthesize(text, {
      speakerId: 0,
      lengthScale: 1.0,
      noiseScale: 0.667,
      noiseW: 0.8
    });
    
    // Convert PCM to WAV
    const wavBuffer = Buffer.concat([
      wavHeader(pcmBuffer.length),
      Buffer.from(pcmBuffer)
    ]);
    
    await fs.promises.writeFile(outputPath, wavBuffer);
    
    // Process with ffmpeg for cleanup
    const cleanPath = outputPath.replace(".wav", "_clean.wav");
    await new Promise<void>((res, rej) => {
      ffmpeg(outputPath)
        .audioFilters([
          "highpass=f=120",
          "lowpass=f=8000",
          "volume=1.0",
          "dynaudnorm=p=0.90:g=15"
        ])
        .audioCodec("pcm_s16le")
        .audioChannels(1)
        .audioFrequency(SAMPLE_RATE)
        .format("wav")
        .on("error", (err) => {
          // If ffmpeg fails, just use raw wav
          console.log("[PIPER] ffmpeg cleanup failed, using raw:", err.message);
          fs.promises.rename(outputPath, cleanPath).then(() => res()).catch(rej);
        })
        .on("end", () => {
          fs.unlinkSync(outputPath);
          fs.renameSync(cleanPath, outputPath);
          res();
        })
        .save(cleanPath);
    });
    
    return outputPath;
  } catch (e: any) {
    console.error("[PIPER] TTS failed:", e.message);
    return null;
  }
}

// ============================================================================
// EDGE TTS FALLBACK
// ============================================================================
async function edgeTTS(text: string, outputPath: string, lang: string): Promise<string | null> {
  try {
    console.log("[TTS] EdgeTTS fallback...");
    const voice = EDGE_TTS_VOICES[lang] || EDGE_TTS_VOICES["en"];
    const tts = new EdgeTTS();
    await tts.ttsPromise(text, outputPath, { voice });
    return outputPath;
  } catch (e: any) {
    console.error("[TTS] EdgeTTS failed:", e.message);
    return null;
  }
}

// ============================================================================
// MASTER TTS FUNCTION - PIPER PRIMARY, EDGE FALLBACK
// ============================================================================
async function generateTTS(text: string, lang: string, id: number): Promise<string | null> {
  const piperPath = path.join(AUDIO_DIR, id + "_piper.wav");
  const edgePath = path.join(AUDIO_DIR, id + "_edge.mp3");

  // Try Piper first (WASM-based, no install needed)
  const piperResult = await piperTTSGenerate(text, piperPath, lang);
  if (piperResult) {
    try { fs.unlinkSync(edgePath); } catch {}
    return piperResult;
  }

  // Fallback to EdgeTTS
  const edgeResult = await edgeTTS(text, edgePath, lang);
  if (edgeResult) {
    try { fs.unlinkSync(piperPath); } catch {}
    return edgeResult;
  }

  try { fs.unlinkSync(piperPath); } catch {}
  try { fs.unlinkSync(edgePath); } catch {}
  return null;
}

// ============================================================================
// PCM GENERATION WITH ANTI-SHHH PROCESSING
// ============================================================================
async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=120",
        "lowpass=f=8000",
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "volume=1.0",
        "dynaudnorm=p=0.90:g=15",
        "afftdn=nf=-25",
        "adeclick"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => {
        let pcm = fs.readFileSync(tmp);
        fs.unlinkSync(tmp);
        pcm = removeDCOffset(pcm);
        pcm = applyFadeInOut(pcm, 200);
        resolve(pcm);
      })
      .save(tmp);
  });
}

function removeDCOffset(pcm: Buffer): Buffer {
  const samples = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.length / 2);
  let sum = 0;
  for (let i = 0; i < samples.length; i++) sum += samples[i];
  const mean = Math.round(sum / samples.length);
  
  if (Math.abs(mean) > 10) {
    for (let i = 0; i < samples.length; i++) samples[i] -= mean;
  }
  return pcm;
}

function applyFadeInOut(pcm: Buffer, fadeSamples: number): Buffer {
  const samples = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.length / 2);
  const fadeLen = Math.min(fadeSamples, Math.floor(samples.length / 4));
  
  for (let i = 0; i < fadeLen; i++) {
    const factor = i / fadeLen;
    samples[i] = Math.round(samples[i] * factor);
  }
  
  for (let i = 0; i < fadeLen; i++) {
    const idx = samples.length - 1 - i;
    const factor = i / fadeLen;
    samples[idx] = Math.round(samples[idx] * factor);
  }
  return pcm;
}

// ============================================================================
// STREAMING WITH SILENCE PAD
// ============================================================================
async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  
  const silenceBytes = Math.floor(SAMPLE_RATE * 0.5 * 2);
  const silenceBuffer = Buffer.alloc(silenceBytes, 0);
  const paddedPCM = Buffer.concat([pcm, silenceBuffer]);
  
  const totalChunks = Math.ceil(paddedPCM.length / CHUNK_SIZE);
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 300));
  ws.send("START_RESPONSE");
  
  let seq = 0;
  for (let i = 0; i < paddedPCM.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = paddedPCM.subarray(i, i + CHUNK_SIZE);
    
    let packet: Buffer;
    if (chunk.length < CHUNK_SIZE) {
      const padded = Buffer.alloc(CHUNK_SIZE);
      chunk.copy(padded);
      packet = Buffer.allocUnsafe(2 + CHUNK_SIZE);
      packet.writeUInt16BE(seq & 0xFFFF, 0);
      padded.copy(packet, 2);
    } else {
      packet = Buffer.allocUnsafe(2 + chunk.length);
      packet.writeUInt16BE(seq & 0xFFFF, 0);
      chunk.copy(packet, 2);
    }
    
    try {
      ws.send(packet, { binary: true });
    } catch (e) {
      console.error("[STREAM] Send failed:", e);
      return;
    }
    seq++;
    await new Promise(r => setTimeout(r, CHUNK_DELAY_MS));
  }
  
  ws.send("FINISH_RESPONSE");
  console.log("[STREAM] Sent " + seq + " chunks + silence pad");
}

function detectLanguage(text: string): string {
  const tagalogMarkers = [
    "ang", "ng", "sa", "mga", "ko", "mo", "niya", "nila", "naman", "po", "opo",
    "kumusta", "salamat", "oo", "hindi", "wala", "meron", "dito", "doon", "siya",
    "tayo", "kayo", "ako", "ikaw", "ka", "ba", "na", "pa", "lang", "din", "rin",
    "pero", "kasi", "dahil", "kung", "nang", "para", "pag", "kapag", "natin",
    "atin", "kanila", "kaniya", "sakin", "sayo", "samin", "sainyo", "niyo",
    "gusto", "ayaw", "maganda", "pangit", "mabuti", "masama", "malaki", "maliit",
    "mainit", "malamig", "bago", "luma", "bilis", "mabagal", "takbo", "lakad",
    "kain", "inom", "tulog", "gising", "upo", "tawa", "iyak", "takot",
    "galit", "tuwa", "lungkot", "pagod", "gutom", "uhaw", "antok"
  ];

  const lower = text.toLowerCase();
  const words = lower.split(/\s+/);
  let tagalogCount = 0;

  for (const word of words) {
    if (tagalogMarkers.includes(word)) tagalogCount++;
  }

  const ratio = words.length > 0 ? tagalogCount / words.length : 0;
  return ratio > 0.15 ? "tl" : "en";
}

// ============================================================================
// WEB SEARCH
// ============================================================================
async function webSearch(query: string): Promise<string> {
  try {
    console.log("[WEB_SEARCH] Searching:", query);
    const searchUrl = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
    const response = await fetch(searchUrl, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" }
    });
    const html = await response.text();
    
    const snippets: string[] = [];
    const resultRegex = /<a class="result__a"[^>]*>.*?<\/a>.*?<a class="result__snippet"[^>]*>(.*?)<\/a>/gs;
    let match;
    while ((match = resultRegex.exec(html)) !== null && snippets.length < 3) {
      const snippet = match[1].replace(/<[^>]+>/g, "").replace(/&quot;/g, '"').replace(/&amp;/g, '&').trim();
      if (snippet.length > 20) snippets.push(snippet);
    }
    
    if (snippets.length === 0) return "No search results found.";
    return snippets.join(". ");
  } catch (e: any) {
    console.error("[WEB_SEARCH] Error:", e.message);
    return "";
  }
}

// ============================================================================
// VOLUME COMMAND PARSER
// ============================================================================
function parseVolumeCommand(text: string): number | null {
  const lower = text.toLowerCase();
  const volumeMatch = lower.match(/(?:set\s+)?volume\s+(?:to\s+)?(\d+)(?:\s*percent?)?/);
  if (volumeMatch) return parseInt(volumeMatch[1]) / 100.0;
  if (lower.includes("mute") && !lower.includes("unmute")) return -1;
  if (lower.includes("unmute")) return -2;
  return null;
}

// ============================================================================
// PROCESS AUDIO AND RESPOND
// ============================================================================
async function processAndRespond(ws: WebSocket, audioBuffer: Buffer) {
  try {
    if (audioBuffer.length < 800) {
      ws.send("ERROR:AUDIO_TOO_SHORT");
      return;
    }

    const id = Date.now();
    const wavPath = path.join(UPLOAD_DIR, id + ".wav");

    fs.writeFileSync(wavPath, Buffer.concat([
      wavHeader(audioBuffer.length),
      audioBuffer
    ]));

    console.log("[PROCESS] Audio: " + audioBuffer.length + " bytes");

    const resampled = path.join(UPLOAD_DIR, id + "_16k.wav");
    await new Promise<void>((res, rej) => {
      ffmpeg(wavPath)
        .audioFrequency(16000)
        .audioChannels(1)
        .format("wav")
        .on("end", res)
        .on("error", rej)
        .save(resampled);
    });

    const stt = await Promise.race([
      sttClient.audio.transcriptions.create({
        file: fs.createReadStream(resampled),
        model: "whisper-large-v3-turbo"
      }),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("STT_TIMEOUT")), 15000))
    ]);

    const userText = stt.text?.trim();
    if (!userText) {
      ws.send("ERROR:NO_SPEECH");
      return;
    }

    console.log("USER:", userText);

    const volCmd = parseVolumeCommand(userText);
    if (volCmd !== null) {
      if (volCmd === -1) {
        ws.send("MUTE");
        await generateTTSAndSend(ws, "Muted", "en", id);
      } else if (volCmd === -2) {
        ws.send("UNMUTE");
        await generateTTSAndSend(ws, "Unmuted", "en", id);
      } else {
        ws.send("VOLUME:" + volCmd.toFixed(2));
        const volPercent = Math.round(volCmd * 100);
        await generateTTSAndSend(ws, `Volume set to ${volPercent} percent`, "en", id);
      }
      
      try { fs.unlinkSync(wavPath); fs.unlinkSync(resampled); } catch {}
      return;
    }

    const detectedLang = detectLanguage(userText);
    console.log("[LANG] Detected:", detectedLang);

    const searchKeywords = ["latest", "news", "current", "today", "weather", "price", "who won", "score", "update", "balita", "panahon", "presyo", "nanalo"];
    const needsSearch = searchKeywords.some(k => userText.toLowerCase().includes(k.toLowerCase()));
    
    let aiResponse = "";
    let responseLang = detectedLang;

    if (needsSearch) {
      console.log("[SEARCH] Triggered!");
      const searchResults = await webSearch(userText);
      
      const searchPrompt = detectedLang === "tl"
        ? `Batay sa mga resulta ng paghahanap: "${searchResults}". Sumagot sa Tagalog sa tanong na ito: "${userText}"`
        : `Based on these search results: "${searchResults}". Answer this question: "${userText}"`;
      
      const ai = await llmClient.chat.completions.create({
        model: "llama-3.3-70b-versatile",
        messages: [
          { role: "system", content: "Provide a concise, helpful answer based on the search results provided." },
          { role: "user", content: searchPrompt }
        ],
        temperature: 0.7,
        max_tokens: 300
      });
      
      aiResponse = (ai as any).choices[0].message.content || "";
    } else {
      const systemPrompt = detectedLang === "tl"
        ? 'Sumagot ka sa Tagalog. JSON format: {"text": "sagot", "language": "tl"}'
        : 'Respond in English. JSON format: {"text": "answer", "language": "en"}';

      const ai = await llmClient.chat.completions.create({
        model: "groq/compound-mini",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userText }
        ],
        temperature: 1,
        max_completion_tokens: 1024,
        top_p: 1,
        stream: false
      });

      const raw = (ai as any).choices[0].message.content || "{}";
      console.log("[AI RAW]:", raw);

      try {
        const cleaned = raw.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
        const parsed = JSON.parse(cleaned);
        aiResponse = parsed.text || raw;
        responseLang = parsed.language || detectedLang;
      } catch {
        aiResponse = raw;
      }
    }

    if (!aiResponse) {
      aiResponse = detectedLang === "tl" 
        ? "Pasensya na, hindi ko naintindihan." 
        : "Sorry, I didn't understand.";
    }

    console.log("[AI]:", aiResponse);
    console.log("[LANG]:", responseLang);

    await storage.createInteraction({
      transcript: userText,
      response: aiResponse
    });

    await generateTTSAndSend(ws, aiResponse, responseLang, id);

    try { fs.unlinkSync(wavPath); fs.unlinkSync(resampled); } catch {}

  } catch (e: any) {
    console.error("[PROCESS] Error:", e);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
  }
}

async function generateTTSAndSend(ws: WebSocket, text: string, lang: string, id: number) {
  const ttsPath = await generateTTS(text, lang, id);
  if (!ttsPath) {
    ws.send("ERROR:TTS_FAILED");
    return;
  }

  const pcm = await generatePCM(ttsPath);
  console.log("[TTS] PCM: " + pcm.length + " bytes");
  await streamPCM(ws, pcm);

  try { fs.unlinkSync(ttsPath); } catch {}
}

// ============================================================================
// ROUTES
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  // Initialize Piper WASM on startup
  await initPiper();
  
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 2 * 1024 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - V23 Piper TTS Edition");

    let chunks: Buffer[] = [];
    let processing = false;
    let isRecording = false;

    ws.on("message", async (data: any, isBinary: boolean) => {
      if (!isBinary) {
        const msg = data.toString();
        console.log("[WS] TEXT:", msg);

        if (msg === "READY") {
          console.log("[WS] ESP ready");
          return;
        }

        if (msg === "START_STREAM") {
          console.log("[STREAM] Start");
          isRecording = true;
          chunks = [];
          return;
        }

        if (msg === "END_STREAM") {
          if (!isRecording) return;
          isRecording = false;
          if (processing) return;
          processing = true;

          const audio = Buffer.concat(chunks);
          chunks = [];
          console.log("[STREAM] Total: " + audio.length + " bytes");

          processAndRespond(ws, audio).finally(() => {
            processing = false;
          });
          return;
        }
        return;
      }

      if (isRecording) {
        chunks.push(Buffer.from(data));
      }
    });

    ws.on("close", () => {
      console.log("ESP disconnected");
      chunks = [];
      processing = false;
      isRecording = false;
    });

    ws.on("error", (err) => {
      console.error("[WS] Error:", err);
    });

    const pingInterval = setInterval(() => {
      if (ws.readyState === ws.OPEN) {
        ws.ping();
      } else {
        clearInterval(pingInterval);
      }
    }, 10000);
  });

  return httpServer;
}
