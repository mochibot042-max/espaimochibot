import type { Express } from "express";
import { Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import Groq from "groq-sdk";
import fs from "fs";
import path from "path";
import ffmpeg from "fluent-ffmpeg";
import { EdgeTTS } from "node-edge-tts";
import { storage } from "./storage";

const GROQ_API_KEY = process.env.GROQ_API_KEY || "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";

const sttClient = new Groq({ apiKey: GROQ_API_KEY });
const llmClient = new Groq({ apiKey: GROQ_API_KEY });
const ttsClient = new Groq({ apiKey: GROQ_API_KEY });

const AUDIO_DIR = path.join(process.cwd(), "audio");
const UPLOAD_DIR = path.join(process.cwd(), "uploads");

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 512;
const CHUNK_DELAY_MS = 15;

const EDGE_TTS_VOICES: Record<string, string> = {
  "en": "en-US-AriaNeural",
  "tl": "fil-PH-BlessicaNeural",
  "fil": "fil-PH-BlessicaNeural",
  "tagalog": "fil-PH-BlessicaNeural",
  "english": "en-US-AriaNeural"
};

const GROQ_TTS_VOICE = "lulwa";

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

async function generatePCM(input: string): Promise<Buffer> {
  const tmp = path.join(AUDIO_DIR, "raw_" + Date.now() + ".pcm");
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "highpass=f=80",
        "lowpass=f=16000",
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "volume=1.2",
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

async function streamPCM(ws: WebSocket, pcm: Buffer) {
  if (ws.readyState !== ws.OPEN) return;
  const totalChunks = Math.ceil(pcm.length / CHUNK_SIZE);
  ws.send("PREPARE_RESPONSE:" + totalChunks);
  await new Promise(r => setTimeout(r, 500));
  ws.send("START_RESPONSE");
  
  let seq = 0;
  for (let i = 0; i < pcm.length; i += CHUNK_SIZE) {
    if (ws.readyState !== ws.OPEN) return;
    const chunk = pcm.subarray(i, i + CHUNK_SIZE);
    const packet = Buffer.allocUnsafe(2 + chunk.length);
    packet.writeUInt16BE(seq & 0xFFFF, 0);
    chunk.copy(packet, 2);
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
  console.log("[STREAM] Sent " + seq + " chunks");
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
// REAL WEB SEARCH USING SERPER.DEV (FREE TIER) OR DUCKDUCKGO
// ============================================================================
async function webSearch(query: string): Promise<string> {
  try {
    console.log("[WEB_SEARCH] Searching:", query);
    
    // Try DuckDuckGo Lite (no API key needed)
    const searchUrl = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
    
    const response = await fetch(searchUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      }
    });
    
    const html = await response.text();
    
    // Extract snippets from results
    const snippets: string[] = [];
    const resultRegex = /<a class="result__a"[^>]*>.*?<\/a>.*?<a class="result__snippet"[^>]*>(.*?)<\/a>/gs;
    let match;
    while ((match = resultRegex.exec(html)) !== null && snippets.length < 3) {
      const snippet = match[1].replace(/<[^>]+>/g, "").replace(/&quot;/g, '"').replace(/&amp;/g, '&').trim();
      if (snippet.length > 20) snippets.push(snippet);
    }
    
    if (snippets.length === 0) {
      return "No search results found.";
    }
    
    return snippets.join(". ");
  } catch (e: any) {
    console.error("[WEB_SEARCH] Error:", e.message);
    return "";
  }
}

// ============================================================================
// GROQ TTS
// ============================================================================
async function groqTTS(text: string, outputPath: string): Promise<string | null> {
  try {
    console.log("[TTS] Groq TTS...");
    const wav = await ttsClient.audio.speech.create({
      model: "canopylabs/orpheus-arabic-saudi",
      voice: GROQ_TTS_VOICE,
      response_format: "wav",
      input: text,
    });

    const buffer = Buffer.from(await wav.arrayBuffer());
    const rawPath = outputPath.replace(".wav", "_raw.wav");
    await fs.promises.writeFile(rawPath, buffer);

    try {
      const pcmPath = outputPath.replace(".wav", "_temp.pcm");
      await new Promise<void>((res, rej) => {
        ffmpeg(rawPath)
          .audioCodec("pcm_s16le")
          .audioChannels(1)
          .audioFrequency(24000)
          .format("s16le")
          .on("error", rej)
          .on("end", res)
          .save(pcmPath);
      });
      
      fs.unlinkSync(rawPath);
      fs.renameSync(pcmPath, outputPath);
      return outputPath;
    } catch {
      const fixedPath = outputPath.replace(".wav", "_fixed.wav");
      await new Promise<void>((res, rej) => {
        ffmpeg(rawPath)
          .audioCodec("pcm_s16le")
          .audioChannels(1)
          .audioFrequency(24000)
          .format("wav")
          .on("error", rej)
          .on("end", res)
          .save(fixedPath);
      });
      
      fs.unlinkSync(rawPath);
      fs.renameSync(fixedPath, outputPath);
      return outputPath;
    }
  } catch (e: any) {
    console.error("[TTS] Groq failed:", e.message);
    return null;
  }
}

async function edgeTTS(text: string, outputPath: string, lang: string): Promise<string | null> {
  try {
    console.log("[TTS] EdgeTTS...");
    const voice = EDGE_TTS_VOICES[lang] || EDGE_TTS_VOICES["en"];
    const tts = new EdgeTTS();
    await tts.ttsPromise(text, outputPath, { voice });
    return outputPath;
  } catch (e: any) {
    console.error("[TTS] EdgeTTS failed:", e.message);
    return null;
  }
}

async function generateTTS(text: string, lang: string, id: number): Promise<string | null> {
  const groqPath = path.join(AUDIO_DIR, id + "_groq.wav");
  const edgePath = path.join(AUDIO_DIR, id + "_edge.mp3");

  const groqResult = await groqTTS(text, groqPath);
  if (groqResult) {
    try { fs.unlinkSync(edgePath); } catch {}
    return groqResult;
  }

  const edgeResult = await edgeTTS(text, edgePath, lang);
  if (edgeResult) {
    try { fs.unlinkSync(groqPath); } catch {}
    try { fs.unlinkSync(groqPath.replace(".wav", "_raw.wav")); } catch {}
    return edgeResult;
  }

  try { fs.unlinkSync(groqPath); } catch {}
  try { fs.unlinkSync(edgePath); } catch {}
  return null;
}

// ============================================================================
// CHECK IF VOLUME COMMAND
// ============================================================================
function parseVolumeCommand(text: string): number | null {
  const lower = text.toLowerCase();
  
  // Match patterns like "set volume to 50", "volume 10 percent", "mute", "unmute"
  const volumeMatch = lower.match(/(?:set\s+)?volume\s+(?:to\s+)?(\d+)(?:\s*percent?)?/);
  if (volumeMatch) {
    return parseInt(volumeMatch[1]) / 100.0;
  }
  
  if (lower.includes("mute") && !lower.includes("unmute")) return -1; // Mute signal
  if (lower.includes("unmute")) return -2; // Unmute signal
  
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

    // CHECK FOR VOLUME COMMAND FIRST
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
      
      // Cleanup
      try {
        fs.unlinkSync(wavPath);
        fs.unlinkSync(resampled);
      } catch {}
      return;
    }

    const detectedLang = detectLanguage(userText);
    console.log("[LANG] Detected:", detectedLang);

    // CHECK IF NEEDS WEB SEARCH
    const searchKeywords = ["latest", "news", "current", "today", "weather", "price", "who won", "score", "update", "balita", "panahon", "presyo", "nanalo"];
    const needsSearch = searchKeywords.some(k => userText.toLowerCase().includes(k.toLowerCase()));
    
    let aiResponse = "";
    let responseLang = detectedLang;

    if (needsSearch) {
      console.log("[SEARCH] Triggered!");
      const searchResults = await webSearch(userText);
      
      // Use LLM to summarize search results
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
      // Normal response
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

    // Cleanup
    try {
      fs.unlinkSync(wavPath);
      fs.unlinkSync(resampled);
    } catch {}

  } catch (e: any) {
    console.error("[PROCESS] Error:", e);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
  }
}

// Helper to generate TTS and send
async function generateTTSAndSend(ws: WebSocket, text: string, lang: string, id: number) {
  const ttsPath = await generateTTS(text, lang, id);
  if (!ttsPath) {
    ws.send("ERROR:TTS_FAILED");
    return;
  }

  const pcm = await generatePCM(ttsPath);
  console.log("[TTS] PCM: " + pcm.length + " bytes");
  await streamPCM(ws, pcm);

  try {
    fs.unlinkSync(ttsPath);
  } catch {}
}

// ============================================================================
// ROUTES
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 2 * 1024 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - V20 Web Search + Volume");

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
