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

// ============================================================================
// V19: SETTINGS
// ============================================================================
const SAMPLE_RATE = 16000;
const CHUNK_SIZE = 512;
const CHUNK_DELAY_MS = 15;

// ============================================================================
// TTS VOICE MAP
// ============================================================================
const EDGE_TTS_VOICES: Record<string, string> = {
  "en": "en-US-AriaNeural",
  "tl": "fil-PH-BlessicaNeural",
  "fil": "fil-PH-BlessicaNeural",
  "tagalog": "fil-PH-BlessicaNeural",
  "english": "en-US-AriaNeural"
};

const GROQ_TTS_VOICE = "lulwa";

// ============================================================================
// WAV HEADER
// ============================================================================
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
// PCM GENERATOR
// ============================================================================
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

// ============================================================================
// RESAMPLE TO 16kHz
// ============================================================================
async function resampleTo16k(input: string, output: string): Promise<void> {
  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .audioFilters([
        "aresample=" + SAMPLE_RATE + ":resampler=soxr:precision=28",
        "pan=mono|c0=c0"
      ])
      .audioCodec("pcm_s16le")
      .audioChannels(1)
      .audioFrequency(SAMPLE_RATE)
      .format("s16le")
      .on("error", reject)
      .on("end", () => resolve())
      .save(output);
  });
}

// ============================================================================
// STREAM PCM BACK TO ESP
// ============================================================================
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
  console.log("[STREAM] Sent " + seq + " chunks, total " + pcm.length + " bytes");
}

// ============================================================================
// DETECT LANGUAGE
// ============================================================================
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
// GROQ TTS - PRIMARY
// ============================================================================
async function groqTTS(text: string, outputPath: string): Promise<string | null> {
  try {
    console.log("[TTS] Trying Groq TTS...");
    const wav = await ttsClient.audio.speech.create({
      model: "canopylabs/orpheus-arabic-saudi",
      voice: GROQ_TTS_VOICE,
      response_format: "wav",
      input: text,
    });

    const buffer = Buffer.from(await wav.arrayBuffer());
    const rawPath = outputPath.replace(".wav", "_raw.wav");
    await fs.promises.writeFile(rawPath, buffer);
    console.log("[TTS] Groq raw saved:", rawPath, "size:", buffer.length);

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
      console.log("[TTS] Groq TTS success:", outputPath);
      return outputPath;
    } catch (probeErr: any) {
      console.error("[TTS] Groq output invalid, trying fix...", probeErr.message);
      
      const fixedPath = outputPath.replace(".wav", "_fixed.wav");
      try {
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
        console.log("[TTS] Groq TTS fixed:", outputPath);
        return outputPath;
      } catch (fixErr: any) {
        console.error("[TTS] Fix failed:", fixErr.message);
        try { fs.unlinkSync(rawPath); } catch {}
        try { fs.unlinkSync(fixedPath); } catch {}
        return null;
      }
    }
  } catch (e: any) {
    console.error("[TTS] Groq TTS failed:", e.message);
    return null;
  }
}

// ============================================================================
// EDGETTS - FALLBACK
// ============================================================================
async function edgeTTS(text: string, outputPath: string, lang: string): Promise<string | null> {
  try {
    console.log("[TTS] Falling back to EdgeTTS...");
    const voice = EDGE_TTS_VOICES[lang] || EDGE_TTS_VOICES["en"];
    const tts = new EdgeTTS();
    await tts.ttsPromise(text, outputPath, { voice });
    console.log("[TTS] EdgeTTS success:", outputPath);
    return outputPath;
  } catch (e: any) {
    console.error("[TTS] EdgeTTS failed:", e.message);
    return null;
  }
}

// ============================================================================
// GENERATE TTS
// ============================================================================
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
// V19: WEB SEARCH WITH GROQ COMPOUND MINI - FIXED
// ============================================================================
async function webSearchWithGroq(query: string, lang: string): Promise<string> {
  try {
    console.log("[WEB_SEARCH] Searching for:", query);
    
    // Use compound-mini with explicit web search
    const searchPrompt = lang === "tl" 
      ? `Hanapin mo sa internet ang sagot sa tanong na ito: "${query}". Ibigay ang sagot sa Tagalog.`
      : `Search the internet for: "${query}". Provide a concise answer.`;
    
    const response = await llmClient.chat.completions.create({
      model: "groq/compound-mini",
      messages: [
        {
          role: "system",
          content: "You are a web search assistant. Use your web_search tool to find current information. Always provide factual, up-to-date answers based on search results."
        },
        {
          role: "user",
          content: searchPrompt
        }
      ],
      temperature: 0.7,
      max_completion_tokens: 500,
      top_p: 1,
      stream: false,
      tools: [
        {
          type: "function",
          function: {
            name: "web_search",
            description: "Search the web for current information",
            parameters: {
              type: "object",
              properties: {
                query: {
                  type: "string",
                  description: "The search query"
                }
              },
              required: ["query"]
            }
          }
        }
      ],
      tool_choice: "auto"
    });

    const content = (response as any).choices[0]?.message?.content || "";
    console.log("[WEB_SEARCH] Result:", content.substring(0, 200));
    return content;
  } catch (e: any) {
    console.error("[WEB_SEARCH] Failed:", e.message);
    return "";
  }
}

// ============================================================================
// PROCESS AUDIO AND RESPOND - WITH WEB SEARCH
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

    console.log("[PROCESS] Received audio: " + audioBuffer.length + " bytes");

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

    const detectedLang = detectLanguage(userText);
    console.log("[LANG] Detected:", detectedLang);

    // Check if user is asking about current info / web search
    const searchKeywords = ["latest", "news", "current", "today", "weather", "price", "who won", "score", "update", "ngayon", "balita", "panahon"];
    const needsSearch = searchKeywords.some(k => userText.toLowerCase().includes(k.toLowerCase()));
    
    let aiResponse = "";
    let responseLang = detectedLang;

    if (needsSearch) {
      console.log("[SEARCH] Web search triggered!");
      const searchResult = await webSearchWithGroq(userText, detectedLang);
      if (searchResult) {
        aiResponse = searchResult;
      }
    }

    // If no search result or not searching, use normal LLM
    if (!aiResponse) {
      const systemPrompt = detectedLang === "tl"
        ? 'Ikaw ay isang Pilipinong AI assistant. Sumagot ka sa Tagalog/Filipino. Ang response mo ay DAPAT JSON format lamang: {"text": "iyong sagot dito", "language": "tl"}. Walang ibang text maliban sa JSON.'
        : 'You are a helpful AI assistant. Respond in English. Your response MUST be JSON format only: {"text": "your answer here", "language": "en"}. No other text besides JSON.';

      const ai = await Promise.race([
        llmClient.chat.completions.create({
          model: "groq/compound-mini",
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userText }
          ],
          temperature: 1,
          max_completion_tokens: 1024,
          top_p: 1,
          stream: false
        }),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("LLM_TIMEOUT")), 20000)
        )
      ]);

      const raw = (ai as any).choices[0].message.content || "{}";
      console.log("[AI RAW]:", raw);

      try {
        const cleaned = raw.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
        const parsed = JSON.parse(cleaned);
        aiResponse = parsed.text || "";
        responseLang = parsed.language || detectedLang;
      } catch (e) {
        const jsonMatch = raw.match(/\{[\s\S]*?\}/);
        if (jsonMatch) {
          try {
            const parsed = JSON.parse(jsonMatch[0]);
            aiResponse = parsed.text || raw;
            responseLang = parsed.language || detectedLang;
          } catch {
            aiResponse = raw;
          }
        } else {
          aiResponse = raw;
        }
      }
    }

    if (!aiResponse) {
      aiResponse = detectedLang === "tl" 
        ? "Pasensya na, hindi ko naintindihan." 
        : "Sorry, I didn't understand.";
    }

    console.log("[AI RESPONSE]:", aiResponse);
    console.log("[RESPONSE LANG]:", responseLang);

    await storage.createInteraction({
      transcript: userText,
      response: aiResponse
    });

    const ttsPath = await generateTTS(aiResponse, responseLang, id);
    if (!ttsPath) {
      ws.send("ERROR:TTS_FAILED");
      return;
    }

    const pcm = await generatePCM(ttsPath);
    console.log("[TTS] Generated PCM: " + pcm.length + " bytes");
    await streamPCM(ws, pcm);

    try {
      fs.unlinkSync(ttsPath);
      fs.unlinkSync(wavPath);
      fs.unlinkSync(resampled);
    } catch (e) {
      console.error("[CLEANUP] Error:", e);
    }

  } catch (e: any) {
    console.error("[PROCESS] Error:", e);
    ws.send("ERROR:" + (e.message || "UNKNOWN"));
  }
}

// ============================================================================
// V19: ROUTES
// ============================================================================
export async function registerRoutes(httpServer: Server, app: Express) {
  const wss = new WebSocketServer({
    server: httpServer,
    path: "/ws/audio",
    perMessageDeflate: false,
    maxPayload: 2 * 1024 * 1024
  });

  wss.on("connection", (ws: WebSocket) => {
    console.log("ESP connected - V19 LED FX + Web Search");

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
          console.log("[STREAM] User started speaking");
          isRecording = true;
          chunks = [];
          return;
        }

        if (msg === "END_STREAM") {
          if (!isRecording) return;
          isRecording = false;

          if (processing) return;
          processing = true;

          console.log("[STREAM] User stopped speaking, processing...");

          const audio = Buffer.concat(chunks);
          chunks = [];

          console.log("[STREAM] Total audio received: " + audio.length + " bytes");

          processAndRespond(ws, audio).finally(() => {
            processing = false;
          });

          return;
        }

        return;
      }

      if (isRecording) {
        chunks.push(Buffer.from(data));

        if (chunks.length % 20 === 0) {
          const totalBytes = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
          console.log("[STREAM] Received " + chunks.length + " chunks, " + totalBytes + " bytes");
        }
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
