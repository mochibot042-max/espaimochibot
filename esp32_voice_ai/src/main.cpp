// ============================================================================
// ESP32 AUDIO CHATBOT - V30 (UNLIMITED STT STREAMING)
// ============================================================================
// FIXED: No max recording time limit — only silence stops recording
// FIXED: Circular buffer for overflow protection instead of hard stop
// FIXED: Streaming protocol aligned with server V28
// ============================================================================

#include <WiFi.h>
#include <WiFiManager.h>
#include <WebSocketsClient.h>
#include <driver/i2s.h>
#include <Adafruit_NeoPixel.h>
#include <Preferences.h>
#include <string.h>

// ============================================================================
// CONFIG
// ============================================================================
const char* WS_HOST = "espaimochibot-q3xu.onrender.com";
const int WS_PORT = 443;
const char* WS_PATH = "/ws/audio";

#define LED_PIN 48

// MIC PINS
#define MIC_BCK 15
#define MIC_WS  16
#define MIC_SD  17

// DAC PINS (PCM5100A)
#define I2S_BCK 4
#define I2S_WS  5
#define I2S_DOUT 6

// ============================================================================
// GLOBALS
// ============================================================================
Adafruit_NeoPixel pixels(1, LED_PIN, NEO_GRB + NEO_KHZ800);
WebSocketsClient webSocket;
Preferences prefs;

bool is_recording = false;
bool isPlaying = false;
bool isWSConnected = false;
bool isPreparingPlayback = false;
bool isMusicMode = false;
float currentVolume = 0.32f;

// ============================================================================
// RECORDING — UNLIMITED STREAMING
// ============================================================================
#define RECORD_RATE 16000
#define BUFFER_SIZE 512

// Circular buffer for local overflow protection (keeps last ~2 seconds)
// Instead of stopping, we overwrite old data so user can keep talking
#define CIRCULAR_BUFFER_SIZE (16000 * 2 * 2)  // 2 seconds of 16-bit mono = 64KB

uint8_t* circular_buffer = NULL;
size_t circ_write_pos = 0;
size_t circ_data_len = 0;

const int START_THRESHOLD = 200;
const int SILENCE_RMS = 60;
const int SILENCE_THRESH = 25;
int silence_counter = 0;
int speech_frames = 0;
const int SPEECH_CONFIRM = 2;

// ============================================================================
// PLAYBACK STATE
// ============================================================================
volatile bool playback_active = false;
volatile bool finish_received = false;
volatile uint32_t chunks_received = 0;
volatile uint32_t chunks_expected = 0;
unsigned long finish_receive_time = 0;
const unsigned long FINISH_GRACE_MS = 300;

// ============================================================================
// STREAMING STATE
// ============================================================================
bool stream_started = false;

// ============================================================================
// TIMING
// ============================================================================
unsigned long lastWSPing = 0;
unsigned long lastActivity = 0;

// ============================================================================
// LED
// ============================================================================
void setColor(uint32_t color) {
  pixels.setPixelColor(0, color);
  pixels.show();
}

// ============================================================================
// I2S SAMPLE RATE SWITCHER
// ============================================================================
void setI2SRate(uint32_t rate) {
  esp_err_t err = i2s_set_clk(I2S_NUM_0, rate, I2S_BITS_PER_SAMPLE_16BIT, I2S_CHANNEL_MONO);
  if (err != ESP_OK) {
    Serial.printf("[I2S] Failed to set rate %lu, err=%d\n", rate, err);
  } else {
    Serial.printf("[I2S] Sample rate set to %lu Hz\n", rate);
  }
}

// ============================================================================
// AUDIO HELPERS
// ============================================================================
float calculateRMS(int16_t* buffer, size_t samples) {
  if (samples == 0) return 0;
  float sum = 0;
  for (size_t i = 0; i < samples; i++) {
    float s = buffer[i];
    sum += s * s;
  }
  return sqrt(sum / samples);
}

// ============================================================================
// CIRCULAR BUFFER HELPERS
// ============================================================================
void circWrite(uint8_t* data, size_t len) {
  if (!circular_buffer || len == 0) return;
  
  for (size_t i = 0; i < len; i++) {
    circular_buffer[circ_write_pos] = data[i];
    circ_write_pos = (circ_write_pos + 1) % CIRCULAR_BUFFER_SIZE;
  }
  
  circ_data_len += len;
  if (circ_data_len > CIRCULAR_BUFFER_SIZE) {
    circ_data_len = CIRCULAR_BUFFER_SIZE; // cap at max
  }
}

void circReset() {
  circ_write_pos = 0;
  circ_data_len = 0;
  memset(circular_buffer, 0, CIRCULAR_BUFFER_SIZE);
}

// ============================================================================
// DIRECT I2S WRITE
// ============================================================================
void directI2SWrite(uint8_t* data, size_t len) {
  if (!playback_active || len == 0) return;
  
  size_t totalWritten = 0;
  int retries = 0;
  const int MAX_RETRIES = 10;
  
  while (totalWritten < len && retries < MAX_RETRIES) {
    size_t written = 0;
    esp_err_t err = i2s_write(I2S_NUM_0, data + totalWritten, len - totalWritten, &written, pdMS_TO_TICKS(50));
    
    if (err != ESP_OK) {
      Serial.printf("[I2S] Write error: %d\n", err);
      break;
    }
    
    if (written == 0) {
      retries++;
      delay(1);
      continue;
    }
    
    totalWritten += written;
    retries = 0;
  }
  
  if (totalWritten < len) {
    Serial.printf("[I2S] Partial write: %d/%d bytes\n", totalWritten, len);
  }
}

// ============================================================================
// FLUSH AND STOP
// ============================================================================
void stopPlayback() {
  Serial.println("[PLAY] Stopping");
  
  delay(50);
  i2s_zero_dma_buffer(I2S_NUM_0);
  
  uint8_t silence[512];
  memset(silence, 0, 512);
  size_t bw = 0;
  for (int i = 0; i < 4; i++) {
    i2s_write(I2S_NUM_0, silence, 512, &bw, pdMS_TO_TICKS(10));
  }
  
  setI2SRate(RECORD_RATE);
  
  playback_active = false;
  finish_received = false;
  chunks_received = 0;
  chunks_expected = 0;
  finish_receive_time = 0;
  isPlaying = false;
  isPreparingPlayback = false;
  isMusicMode = false;
  
  setColor(pixels.Color(0, 0, 100));
  Serial.println("[PLAY] Stopped -> IDLE (16kHz)");
  
  delay(50);
  if (webSocket.isConnected()) {
    webSocket.sendTXT("READY");
    Serial.println("[WS] Sent READY");
  }
}

// ============================================================================
// WEBSOCKET EVENT
// ============================================================================
void webSocketEvent(WStype_t type, uint8_t* payload, size_t length) {
  switch (type) {
    case WStype_DISCONNECTED:
      Serial.println("[WS] Disconnected");
      isWSConnected = false;
      isPlaying = false;
      playback_active = false;
      isPreparingPlayback = false;
      isMusicMode = false;
      is_recording = false;
      stream_started = false;
      circReset();
      setI2SRate(RECORD_RATE);
      setColor(pixels.Color(100, 0, 0));
      break;

    case WStype_CONNECTED:
      isWSConnected = true;
      isPlaying = false;
      playback_active = false;
      isPreparingPlayback = false;
      isMusicMode = false;
      is_recording = false;
      stream_started = false;
      circReset();
      setI2SRate(RECORD_RATE);
      setColor(pixels.Color(0, 0, 100));
      Serial.println("[WS] Connected");
      break;

    case WStype_TEXT: {
      String msg = (char*)payload;
      Serial.printf("[WS] TXT: %s\n", msg.c_str());
      lastActivity = millis();

      if (msg == "START_RESPONSE") {
        isPlaying = true;
        playback_active = true;
        isPreparingPlayback = false;
        finish_received = false;
        chunks_received = 0;
        finish_receive_time = 0;
        isMusicMode = false;
        setColor(pixels.Color(200, 0, 200));
        Serial.println("[PLAY] AI Start (16kHz)");
      }
      else if (msg == "START_MUSIC") {
        isPlaying = true;
        playback_active = true;
        isPreparingPlayback = false;
        finish_received = false;
        chunks_received = 0;
        finish_receive_time = 0;
        isMusicMode = true;
        setColor(pixels.Color(255, 165, 0));
        Serial.println("[PLAY] Music Start (48kHz)");
      }
      else if (msg.startsWith("FINISH_RESPONSE:") || msg == "FINISH_RESPONSE" || 
               msg.startsWith("FINISH_MUSIC:") || msg == "FINISH_MUSIC") {
        Serial.println("[PLAY] Finish received");
        finish_received = true;
        finish_receive_time = millis();
        if (chunks_received >= chunks_expected && chunks_expected > 0) {
          Serial.println("[PLAY] All chunks already received, stopping");
          stopPlayback();
        }
      }
      else if (msg.startsWith("PREPARE_MUSIC:")) {
        int totalChunks = msg.substring(14).toInt();
        chunks_expected = totalChunks;
        Serial.printf("[PREPARE] Music %d chunks @ 48kHz\n", totalChunks);
        
        setI2SRate(48000);
        i2s_zero_dma_buffer(I2S_NUM_0);
        
        isPreparingPlayback = true;
        isPlaying = false;
        playback_active = true;
        finish_received = false;
        chunks_received = 0;
        finish_receive_time = 0;
        isMusicMode = true;
        
        delay(200);
        if (webSocket.isConnected()) {
          webSocket.sendTXT("READY");
        }
        setColor(pixels.Color(255, 100, 0));
      }
      else if (msg.startsWith("PREPARE_RESPONSE:")) {
        int totalChunks = msg.substring(17).toInt();
        chunks_expected = totalChunks;
        Serial.printf("[PREPARE] AI %d chunks @ 16kHz\n", totalChunks);
        
        setI2SRate(RECORD_RATE);
        i2s_zero_dma_buffer(I2S_NUM_0);
        
        isPreparingPlayback = true;
        isPlaying = false;
        playback_active = true;
        finish_received = false;
        chunks_received = 0;
        finish_receive_time = 0;
        isMusicMode = false;
        
        delay(200);
        if (webSocket.isConnected()) {
          webSocket.sendTXT("READY");
        }
        setColor(pixels.Color(100, 100, 0));
      }
      else if (msg.startsWith("VOLUME:")) {
        currentVolume = msg.substring(7).toFloat();
        prefs.putFloat("volume", currentVolume);
      }
      else if (msg.startsWith("ERROR")) {
        Serial.printf("[ERR] %s\n", msg.c_str());
        stopPlayback();
        isPreparingPlayback = false;
        is_recording = false;
        setColor(pixels.Color(100, 0, 0));
      }
      else if (msg == "STREAM_READY") {
        Serial.println("[STREAM] Server ready for audio chunks");
      }
      else if (msg.startsWith("STT_RESULT:")) {
        Serial.printf("[STT] Server result: %s\n", msg.c_str());
      }
      else if (msg == "STATE:IDLE") {
        Serial.println("[STATE] Server idle, ready for next");
      }
      break;
    }

    case WStype_BIN: {
      if (length < 2) return;
      
      uint8_t* audioData = payload + 2;
      size_t audioLen = length - 2;

      if (audioLen % 2 != 0) audioLen--;
      
      if (audioLen > 0 && playback_active) {
        directI2SWrite(audioData, audioLen);
        chunks_received++;
      }
      
      lastActivity = millis();
      break;
    }

    case WStype_PING:
    case WStype_PONG:
      lastWSPing = millis();
      break;

    case WStype_ERROR:
      Serial.println("[WS] Error");
      isWSConnected = false;
      break;
  }
}

// ============================================================================
// SETUP
// ============================================================================
void setup() {
  Serial.begin(115200);
  pixels.begin();
  setColor(pixels.Color(50, 50, 0));

  prefs.begin("alexatron", false);
  currentVolume = prefs.getFloat("volume", 0.32f);

  // Allocate circular buffer for overflow protection
  circular_buffer = (uint8_t*)ps_malloc(CIRCULAR_BUFFER_SIZE);
  if (!circular_buffer) {
    Serial.println("[FATAL] No mem for circular buffer");
    while (1) {
      setColor(pixels.Color(255, 0, 0));
      delay(200);
      setColor(pixels.Color(0, 0, 0));
      delay(200);
    }
  }
  circReset();

  WiFiManager wm;
  wm.setConfigPortalTimeout(60);
  wm.setDebugOutput(false);
  setColor(pixels.Color(255, 255, 0));
  
  if (!wm.autoConnect("Alexatron")) {
    delay(3000);
    ESP.restart();
  }
  
  Serial.printf("[WIFI] %s\n", WiFi.localIP().toString().c_str());
  setColor(pixels.Color(0, 255, 0));
  WiFi.setSleep(false);

  // I2S MIC
  i2s_config_t mic_cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX),
    .sample_rate = RECORD_RATE,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format = I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format = I2S_COMM_FORMAT_STAND_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 8,
    .dma_buf_len = 1024,
    .use_apll = false,
    .tx_desc_auto_clear = false,
    .fixed_mclk = I2S_PIN_NO_CHANGE
  };
  i2s_pin_config_t mic_p = {
    .bck_io_num = MIC_BCK,
    .ws_io_num = MIC_WS,
    .data_out_num = I2S_PIN_NO_CHANGE,
    .data_in_num = MIC_SD
  };
  
  esp_err_t err = i2s_driver_install(I2S_NUM_1, &mic_cfg, 0, NULL);
  if (err != ESP_OK) {
    Serial.printf("[FATAL] MIC I2S err: %d\n", err);
    while (1) delay(100);
  }
  i2s_set_pin(I2S_NUM_1, &mic_p);
  i2s_set_clk(I2S_NUM_1, RECORD_RATE, I2S_BITS_PER_SAMPLE_16BIT, I2S_CHANNEL_MONO);

  // I2S DAC (PCM5100A)
  i2s_config_t dac_cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX),
    .sample_rate = RECORD_RATE,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format = I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format = I2S_COMM_FORMAT_STAND_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 8,
    .dma_buf_len = 1024,
    .use_apll = false,
    .tx_desc_auto_clear = true,
    .fixed_mclk = I2S_PIN_NO_CHANGE
  };
  
  i2s_pin_config_t dac_p = {
    .bck_io_num = I2S_BCK,
    .ws_io_num = I2S_WS,
    .data_out_num = I2S_DOUT,
    .data_in_num = I2S_PIN_NO_CHANGE
  };
  
  err = i2s_driver_install(I2S_NUM_0, &dac_cfg, 0, NULL);
  if (err != ESP_OK) {
    Serial.printf("[FATAL] DAC err: %d\n", err);
    while (1) delay(100);
  }
  i2s_set_pin(I2S_NUM_0, &dac_p);
  i2s_set_clk(I2S_NUM_0, RECORD_RATE, I2S_BITS_PER_SAMPLE_16BIT, I2S_CHANNEL_MONO);

  i2s_zero_dma_buffer(I2S_NUM_0);

  webSocket.beginSSL(WS_HOST, WS_PORT, WS_PATH);
  webSocket.onEvent(webSocketEvent);
  webSocket.setReconnectInterval(10000);
  webSocket.enableHeartbeat(30000, 10000, 2);

  Serial.println("=================================");
  Serial.println("[SETUP] V30 UNLIMITED STT STREAMING");
  Serial.println("=================================");
  Serial.printf("[MIC] SCK: GPIO%d, WS: GPIO%d, SD: GPIO%d\n", MIC_BCK, MIC_WS, MIC_SD);
  Serial.printf("[DAC] BCK: GPIO%d, WS: GPIO%d, DOUT: GPIO%d\n", I2S_BCK, I2S_WS, I2S_DOUT);
  Serial.printf("[BUF] Circular buffer: %d bytes (~%d seconds)\n", CIRCULAR_BUFFER_SIZE, CIRCULAR_BUFFER_SIZE / 32000);
  setColor(pixels.Color(0, 100, 0));

  lastActivity = millis();
  lastWSPing = millis();
}

// ============================================================================
// LOOP — UNLIMITED STREAMING, NO TIME LIMIT
// ============================================================================
void loop() {
  webSocket.loop();
  yield();

  unsigned long now = millis();

  if (playback_active && finish_received) {
    if (now - finish_receive_time > FINISH_GRACE_MS) {
      Serial.println("[PLAY] Grace period over, stopping");
      stopPlayback();
    }
  }

  if (isWSConnected && (now - lastActivity > 60000)) {
    if (playback_active) {
      Serial.println("[TIMEOUT] No activity, forcing stop");
      stopPlayback();
    }
  }

  if (!isWSConnected) {
    if (is_recording) {
      is_recording = false;
      stream_started = false;
      circReset();
    }
    return;
  }

  // Don't read mic during playback
  if (playback_active || isPreparingPlayback) return;

  int16_t sample_buffer[BUFFER_SIZE / 2];
  size_t bytes_read = 0;

  i2s_read(I2S_NUM_1, sample_buffer, BUFFER_SIZE, &bytes_read, 10);
  if (bytes_read == 0) return;

  float rms = calculateRMS(sample_buffer, bytes_read / 2);

  if (!is_recording) {
    if (rms > START_THRESHOLD) speech_frames++;
    else speech_frames = 0;

    if (speech_frames >= SPEECH_CONFIRM) {
      is_recording = true;
      silence_counter = 0;
      speech_frames = 0;
      stream_started = false;
      circReset();  // Clear old data, start fresh
      setColor(pixels.Color(0, 255, 255));
      Serial.println("[REC] Start — UNLIMITED (silence only stops)");
    }
  }
  else {
    // ============================================
    // UNLIMITED STREAMING — NO TIME LIMIT
    // Only silence stops recording
    // ============================================
    
    // Send STREAM_START on first chunk
    if (!stream_started) {
      if (webSocket.isConnected()) {
        webSocket.sendTXT("STREAM_START");
        Serial.println("[STREAM] Sent STREAM_START");
      }
      stream_started = true;
      delay(50);
    }
    
    // Send raw PCM data immediately (real-time streaming)
    if (bytes_read > 0 && webSocket.isConnected()) {
      size_t sendLen = bytes_read;
      if (sendLen % 2 != 0) sendLen--;
      webSocket.sendBIN((uint8_t*)sample_buffer, sendLen);
    }

    // Also save to circular buffer (keeps last 2 seconds as backup)
    circWrite((uint8_t*)sample_buffer, bytes_read);

    // Check silence — THIS IS THE ONLY WAY TO STOP RECORDING
    if (rms < SILENCE_RMS) {
      silence_counter++;
      if (silence_counter > SILENCE_THRESH) {
        Serial.println("[REC] Silence detected -> STREAM_END");
        
        if (webSocket.isConnected()) {
          webSocket.sendTXT("STREAM_END");
          Serial.println("[STREAM] Sent STREAM_END");
        }
        
        is_recording = false;
        stream_started = false;
        circReset();
        return;
      }
    }
    else {
      silence_counter = 0;
    }

    // REMOVED: No max recording time check!
    // You can talk as long as you want, only silence stops it
  }
}
