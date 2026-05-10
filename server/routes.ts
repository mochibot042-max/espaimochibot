// ============================================================================
// ESP32 AUDIO CHATBOT - V13 (CHOPPY AUDIO FIX)
// ============================================================================
// - Pre-buffering: wait for minimum data before playing
// - Larger playback chunks (1024)
// - Relaxed task timing
// - Better underrun handling
// ============================================================================

#include <WiFi.h>
#include <WiFiManager.h>
#include <WebSocketsClient.h>
#include <driver/i2s.h>
#include <Adafruit_NeoPixel.h>
#include <Preferences.h>
#include <algorithm>
#include <string.h>

// ============================================================================
// CONFIGURATION
// ============================================================================
const char* WS_HOST = "assistant-xf3o.onrender.com";
const int WS_PORT = 443;
const char* WS_PATH = "/ws/audio";

#define LED_PIN 48
#define MIC_BCK 16
#define MIC_WS  17
#define MIC_SD  18
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
float currentVolume = 0.32f;

volatile float audioLevel = 0.0f;

// ============================================================================
// RECORDING SETTINGS
// ============================================================================
#define RECORD_RATE 16000
#define BUFFER_SIZE 512
#define MAX_CHUNK_SIZE 2048
uint8_t tempBuffer[MAX_CHUNK_SIZE];

#define MAX_RECORDING_SIZE (16000 * 2 * 4)
uint8_t* record_buffer = NULL;
size_t record_pos = 0;

const int START_THRESHOLD = 200;
const int SILENCE_RMS = 60;
const int SILENCE_THRESH = 30;

int silence_counter = 0;
int speech_frames = 0;
const int SPEECH_CONFIRM = 2;

unsigned long record_start_time = 0;
const unsigned long MAX_RECORD_MS = 4000;

// ============================================================================
// PLAYBACK RING BUFFER - LARGER CHUNKS
// ============================================================================
#define PLAYBACK_BUFFER_SIZE (20 * 1024)  // Slightly larger
#define PLAYBACK_CHUNK_SIZE 1024           // LARGER: less overhead
#define I2S_WRITE_TIMEOUT_MS 100

uint8_t playback_buffer[PLAYBACK_BUFFER_SIZE];
volatile size_t playback_write_pos = 0;
volatile size_t playback_read_pos = 0;
volatile bool playback_active = false;
volatile uint16_t expected_seq = 0;
volatile bool seq_initialized = false;

// PRE-BUFFER: minimum bytes before starting playback
#define PREBUFFER_THRESHOLD (PLAYBACK_BUFFER_SIZE / 2)  // 50% full bago mag-play

// ============================================================================
// TASK HANDLES
// ============================================================================
TaskHandle_t i2sTaskHandle = NULL;

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

float computeAudioLevel(uint8_t* data, size_t len) {
  int16_t* samples = (int16_t*)data;
  int count = len / 2;
  if (count == 0) return 0;
  float sum = 0;
  for (int i = 0; i < count; i++) {
    float s = samples[i];
    sum += s * s;
  }
  return sqrt(sum / count);
}

// ============================================================================
// RING BUFFER
// ============================================================================
size_t ringBufferWrite(const uint8_t* data, size_t len) {
  size_t written = 0;
  for (size_t i = 0; i < len; i++) {
    size_t next_write = (playback_write_pos + 1) % PLAYBACK_BUFFER_SIZE;
    if (next_write == playback_read_pos) {
      playback_read_pos = (playback_read_pos + 1) % PLAYBACK_BUFFER_SIZE;
    }
    playback_buffer[playback_write_pos] = data[i];
    playback_write_pos = next_write;
    written++;
  }
  return written;
}

size_t ringBufferRead(uint8_t* data, size_t len) {
  size_t read = 0;
  for (size_t i = 0; i < len; i++) {
    if (playback_read_pos == playback_write_pos) break;
    data[i] = playback_buffer[playback_read_pos];
    playback_read_pos = (playback_read_pos + 1) % PLAYBACK_BUFFER_SIZE;
    read++;
  }
  return read;
}

size_t ringBufferAvailable() {
  if (playback_write_pos >= playback_read_pos) {
    return playback_write_pos - playback_read_pos;
  }
  return PLAYBACK_BUFFER_SIZE - playback_read_pos + playback_write_pos;
}

void ringBufferClear() {
  playback_write_pos = 0;
  playback_read_pos = 0;
  seq_initialized = false;
  expected_seq = 0;
}

// ============================================================================
// VOLUME (disabled by default - uncomment if needed)
// ============================================================================
/*
void applyVolume(uint8_t* data, size_t len, float vol) {
  int16_t* samples = (int16_t*)data;
  for (size_t i = 0; i < len / 2; i++) {
    float out = samples[i] * vol;
    if (out > 32767) out = 32767;
    if (out < -32768) out = -32768;
    samples[i] = (int16_t)out;
  }
}
*/

// ============================================================================
// WAV HEADER
// ============================================================================
void buildWavHeader(uint8_t* header, uint32_t dataLen) {
  memcpy(header + 0, "RIFF", 4);
  uint32_t fileSize = 36 + dataLen;
  header[4] = fileSize & 0xFF;
  header[5] = (fileSize >> 8) & 0xFF;
  header[6] = (fileSize >> 16) & 0xFF;
  header[7] = (fileSize >> 24) & 0xFF;
  memcpy(header + 8, "WAVE", 4);
  
  memcpy(header + 12, "fmt ", 4);
  header[16] = 16; header[17] = 0; header[18] = 0; header[19] = 0;
  header[20] = 1; header[21] = 0;
  header[22] = 1; header[23] = 0;
  uint32_t sampleRate = RECORD_RATE;
  header[24] = sampleRate & 0xFF;
  header[25] = (sampleRate >> 8) & 0xFF;
  header[26] = (sampleRate >> 16) & 0xFF;
  header[27] = (sampleRate >> 24) & 0xFF;
  uint32_t byteRate = RECORD_RATE * 2;
  header[28] = byteRate & 0xFF;
  header[29] = (byteRate >> 8) & 0xFF;
  header[30] = (byteRate >> 16) & 0xFF;
  header[31] = (byteRate >> 24) & 0xFF;
  header[32] = 2; header[33] = 0;
  header[34] = 16; header[35] = 0;
  
  memcpy(header + 36, "data", 4);
  header[40] = dataLen & 0xFF;
  header[41] = (dataLen >> 8) & 0xFF;
  header[42] = (dataLen >> 16) & 0xFF;
  header[43] = (dataLen >> 24) & 0xFF;
}

// ============================================================================
// UPLOAD RECORDING
// ============================================================================
void uploadRecording() {
  if (record_pos == 0 || !webSocket.isConnected()) {
    record_pos = 0;
    return;
  }
  
  if (record_pos % 2 != 0) record_pos--;
  
  uint32_t dataLen = record_pos;
  uint32_t totalSize = 44 + dataLen;
  
  uint8_t* uploadBuffer = (uint8_t*)ps_malloc(totalSize);
  if (!uploadBuffer) {
    Serial.println("[UPLOAD] No memory");
    record_pos = 0;
    return;
  }
  
  buildWavHeader(uploadBuffer, dataLen);
  memcpy(uploadBuffer + 44, record_buffer, dataLen);
  
  Serial.printf("[UPLOAD] WAV: %d bytes\n", totalSize);
  webSocket.sendBIN(uploadBuffer, totalSize);
  free(uploadBuffer);
  record_pos = 0;
  
  Serial.println("[UPLOAD] Sent");
}

// ============================================================================
// RECORD TO BUFFER
// ============================================================================
void recordToBuffer(uint8_t* data, size_t len) {
  if (record_buffer == NULL) return;
  size_t available = MAX_RECORDING_SIZE - record_pos;
  if (len > available) len = available;
  if (len % 2 != 0) len--;
  if (len > 0) {
    memcpy(record_buffer + record_pos, data, len);
    record_pos += len;
  }
}

// ============================================================================
// I2S PLAYBACK TASK - V13 FIX: PRE-BUFFERING + RELAXED TIMING
// ============================================================================
void i2sPlaybackTask(void* parameter) {
  uint8_t i2sWriteBuffer[PLAYBACK_CHUNK_SIZE];
  
  while (true) {
    if (playback_active && isPlaying) {
      size_t buffered = ringBufferAvailable();
      
      // PRE-BUFFER: kung kulang pa, mag-silence muna
      if (buffered < PREBUFFER_THRESHOLD) {
        // Not enough data yet - write silence
        memset(i2sWriteBuffer, 0, PLAYBACK_CHUNK_SIZE);
        size_t bytes_written = 0;
        i2s_write(I2S_NUM_0, i2sWriteBuffer, PLAYBACK_CHUNK_SIZE, 
                  &bytes_written, pdMS_TO_TICKS(I2S_WRITE_TIMEOUT_MS));
      }
      else if (buffered >= PLAYBACK_CHUNK_SIZE) {
        // Enough data - play normally
        size_t read = ringBufferRead(i2sWriteBuffer, PLAYBACK_CHUNK_SIZE);
        if (read > 0) {
          size_t bytes_written = 0;
          i2s_write(I2S_NUM_0, i2sWriteBuffer, read, 
                    &bytes_written, pdMS_TO_TICKS(I2S_WRITE_TIMEOUT_MS));
        }
      }
      else {
        // Partial data - write silence to prevent gaps
        memset(i2sWriteBuffer, 0, PLAYBACK_CHUNK_SIZE);
        size_t bytes_written = 0;
        i2s_write(I2S_NUM_0, i2sWriteBuffer, PLAYBACK_CHUNK_SIZE, 
                  &bytes_written, pdMS_TO_TICKS(I2S_WRITE_TIMEOUT_MS));
      }
    } else {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
    
    // RELAXED: every 8ms instead of 1ms (125Hz = smooth enough)
    vTaskDelay(pdMS_TO_TICKS(8));
  }
}

// ============================================================================
// WEBSOCKET EVENT HANDLER
// ============================================================================
void webSocketEvent(WStype_t type, uint8_t* payload, size_t length) {
  switch (type) {
    case WStype_DISCONNECTED:
      Serial.printf("[WS] Disconnected\n");
      isWSConnected = false;
      isPlaying = false;
      playback_active = false;
      isPreparingPlayback = false;
      is_recording = false;
      record_pos = 0;
      ringBufferClear();
      setColor(pixels.Color(100, 0, 0));
      break;

    case WStype_CONNECTED:
      isWSConnected = true;
      isPlaying = false;
      playback_active = false;
      isPreparingPlayback = false;
      is_recording = false;
      record_pos = 0;
      ringBufferClear();
      setColor(pixels.Color(0, 0, 100));
      Serial.printf("[WS] Connected\n");
      break;

    case WStype_TEXT: {
      String msg = (char*)payload;
      Serial.printf("[WS] TEXT: %s\n", msg.c_str());
      lastActivity = millis();

      if (msg == "START_RESPONSE" || msg == "START_MUSIC") {
        isPlaying = true;
        playback_active = true;
        isPreparingPlayback = false;
        setColor(pixels.Color(200, 0, 200));
        Serial.println("[PLAY] Start");
      }
      else if (msg == "FINISH_RESPONSE" || msg == "FINISH_MUSIC") {
        isPlaying = false;
        playback_active = false;
        ringBufferClear();
        setColor(pixels.Color(0, 0, 100));
        Serial.println("[PLAY] Done");
      }
      else if (msg.startsWith("PREPARE_RESPONSE:")) {
        int totalChunks = msg.substring(17).toInt();
        Serial.printf("[PREPARE] %d chunks\n", totalChunks);
        isPreparingPlayback = true;
        isPlaying = false;
        playback_active = true;
        ringBufferClear();
        delay(300);  // Longer prep
        if (webSocket.isConnected()) {
          webSocket.sendTXT("READY");
        }
        setColor(pixels.Color(100, 100, 0));
      }
      else if (msg.startsWith("VOLUME:")) {
        currentVolume = msg.substring(7).toFloat();
        prefs.putFloat("volume", currentVolume);
        Serial.printf("[VOL] %.2f\n", currentVolume);
      }
      else if (msg.startsWith("ERROR")) {
        Serial.printf("[ERR] %s\n", msg.c_str());
        isPlaying = false;
        playback_active = false;
        isPreparingPlayback = false;
        is_recording = false;
        setColor(pixels.Color(100, 0, 0));
      }
      break;
    }

    case WStype_BIN: {
      if (length < 2) return;
      uint16_t seq = (payload[0] << 8) | payload[1];
      uint8_t* audioData = payload + 2;
      size_t audioLen = length - 2;

      if (seq_initialized) {
        if (seq != expected_seq) {
          Serial.printf("[SEQ] Skip exp:%d got:%d\n", expected_seq, seq);
        }
      } else {
        seq_initialized = true;
      }
      expected_seq = seq + 1;

      ringBufferWrite(audioData, audioLen);
      audioLevel = computeAudioLevel(audioData, audioLen);
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
// SETUP - V13
// ============================================================================
void setup() {
  Serial.begin(115200);
  pixels.begin();
  setColor(pixels.Color(50, 50, 0));

  prefs.begin("alexatron", false);
  currentVolume = prefs.getFloat("volume", 0.32f);

  // Memory
  record_buffer = (uint8_t*)ps_malloc(MAX_RECORDING_SIZE);
  if (!record_buffer) {
    Serial.println("[FATAL] No mem");
    while (1) {
      setColor(pixels.Color(255, 0, 0));
      delay(200);
      setColor(pixels.Color(0, 0, 0));
      delay(200);
    }
  }

  // WiFi
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
    .communication_format = I2S_COMM_FORMAT_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 4,
    .dma_buf_len = 128
  };
  i2s_pin_config_t mic_p = {
    .bck_io_num = MIC_BCK,
    .ws_io_num = MIC_WS,
    .data_out_num = I2S_PIN_NO_CHANGE,
    .data_in_num = MIC_SD
  };
  i2s_driver_install(I2S_NUM_1, &mic_cfg, 0, NULL);
  i2s_set_pin(I2S_NUM_1, &mic_p);

  // I2S DAC
  i2s_config_t dac_cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX),
    .sample_rate = RECORD_RATE,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format = I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format = I2S_COMM_FORMAT_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 8,
    .dma_buf_len = 256
  };
  i2s_pin_config_t dac_p = {
    .bck_io_num = I2S_BCK,
    .ws_io_num = I2S_WS,
    .data_out_num = I2S_DOUT,
    .data_in_num = I2S_PIN_NO_CHANGE
  };
  i2s_driver_install(I2S_NUM_0, &dac_cfg, 0, NULL);
  i2s_set_pin(I2S_NUM_0, &dac_p);

  // Prime DAC
  uint8_t silence[256];
  memset(silence, 0, 256);
  for (int i = 0; i < 4; i++) {
    size_t bw = 0;
    i2s_write(I2S_NUM_0, silence, 256, &bw, portMAX_DELAY);
  }

  // WebSocket
  webSocket.beginSSL(WS_HOST, WS_PORT, WS_PATH);
  webSocket.onEvent(webSocketEvent);
  webSocket.setReconnectInterval(10000);
  webSocket.enableHeartbeat(30000, 10000, 2);

  // Task
  xTaskCreatePinnedToCore(
    i2sPlaybackTask,
    "I2S_Playback",
    4096,
    NULL,
    3,  // HIGHER priority!
    &i2sTaskHandle,
    0
  );

  Serial.println("=================================");
  Serial.println("[SETUP] V13 Choppy Fix");
  Serial.println("=================================");
  setColor(pixels.Color(0, 100, 0));

  lastActivity = millis();
  lastWSPing = millis();
}

// ============================================================================
// LOOP
// ============================================================================
void loop() {
  webSocket.loop();
  yield();

  unsigned long now = millis();

  if (isWSConnected && (now - lastActivity > 60000)) {
    if (isPlaying || isPreparingPlayback) {
      isPlaying = false;
      playback_active = false;
      isPreparingPlayback = false;
      ringBufferClear();
      setColor(pixels.Color(0, 0, 100));
    }
  }

  if (!isWSConnected) {
    if (is_recording) {
      is_recording = false;
      record_pos = 0;
    }
    return;
  }

  if (isPlaying || isPreparingPlayback) return;

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
      record_pos = 0;
      silence_counter = 0;
      speech_frames = 0;
      record_start_time = millis();
      setColor(pixels.Color(0, 255, 255));
      Serial.println("[REC] Start");
    }
  }
  else {
    recordToBuffer((uint8_t*)sample_buffer, bytes_read);

    if (rms < SILENCE_RMS) {
      silence_counter++;
      if (silence_counter > SILENCE_THRESH) {
        Serial.println("[REC] Silence -> Upload");
        uploadRecording();
        is_recording = false;
        return;
      }
    }
    else {
      silence_counter = 0;
    }

    if (millis() - record_start_time > MAX_RECORD_MS) {
      Serial.println("[REC] Max -> Upload");
      uploadRecording();
      is_recording = false;
    }
  }
}
