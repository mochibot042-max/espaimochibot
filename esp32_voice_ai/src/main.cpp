#include <WiFi.h>
#include <WiFiManager.h>
#include <WebSocketsClient.h>
#include <driver/i2s.h>
#include <Adafruit_NeoPixel.h>
#include <Preferences.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SH110X.h>

const char* WS_HOST = "espaimochibot.onrender.com";
const int WS_PORT   = 443;
const char* WS_PATH = "/ws/audio";

#define LED_PIN        48
#define MIC_BCK        16
#define MIC_WS         17
#define MIC_SD         18
#define I2S_BCK         4
#define I2S_WS          5
#define I2S_DOUT        6
#define OLED_SDA       20
#define OLED_SCL       21

#define SERVO_TILT_PIN 13
#define SERVO_PAN_PIN  14

#define SERVO_PWM_FREQ     50
#define SERVO_PWM_RES      14

#define SERVO_MIN_US       500
#define SERVO_MAX_US       2400

#define PAN_MIN_ANGLE      0
#define PAN_MAX_ANGLE      180
#define TILT_MIN_ANGLE     0
#define TILT_MAX_ANGLE     90

#define IDLE_TILT_MIN      50
#define IDLE_TILT_MAX      85
#define IDLE_PAN_MIN       60
#define IDLE_PAN_MAX       120

// TTS/MUSIC BUFFER - Mas malaki para sure
#define AUDIO_CHUNK_SIZE 2048
#define RECORD_BUFFER_SIZE (64*1024)

Adafruit_NeoPixel pixels(1, LED_PIN, NEO_GRB + NEO_KHZ800);
Adafruit_SH1106G display = Adafruit_SH1106G(128, 64, &Wire, -1);

WebSocketsClient webSocket;
Preferences prefs;
TaskHandle_t faceTaskHandle = NULL;

volatile bool is_recording  = false;
volatile bool isPlaying     = false;
volatile bool isWSConnected = false;
volatile bool isWiFiConnected = false;

float currentVolume = 0.32f;
volatile float audioLevel = 0.0f;

#define RECORD_RATE 44100
#define BUFFER_SIZE 512

uint8_t* record_buffer = nullptr;
size_t record_pos = 0;

const int START_THRESHOLD = 260;
const int SILENCE_RMS     = 120;
const int SILENCE_THRESH  = 110;
int silence_counter = 0;
int speech_frames   = 0;
const int SPEECH_CONFIRM = 4;
unsigned long record_start_time = 0;
const unsigned long MAX_RECORD_MS = 8000;

float currentFaceX = 0, currentFaceY = 0;
float targetFaceX = 0, targetFaceY = 0;
float easing = 0.15f;
unsigned long blinkTimer = 0, eyeMoveTimer = 0, animUpdateTimer = 0;
bool blink = false;

float current_pan  = 90;
float current_tilt = 60;
int target_pan   = 90;
int target_tilt  = 60;
unsigned long lastServoUpdate = 0;
unsigned long headMoveTimer = 0;
unsigned long servoOverrideTimer = 0;

bool danceMode = false;
unsigned long lastDanceMove = 0;
int danceStep = 0;

bool servoInitialized = false;
bool servosEnabled = false;
bool servoOverride = false;

char statusMsg[32] = "Boot...";

// Audio playback buffer - CRITICAL FOR TTS
uint8_t audioBuffer[AUDIO_CHUNK_SIZE];
volatile size_t audioBufferLen = 0;
volatile bool audioDataReady = false;

uint32_t usToDuty(int microseconds) {
  return (uint32_t)(microseconds * 16383.0 / 20000.0);
}

int angleToUs(int angle) {
  if (angle < 0) angle = 0;
  if (angle > 180) angle = 180;
  return map(angle, 0, 180, SERVO_MIN_US, SERVO_MAX_US);
}

void servoWrite(int pin, int angle) {
  if (pin == SERVO_PAN_PIN) {
    if (angle < PAN_MIN_ANGLE) angle = PAN_MIN_ANGLE;
    if (angle > PAN_MAX_ANGLE) angle = PAN_MAX_ANGLE;
  }
  if (pin == SERVO_TILT_PIN) {
    if (angle < TILT_MIN_ANGLE) angle = TILT_MIN_ANGLE;
    if (angle > TILT_MAX_ANGLE) angle = TILT_MAX_ANGLE;
  }
  int us = angleToUs(angle);
  uint32_t duty = usToDuty(us);
  ledcWrite(pin, duty);
}

void initServos() {
  if (!ledcAttach(SERVO_PAN_PIN, SERVO_PWM_FREQ, SERVO_PWM_RES)) {
    Serial.println("[SERVO] Failed PAN!");
    return;
  }
  delay(50);
  if (!ledcAttach(SERVO_TILT_PIN, SERVO_PWM_FREQ, SERVO_PWM_RES)) {
    Serial.println("[SERVO] Failed TILT!");
    return;
  }
  delay(50);

  target_pan = 90; target_tilt = 60;
  current_pan = 90.0f; current_tilt = 60.0f;
  servoWrite(SERVO_PAN_PIN, 90);
  servoWrite(SERVO_TILT_PIN, 60);

  servoInitialized = true;
  servosEnabled = true;
  headMoveTimer = millis() + 3000;
  Serial.println("[SERVO] Ready!");
}

void setColor(uint32_t color) {
  pixels.setPixelColor(0, color);
  pixels.show();
}

void oledPrint(const char* line1, const char* line2) {
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SH110X_WHITE);
  display.setCursor(0, 0);
  display.print(line1);
  if (strlen(line2) > 0) {
    display.setCursor(0, 10);
    display.print(line2);
  }
  display.display();
}

void updateStatus(const char* msg) {
  strncpy(statusMsg, msg, 31);
  statusMsg[31] = '\0';
  Serial.println(msg);
  oledPrint(statusMsg, "");
}

float calculateRMS(int16_t* buffer, size_t samples) {
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
  for (int i = 0; i < count; i++) sum += (float)samples[i] * samples[i];
  return sqrt(sum / count);
}

void applyVolume(uint8_t* data, size_t len, float vol) {
  int16_t* samples = (int16_t*)data;
  for (size_t i = 0; i < len / 2; i++) {
    int32_t scaled = (int32_t)(samples[i] * vol);
    if (scaled > 32767) scaled = 32767;
    if (scaled < -32768) scaled = -32768;
    samples[i] = (int16_t)scaled;
  }
}

// STREAMING: Send chunk immediately
void streamChunkToServer(int16_t* samples, size_t sampleCount) {
  if (!isWSConnected) return;
  size_t byteCount = sampleCount * 2;
  webSocket.sendBIN((uint8_t*)samples, byteCount);
}

void sendToServer() {
  is_recording = false;
  updateStatus("Sending...");
  webSocket.sendTXT("END_SPEECH");
  setColor(pixels.Color(0, 0, 100));
  record_pos = 0;
  updateStatus("Sent!");
}

void updateDance() {
  if (!danceMode || !isPlaying) return;
  if (millis() - lastDanceMove < 400) return;
  
  lastDanceMove = millis();
  danceStep++;
  
  switch (danceStep % 8) {
    case 0: target_pan = 0; target_tilt = 45; break;
    case 1: target_pan = 180; target_tilt = 45; break;
    case 2: target_pan = 90; target_tilt = 90; break;
    case 3: target_pan = 90; target_tilt = 0; break;
    case 4: target_pan = 45; target_tilt = 70; break;
    case 5: target_pan = 135; target_tilt = 70; break;
    case 6: target_pan = 0; target_tilt = 20; break;
    case 7: target_pan = 180; target_tilt = 20; break;
  }
  
  servoOverride = true;
  servoOverrideTimer = millis() + 500;
}

void faceTask(void* pv) {
  while (true) {
    if (isWSConnected && millis() - animUpdateTimer >= 30) {
      animUpdateTimer = millis();

      if (!blink && millis() > blinkTimer) {
        blink = true;
        blinkTimer = millis() + 150;
      }
      if (blink && millis() > blinkTimer) {
        blink = false;
        blinkTimer = millis() + random(2000, 5000);
      }

      if (millis() > eyeMoveTimer) {
        targetFaceX = random(-12, 13);
        targetFaceY = random(-6, 7);
        eyeMoveTimer = millis() + random(1000, 3000);
      }

      display.clearDisplay();
      display.setTextSize(1);
      display.setTextColor(SH110X_WHITE);
      display.setCursor(0, 0);
      display.print(statusMsg);
      display.setCursor(110, 0);
      if (isWSConnected) display.print("OK");
      else if (isWiFiConnected) display.print("WF");
      else display.print("--");

      currentFaceX += (targetFaceX - currentFaceX) * easing;
      currentFaceY += (targetFaceY - currentFaceY) * easing;

      int centerX = 64 + currentFaceX;
      int centerY = 36 + currentFaceY;
      int eyeDist = 22;
      int leftEyeX = centerX - eyeDist;
      int rightEyeX = centerX + eyeDist;
      int eyeY = centerY - 5;
      int mouthX = centerX;
      int mouthY = centerY + 12;

      if (blink) {
        display.fillRect(leftEyeX - 5, eyeY, 10, 2, SH110X_WHITE);
        display.fillRect(rightEyeX - 5, eyeY, 10, 2, SH110X_WHITE);
      } else {
        display.fillCircle(leftEyeX, eyeY, 4, SH110X_WHITE);
        display.fillCircle(rightEyeX, eyeY, 4, SH110X_WHITE);
      }

      if (is_recording) {
        display.fillRoundRect(mouthX - 10, mouthY - 4, 20, 10, 3, SH110X_WHITE);
      } 
      else if (isPlaying) {
        if (danceMode) {
          display.fillCircle(mouthX, mouthY + 5, 8, SH110X_WHITE);
        }
        else if (audioLevel > 2000) display.fillRoundRect(mouthX - 12, mouthY - 6, 24, 14, 4, SH110X_WHITE);
        else if (audioLevel > 900) display.fillRoundRect(mouthX - 10, mouthY - 3, 20, 8, 4, SH110X_WHITE);
        else display.fillRoundRect(mouthX - 8, mouthY, 16, 4, 2, SH110X_WHITE);
      } 
      else {
        display.fillRect(mouthX - 12, mouthY, 24, 3, SH110X_WHITE);
      }

      display.display();
    }
    vTaskDelay(10 / portTICK_PERIOD_MS);
  }
}

void webSocketEvent(WStype_t type, uint8_t* payload, size_t length) {
  switch (type) {
    case WStype_DISCONNECTED:
      Serial.println("[WS] Disconnected!");
      isWSConnected = false;
      is_recording = false;
      danceMode = false;
      isPlaying = false;
      setColor(pixels.Color(100, 0, 0));
      break;

    case WStype_CONNECTED:
      Serial.println("[WS] Connected!");
      isWSConnected = true;
      setColor(pixels.Color(0, 0, 100));
      updateStatus("Connected!");
      webSocket.sendTXT("ping");
      break;

    case WStype_TEXT: {
      String msg = (char*)payload;
      Serial.printf("[TXT] %s\n", msg.c_str());
      
      if (msg == "START_RESPONSE") {
        Serial.println("[TTS] Starting playback...");
        isPlaying = true;
        danceMode = false;
        setColor(pixels.Color(200, 0, 200));
        updateStatus("Speaking...");
      } 
      else if (msg == "START_MUSIC") {
        Serial.println("[MUSIC] Starting playback...");
        isPlaying = true;
        audioBufferLen = 0;
        audioDataReady = false;
        setColor(pixels.Color(0, 200, 200));
        updateStatus("Playing...");
      }
      else if (msg == "FINISH_RESPONSE") {
        Serial.println("[TTS] Finished");
        isPlaying = false;
        setColor(pixels.Color(0, 0, 100));
        updateStatus("Ready");
      } 
      else if (msg == "FINISH_MUSIC") {
        Serial.println("[MUSIC] Finished");
        isPlaying = false;
        danceMode = false;
        setColor(pixels.Color(0, 0, 100));
        target_pan = 90;
        target_tilt = 60;
        servoOverride = false;
        updateStatus("Done!");
      } 
      else if (msg == "pong") {
        // Heartbeat
      }
      else if (msg.indexOf("\"type\":\"dance\"") >= 0) {
        if (msg.indexOf("\"action\":\"start\"") >= 0) {
          danceMode = true;
          danceStep = 0;
          lastDanceMove = millis();
          updateStatus("DANCING!");
        }
        else if (msg.indexOf("\"action\":\"stop\"") >= 0) {
          danceMode = false;
          servoOverride = false;
          target_pan = 90;
          target_tilt = 60;
        }
      }
      else if (msg.indexOf("\"type\":\"volume\"") >= 0) {
        int volIdx = msg.indexOf("\"volume\":");
        if (volIdx >= 0) {
          int start = volIdx + 9;
          int end = msg.indexOf(',', start);
          if (end < 0) end = msg.indexOf('}', start);
          float newVol = msg.substring(start, end).toFloat();
          if (newVol > 0 && newVol <= 2.0) {
            currentVolume = newVol;
            prefs.putFloat("volume", currentVolume);
            Serial.printf("[VOL] %.2f\n", currentVolume);
          }
        }
      }
      else if (msg.indexOf("\"type\":\"servo\"") >= 0) {
        int panIdx = msg.indexOf("\"pan\":");
        int tiltIdx = msg.indexOf("\"tilt\":");
        
        if (panIdx >= 0) {
          int start = panIdx + 6;
          int end = msg.indexOf(',', start);
          if (end < 0) end = msg.indexOf('}', start);
          int cmdPan = msg.substring(start, end).toInt();
          if (cmdPan >= 0) target_pan = cmdPan;
        }
        
        if (tiltIdx >= 0) {
          int start = tiltIdx + 7;
          int end = msg.indexOf(',', start);
          if (end < 0) end = msg.indexOf('}', start);
          int cmdTilt = msg.substring(start, end).toInt();
          if (cmdTilt >= 0) target_tilt = cmdTilt;
        }
        
        servoOverride = true;
        servoOverrideTimer = millis() + 10000;
        headMoveTimer = millis() + 10000;
      }
      break;
    }

    case WStype_BIN: {
      // CRITICAL: TTS Audio playback
      if (!isPlaying) {
        Serial.println("[BIN] Warning: Received audio but isPlaying=false");
        return;
      }
      
      if (length > 0 && length <= AUDIO_CHUNK_SIZE) {
        memcpy((void*)audioBuffer, payload, length);
        audioBufferLen = length;
        audioDataReady = true;
        Serial.printf("[BIN] Received %d bytes\n", length);
      } else {
        Serial.printf("[BIN] Error: Invalid size %d\n", length);
      }
      break;
    }

    default:
      break;
  }
}

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n\n[BOOT] Starting...");

  pixels.begin();
  setColor(pixels.Color(50, 50, 0));

  Wire.begin(OLED_SDA, OLED_SCL, 400000);
  display.begin(0x3C, true);
  display.clearDisplay();
  display.setTextSize(2);
  display.setCursor(0, 20);
  display.println("ALEXATRON");
  display.display();
  delay(1500);

  prefs.begin("alexatron", false);
  currentVolume = prefs.getFloat("volume", 0.32f);
  Serial.printf("[BOOT] Volume: %.2f\n", currentVolume);

  // Allocate record buffer in PSRAM
  record_buffer = (uint8_t*)ps_malloc(RECORD_BUFFER_SIZE);
  if (!record_buffer) {
    Serial.println("[ERROR] Failed to allocate record buffer!");
  }

  WiFiManager wm;
  if (!wm.autoConnect("Alexatron")) {
    ESP.restart();
  }
  isWiFiConnected = true;
  Serial.println("[WIFI] Connected");

  // I2S MIC
  i2s_config_t mic_cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX),
    .sample_rate = RECORD_RATE,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format = I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format = I2S_COMM_FORMAT_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 4,
    .dma_buf_len = 256
  };
  i2s_pin_config_t mic_p = {
    .bck_io_num = MIC_BCK,
    .ws_io_num = MIC_WS,
    .data_out_num = I2S_PIN_NO_CHANGE,
    .data_in_num = MIC_SD
  };
  i2s_driver_install(I2S_NUM_1, &mic_cfg, 0, NULL);
  i2s_set_pin(I2S_NUM_1, &mic_p);
  Serial.println("[I2S] MIC ready");

  // I2S DAC
  i2s_config_t dac_cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX),
    .sample_rate = RECORD_RATE,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format = I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format = I2S_COMM_FORMAT_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 4,
    .dma_buf_len = 512
  };
  i2s_pin_config_t dac_p = {
    .bck_io_num = I2S_BCK,
    .ws_io_num = I2S_WS,
    .data_out_num = I2S_DOUT,
    .data_in_num = I2S_PIN_NO_CHANGE
  };
  i2s_driver_install(I2S_NUM_0, &dac_cfg, 0, NULL);
  i2s_set_pin(I2S_NUM_0, &dac_p);
  Serial.println("[I2S] DAC ready");

  webSocket.beginSSL(WS_HOST, WS_PORT, WS_PATH);
  webSocket.onEvent(webSocketEvent);
  webSocket.setReconnectInterval(3000);
  webSocket.enableHeartbeat(60000, 10000, 2);
  Serial.println("[WS] Initialized");

  xTaskCreatePinnedToCore(faceTask, "faceTask", 4096, NULL, 1, &faceTaskHandle, 1);
}

void loop() {
  // Handle WebSocket
  webSocket.loop();

  // Servo init
  if (!servoInitialized && millis() > 10000) {
    Serial.println("[SERVO] Init...");
    initServos();
  }

  // Servo movement
  if (servoInitialized && servosEnabled && millis() - lastServoUpdate > 20) {
    lastServoUpdate = millis();

    if (danceMode) updateDance();

    if (servoOverride && millis() > servoOverrideTimer) {
      servoOverride = false;
    }

    if (!servoOverride && !danceMode && millis() > headMoveTimer) {
      target_pan = random(IDLE_PAN_MIN, IDLE_PAN_MAX + 1);
      target_tilt = random(IDLE_TILT_MIN, IDLE_TILT_MAX + 1);
      headMoveTimer = millis() + random(3000, 7000);
    }

    float servoEase = danceMode ? 0.25f : 0.08f;
    current_pan += (target_pan - current_pan) * servoEase;
    current_tilt += (target_tilt - current_tilt) * servoEase;

    servoWrite(SERVO_PAN_PIN, (int)round(current_pan));
    servoWrite(SERVO_TILT_PIN, (int)round(current_tilt));
  }

  // ===== TTS/MUSIC PLAYBACK =====
  if (isPlaying && audioDataReady) {
    size_t bytes_written = 0;
    
    // Apply volume
    if (currentVolume != 1.0f) {
      applyVolume((uint8_t*)audioBuffer, audioBufferLen, currentVolume);
    }
    
    // Write to DAC
    esp_err_t err = i2s_write(I2S_NUM_0, audioBuffer, audioBufferLen, &bytes_written, portMAX_DELAY);
    
    if (err != ESP_OK) {
      Serial.printf("[I2S] Write error: %d\n", err);
    } else if (bytes_written != audioBufferLen) {
      Serial.printf("[I2S] Partial write: %d/%d\n", bytes_written, audioBufferLen);
    }
    
    // Update audio level for visualization
    audioLevel = computeAudioLevel(audioBuffer, audioBufferLen);
    
    // Mark as consumed
    audioDataReady = false;
    audioBufferLen = 0;
  }

  // Don't record while playing
  if (!isWSConnected || isPlaying) return;

  // ===== STREAMING AUDIO RECORDING =====
  int16_t sample_buffer[BUFFER_SIZE / 2];
  size_t bytes_read = 0;
  i2s_read(I2S_NUM_1, sample_buffer, BUFFER_SIZE, &bytes_read, 10);
  if (bytes_read == 0) return;

  int samplesRead = bytes_read / 2;
  float rms = calculateRMS(sample_buffer, samplesRead);

  if (!is_recording) {
    // Waiting for speech
    if (rms > START_THRESHOLD) speech_frames++;
    else speech_frames = 0;

    if (speech_frames >= SPEECH_CONFIRM) {
      // START STREAMING
      is_recording = true;
      record_pos = 0;
      silence_counter = 0;
      speech_frames = 0;
      record_start_time = millis();
      setColor(pixels.Color(0, 255, 0));
      updateStatus("Listening...");
      
      // Send first chunk
      streamChunkToServer(sample_buffer, samplesRead);
    }
  } else {
    // STREAMING: Send every chunk
    streamChunkToServer(sample_buffer, samplesRead);
    
    // Check for silence
    if (rms < SILENCE_RMS) {
      silence_counter++;
    } else {
      silence_counter = 0;
    }

    // Stop conditions
    if (silence_counter > SILENCE_THRESH || (millis() - record_start_time > MAX_RECORD_MS)) {
      sendToServer();
    }
  }
}
