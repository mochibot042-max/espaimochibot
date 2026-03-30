#include <WiFi.h>
#include <WiFiManager.h>
#include <WebSocketsClient.h>
#include <driver/i2s.h>
#include <Adafruit_NeoPixel.h>
#include <Preferences.h>
#include <algorithm>
#include <string.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SH110X.h>

const char* WS_HOST = "espaimochibot.onrender.com";
const int WS_PORT = 443;
const char* WS_PATH = "/ws/audio";

#define LED_PIN 48
#define MIC_BCK 16
#define MIC_WS  17
#define MIC_SD  18
#define I2S_BCK 4
#define I2S_WS  5
#define I2S_DOUT 6
#define OLED_SDA 20
#define OLED_SCL 21

Adafruit_NeoPixel pixels(1, LED_PIN, NEO_GRB + NEO_KHZ800);
Adafruit_SH1106G display = Adafruit_SH1106G(128, 64, &Wire, -1);

WebSocketsClient webSocket;
Preferences prefs;
TaskHandle_t faceTaskHandle;

bool is_recording = false;
bool isPlaying = false;
bool isWSConnected = false;
float currentVolume = 0.32f;

volatile float audioLevel = 0.0f;

// --------------------
#define RECORD_RATE 44100
#define BUFFER_SIZE 1024
#define MAX_CHUNK_SIZE 4096
uint8_t tempBuffer[MAX_CHUNK_SIZE];

#define RECORD_BUFFER_SIZE (256*1024)
uint8_t* record_buffer;
size_t record_pos = 0;

const int START_THRESHOLD = 260;
const int SILENCE_RMS = 120;
const int SILENCE_THRESH = 110;

int silence_counter = 0;
int speech_frames = 0;

const int SPEECH_CONFIRM = 4;

unsigned long record_start_time = 0;
const unsigned long MAX_RECORD_MS = 8000;

float currentFaceX = 0;
float currentFaceY = 0;

float targetFaceX = 0;
float targetFaceY = 0;

float easing = 0.15f;

unsigned long blinkTimer = 0;
unsigned long eyeMoveTimer = 0;
unsigned long animUpdateTimer = 0;

bool blink = false;
bool mouthOpen = false;

// --------------------
// Helpers
// --------------------

void setColor(uint32_t color){
  pixels.setPixelColor(0,color);
  pixels.show();
}

void showStatus(const char* msg,bool clear=true){

  if(clear) display.clearDisplay();

  display.setTextSize(1);
  display.setTextColor(SH110X_WHITE);
  display.setCursor(0,28);
  display.println(msg);
  display.display();

}

float calculateRMS(int16_t* buffer,size_t samples){

  float sum=0;

  for(size_t i=0;i<samples;i++){
    float s=buffer[i];
    sum+=s*s;
  }

  return sqrt(sum/samples);
}

// --------------------
// Audio level detect
// --------------------

float computeAudioLevel(uint8_t* data,size_t len){

  int16_t* samples=(int16_t*)data;
  int count=len/2;

  float sum=0;

  for(int i=0;i<count;i++){

    float s=samples[i];
    sum+=s*s;

  }

  return sqrt(sum/count);
}

// --------------------
// Audio Processing
// --------------------

void applyVolume(uint8_t* data,size_t len,float vol){

  int16_t* samples=(int16_t*)data;

  static float prevInput1=0;
  static float prevOutput1=0;

  static float prevInput2=0;
  static float prevOutput2=0;

  const float alpha1=0.998f;
  const float alpha2=0.996f;

  for(size_t i=0;i<len/2;i++){

    float x=(float)samples[i];

    float y1=alpha1*(prevOutput1+x-prevInput1);
    prevInput1=x;
    prevOutput1=y1;

    float y2=alpha2*(prevOutput2+y1-prevInput2);
    prevInput2=y1;
    prevOutput2=y2;

    float out=y2*vol*0.9f;

    if(out>32767) out=32767;
    if(out<-32768) out=-32768;

    samples[i]=(int16_t)out;

  }

}

// --------------------
// Send Audio
// --------------------

void sendToServer(){

  is_recording=false;

  size_t sent=0;

  while(sent<record_pos){

    size_t to_send=min((size_t)2048,record_pos-sent);

    if(webSocket.sendBIN(record_buffer+sent,to_send))
      sent+=to_send;

    webSocket.loop();

  }

  webSocket.sendTXT("END_STREAM");

  setColor(pixels.Color(0,0,100));

  record_pos=0;

}

// --------------------
// WebSocket
// --------------------

void webSocketEvent(WStype_t type,uint8_t* payload,size_t length){

  switch(type){

    case WStype_DISCONNECTED:

      isWSConnected=false;
      setColor(pixels.Color(100,0,0));
      break;

    case WStype_CONNECTED:

      isWSConnected=true;
      setColor(pixels.Color(0,0,100));
      break;

    case WStype_TEXT:{

      String msg=(char*)payload;

      if(msg=="START_RESPONSE"||msg=="START_MUSIC"){

        isPlaying=true;
        setColor(pixels.Color(200,0,200));

      }

      else if(msg=="FINISH_RESPONSE"||msg=="FINISH_MUSIC"){

        isPlaying=false;
        setColor(pixels.Color(0,0,100));

      }

      else if(msg.startsWith("VOLUME:")){

        currentVolume=msg.substring(7).toFloat();
        prefs.putFloat("volume",currentVolume);

      }

      break;
    }

    case WStype_BIN:{

      size_t bytes_written;
      uint8_t* p=payload;

      if(currentVolume!=1.0f && length<=MAX_CHUNK_SIZE){

        memcpy(tempBuffer,payload,length);
        applyVolume(tempBuffer,length,currentVolume);
        p=tempBuffer;

      }

      audioLevel=computeAudioLevel(p,length);

      i2s_write(I2S_NUM_0,p,length,&bytes_written,portMAX_DELAY);

      break;
    }

  }

}

// --------------------
// Face Animation
// --------------------

void drawFace(){

  display.clearDisplay();

  currentFaceX+=(targetFaceX-currentFaceX)*easing;
  currentFaceY+=(targetFaceY-currentFaceY)*easing;

  int centerX=64+currentFaceX;
  int centerY=32+currentFaceY;

  int eyeDist=22;

  int leftEyeX=centerX-eyeDist;
  int rightEyeX=centerX+eyeDist;

  int eyeY=centerY-5;

  int mouthX=centerX;
  int mouthY=centerY+12;

  if(isPlaying){

    if(blink){

      display.fillRect(leftEyeX-6,eyeY,12,2,SH110X_WHITE);
      display.fillRect(rightEyeX-6,eyeY,12,2,SH110X_WHITE);

    }

    else{

      display.fillCircle(leftEyeX,eyeY,5,SH110X_WHITE);
      display.fillCircle(rightEyeX,eyeY,5,SH110X_WHITE);

    }

    if(audioLevel>2000){

      display.fillRoundRect(mouthX-12,mouthY-6,24,14,4,SH110X_WHITE);

    }

    else if(audioLevel>900){

      display.fillRoundRect(mouthX-10,mouthY-3,20,8,4,SH110X_WHITE);

    }

    else{

      display.fillRoundRect(mouthX-8,mouthY,16,4,2,SH110X_WHITE);

    }

  }

  else{

    if(blink){

      display.fillRect(leftEyeX-5,eyeY,10,2,SH110X_WHITE);
      display.fillRect(rightEyeX-5,eyeY,10,2,SH110X_WHITE);

    }

    else{

      display.fillCircle(leftEyeX,eyeY,4,SH110X_WHITE);
      display.fillCircle(rightEyeX,eyeY,4,SH110X_WHITE);

    }

    display.fillRect(mouthX-12,mouthY,24,3,SH110X_WHITE);

  }

  display.display();

}

void updateFaceAnim(){

  if(millis()-animUpdateTimer<30) return;
  animUpdateTimer=millis();

  if(!blink && millis()>blinkTimer){

    blink=true;
    blinkTimer=millis()+150;

  }

  if(blink && millis()>blinkTimer){

    blink=false;
    blinkTimer=millis()+random(2000,5000);

  }

  if(millis()>eyeMoveTimer){

    targetFaceX=random(-12,13);
    targetFaceY=random(-6,7);

    eyeMoveTimer=millis()+random(1000,3000);

  }

  drawFace();

}

void faceTask(void* pv){

  while(true){

    if(isWSConnected) updateFaceAnim();

    vTaskDelay(10/portTICK_PERIOD_MS);

  }

}

// --------------------
// Setup
// --------------------

void setup(){

  Serial.begin(115200);

  pixels.begin();

  setColor(pixels.Color(50,50,0));

  Wire.begin(OLED_SDA,OLED_SCL,400000);

  display.begin(0x3C,true);

  display.clearDisplay();
  display.setTextSize(2);
  display.setCursor(0,20);
  display.println("ALEXATRON");
  display.display();

  delay(1500);

  prefs.begin("alexatron",false);

  currentVolume=prefs.getFloat("volume",0.32f);

  record_buffer=(uint8_t*)ps_malloc(RECORD_BUFFER_SIZE);

  WiFiManager wm;

  if(!wm.autoConnect("Alexatron")){

    ESP.restart();

  }

  i2s_config_t mic_cfg={
    .mode=(i2s_mode_t)(I2S_MODE_MASTER|I2S_MODE_RX),
    .sample_rate=RECORD_RATE,
    .bits_per_sample=I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format=I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format=I2S_COMM_FORMAT_I2S,
    .intr_alloc_flags=ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count=8,
    .dma_buf_len=256
  };

  i2s_pin_config_t mic_p={
    .bck_io_num=MIC_BCK,
    .ws_io_num=MIC_WS,
    .data_out_num=I2S_PIN_NO_CHANGE,
    .data_in_num=MIC_SD
  };

  i2s_driver_install(I2S_NUM_1,&mic_cfg,0,NULL);
  i2s_set_pin(I2S_NUM_1,&mic_p);

  i2s_config_t dac_cfg={
    .mode=(i2s_mode_t)(I2S_MODE_MASTER|I2S_MODE_TX),
    .sample_rate=RECORD_RATE,
    .bits_per_sample=I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format=I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format=I2S_COMM_FORMAT_I2S,
    .intr_alloc_flags=ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count=32,
    .dma_buf_len=512
  };

  i2s_pin_config_t dac_p={
    .bck_io_num=I2S_BCK,
    .ws_io_num=I2S_WS,
    .data_out_num=I2S_DOUT,
    .data_in_num=I2S_PIN_NO_CHANGE
  };

  i2s_driver_install(I2S_NUM_0,&dac_cfg,0,NULL);
  i2s_set_pin(I2S_NUM_0,&dac_p);

  webSocket.beginSSL(WS_HOST,WS_PORT,WS_PATH);

  webSocket.onEvent(webSocketEvent);

  xTaskCreatePinnedToCore(faceTask,"faceTask",4096,NULL,1,&faceTaskHandle,1);

}

// --------------------
// Loop
// --------------------

void loop(){

  webSocket.loop();

  if(!isWSConnected) return;
  if(isPlaying) return;

  int16_t sample_buffer[BUFFER_SIZE/2];

  size_t bytes_read=0;

  i2s_read(I2S_NUM_1,sample_buffer,BUFFER_SIZE,&bytes_read,10);

  if(bytes_read==0) return;

  float rms=calculateRMS(sample_buffer,bytes_read/2);

  if(!is_recording){

    if(rms>START_THRESHOLD) speech_frames++;
    else speech_frames=0;

    if(speech_frames>=SPEECH_CONFIRM){

      is_recording=true;

      record_pos=0;

      silence_counter=0;

      speech_frames=0;

      record_start_time=millis();

      setColor(pixels.Color(0,255,255));

    }

  }

  else{

    if(record_pos+bytes_read<=RECORD_BUFFER_SIZE){

      memcpy(record_buffer+record_pos,sample_buffer,bytes_read);
      record_pos+=bytes_read;

    }

    if(rms<SILENCE_RMS){

      silence_counter++;

      if(silence_counter>SILENCE_THRESH){

        sendToServer();
        return;

      }

    }

    else silence_counter=0;

    if(millis()-record_start_time>MAX_RECORD_MS){

      sendToServer();

    }

  }

}
