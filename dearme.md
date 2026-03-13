# Deployment Guide for Render

## Build and Run Commands

- **Build Command**: `npm run build`
- **Start Command**: `npm start`

## Environment Variables

Ensure the following environment variables are set in your Render dashboard:

- `DATABASE_URL`: Your PostgreSQL connection string.
- `GROQ_API_KEY`: Your Groq API key for transcription, chat, and TTS.
- `NODE_ENV`: `production`

## Deployment Steps

1. Connect your GitHub repository to Render.
2. Select **Web Service**.
3. Set the runtime to **Node**.
4. Configure the build and start commands as listed above.
5. Add the necessary environment variables.
6. Deploy!
