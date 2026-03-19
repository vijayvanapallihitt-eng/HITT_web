# Broker Pipeline

Find and research companies automatically. Enter what you're looking for, and Broker scrapes the web, gathers news articles, and organizes everything so you can search it with plain English questions.

---

## Getting Started

### What You Need

1. **Docker Desktop** — download free from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)  
   Install it, open it, and make sure it says **"Docker Desktop is running"** in the bottom-left corner.

2. **An OpenAI API key** — you should have been given one. It looks like `sk-proj-...`

### First-Time Setup (One Time Only)

1. Find the file called **`.env.example`** in this folder
2. Make a copy of it and rename the copy to **`.env`** (just `.env`, nothing else)
3. Open `.env` with Notepad
4. Paste your OpenAI API key after `OPENAI_API_KEY=`  
   So it looks like: `OPENAI_API_KEY=sk-proj-your-key-here`
5. Save and close

### Starting the App

**Double-click `START.bat`**

Wait for it to say **"Ready!"** — then open your browser to:

### 👉 **http://localhost:8000**

The first time you start it takes 2–3 minutes to set up. After that it starts in seconds.

### Stopping the App

**Double-click `STOP.bat`**

Your data is saved automatically and will be there next time you start.

---

## How to Use the Dashboard

### The Sidebar

On the left side you'll see a **Database** dropdown. This is where your lead data lives. Click **+ New Database** to create a new one for a project (e.g., `charlotte_leads`).

### The 4 Tabs

| Tab | What You Do There |
|---|---|
| **Dashboard** | See an overview — how many companies, documents, and what's running |
| **Pipeline** | **This is the main screen.** Enter your searches here and hit Run |
| **Results** | Browse the companies the pipeline found |
| **Ask** | Type a question about your data and get an AI-powered answer |

### Step by Step: Running a Search

1. Click **Pipeline** in the sidebar
2. In the text box, type what you're looking for — one search per line:
   ```
   Construction companies in Charlotte NC
   Home builders in Raleigh NC
   General contractors in Atlanta GA
   ```
   **Tip:** Click _"Generate queries for a city"_ to auto-fill searches — just pick a city, state, and the types of companies you want.

3. Click the green **Run Pipeline** button
4. Watch the progress bar — it will:
   - 🔍 Find companies on Google Maps
   - 📰 Gather news articles about each one
   - 🌐 Visit their websites
   - 📦 Organize everything for you to search
5. When it's done, go to the **Ask** tab and ask questions like:
   - *"What is the revenue of Turner Construction?"*
   - *"Which companies do commercial work?"*
   - *"Who are the largest home builders in Charlotte?"*

---

## Accessing from Another Computer

Anyone on the same WiFi or network can use the dashboard.

1. On the computer running Broker, open a command prompt and type `ipconfig`
2. Find the line that says **IPv4 Address** — it looks like `192.168.1.50`
3. On the other computer, open a browser and go to `http://192.168.1.50:8000`

> **If it doesn't connect:** Windows Firewall might be blocking it. Go to Settings → Windows Security → Firewall → Allow an app → allow port 8000.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| START.bat says "Something went wrong" | Open Docker Desktop and make sure it's running |
| Page won't load in browser | Wait 30 seconds and refresh — the app may still be starting |
| "Failed to fetch" error on the page | The server is restarting — wait a moment and try again |
| Pipeline isn't finding companies | Make sure Docker Desktop is running — the scraper needs it |
| Ask tab says "no results" | Run the Pipeline first — it needs data before you can search |

---
---

# Developer Reference

_Everything below is for developers only._

## Local Development (without Docker)

### Prerequisites

- Python 3.11+
- Node.js 18+
- PostgreSQL running locally

### Setup

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install uvicorn[standard] fastapi

cd frontend
npm install
cd ..
```

### Run

```
python start.py
```

This starts both the API (port 8000) and frontend dev server (port 5173).  
It will print the local and network URLs.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `OPENAI_API_KEY` | ✅ | — | OpenAI API key for embeddings and AI answers |
| `POSTGRES_USER` | — | `postgres` | Database user |
| `POSTGRES_PASSWORD` | — | `postgres` | Database password |
| `POSTGRES_HOST` | — | `postgres` (Docker) / `localhost` (local) | Database host |
| `POSTGRES_PORT` | — | `5432` | Database port |
| `BROKER_PORT` | — | `8000` | Port the dashboard runs on |
| `CHROMA_DIR` | — | `./runtime/chroma` | Path to ChromaDB storage |

## Docker Commands

```
docker compose up -d --build     # Start
docker compose down              # Stop (keeps data)
docker compose down -v           # Stop and delete all data
docker compose logs -f broker    # View live logs
```

## Project Structure

```
broker/          Python backend modules (enrichment, documents, storage)
server/          FastAPI API server + route handlers
frontend/        React dashboard (Vite + Tailwind)
queries/         Saved search query files
runtime/         ChromaDB data, logs, status files (auto-created)
data/            CSV exports
Dockerfile       Multi-stage build (Node frontend + Python backend)
docker-compose.yml   Postgres + Broker containers
start.py         Local dev launcher (API + frontend together)
START.bat        One-click start for non-technical users
STOP.bat         One-click stop for non-technical users
```
