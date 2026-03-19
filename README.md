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

### Deleting All Data and Starting Fresh

Open a terminal in this folder and run:

```
docker compose down -v
```

Then double-click `START.bat` again.

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

### Common Problems

| Problem | Solution |
|---|---|
| START.bat says "Something went wrong" | Open Docker Desktop and make sure it's running |
| Page won't load in browser | Wait 30 seconds and refresh — the app may still be starting |
| "Failed to fetch" error on the page | The server is restarting — wait a moment and try again |
| 500 error when creating a database | Postgres is still starting — wait 10 seconds and retry |
| Pipeline isn't finding companies | Make sure Docker Desktop is running — the scraper needs it |
| Ask tab says "no results" | Run the Pipeline first — it needs data before you can search |
| Can't connect from another computer | Windows Firewall is blocking port 8000 (see section above) |

### How to Check if the App is Running

Open a browser and go to:

```
http://localhost:8000/api/health
```

If it shows `{"status":"ok"}` — the server is running. If the page doesn't load, the server is down.

### How to View Logs

Logs tell you what the app is doing behind the scenes. Open a terminal in this folder and run:

```
docker compose logs -f broker
```

This will show a live stream of everything the server is doing. Press **Ctrl+C** to stop watching.

To see just the last 50 lines:

```
docker compose logs --tail 50 broker
```

### How to View Database Logs

If database errors occur:

```
docker compose logs -f postgres
```

### How to Restart the App

If something is stuck, restart everything:

```
docker compose restart
```

Or do a full stop and start:

```
docker compose down
docker compose up -d
```

### How to Rebuild After an Update

If you pulled new code from GitHub:

```
git pull
docker compose up -d --build
```

### How to Check What Containers Are Running

```
docker compose ps
```

You should see two containers:
- `broker` — status `Up`
- `postgres` — status `Up (healthy)`

If either one says `Exited` or `Restarting`, check its logs (see above).

### How to Get Into the Container

If you need to poke around inside the running app:

```
docker compose exec broker bash
```

This gives you a shell inside the container. Type `exit` to leave.

### How to Connect to the Database Directly

```
docker compose exec postgres psql -U postgres
```

Useful commands inside `psql`:
- `\l` — list all databases
- `\c my_database` — switch to a database
- `\dt` — list tables
- `SELECT count(*) FROM results;` — count companies
- `\q` — quit

### API Endpoints for Debugging

You can hit these URLs directly in your browser to check data:

| URL | What It Shows |
|---|---|
| `http://localhost:8000/api/health` | Server status |
| `http://localhost:8000/api/databases` | All databases and their table counts |
| `http://localhost:8000/api/enrichment/stats?db=YOUR_DB` | Pipeline stats for a database |
| `http://localhost:8000/api/workers/processes` | Running worker processes |
| `http://localhost:8000/api/workers/status` | Worker status files |
| `http://localhost:8000/api/scraper/status` | Docker scraper containers |
| `http://localhost:8000/api/vectors/collections` | ChromaDB collections and sizes |
| `http://localhost:8000/api/queries` | Saved query files |

Replace `YOUR_DB` with your actual database name (e.g., `charlotte_leads`).

### Common Error Messages

| Error | What It Means | Fix |
|---|---|---|
| `connection refused` | Postgres hasn't started yet | Wait 10–15 seconds, the database starts after the app |
| `database "xyz" does not exist` | You're pointing to a database that was deleted | Create a new one from the dashboard sidebar |
| `OPENAI_API_KEY not set` | The `.env` file is missing or the key is blank | Check your `.env` file — make sure the key is pasted |
| `rate limit exceeded` | Too many OpenAI API calls | Wait a minute and try again — or reduce batch size in Advanced Settings |
| `chromadb collection not found` | No documents have been embedded yet | Run the pipeline to embed documents first |

---
---

# Developer Reference

_Everything below is for developers only._

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  User's Browser                  │
│              http://localhost:8000                │
└───────────────────┬─────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────┐
│              Broker Container                    │
│                                                  │
│   ┌──────────────┐    ┌───────────────────────┐ │
│   │  FastAPI API  │    │  React Frontend       │ │
│   │  (uvicorn)    │    │  (static files)       │ │
│   │  port 8000    │    │  served by FastAPI     │ │
│   └──────┬───────┘    └───────────────────────┘ │
│          │                                       │
│   ┌──────▼───────┐    ┌───────────────────────┐ │
│   │ Unified      │    │  ChromaDB             │ │
│   │ Pipeline     │    │  (vector embeddings)  │ │
│   │ Worker       │    │  /app/runtime/chroma  │ │
│   └──────┬───────┘    └───────────────────────┘ │
│          │                                       │
└──────────┼───────────────────────────────────────┘
           │
┌──────────▼───────────────────────────────────────┐
│            PostgreSQL Container                   │
│            (all lead data, documents)             │
│            port 5432                              │
└──────────────────────────────────────────────────┘
```

### Key Components

| Component | What It Does |
|---|---|
| **FastAPI Server** | REST API — handles all dashboard requests |
| **React Frontend** | The web UI you see in the browser |
| **Unified Pipeline** | Scrapes Google Maps, discovers news, fetches websites, chunks & embeds documents |
| **PostgreSQL** | Stores company data, links, documents |
| **ChromaDB** | Stores vector embeddings for semantic search |
| **OpenAI API** | Generates embeddings (`text-embedding-3-small`) and AI answers (`gpt-4o-mini`) |

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
It will print the local and network URLs. Open `http://localhost:5173` for hot-reload development.

### Running the Pipeline Worker Directly

```
python worker_unified.py --dsn postgresql://postgres:postgres@localhost:5432/my_db --once
```

Flags:
- `--once` — process one batch and exit
- `--batch 5` — process 5 companies per cycle
- `--poll 30` — wait 30 seconds between cycles

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

## Docker Commands Reference

```
docker compose up -d --build       # Build and start
docker compose down                # Stop (keeps data)
docker compose down -v             # Stop and delete all data
docker compose restart             # Restart everything
docker compose ps                  # Check container status
docker compose logs -f broker      # Live app logs
docker compose logs -f postgres    # Live database logs
docker compose exec broker bash    # Shell into the app container
docker compose exec postgres psql -U postgres   # Database shell
```

## Project Structure

```
HITT_web/
├── START.bat                  ← Double-click to start (Windows)
├── STOP.bat                   ← Double-click to stop (Windows)
├── .env.example               ← Template — copy to .env and add your API key
├── .env                       ← YOUR config (not in git, contains API key)
├── docker-compose.yml         ← Defines Postgres + Broker containers
├── Dockerfile                 ← Builds the Broker container image
├── README.md                  ← This file
├── start.py                   ← Local dev launcher (no Docker)
│
├── broker/                    ← Python backend modules
│   ├── config.py              ← Environment & path configuration
│   ├── models.py              ← Data models
│   ├── documents/             ← Document fetching & website spidering
│   ├── embeddings/            ← OpenAI embedding generation
│   ├── enrichment/            ← News discovery, web research
│   ├── orchestration/         ← Unified pipeline logic
│   ├── query/                 ← Vector search & AI answers
│   └── storage/               ← Postgres & ChromaDB interfaces
│
├── server/                    ← FastAPI API server
│   ├── main.py                ← App entry point, middleware, static serving
│   ├── deps.py                ← Database connection helpers
│   └── routers/               ← API route handlers
│
├── frontend/                  ← React web dashboard (Vite + Tailwind)
│   ├── src/App.jsx            ← Main layout, sidebar, routing
│   ├── src/api.js             ← API client
│   └── src/pages/             ← Dashboard, Pipeline, Results, Ask pages
│
├── queries/                   ← Saved search query files
├── worker_unified.py          ← Standalone unified pipeline worker
├── pipeline.py                ← Full pipeline orchestrator (CLI)
└── requirements.txt           ← Python dependencies
```
