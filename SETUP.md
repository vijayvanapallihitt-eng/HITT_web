# Broker Pipeline — Complete Setup Guide

This document walks through everything needed to get the Broker Pipeline running on a fresh machine. There are two paths:

- **Path A: Docker (Recommended)** — simplest, works on any machine
- **Path B: Local Development** — for developers who want to modify code

---

## Path A: Docker Setup (Recommended)

### Prerequisites

| Requirement | How to Get It |
|---|---|
| **Docker Desktop** | Download from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/). Install, open it, wait until it says "Docker Desktop is running" in the bottom-left. |
| **OpenAI API Key** | You need a key that starts with `sk-proj-...`. Get one from [platform.openai.com/api-keys](https://platform.openai.com/api-keys) or ask your admin. |

### Step 1: Get the Code

Open a terminal (Command Prompt or PowerShell) and run:

```
git clone https://github.com/vijayvanapallihitt-eng/HITT_web.git
cd HITT_web
```

Or download the ZIP from GitHub and extract it.

### Step 2: Configure the Environment

1. In the `HITT_web` folder, find the file called `.env.example`
2. Make a copy and rename it to `.env`
3. Open `.env` with Notepad (or any text editor)
4. Paste your OpenAI API key on the `OPENAI_API_KEY=` line:

```
OPENAI_API_KEY=sk-proj-your-actual-key-here
```

5. Save and close the file

> **That's the only change needed.** Everything else has working defaults.

### Step 3: Start the Application

**Option A — Double-click:**

Double-click `START.bat` in the folder.

**Option B — Command line:**

```
docker compose up -d --build
```

The first time takes 2–3 minutes to download and build. After that, restarts take seconds.

### Step 4: Open the Dashboard

Open your browser and go to:

**http://localhost:8000**

### Stopping the Application

**Option A:** Double-click `STOP.bat`

**Option B:**

```
docker compose down
```

Your data is saved automatically in Docker volumes. It will be there next time you start.

### Deleting All Data and Starting Fresh

```
docker compose down -v
```

The `-v` flag removes the database and all stored data.

---

## Path B: Local Development Setup

### Prerequisites

| Requirement | Version | How to Check |
|---|---|---|
| **Python** | 3.11 or higher | `python --version` |
| **Node.js** | 18 or higher | `node --version` |
| **PostgreSQL** | 14 or higher | `psql --version` |
| **Git** | Any | `git --version` |
| **OpenAI API Key** | — | Starts with `sk-proj-...` |

### Step 1: Clone the Repo

```
git clone https://github.com/vijayvanapallihitt-eng/HITT_web.git
cd HITT_web
```

### Step 2: Set Up Python Environment

```
python -m venv .venv

# Activate it:
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

# Install dependencies:
pip install -r requirements.txt
pip install uvicorn[standard] fastapi
```

### Step 3: Set Up the Frontend

```
cd frontend
npm install
cd ..
```

### Step 4: Configure Environment

Copy `.env.example` to `.env` and fill in:

```
OPENAI_API_KEY=sk-proj-your-key-here

# Point to your local Postgres:
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
# Change to 5433 if your Postgres runs on a non-standard port
```

### Step 5: Make Sure PostgreSQL is Running

The app will create databases automatically through the dashboard, but Postgres itself must be running.

- **Windows:** Check Services (Win+R → `services.msc`) — look for `postgresql`
- **Mac:** `brew services start postgresql`
- **Linux:** `sudo systemctl start postgresql`

### Step 6: Start Everything

```
python start.py
```

This launches both the API server and the frontend dev server. You'll see:

```
==================================================
  Broker Dashboard
==================================================
[start] Starting API server on port 8000...
[start] Starting frontend on port 5173...

  ✓ Broker Dashboard is running!

    API (local):   http://localhost:8000
    API (network): http://192.168.1.50:8000
    UI  (local):   http://localhost:5173
    UI  (network): http://192.168.1.50:5173

  Anyone on your network can open the network URL above.
  Press Ctrl+C to stop all servers.
==================================================
```

Open **http://localhost:5173** for local development (with hot-reload).

Press **Ctrl+C** to stop both servers.

---

## Using the Dashboard

### First Time: Create a Database

1. In the sidebar, click **+ New Database**
2. Type a name like `charlotte_leads` (lowercase, underscores ok)
3. Click **Create**
4. It will appear in the database dropdown — select it

### Running a Search

1. Click **Pipeline** in the sidebar
2. Type your searches in the text box — one per line:
   ```
   Construction companies in Charlotte NC
   Home builders in Raleigh NC
   General contractors in Atlanta GA
   ```
3. **Or** click _"Generate queries for a city"_ — pick city, state, and trades to auto-generate them
4. Click the green **Run Pipeline** button
5. The pipeline will:
   - 🔍 Scrape Google Maps for companies
   - 📰 Find news articles about each company
   - 🌐 Spider their websites
   - 📦 Chunk and embed documents into the vector database
6. Watch the progress bar — it updates automatically

### Searching Your Data

1. Click **Ask** in the sidebar
2. Type a question in plain English:
   - *"What is the revenue of Turner Construction?"*
   - *"Which companies do commercial work in Charlotte?"*
   - *"Who are the largest general contractors?"*
3. Toggle **AI Answer** to get a synthesized answer with source citations
4. Filter by company using the Company Filter dropdown

### Browsing Results

Click **Results** to see all companies found, with their details, contact info, and documents.

---

## Network Access

The dashboard is accessible from any device on the same network (WiFi, LAN, VPN).

### Find Your Machine's IP

**Windows:**
```
ipconfig
```
Look for **IPv4 Address** under your network adapter (e.g., `192.168.1.50`)

**Mac/Linux:**
```
ifconfig
# or
ip addr
```

### Access from Another Device

On any other computer, phone, or tablet on the same network, open a browser and go to:

```
http://YOUR-IP:8000
```

For example: `http://192.168.1.50:8000`

### Firewall

If other devices can't connect, you may need to allow port 8000 through the firewall:

**Windows:**
1. Open Start → search "Windows Firewall"
2. Click "Advanced settings"
3. Click "Inbound Rules" → "New Rule"
4. Select "Port" → TCP → Specific port: `8000`
5. Allow the connection → give it a name like "Broker Dashboard"

---

## Architecture Overview

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

---

## Troubleshooting

| Problem | Cause | Solution |
|---|---|---|
| START.bat says "Something went wrong" | Docker Desktop not running | Open Docker Desktop, wait for it to say "running" |
| Browser shows "can't connect" | App still starting | Wait 30 seconds, refresh the page |
| "Failed to fetch" on the dashboard | Server reloading after a change | Wait a few seconds, try again |
| 500 error when creating a database | Postgres not ready | Wait 10 seconds and retry — Postgres may still be starting |
| Pipeline says "0 companies pending" | No data scraped yet | Make sure you entered queries and clicked "Run Pipeline" |
| Ask tab returns empty results | No data has been embedded yet | Run the pipeline first — it needs to process companies before you can search |
| Can't connect from another computer | Firewall blocking port 8000 | Add a firewall rule to allow TCP port 8000 (see Network Access section) |
| Docker build fails | Outdated Docker | Update Docker Desktop to the latest version |

---

## File Reference

```
HITT_web/
├── START.bat                  ← Double-click to start (Windows)
├── STOP.bat                   ← Double-click to stop (Windows)
├── .env.example               ← Template — copy to .env and add your API key
├── .env                       ← YOUR config (not in git, contains API key)
├── docker-compose.yml         ← Defines Postgres + Broker containers
├── Dockerfile                 ← Builds the Broker container image
├── README.md                  ← Quick-start guide
├── SETUP.md                   ← This file
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
│       ├── databases.py       ← Create/list/delete databases
│       ├── companies.py       ← Browse companies
│       ├── enrichment.py      ← Pipeline stats
│       ├── documents.py       ← Document listing
│       ├── vectors.py         ← Semantic search + AI answers
│       ├── scraper.py         ← Google Maps scraper control
│       ├── workers.py         ← Pipeline worker management
│       ├── queries.py         ← Query file management
│       └── research.py        ← Deep research agent
│
├── frontend/                  ← React web dashboard
│   ├── src/App.jsx            ← Main layout, sidebar, routing
│   ├── src/api.js             ← API client
│   ├── src/DbContext.jsx      ← Database selection context
│   └── src/pages/
│       ├── OverviewPage.jsx   ← Dashboard tab
│       ├── PipelinePage.jsx   ← Pipeline tab (queries + run)
│       ├── CompaniesPage.jsx  ← Results tab
│       ├── VectorSearchPage.jsx ← Ask tab
│       └── DatabasesPage.jsx  ← Settings/databases
│
├── queries/                   ← Saved search query files
├── worker_unified.py          ← Standalone unified pipeline worker
├── pipeline.py                ← Full pipeline orchestrator (CLI)
└── requirements.txt           ← Python dependencies
```
