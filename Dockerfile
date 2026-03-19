# =============================================================================
# Broker Pipeline — multi-stage Docker build
# Stage 1: build the React frontend
# Stage 2: Python app server (FastAPI + uvicorn) serving API + static files
# =============================================================================

# ── Stage 1: Frontend build ──────────────────────────────────────────────────
FROM node:20-alpine AS frontend

WORKDIR /build
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ── Stage 2: Python backend ─────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# System deps needed by psycopg2-binary, chromadb, crawl4ai, playwright etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        gnupg \
        # Playwright browser deps
        libnss3 \
        libnspr4 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxrandr2 \
        libgbm1 \
        libpango-1.0-0 \
        libcairo2 \
        libasound2 \
        libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI so the broker can manage scraper containers on the host
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (cache-friendly)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir uvicorn[standard] fastapi \
    && python -m playwright install chromium 2>/dev/null || true

# Copy application code
COPY broker/ broker/
COPY server/ server/
COPY queries/ queries/
COPY worker_unified.py pipeline.py start.py ./

# Copy built frontend into a static dir the server will serve
COPY --from=frontend /build/dist /app/frontend/dist

# Create runtime dirs (will be overlaid by volumes)
RUN mkdir -p /app/runtime/chroma \
             /app/runtime/status \
             /app/runtime/logs \
             /app/runtime/downloaded_files \
             /app/runtime/gmapsdata \
             /app/data

# Default env vars (override via docker-compose / .env)
ENV PYTHONUNBUFFERED=1 \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres \
    POSTGRES_HOST=postgres \
    POSTGRES_PORT=5432 \
    CHROMA_DIR=/app/runtime/chroma \
    OPENAI_API_KEY=""

EXPOSE 8000

# Serve API + static frontend
CMD ["python", "-m", "uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8000"]
