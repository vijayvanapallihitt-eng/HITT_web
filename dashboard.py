"""
Broker Pipeline Dashboard — Streamlit app.

Launch:
    streamlit run dashboard.py

Pages:
  1. Overview          — project state at a glance, architecture, funnel
  2. Company Browser   — search / filter / drill into any company
  3. Enrichment Monitor — progress, fetch-status breakdown, recent activity
  4. Vector Search     — query ChromaDB, get AI-grounded answers
  5. Scrape Queries    — manage queries, seed Docker jobs, launch full pipeline
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
QUERIES_DIR = PROJECT_ROOT / "queries"
STATUS_DIR = PROJECT_ROOT / "runtime" / "status"
CHROMA_DIR = PROJECT_ROOT / "runtime" / "chroma" / "chroma_smoke_db"
COLLECTION_NAME = "construction_docs_openai1536_live"
EMBEDDING_MODEL = "text-embedding-3-small"
VENV_PYTHON = str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")

# Load .env for OPENAI key etc.
from dotenv import load_dotenv as _load_dotenv
_load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)

_ENV_DSN = os.getenv("BROKER_CONSTRUCTION_DSN", "postgresql://postgres:postgres@localhost:5432/construction")
_ENV_DB_NAME = _ENV_DSN.rsplit("/", 1)[-1] if "/" in _ENV_DSN else "construction"

# Derive host:port from the env DSN so everything stays consistent
_parsed_dsn = _ENV_DSN.split("@")[-1].rsplit("/", 1)[0] if "@" in _ENV_DSN else "localhost:5432"
ADMIN_DSN = f"postgresql://postgres:postgres@{_parsed_dsn}/postgres"


def _list_pipeline_databases() -> list[str]:
    """Return database names that look like pipeline DBs (have a 'results' table)."""
    try:
        conn = psycopg2.connect(ADMIN_DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false AND datname NOT IN ('postgres') "
            "ORDER BY datname"
        )
        candidates = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception:
        return [_ENV_DB_NAME]

    pipeline_dbs: list[str] = []
    for db_name in candidates:
        try:
            c = psycopg2.connect(_dsn_for_db(db_name))
            c.autocommit = True
            cr = c.cursor()
            cr.execute("SELECT to_regclass('public.results')")
            has_results = cr.fetchone()[0] is not None
            cr.close()
            c.close()
            if has_results:
                pipeline_dbs.append(db_name)
        except Exception:
            continue
    return pipeline_dbs if pipeline_dbs else [_ENV_DB_NAME]


def _create_pipeline_database(db_name: str) -> str | None:
    """Create a new pipeline database with all required tables. Returns error string or None."""
    try:
        conn = psycopg2.connect(ADMIN_DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        if cur.fetchone():
            cur.close()
            conn.close()
            return f"Database '{db_name}' already exists."
        cur.execute(f'CREATE DATABASE "{db_name}"')
        cur.close()
        conn.close()
    except Exception as exc:
        return f"Failed to create database: {exc}"

    dsn = _dsn_for_db(db_name)
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id SERIAL PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gmaps_jobs (
                id UUID PRIMARY KEY,
                priority SMALLINT DEFAULT 1,
                payload_type TEXT DEFAULT '',
                payload BYTEA,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                status TEXT DEFAULT 'new'
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        return f"Tables creation failed: {exc}"

    # Create pipeline tables (link_candidates, documents, document_chunks)
    try:
        from broker.storage.postgres import ensure_pipeline_schema
        ensure_pipeline_schema(dsn=dsn)
    except Exception as exc:
        return f"Pipeline schema creation failed: {exc}"

    # Update .env to point at this database
    _update_env_file(db_name)
    return None


def _update_env_file(db_name: str) -> None:
    """Update .env so the pipeline workers also use this DB."""
    env_path = PROJECT_ROOT / ".env"
    new_dsn = _dsn_for_db(db_name)
    new_docker_dsn = _docker_dsn_for_db(db_name)
    keys_to_set = {
        "BROKER_CONSTRUCTION_DSN": new_dsn,
        "BROKER_DOCKER_CONSTRUCTION_DSN": new_docker_dsn,
    }
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    updated_keys: set[str] = set()
    new_lines: list[str] = []
    for line in lines:
        replaced = False
        for key, value in keys_to_set.items():
            if line.strip().startswith(f"{key}=") or line.strip().startswith(f"# {key}="):
                new_lines.append(f"{key}={value}")
                updated_keys.add(key)
                replaced = True
                break
        if not replaced:
            new_lines.append(line)
    for key, value in keys_to_set.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={value}")
    env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def _dsn_for_db(db_name: str) -> str:
    return f"postgresql://postgres:postgres@{_parsed_dsn}/{db_name}"


def _docker_dsn_for_db(db_name: str) -> str:
    _docker_host = _parsed_dsn.replace("localhost", "host.docker.internal")
    return f"postgres://postgres:postgres@{_docker_host}/{db_name}"


def _scraper_container_name(db_name: str) -> str:
    return f"{db_name}-scraper"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _active_db() -> str:
    return st.session_state.get("active_db", _ENV_DB_NAME)


def _active_dsn() -> str:
    return _dsn_for_db(_active_db())


def _active_docker_dsn() -> str:
    return _docker_dsn_for_db(_active_db())


def _force_reconnect():
    """Drop the cached connection and open a fresh one."""
    old = st.session_state.pop("pg_conn", None)
    if old:
        try:
            old.close()
        except Exception:
            pass
    dsn = _active_dsn()
    st.session_state.pg_conn = psycopg2.connect(dsn)
    st.session_state.pg_conn.autocommit = True
    st.session_state._pg_conn_dsn = dsn
    return st.session_state.pg_conn


def get_conn():
    dsn = _active_dsn()
    need_new = (
        "pg_conn" not in st.session_state
        or st.session_state.pg_conn.closed
        or st.session_state.get("_pg_conn_dsn") != dsn
    )
    if need_new:
        return _force_reconnect()
    # Verify the connection is still alive
    try:
        st.session_state.pg_conn.cursor().execute("SELECT 1")
    except Exception:
        return _force_reconnect()
    return st.session_state.pg_conn


def run_query(sql: str, params=None) -> pd.DataFrame:
    try:
        return pd.read_sql(sql, get_conn(), params=params)
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        return pd.read_sql(sql, _force_reconnect(), params=params)
    except psycopg2.Error:
        _force_reconnect()
        raise


def run_scalar(sql: str, params=None):
    try:
        cur = get_conn().cursor()
        cur.execute(sql, params)
        val = cur.fetchone()[0]
        cur.close()
        return val
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        cur = _force_reconnect().cursor()
        cur.execute(sql, params)
        val = cur.fetchone()[0]
        cur.close()
        return val
    except psycopg2.Error:
        _force_reconnect()
        raise


# ---------------------------------------------------------------------------
# Chroma helpers (lazy-loaded)
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner="Loading ChromaDB…")
def get_chroma_collection():
    import chromadb
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    return client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )


def get_openai_client():
    """Return a cached OpenAI client, or None if key not set."""
    if "openai_client" not in st.session_state:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)
        api_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")
        if api_key:
            from openai import OpenAI
            st.session_state.openai_client = OpenAI(api_key=api_key)
        else:
            st.session_state.openai_client = None
    return st.session_state.openai_client


def embed_query(text: str) -> list[float]:
    client = get_openai_client()
    if client is None:
        raise RuntimeError("OPENAI_API_KEY not set in .env — cannot embed query.")
    return client.embeddings.create(model=EMBEDDING_MODEL, input=[text]).data[0].embedding


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Broker Pipeline",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.title("🏗️ Broker Pipeline")

# ── Database Selector ──────────────────────────────────────────────────
st.sidebar.subheader("🗄️ Database")
_available_dbs = _list_pipeline_databases()
if "active_db" not in st.session_state:
    st.session_state.active_db = _ENV_DB_NAME

_db_idx = _available_dbs.index(st.session_state.active_db) if st.session_state.active_db in _available_dbs else 0
_selected_db = st.sidebar.selectbox(
    "Active database",
    _available_dbs,
    index=_db_idx,
    key="_db_selector",
)
if _selected_db != st.session_state.active_db:
    st.session_state.active_db = _selected_db
    _update_env_file(_selected_db)
    st.rerun()

with st.sidebar.expander("➕ Create New Database", expanded=False):
    _new_db_name = st.text_input("Database name", value="", placeholder="construction_v3", key="_new_db_input")
    if st.button("🆕 Create & Switch", key="_btn_create_db"):
        name = _new_db_name.strip().lower().replace(" ", "_")
        if not name:
            st.warning("Enter a database name.")
        else:
            with st.spinner(f"Creating {name}…"):
                err = _create_pipeline_database(name)
            if err:
                st.error(err)
            else:
                st.success(f"✅ Created **{name}**")
                st.session_state.active_db = name
                st.rerun()

st.sidebar.divider()
page = st.sidebar.radio(
    "Navigate",
    [
        "📊 Overview",
        "🔍 Company Browser",
        "⚙️ Enrichment Monitor",
        "🧠 Vector Search",
        "📈 Company Evaluations",
        "🚀 Scrape Queries",
    ],
)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 1 — Overview
# ═══════════════════════════════════════════════════════════════════════════

if page == "📊 Overview":
    st.title("📊 Pipeline Overview")

    # ── Architecture diagram ───────────────────────────────────────────
    with st.expander("🏛️ How the pipeline works", expanded=False):
        st.markdown("""
        ```
        ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐   ┌──────────────┐   ┌──────────────┐
        │ Google Maps  │   │ worker_      │   │ run_document_    │   │ ChromaDB     │   │ worker_      │
        │ Scraper      │──▶│ enrich.py    │──▶│ ingest           │──▶│ Vector DB    │──▶│ evaluate.py  │
        │ (Docker)     │   │ (News + Web) │   │ (fetch/chunk)    │   │              │   │ (GPT-4o-mini)│
        │              │   │              │   │                  │   │              │   │              │
        │ ──▶ results  │   │ ──▶ link_    │   │ ──▶ documents   │   │ ──▶ vectors  │   │ ──▶ company_ │
        │   (JSONB)    │   │  candidates  │   │ ──▶ doc_chunks  │   │   (1536-dim) │   │  evaluations │
        └──────────────┘   └──────────────┘   └──────────────────┘   └──────────────┘   └──────────────┘
              │                   │                    │                    │                    │
              │                   │                    │                    │                    │
         Google Maps         Google News          Fetches URLs,       OpenAI embed        Pulls ALL chunks
         queries →           articles +           extracts text,      text-embedding-     per company →
         company name,       company website      chunks (220w),      3-small →           GPT-4o-mini
         phone, address,     URLs seeded as       embeds via          cosine similarity   extracts revenue,
         website, rating,    link_candidates      OpenAI → Chroma     search              headcount, evidence
         category, etc.      (news + website)                                             with article citations
        ```

        **Stage 1 — Scrape:** Docker container (`gosom/google-maps-scraper`) runs Google Maps queries
        → stores company listings as JSONB in `results` table (name, phone, website, address, category, rating).

        **Stage 2 — Enrich:** `worker_enrich.py` does two things:
        - 📰 **News discovery** — searches Google News for `"Company Name" construction` → stores article URLs
        - 🌐 **Website seeding** — takes each company's `web_site` from Google Maps → adds it as a link candidate
        Both go into `link_candidates` with `source_type` = `news` or `website`.

        **Stage 3 — Ingest:** `run_document_ingest.py` fetches all pending URLs (news articles + company websites),
        extracts text, chunks it (220 words, 50-word overlap), embeds with OpenAI `text-embedding-3-small` (1536-dim)
        → stores in `documents`, `document_chunks` (Postgres) and vectors in ChromaDB.

        **Stage 4 — Evaluate:** `worker_evaluate.py` pulls **ALL** embedded chunks for each company
        (news + website content) → sends to GPT-4o-mini for structured extraction → stores revenue estimate,
        headcount estimate, confidence levels, and evidence summary (with article title + URL citations)
        in `company_evaluations`.

        **Stage 5 — Query:** Ask natural-language questions on the **🧠 Vector Search** page → retrieves
        relevant chunks via cosine similarity → optional GPT-grounded answer.

        **View Results:** The **📈 Company Evaluations** page shows all companies in a CSV-style table
        with filters, revenue/headcount data, confidence levels, and evidence summaries citing source articles.
        """)

    # ── Top-level metrics ──────────────────────────────────────────────
    counts = run_query("""
        SELECT
            (SELECT count(*) FROM results)                              AS results,
            (SELECT count(DISTINCT result_id) FROM link_candidates)     AS enriched,
            (SELECT count(*) FROM link_candidates)                      AS link_candidates,
            (SELECT count(*) FROM documents)                            AS documents,
            (SELECT count(*) FROM documents WHERE fetch_status = 'ok')  AS docs_ok,
            (SELECT count(*) FROM document_chunks)                      AS chunks,
            (SELECT count(*) FROM gmaps_jobs)                           AS scraper_jobs
    """).iloc[0]

    # Chroma count
    try:
        chroma_count = get_chroma_collection().count()
    except Exception:
        chroma_count = 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🏢 Companies Scraped", f"{int(counts.results):,}")
    c2.metric("🔗 Enriched", f"{int(counts.enriched):,}",
              delta=f"{int(counts.enriched)/max(int(counts.results),1)*100:.1f}% of total")
    c3.metric("📄 Documents OK", f"{int(counts.docs_ok):,}")
    c4.metric("🧠 Vectors in Chroma", f"{chroma_count:,}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Link Candidates", f"{int(counts.link_candidates):,}")
    c6.metric("Total Documents", f"{int(counts.documents):,}")
    c7.metric("Text Chunks", f"{int(counts.chunks):,}")
    c8.metric("Scraper Jobs", f"{int(counts.scraper_jobs):,}")

    st.divider()

    # ── Funnel chart ───────────────────────────────────────────────────
    col_funnel, col_health = st.columns([3, 2])

    with col_funnel:
        funnel_data = pd.DataFrame({
            "Stage": [
                "Scraped Companies",
                "Enriched (has links)",
                "Documents Fetched",
                "Docs OK (clean text)",
                "Chunks in Postgres",
                "Vectors in Chroma",
            ],
            "Count": [
                int(counts.results), int(counts.enriched),
                int(counts.documents), int(counts.docs_ok),
                int(counts.chunks), chroma_count,
            ],
        })
        fig_funnel = go.Figure(go.Funnel(
            y=funnel_data["Stage"],
            x=funnel_data["Count"],
            textinfo="value+percent initial",
            marker=dict(color=["#1e40af", "#2563eb", "#3b82f6", "#60a5fa", "#93c5fd", "#bfdbfe"]),
        ))
        fig_funnel.update_layout(title="Pipeline Funnel", height=380, margin=dict(t=40, b=20))
        st.plotly_chart(fig_funnel, use_container_width=True)

    with col_health:
        st.subheader("Data Health")
        fetch_df = run_query("""
            SELECT fetch_status, count(*) AS cnt
            FROM documents GROUP BY 1 ORDER BY 2 DESC
        """)
        if not fetch_df.empty:
            colors = {
                "ok": "#22c55e", "http_error": "#ef4444", "request_error": "#f97316",
                "irrelevant": "#a3a3a3", "empty_text": "#eab308",
                "unsupported_content_type": "#8b5cf6",
            }
            fig_health = px.pie(fetch_df, values="cnt", names="fetch_status",
                                color="fetch_status", color_discrete_map=colors,
                                hole=0.4)
            fig_health.update_layout(height=380, margin=dict(t=20, b=20),
                                     legend=dict(orientation="h", y=-0.15))
            st.plotly_chart(fig_health, use_container_width=True)
        else:
            st.info("No documents fetched yet.")

    # ── Two columns: category + state breakdown ────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top Categories")
        cats = run_query("""
            SELECT data->>'category' AS category, count(*) AS cnt
            FROM results WHERE data->>'category' IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 12
        """)
        if not cats.empty:
            fig_cat = px.bar(cats, x="cnt", y="category", orientation="h",
                             color_discrete_sequence=["#2563eb"])
            fig_cat.update_layout(height=380, yaxis=dict(autorange="reversed"),
                                  xaxis_title="Companies", yaxis_title="")
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.info("No category data yet.")

    with col_right:
        st.subheader("Top States")
        states = run_query("""
            SELECT data->'complete_address'->>'state' AS state, count(*) AS cnt
            FROM results
            WHERE data->'complete_address'->>'state' IS NOT NULL
              AND data->'complete_address'->>'state' <> ''
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """)
        if not states.empty:
            fig_st = px.bar(states, x="cnt", y="state", orientation="h",
                            color_discrete_sequence=["#059669"])
            fig_st.update_layout(height=380, yaxis=dict(autorange="reversed"),
                                 xaxis_title="Companies", yaxis_title="")
            st.plotly_chart(fig_st, use_container_width=True)
        else:
            st.info("No state data yet.")

    # ── Worker status ──────────────────────────────────────────────────
    st.subheader("Worker Status")
    for sf in sorted(STATUS_DIR.glob("*.json")):
        try:
            data = json.loads(sf.read_text())
            stage = data.get("stage", "unknown")
            updated = data.get("updated_at", "")[:19]
            icon = "🟢" if stage in ("idle", "cycle_complete") else "🔴" if stage == "failed" else "🟡"
            with st.expander(f"{icon} {sf.name}  —  stage: **{stage}**  |  updated: {updated}"):
                st.json(data)
        except Exception as e:
            st.error(f"{sf.name}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 2 — Company Browser
# ═══════════════════════════════════════════════════════════════════════════

elif page == "🔍 Company Browser":
    st.title("🔍 Company Browser")
    st.caption(f"Database: **{_active_db()}**")

    # ── Filters ────────────────────────────────────────────────────────
    f1, f2, f3, f4 = st.columns([3, 1, 1, 1])
    with f1:
        search = st.text_input("🔎 Search company name", "", key="cb_search")
    with f2:
        # Dynamic category list
        try:
            _cat_rows = run_query("""
                SELECT DISTINCT data->>'category' AS cat
                FROM results
                WHERE data->>'category' IS NOT NULL AND data->>'category' <> ''
                ORDER BY 1
            """)
            _cat_options = ["All"] + _cat_rows["cat"].tolist()
        except Exception:
            _cat_options = ["All"]
        category_filter = st.selectbox("Category", _cat_options, key="cb_cat")
    with f3:
        # Dynamic state list
        try:
            _state_rows = run_query("""
                SELECT DISTINCT data->'complete_address'->>'state' AS state
                FROM results
                WHERE data->'complete_address'->>'state' IS NOT NULL
                  AND data->'complete_address'->>'state' <> ''
                ORDER BY 1
            """)
            _state_options = ["All"] + _state_rows["state"].tolist()
        except Exception:
            _state_options = ["All"]
        state_filter = st.selectbox("State", _state_options, key="cb_state")
    with f4:
        # Dynamic city list (filtered by state if set)
        try:
            if state_filter != "All":
                _city_rows = run_query("""
                    SELECT DISTINCT data->'complete_address'->>'city' AS city
                    FROM results
                    WHERE data->'complete_address'->>'city' IS NOT NULL
                      AND data->'complete_address'->>'city' <> ''
                      AND data->'complete_address'->>'state' = %s
                    ORDER BY 1
                """, [state_filter])
            else:
                _city_rows = run_query("""
                    SELECT DISTINCT data->'complete_address'->>'city' AS city
                    FROM results
                    WHERE data->'complete_address'->>'city' IS NOT NULL
                      AND data->'complete_address'->>'city' <> ''
                    ORDER BY 1
                """)
            _city_options = ["All"] + _city_rows["city"].tolist()
        except Exception:
            _city_options = ["All"]
        city_filter = st.selectbox("City", _city_options, key="cb_city")

    f5, f6, f7 = st.columns([1, 1, 1])
    with f5:
        enrichment_filter = st.selectbox("Enrichment", ["All", "Enriched", "Not enriched"], key="cb_enrich")
    with f6:
        min_rating = st.slider("Min rating ⭐", 0.0, 5.0, 0.0, 0.5, key="cb_rating")
    with f7:
        has_website = st.selectbox("Has website?", ["All", "Yes", "No"], key="cb_website")

    # ── Build WHERE clause ─────────────────────────────────────────────
    where_clauses: list[str] = []
    params: list = []
    if search.strip():
        where_clauses.append("(data->>'title' ILIKE %s OR data->>'address' ILIKE %s)")
        params.extend([f"%{search.strip()}%", f"%{search.strip()}%"])
    if category_filter != "All":
        where_clauses.append("data->>'category' = %s")
        params.append(category_filter)
    if state_filter != "All":
        where_clauses.append("data->'complete_address'->>'state' = %s")
        params.append(state_filter)
    if city_filter != "All":
        where_clauses.append("data->'complete_address'->>'city' = %s")
        params.append(city_filter)
    if enrichment_filter == "Enriched":
        where_clauses.append("EXISTS (SELECT 1 FROM link_candidates lc WHERE lc.result_id = r.id)")
    elif enrichment_filter == "Not enriched":
        where_clauses.append("NOT EXISTS (SELECT 1 FROM link_candidates lc WHERE lc.result_id = r.id)")
    if min_rating > 0:
        where_clauses.append("COALESCE((data->>'review_rating')::numeric, 0) >= %s")
        params.append(min_rating)
    if has_website == "Yes":
        where_clauses.append("COALESCE(data->>'web_site', '') <> ''")
    elif has_website == "No":
        where_clauses.append("(data->>'web_site' IS NULL OR data->>'web_site' = '')")

    where_sql = (" AND ".join(where_clauses)) if where_clauses else "TRUE"
    total_matches = run_scalar(f"SELECT count(*) FROM results r WHERE {where_sql}", params)
    st.caption(f"**{total_matches:,}** matching companies")

    # ── Pagination ─────────────────────────────────────────────────────
    page_size = 50
    page_num = st.number_input("Page", min_value=1,
                               max_value=max(1, (total_matches // page_size) + 1), value=1,
                               key="cb_page")
    offset = (page_num - 1) * page_size

    df = run_query(f"""
        SELECT
            r.id,
            COALESCE(r.data->>'title', '') AS company,
            COALESCE(r.data->>'phone', '') AS phone,
            COALESCE(r.data->>'web_site', '') AS website,
            COALESCE(r.data->>'address', '') AS address,
            COALESCE(r.data->>'category', '') AS category,
            COALESCE(r.data->'complete_address'->>'city', '') AS city,
            COALESCE(r.data->'complete_address'->>'state', '') AS state,
            COALESCE(r.data->>'review_rating', '') AS rating,
            COALESCE(r.data->>'review_count', '') AS reviews,
            COALESCE(r.data->>'status', '') AS status,
            (SELECT count(*) FROM link_candidates lc WHERE lc.result_id = r.id) AS links,
            (SELECT count(*) FROM documents d
             JOIN link_candidates lc ON lc.id = d.link_candidate_id
             WHERE lc.result_id = r.id AND d.fetch_status = 'ok') AS docs
        FROM results r
        WHERE {where_sql}
        ORDER BY r.id
        LIMIT {page_size} OFFSET {offset}
    """, params)

    if df.empty:
        st.info("No companies match your filters.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True, column_config={
            "id": st.column_config.NumberColumn("ID", width="small"),
            "company": st.column_config.TextColumn("Company", width="large"),
            "phone": "Phone",
            "website": st.column_config.LinkColumn("Website", width="medium"),
            "address": st.column_config.TextColumn("Address", width="medium"),
            "category": "Category",
            "city": "City",
            "state": "State",
            "rating": "⭐",
            "reviews": "Reviews",
            "status": "Status",
            "links": st.column_config.NumberColumn("Links", help="Link candidates discovered"),
            "docs": st.column_config.NumberColumn("Docs OK", help="Documents fetched successfully"),
        })

    # ── Company Detail Drill-Down ──────────────────────────────────────
    st.divider()
    st.subheader("🔬 Company Detail")
    drill_id = st.number_input("Result ID to inspect", min_value=0, value=0, step=1, key="cb_drill")
    if drill_id > 0:
        # Fetch raw JSONB
        try:
            raw = run_query("SELECT id, data FROM results WHERE id = %s", [int(drill_id)])
        except Exception:
            raw = pd.DataFrame()
        if raw.empty:
            st.warning(f"No result with id={drill_id}")
        else:
            data = raw.iloc[0]["data"]
            name = data.get("title", "Unknown") if isinstance(data, dict) else "Unknown"
            st.subheader(f"📋 {name}  (id={drill_id})")

            # Key info cards
            info_cols = st.columns(4)
            if isinstance(data, dict):
                info_cols[0].metric("Category", data.get("category", "—"))
                info_cols[1].metric("⭐ Rating", data.get("review_rating", "—"))
                info_cols[2].metric("Reviews", data.get("review_count", "—"))
                info_cols[3].metric("Phone", data.get("phone", "—"))

                detail_left, detail_right = st.columns(2)
                with detail_left:
                    st.markdown("**Contact & Location**")
                    st.markdown(f"- **Address:** {data.get('address', '—')}")
                    addr = data.get('complete_address', {})
                    if isinstance(addr, dict) and addr:
                        st.markdown(f"- **City:** {addr.get('city', '—')}, **State:** {addr.get('state', '—')}")
                        st.markdown(f"- **Zip:** {addr.get('postal_code', '—')}, **Country:** {addr.get('country', '—')}")
                    if data.get('web_site'):
                        st.markdown(f"- **Website:** [{data['web_site']}]({data['web_site']})")
                    if data.get('emails'):
                        emails = data['emails'] if isinstance(data['emails'], list) else [data['emails']]
                        st.markdown(f"- **Emails:** {', '.join(str(e) for e in emails)}")
                    categories = data.get('categories', [])
                    if isinstance(categories, list) and categories:
                        st.markdown(f"- **Categories:** {', '.join(str(c) for c in categories)}")

                with detail_right:
                    st.markdown("**Hours & Details**")
                    hours = data.get('open_hours')
                    if isinstance(hours, dict) and hours:
                        for day, times in hours.items():
                            t = ', '.join(times) if isinstance(times, list) else str(times)
                            st.markdown(f"- **{day}:** {t}")
                    desc = data.get('description', '')
                    if desc:
                        st.markdown(f"**Description:** {desc}")

                # Raw JSONB viewer
                with st.expander("🗄️ Raw JSONB Data", expanded=False):
                    st.json(data)

            # Enrichment detail
            try:
                lc_detail = run_query("""
                    SELECT
                        lc.id AS lc_id, lc.source_type, lc.discovery_status,
                        lc.url_discovered, lc.title_discovered,
                        d.id AS doc_id, d.fetch_status, d.page_title,
                        (SELECT count(*) FROM document_chunks dc WHERE dc.document_id = d.id) AS chunks
                    FROM link_candidates lc
                    LEFT JOIN documents d ON d.link_candidate_id = lc.id
                    WHERE lc.result_id = %s
                    ORDER BY lc.source_type, lc.id
                """, [int(drill_id)])
                if not lc_detail.empty:
                    st.markdown("**🔗 Enrichment: Link Candidates & Documents**")
                    st.dataframe(lc_detail, use_container_width=True, hide_index=True)
                else:
                    st.info("No enrichment data yet for this company.")
            except Exception:
                st.info("No enrichment data yet for this company.")


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 3 — Enrichment Monitor
# ═══════════════════════════════════════════════════════════════════════════

elif page == "⚙️ Enrichment Monitor":
    st.title("⚙️ Enrichment Monitor")

    total_results = run_scalar("SELECT count(*) FROM results")
    enriched = run_scalar("SELECT count(DISTINCT result_id) FROM link_candidates")
    remaining = total_results - enriched

    prog_pct = enriched / max(total_results, 1)
    st.progress(prog_pct,
                text=f"{enriched:,} / {total_results:,} enriched ({prog_pct*100:.1f}%)  •  {remaining:,} remaining")

    st.divider()
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Document Fetch Status")
        fetch_df = run_query("SELECT fetch_status, count(*) AS cnt FROM documents GROUP BY 1 ORDER BY 2 DESC")
        if fetch_df.empty:
            st.info("No documents fetched yet — run the ingester to populate this chart.")
        else:
            colors = {"ok": "#22c55e", "http_error": "#ef4444", "request_error": "#f97316",
                      "irrelevant": "#a3a3a3", "empty_text": "#eab308", "unsupported_content_type": "#8b5cf6"}
            fig_fs = px.pie(fetch_df, values="cnt", names="fetch_status",
                            color="fetch_status", color_discrete_map=colors)
            fig_fs.update_layout(height=350)
            st.plotly_chart(fig_fs, use_container_width=True)

    with col_right:
        st.subheader("Link Discovery Breakdown")
        disc_df = run_query("""
            SELECT source_type, discovery_status, count(*) AS cnt
            FROM link_candidates GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """)
        if disc_df.empty:
            st.info("No link candidates yet — run the enricher to populate this chart.")
        else:
            fig_disc = px.bar(disc_df, x="source_type", y="cnt", color="discovery_status",
                              barmode="group",
                              color_discrete_sequence=["#2563eb", "#f97316", "#ef4444", "#a3a3a3"])
            fig_disc.update_layout(height=350, xaxis_title="", yaxis_title="Count")
            st.plotly_chart(fig_disc, use_container_width=True)

    st.divider()
    st.subheader("Recently Enriched")
    recent = run_query("""
        SELECT lc.result_id, r.data->>'title' AS company,
               lc.source_type, lc.discovery_status, lc.discovered_at, lc.url_discovered
        FROM link_candidates lc JOIN results r ON r.id = lc.result_id
        ORDER BY lc.discovered_at DESC NULLS LAST LIMIT 30
    """)
    if not recent.empty:
        st.dataframe(recent, use_container_width=True, hide_index=True, column_config={
            "result_id": "ID", "company": "Company", "source_type": "Type",
            "discovery_status": "Status",
            "discovered_at": st.column_config.DatetimeColumn("Discovered", format="YYYY-MM-DD HH:mm"),
            "url_discovered": st.column_config.LinkColumn("URL"),
        })
    else:
        st.info("No enrichment activity yet — run the enricher worker to discover links.")

    st.divider()
    st.subheader("Recently Fetched Documents")
    recent_docs = run_query("""
        SELECT d.id AS doc_id, r.data->>'title' AS company,
               d.fetch_status, d.page_title, d.url_fetched, d.fetched_at,
               (SELECT count(*) FROM document_chunks dc WHERE dc.document_id = d.id) AS chunks
        FROM documents d
        JOIN link_candidates lc ON lc.id = d.link_candidate_id
        JOIN results r ON r.id = lc.result_id
        ORDER BY d.fetched_at DESC NULLS LAST LIMIT 30
    """)
    if not recent_docs.empty:
        st.dataframe(recent_docs, use_container_width=True, hide_index=True, column_config={
            "doc_id": "Doc ID", "company": "Company", "fetch_status": "Status",
            "page_title": "Page Title",
            "url_fetched": st.column_config.LinkColumn("URL"),
            "fetched_at": st.column_config.DatetimeColumn("Fetched", format="YYYY-MM-DD HH:mm"),
            "chunks": "Chunks",
        })
    else:
        st.info("No documents fetched yet — run the ingester to populate this table.")

    st.divider()
    st.subheader("Worker Status JSON")
    for sf in sorted(STATUS_DIR.glob("*.json")):
        with st.expander(f"📄 {sf.name}", expanded=False):
            try:
                st.json(json.loads(sf.read_text()))
            except Exception as e:
                st.error(str(e))


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 4 — Vector Search
# ═══════════════════════════════════════════════════════════════════════════

elif page == "🧠 Vector Search":
    st.title("🧠 Vector Search")

    # ── Collection overview ────────────────────────────────────────────
    col = get_chroma_collection()
    vec_count = col.count()

    vc1, vc2, vc3 = st.columns(3)
    vc1.metric("Vectors in Collection", f"{vec_count:,}")
    vc2.metric("Collection", COLLECTION_NAME)
    vc3.metric("Embedding Model", EMBEDDING_MODEL)

    # ── Collection sample / stats ──────────────────────────────────────
    with st.expander("📊 Collection Statistics", expanded=True):
        if vec_count == 0:
            st.warning("Collection is empty. Run the document ingest worker first.")
        else:
            sample_size = min(vec_count, 500)
            try:
                sample = col.peek(limit=sample_size)
            except Exception as _peek_err:
                st.error(
                    f"ChromaDB index is corrupt or missing on disk. "
                    f"Delete `{CHROMA_DIR}` and re-run the document ingest worker.\n\n"
                    f"`{_peek_err}`"
                )
                sample = {"metadatas": []}
            metas = sample.get("metadatas") or []

            if metas:
                meta_df = pd.DataFrame(metas)

                stat_col1, stat_col2 = st.columns(2)
                with stat_col1:
                    if "company" in meta_df.columns:
                        company_counts = meta_df["company"].value_counts().head(15)
                        st.markdown("**Top companies by chunk count**")
                        fig_comp = px.bar(
                            x=company_counts.values,
                            y=company_counts.index,
                            orientation="h",
                            color_discrete_sequence=["#7c3aed"],
                        )
                        fig_comp.update_layout(
                            height=380, yaxis=dict(autorange="reversed"),
                            xaxis_title="Chunks", yaxis_title="",
                        )
                        st.plotly_chart(fig_comp, use_container_width=True)

                with stat_col2:
                    if "source_type" in meta_df.columns:
                        src_counts = meta_df["source_type"].value_counts()
                        st.markdown("**Chunks by source type**")
                        fig_src = px.pie(
                            values=src_counts.values,
                            names=src_counts.index,
                            color_discrete_sequence=["#2563eb", "#f97316"],
                            hole=0.4,
                        )
                        fig_src.update_layout(height=380)
                        st.plotly_chart(fig_src, use_container_width=True)

    st.divider()

    # ── Search interface ───────────────────────────────────────────────
    st.subheader("🔎 Search")

    # Prefill from example button
    prefill = st.session_state.pop("_prefill_query", "")

    query_text = st.text_input(
        "Ask a question",
        value=prefill,
        placeholder='e.g. "Recent news about roofing companies in Texas" or "Concrete contractors in California"',
    )

    search_col1, search_col2, search_col3, search_col4 = st.columns([1, 1, 1, 1])
    with search_col1:
        top_k = st.slider("Top K results", 1, 25, 8)
    with search_col2:
        source_filter = st.selectbox("Source type", ["all", "news"])
    with search_col3:
        company_filter = st.text_input("Company filter (exact)", "")
    with search_col4:
        get_answer = st.checkbox("🤖 Get AI answer", value=False)

    _openai_available = get_openai_client() is not None
    if not _openai_available:
        st.warning("⚠️ **OPENAI_API_KEY** not set in `.env` — vector search requires an embedding key. "
                   "Add `OPENAI_API_KEY=sk-...` to your `.env` file and refresh.")

    if st.button("🔍 Search", type="primary", disabled=not _openai_available) and query_text.strip():
        with st.spinner("Embedding query and searching…"):
            try:
                query_emb = embed_query(query_text)

                # Build Chroma where filter
                where = None
                filter_parts = []
                if source_filter in ("news",):
                    filter_parts.append({"source_type": source_filter})
                if company_filter.strip():
                    filter_parts.append({"company": company_filter.strip()})
                if len(filter_parts) == 1:
                    where = filter_parts[0]
                elif len(filter_parts) > 1:
                    where = {"$and": filter_parts}

                result = col.query(
                    query_embeddings=[query_emb],
                    n_results=top_k,
                    where=where,
                    include=["documents", "metadatas", "distances"],
                )

                ids = (result.get("ids") or [[]])[0]
                docs = (result.get("documents") or [[]])[0]
                metas_list = (result.get("metadatas") or [[]])[0]
                dists = (result.get("distances") or [[]])[0]

            except Exception as e:
                st.error(f"Search failed: {e}")
                ids, docs, metas_list, dists = [], [], [], []

        if not ids:
            st.warning("No results found. Try a broader query or remove filters.")
        else:
            st.success(f"Found {len(ids)} chunks (searched {vec_count:,} vectors)")

            # ── AI Answer ──────────────────────────────────────────────
            if get_answer:
                client = get_openai_client()
                if client is None:
                    st.error("OPENAI_API_KEY not set — cannot generate answer.")
                else:
                    with st.spinner("Generating AI answer…"):
                        from broker.query.summarizer import format_context_block, call_openai_answer
                        context = format_context_block(ids, docs, metas_list, max_doc_chars=800)
                        answer = call_openai_answer(
                            client=client,
                            model="gpt-4o-mini",
                            question=query_text,
                            context_block=context,
                            temperature=0.0,
                            summary_mode=True,
                            target_company=company_filter.strip() or None,
                        )
                    st.markdown("---")
                    st.markdown("### 🤖 AI-Grounded Answer")
                    st.info(answer)
                    st.markdown("---")

            # ── Results list ───────────────────────────────────────────
            st.markdown("### Retrieved Chunks")
            for i, (chunk_id, doc, meta, dist) in enumerate(zip(ids, docs, metas_list, dists)):
                meta = meta or {}
                similarity = 1 - dist  # cosine distance → similarity
                company = meta.get("company", "—")
                source = meta.get("source_type", "—")
                title = meta.get("page_title", "") or meta.get("article_title", "")
                url = meta.get("url_fetched", "")

                badge_color = "🟢" if similarity > 0.75 else "🟡" if similarity > 0.55 else "🔴"

                with st.expander(
                    f"{badge_color} **[{i+1}]** sim={similarity:.3f}  •  {company}  •  {source}  •  {title[:80]}",
                    expanded=(i < 3),
                ):
                    mc1, mc2 = st.columns([1, 3])
                    with mc1:
                        st.markdown(f"""
- **Similarity:** `{similarity:.4f}`
- **Company:** {company}
- **Source:** {source}
- **Chunk ID:** `{chunk_id}`
- **Doc ID:** `{meta.get('document_id', '—')}`
""")
                        if url:
                            st.markdown(f"[🔗 Source URL]({url})")
                    with mc2:
                        st.text_area(
                            "Chunk text",
                            value=doc or "",
                            height=200,
                            disabled=True,
                            key=f"chunk_{i}",
                        )

    # ── Example queries ────────────────────────────────────────────────
    st.divider()
    st.subheader("💡 Example Queries")
    examples = [
        "Recent construction news in Texas",
        "Roofing companies with major contracts",
        "Concrete contractors in California",
        "Construction companies in Florida",
        "General contractors recent projects",
    ]
    cols = st.columns(len(examples))
    for idx, ex in enumerate(examples):
        with cols[idx]:
            if st.button(ex, key=f"ex_{idx}"):
                st.session_state["_prefill_query"] = ex
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 5 — Scrape Queries
# ═══════════════════════════════════════════════════════════════════════════

elif page == "🚀 Scrape Queries":
    st.title("🚀 Scrape Query Manager")

    st.subheader("Scraper Job Queue")
    jobs_stats = run_query("SELECT status, count(*) AS cnt FROM gmaps_jobs GROUP BY 1 ORDER BY 2 DESC")
    if not jobs_stats.empty:
        jc1, jc2, jc3 = st.columns(3)
        jc1.metric("Total Jobs", f"{int(jobs_stats['cnt'].sum()):,}")
        for _, row in jobs_stats.iterrows():
            if row["status"] == "ok":
                jc2.metric("Completed", f"{int(row.cnt):,}")
            elif row["status"] in ("new", "queued"):
                jc3.metric("Pending", f"{int(row.cnt):,}")
    else:
        st.info("No scraper jobs yet — seed queries to create jobs.")
    st.dataframe(jobs_stats, use_container_width=True, hide_index=True)

    # ── Live Scraper Status ────────────────────────────────────────────
    st.divider()
    st.subheader("🐳 Live Scraper Status")
    try:
        _ps = subprocess.run(
            ["docker", "ps", "-a", "--filter", "ancestor=gosom/google-maps-scraper:latest",
             "--format", "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}\t{{.Ports}}"],
            capture_output=True, text=True, timeout=10, encoding="utf-8", errors="replace",
        )
        if _ps.stdout.strip():
            st.code(_ps.stdout.strip(), language="text")
        else:
            st.info("No scraper containers found.")
    except Exception as exc:
        st.warning(f"Could not query Docker: {exc}")

    # Docker log viewer
    _cname_log = _scraper_container_name(_active_db())
    try:
        _running_ps = subprocess.run(
            ["docker", "ps", "--filter", "ancestor=gosom/google-maps-scraper:latest",
             "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10, encoding="utf-8", errors="replace",
        )
        _log_containers = [n.strip() for n in (_running_ps.stdout or "").splitlines() if n.strip()]
    except Exception:
        _log_containers = []
    _all_log_targets = _log_containers or [_cname_log]

    log_col1, log_col2 = st.columns([2, 1])
    with log_col1:
        _log_target = st.selectbox("Container logs", _all_log_targets, key="_log_sel")
    with log_col2:
        _log_lines = st.number_input("Tail lines", min_value=10, max_value=500, value=50, step=10, key="_log_lines")

    if st.button("📋 Fetch Logs", key="_btn_logs"):
        try:
            _logs = subprocess.run(
                ["docker", "logs", "--tail", str(_log_lines), _log_target],
                capture_output=True, text=True, timeout=15, encoding="utf-8", errors="replace",
            )
            _log_text = (_logs.stdout or "") + (_logs.stderr or "")
            if _log_text.strip():
                st.code(_log_text.strip(), language="text")
            else:
                st.info(f"No log output from `{_log_target}`.")
        except subprocess.TimeoutExpired:
            st.warning("Docker logs timed out.")
        except Exception as exc:
            st.error(f"Failed to fetch logs: {exc}")

    # ── Worker status files ────────────────────────────────────────────
    st.divider()
    st.subheader("📊 Worker Status")
    _status_files = sorted(STATUS_DIR.glob("*.json"))
    if _status_files:
        for sf in _status_files:
            try:
                _sdata = json.loads(sf.read_text())
                _stage = _sdata.get("stage", "unknown")
                _updated = _sdata.get("updated_at", "")[:19]
                _icon = "🟢" if _stage in ("idle", "cycle_complete") else "🔴" if _stage == "failed" else "🟡"
                with st.expander(f"{_icon} {sf.name}  —  stage: **{_stage}**  |  updated: {_updated}"):
                    st.json(_sdata)
            except Exception as e:
                st.error(f"{sf.name}: {e}")
    else:
        st.info("No worker status files yet.")

    st.divider()
    st.subheader("Query Files")
    query_files = sorted(QUERIES_DIR.glob("*.txt"))
    for qf in query_files:
        lines = qf.read_text(encoding="utf-8").strip().splitlines()
        with st.expander(f"📄 {qf.name} — {len(lines)} queries"):
            st.text("\n".join(lines[:50]))
            if len(lines) > 50:
                st.caption(f"... and {len(lines) - 50} more")

    st.divider()
    st.subheader("Add New Scrape Queries")
    st.caption("One Google Maps search per line.  E.g. *Construction Companies in Miami FL*")

    new_queries = st.text_area("New queries (one per line)", height=200,
                               placeholder="Construction Companies in Austin TX\nGeneral Contractors in Seattle WA")

    col_save, col_seed = st.columns(2)
    with col_save:
        save_filename = st.text_input("Save as file", value="custom_queries.txt")
        if st.button("💾 Save Query File", type="primary"):
            lines = [l.strip() for l in new_queries.strip().splitlines() if l.strip()]
            if not lines:
                st.warning("No queries to save.")
            else:
                target = QUERIES_DIR / save_filename
                target.write_text("\n".join(lines) + "\n", encoding="utf-8")
                st.success(f"Saved {len(lines)} queries to {target.name}")
                st.rerun()

    with col_seed:
        st.caption("Seed into Docker scraper job queue")
        seed_file = st.selectbox("Query file to seed",
                                 [f.name for f in query_files] +
                                 ([save_filename] if save_filename and (QUERIES_DIR / save_filename).exists() else []))
        if st.button("🚀 Seed to gmaps_jobs"):
            seed_path = QUERIES_DIR / seed_file
            if not seed_path.exists():
                st.error(f"Not found: {seed_path}")
            else:
                with st.spinner("Seeding…"):
                    cmd = ["docker", "run", "--rm", "--name", f"{_scraper_container_name(_active_db())}-seed",
                           "-v", f"{seed_path.resolve()}:/queries.txt",
                           "gosom/google-maps-scraper:latest",
                           "-dsn", _active_docker_dsn(), "-produce", "-input", "/queries.txt", "-lang", "en"]
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace")
                    if result.returncode == 0:
                        st.success(f"✅ Seeded from {seed_file}")
                        _seed_text = (result.stdout or "") + (result.stderr or "")
                        if _seed_text.strip():
                            with st.expander("Seed output", expanded=True):
                                st.code(_seed_text.strip(), language="text")
                        st.rerun()
                    else:
                        st.error(f"Failed (exit {result.returncode})")
                        st.code(result.stderr or result.stdout)

    st.divider()
    st.subheader("Quick Query Generator")
    gen_col1, gen_col2 = st.columns(2)
    with gen_col1:
        city = st.text_input("City", placeholder="Miami")
        state_abbr = st.text_input("State", placeholder="FL")
    with gen_col2:
        trade_types = st.multiselect("Trade types", [
            "Construction Companies", "General Contractors", "Home Builders",
            "Roofing Contractors", "Concrete Contractors", "Remodelers",
            "Excavating Contractors", "Custom Home Builders", "Commercial Contractors",
            "Residential Contractors", "Plumbing Contractors", "Electrical Contractors",
            "HVAC Contractors", "Painting Contractors", "Landscaping Companies",
        ], default=["Construction Companies", "General Contractors", "Home Builders"])

    if st.button("📝 Generate Queries"):
        if not city.strip() or not state_abbr.strip():
            st.warning("Enter both city and state.")
        else:
            generated = [f"{t} in {city.strip()} {state_abbr.strip()}" for t in trade_types]
            st.code("\n".join(generated))
            st.caption(f"{len(generated)} queries — copy into the editor above")

    # ── Pipeline Launch Controls ───────────────────────────────────────
    st.divider()
    st.subheader("⚡ Run Pipeline")
    st.caption(f"Target database: **{_active_db()}**  •  Container: `{_scraper_container_name(_active_db())}`")

    # ── One-click full pipeline ────────────────────────────────────────
    if st.button("🚀 Run Full Pipeline (all 5 stages)", type="primary", key="btn_full_pipeline", use_container_width=True):
        _cname = _scraper_container_name(_active_db())
        _qfiles = sorted(QUERIES_DIR.glob("*.txt"))
        _seed_file = _qfiles[0] if _qfiles else None
        launched = []

        # 1. Seed + Scraper
        if _seed_file:
            with st.spinner("Seeding scraper queries…"):
                seed_cmd = [
                    "docker", "run", "--rm", "--name", f"{_cname}-seed",
                    "-v", f"{_seed_file.resolve()}:/queries.txt",
                    "gosom/google-maps-scraper:latest",
                    "-dsn", _active_docker_dsn(), "-produce", "-input", "/queries.txt", "-lang", "en",
                ]
                subprocess.run(seed_cmd, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace")

            subprocess.run(["docker", "rm", "-f", _cname], capture_output=True, encoding="utf-8", errors="replace")
            scraper_cmd = [
                "docker", "run", "-d", "--name", _cname,
                "gosom/google-maps-scraper:latest",
                "-dsn", _active_docker_dsn(),
                "-c", "4", "-depth", "1", "-email", "-exit-on-inactivity", "5m",
            ]
            r = subprocess.run(scraper_cmd, capture_output=True, text=True, timeout=30, encoding="utf-8", errors="replace")
            if r.returncode == 0:
                launched.append("🐳 Scraper")

        # 2. Enricher
        enrich_cmd = (
            f'{VENV_PYTHON} worker_enrich.py'
            f' --batch 25 --poll 10 --news-top 10'
            f' --max-retries 1 --delay-min 0.5 --delay-max 1.5'
        )
        subprocess.Popen(enrich_cmd, cwd=str(PROJECT_ROOT), shell=True,
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        launched.append("🔗 Enricher")

        # 3. Ingester
        ingest_cmd = (
            f'{VENV_PYTHON} scripts/run_document_ingest.py run'
            f' --fetch-batch 25 --chunk-batch 25 --poll 10'
            f' --persist-dir runtime/chroma/chroma_smoke_db'
            f' --collection {COLLECTION_NAME}'
            f' --embedding-backend openai --embedding-model {EMBEDDING_MODEL}'
            f' --env-file .env'
            f' --status-file runtime/status/run_document_ingest_status.json'
        )
        subprocess.Popen(ingest_cmd, cwd=str(PROJECT_ROOT), shell=True,
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        launched.append("📄 Ingester")

        # 4. Dedup
        dedup_cmd = f'{VENV_PYTHON} worker_dedup.py --poll 60 --dsn {_active_dsn()}'
        subprocess.Popen(dedup_cmd, cwd=str(PROJECT_ROOT), shell=True,
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        launched.append("🧹 Dedup")

        # 5. Evaluator
        eval_cmd = (
            f'{VENV_PYTHON} worker_evaluate.py --batch 10 --poll 60'
            f' --chroma-dir runtime/chroma/chroma_smoke_db'
            f' --collection {COLLECTION_NAME}'
        )
        subprocess.Popen(eval_cmd, cwd=str(PROJECT_ROOT), shell=True,
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        launched.append("📈 Evaluator")

        st.success(f"✅ Full pipeline launched: {', '.join(launched)}")
        st.info("📋 Check **Worker Status** section above for live progress.")

    st.caption("Or launch individual stages below:")

    run_col1, run_col2, run_col3, run_col4, run_col5 = st.columns(5)

    with run_col1:
        st.markdown("**Step 1 — Scraper** 🐳")
        st.caption("Docker: seed queries + run Google Maps scraper")
        scrape_qf = st.selectbox(
            "Query file",
            [f.name for f in sorted(QUERIES_DIR.glob("*.txt"))],
            index=0,
            key="pipeline_qf",
        )
        scrape_depth = st.number_input("Depth (scrolls)", min_value=1, max_value=20, value=1, key="scrape_depth")
        scrape_conc = st.number_input("Concurrency", min_value=1, max_value=16, value=4, key="scrape_conc")

        if st.button("🐳 Seed & Start Scraper", key="btn_scraper"):
            _cname = _scraper_container_name(_active_db())
            seed_path = QUERIES_DIR / scrape_qf
            with st.spinner("Seeding queries…"):
                seed_cmd = [
                    "docker", "run", "--rm", "--name", f"{_cname}-seed",
                    "-v", f"{seed_path.resolve()}:/queries.txt",
                    "gosom/google-maps-scraper:latest",
                    "-dsn", _active_docker_dsn(), "-produce", "-input", "/queries.txt", "-lang", "en",
                ]
                r = subprocess.run(seed_cmd, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace")
                if r.returncode != 0:
                    st.error(f"Seed failed: {r.stderr or r.stdout}")
                else:
                    st.success("✅ Queries seeded")
                    _seed_out = (r.stdout or "") + (r.stderr or "")
                    if _seed_out.strip():
                        with st.expander("Seed output", expanded=True):
                            st.code(_seed_out.strip(), language="text")

            with st.spinner("Launching scraper container…"):
                subprocess.run(["docker", "rm", "-f", _cname], capture_output=True, encoding="utf-8", errors="replace")
                scraper_cmd = [
                    "docker", "run", "-d", "--name", _cname,
                    "gosom/google-maps-scraper:latest",
                    "-dsn", _active_docker_dsn(),
                    "-c", str(scrape_conc),
                    "-depth", str(scrape_depth),
                    "-email", "-exit-on-inactivity", "5m",
                ]
                r = subprocess.run(scraper_cmd, capture_output=True, text=True, timeout=30, encoding="utf-8", errors="replace")
                if r.returncode == 0:
                    st.success(f"✅ Scraper `{_cname}` running in background")
                    _scraper_out = (r.stdout or "") + (r.stderr or "")
                    if _scraper_out.strip():
                        with st.expander("Scraper launch output", expanded=True):
                            st.code(_scraper_out.strip(), language="text")
                else:
                    st.error(f"Failed: {r.stderr or r.stdout}")

    with run_col2:
        st.markdown("**Step 2 — Enricher** 🔗")
        st.caption("Google News + company website discovery")
        enrich_batch = st.number_input("Batch size", min_value=1, max_value=100, value=25, key="enrich_batch")
        enrich_poll = st.number_input("Poll interval (s)", min_value=5, max_value=300, value=10, key="enrich_poll")
        news_top = st.number_input("News URLs per co.", min_value=1, max_value=20, value=10, key="news_top")

        if st.button("🔗 Start Enricher", key="btn_enricher"):
            cmd = (
                f'{VENV_PYTHON} worker_enrich.py'
                f' --batch {enrich_batch} --poll {enrich_poll}'
                f' --news-top {news_top}'
                f' --max-retries 1 --delay-min 0.5 --delay-max 1.5'
            )
            subprocess.Popen(cmd, cwd=str(PROJECT_ROOT), shell=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            st.success(f"✅ Enricher started (batch={enrich_batch}, poll={enrich_poll}s)")
            with st.expander("Command launched", expanded=False):
                st.code(cmd, language="shell")
            st.info("📋 Check **Worker Status** section above for live progress.")

    with run_col3:
        st.markdown("**Step 3 — Ingester** 📄")
        st.caption("Fetch → chunk → embed → ChromaDB")
        ingest_fetch = st.number_input("Fetch batch", min_value=1, max_value=100, value=25, key="ingest_fetch")
        ingest_chunk = st.number_input("Chunk batch", min_value=1, max_value=100, value=25, key="ingest_chunk")
        ingest_poll = st.number_input("Poll interval (s)", min_value=5, max_value=300, value=10, key="ingest_poll")

        if st.button("📄 Start Ingester", key="btn_ingester"):
            cmd = (
                f'{VENV_PYTHON} scripts/run_document_ingest.py run'
                f' --fetch-batch {ingest_fetch} --chunk-batch {ingest_chunk} --poll {ingest_poll}'
                f' --persist-dir runtime/chroma/chroma_smoke_db'
                f' --collection {COLLECTION_NAME}'
                f' --embedding-backend openai --embedding-model {EMBEDDING_MODEL}'
                f' --env-file .env'
                f' --status-file runtime/status/run_document_ingest_status.json'
            )
            subprocess.Popen(cmd, cwd=str(PROJECT_ROOT), shell=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            st.success(f"✅ Ingester started (fetch={ingest_fetch}, chunk={ingest_chunk}, poll={ingest_poll}s)")
            with st.expander("Command launched", expanded=False):
                st.code(cmd, language="shell")
            st.info("📋 Check **Worker Status** section above for live progress.")

    with run_col4:
        st.markdown("**Step 4 — Dedup** 🧹")
        st.caption("Continuous duplicate removal")
        dedup_poll = st.number_input("Poll interval (s)", min_value=5, max_value=600, value=60, key="dedup_poll")

        if st.button("🧹 Start Dedup", key="btn_dedup"):
            cmd = (
                f'{VENV_PYTHON} worker_dedup.py'
                f' --poll {dedup_poll}'
                f' --dsn {_active_dsn()}'
            )
            subprocess.Popen(cmd, cwd=str(PROJECT_ROOT), shell=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            st.success(f"✅ Dedup started (poll={dedup_poll}s)")
            with st.expander("Command launched", expanded=False):
                st.code(cmd, language="shell")
            st.info("📋 Check **Worker Status** section above for live progress.")

    with run_col5:
        st.markdown("**Step 5 — Evaluator** 📈")
        st.caption("AI revenue & headcount extraction")
        eval_batch_sq = st.number_input("Batch size", min_value=1, max_value=100, value=20, key="eval_batch_sq")
        eval_poll_sq = st.number_input("Poll interval (s)", min_value=5, max_value=300, value=60, key="eval_poll_sq")

        if st.button("📈 Start Evaluator", key="btn_evaluator_sq"):
            cmd = (
                f'{VENV_PYTHON} worker_evaluate.py'
                f' --batch {eval_batch_sq} --poll {eval_poll_sq}'
                f' --chroma-dir runtime/chroma/chroma_smoke_db'
                f' --collection {COLLECTION_NAME}'
            )
            subprocess.Popen(cmd, cwd=str(PROJECT_ROOT), shell=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            st.success(f"✅ Evaluator started (batch={eval_batch_sq}, poll={eval_poll_sq}s)")
            with st.expander("Command launched", expanded=False):
                st.code(cmd, language="shell")

        if st.button("📈 Run Once", key="btn_evaluator_once_sq"):
            cmd = (
                f'{VENV_PYTHON} worker_evaluate.py --once'
                f' --batch {eval_batch_sq}'
                f' --chroma-dir runtime/chroma/chroma_smoke_db'
                f' --collection {COLLECTION_NAME}'
            )
            subprocess.Popen(cmd, cwd=str(PROJECT_ROOT), shell=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            st.success("✅ Single evaluation pass launched")

    # ── Stop controls ──────────────────────────────────────────────────
    st.divider()
    st.subheader("🛑 Stop Workers")
    _cname_stop = _scraper_container_name(_active_db())
    stop_col1, stop_col2, stop_col3, stop_col4 = st.columns(4)
    with stop_col1:
        # Show running scraper containers
        _running = subprocess.run(
            ["docker", "ps", "--filter", "ancestor=gosom/google-maps-scraper:latest", "--format", "{{.Names}}"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        _running_names = [n.strip() for n in (_running.stdout or "").splitlines() if n.strip()]
        if _running_names:
            st.caption(f"Running: {', '.join(_running_names)}")
        else:
            st.caption("No scrapers running")
        _stop_target = st.selectbox("Container to stop", _running_names or [_cname_stop], key="_stop_scraper_sel")
        if st.button("🛑 Stop Scraper", key="stop_scraper"):
            subprocess.run(["docker", "stop", _stop_target], capture_output=True, encoding="utf-8", errors="replace")
            st.info(f"Stopped `{_stop_target}`.")
    with stop_col2:
        if st.button("🛑 Stop Enricher", key="stop_enricher"):
            subprocess.run(["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq worker_enrich*"], capture_output=True, encoding="utf-8", errors="replace")
            st.info("Sent stop signal to enricher.")
    with stop_col3:
        if st.button("🛑 Stop Ingester", key="stop_ingester"):
            subprocess.run(["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq run_document_ingest*"], capture_output=True, encoding="utf-8", errors="replace")
            st.info("Sent stop signal to ingester.")
    with stop_col4:
        if st.button("🛑 Stop Dedup", key="stop_dedup"):
            subprocess.run(["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq worker_dedup*"], capture_output=True, encoding="utf-8", errors="replace")
            st.info("Sent stop signal to dedup.")

    if st.button("🛑 Stop ALL Workers", key="stop_all", use_container_width=True):
        _cname_all = _scraper_container_name(_active_db())
        subprocess.run(["docker", "stop", _cname_all], capture_output=True, encoding="utf-8", errors="replace")
        subprocess.run(["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq worker_enrich*"], capture_output=True, encoding="utf-8", errors="replace")
        subprocess.run(["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq run_document_ingest*"], capture_output=True, encoding="utf-8", errors="replace")
        subprocess.run(["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq worker_dedup*"], capture_output=True, encoding="utf-8", errors="replace")
        st.info("Sent stop signals to all workers and scraper.")


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 6 — Company Evaluations
# ═══════════════════════════════════════════════════════════════════════════

elif page == "📈 Company Evaluations":
    st.title("📈 Company Evaluations")
    st.caption(f"Database: **{_active_db()}** — All companies with AI-extracted revenue & headcount")

    # ── Ensure company_evaluations table exists ────────────────────────
    _eval_table_exists = False
    try:
        run_scalar("SELECT 1 FROM company_evaluations LIMIT 1")
        _eval_table_exists = True
    except Exception:
        # Table doesn't exist yet — create it
        try:
            _force_reconnect()
            _admin_conn = psycopg2.connect(_active_dsn())
            _admin_conn.autocommit = False
            _admin_cur = _admin_conn.cursor()
            _admin_cur.execute("""
                CREATE TABLE IF NOT EXISTS company_evaluations (
                    id BIGSERIAL PRIMARY KEY,
                    result_id INTEGER NOT NULL REFERENCES results(id) ON DELETE CASCADE,
                    company TEXT NOT NULL DEFAULT '',
                    estimated_revenue TEXT NOT NULL DEFAULT '',
                    revenue_confidence TEXT NOT NULL DEFAULT '',
                    estimated_headcount TEXT NOT NULL DEFAULT '',
                    headcount_confidence TEXT NOT NULL DEFAULT '',
                    evidence_summary TEXT NOT NULL DEFAULT '',
                    chunks_used INTEGER NOT NULL DEFAULT 0,
                    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_company_evaluations_result UNIQUE (result_id)
                )
            """)
            _admin_cur.execute("CREATE INDEX IF NOT EXISTS idx_company_evaluations_result_id ON company_evaluations(result_id)")
            _admin_conn.commit()
            _admin_cur.close()
            _admin_conn.close()
            _force_reconnect()
            _eval_table_exists = True
            st.success("✅ Created `company_evaluations` table.")
        except Exception as _create_exc:
            st.error(f"Could not create company_evaluations table: {_create_exc}")
            _force_reconnect()

    # ── Summary metrics ────────────────────────────────────────────────
    if _eval_table_exists:
        eval_total = run_scalar("SELECT count(*) FROM company_evaluations") or 0
        eval_with_rev = run_scalar(
            "SELECT count(*) FROM company_evaluations WHERE estimated_revenue <> 'Unknown' AND estimated_revenue <> ''"
        ) or 0
        eval_with_hc = run_scalar(
            "SELECT count(*) FROM company_evaluations WHERE estimated_headcount <> 'Unknown' AND estimated_headcount <> ''"
        ) or 0
    else:
        eval_total = eval_with_rev = eval_with_hc = 0
    total_results = run_scalar("SELECT count(*) FROM results") or 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Companies", f"{total_results:,}")
    m2.metric("Evaluated", f"{eval_total:,}")
    m3.metric("Revenue Found", f"{eval_with_rev:,}")
    m4.metric("Headcount Found", f"{eval_with_hc:,}")

    # ── Filters ────────────────────────────────────────────────────────
    st.divider()
    ef1, ef2, ef3, ef4 = st.columns([3, 1, 1, 1])
    with ef1:
        ev_search = st.text_input("🔎 Search company name", "", key="ev_search")
    with ef2:
        try:
            _ev_cat_rows = run_query("""
                SELECT DISTINCT data->>'category' AS cat FROM results
                WHERE data->>'category' IS NOT NULL AND data->>'category' <> '' ORDER BY 1
            """)
            _ev_cat_opts = ["All"] + _ev_cat_rows["cat"].tolist()
        except Exception:
            _ev_cat_opts = ["All"]
        ev_cat = st.selectbox("Category", _ev_cat_opts, key="ev_cat")
    with ef3:
        try:
            _ev_state_rows = run_query("""
                SELECT DISTINCT data->'complete_address'->>'state' AS state FROM results
                WHERE data->'complete_address'->>'state' IS NOT NULL
                  AND data->'complete_address'->>'state' <> '' ORDER BY 1
            """)
            _ev_state_opts = ["All"] + _ev_state_rows["state"].tolist()
        except Exception:
            _ev_state_opts = ["All"]
        ev_state = st.selectbox("State", _ev_state_opts, key="ev_state")
    with ef4:
        ev_eval_filter = st.selectbox("Evaluation", ["All", "Evaluated", "Not evaluated",
                                                      "Revenue found", "Headcount found"], key="ev_eval")

    ef5, ef6 = st.columns(2)
    with ef5:
        ev_rev_conf = st.selectbox("Revenue confidence", ["All", "high", "medium", "low", "none"], key="ev_rc")
    with ef6:
        ev_hc_conf = st.selectbox("Headcount confidence", ["All", "high", "medium", "low", "none"], key="ev_hc")

    # ── Build query ────────────────────────────────────────────────────
    ev_wheres: list[str] = []
    ev_params: list = []
    if ev_search.strip():
        ev_wheres.append("(r.data->>'title' ILIKE %s)")
        ev_params.append(f"%{ev_search.strip()}%")
    if ev_cat != "All":
        ev_wheres.append("r.data->>'category' = %s")
        ev_params.append(ev_cat)
    if ev_state != "All":
        ev_wheres.append("r.data->'complete_address'->>'state' = %s")
        ev_params.append(ev_state)
    if ev_eval_filter == "Evaluated":
        ev_wheres.append("ce.id IS NOT NULL")
    elif ev_eval_filter == "Not evaluated":
        ev_wheres.append("ce.id IS NULL")
    elif ev_eval_filter == "Revenue found":
        ev_wheres.append("ce.estimated_revenue IS NOT NULL AND ce.estimated_revenue <> 'Unknown' AND ce.estimated_revenue <> ''")
    elif ev_eval_filter == "Headcount found":
        ev_wheres.append("ce.estimated_headcount IS NOT NULL AND ce.estimated_headcount <> 'Unknown' AND ce.estimated_headcount <> ''")
    if ev_rev_conf != "All":
        ev_wheres.append("ce.revenue_confidence = %s")
        ev_params.append(ev_rev_conf)
    if ev_hc_conf != "All":
        ev_wheres.append("ce.headcount_confidence = %s")
        ev_params.append(ev_hc_conf)

    ev_where_sql = (" AND ".join(ev_wheres)) if ev_wheres else "TRUE"

    ev_total_matches = run_scalar(f"""
        SELECT count(*) FROM results r
        LEFT JOIN company_evaluations ce ON ce.result_id = r.id
        WHERE {ev_where_sql}
    """, ev_params)
    st.caption(f"**{ev_total_matches:,}** matching companies")

    # ── Pagination ─────────────────────────────────────────────────────
    ev_page_size = 50
    ev_page_num = st.number_input("Page", min_value=1,
                                  max_value=max(1, (ev_total_matches // ev_page_size) + 1),
                                  value=1, key="ev_page")
    ev_offset = (ev_page_num - 1) * ev_page_size

    ev_df = run_query(f"""
        SELECT
            r.id,
            COALESCE(r.data->>'title', '') AS company,
            COALESCE(r.data->>'phone', '') AS phone,
            COALESCE(r.data->>'web_site', '') AS website,
            COALESCE(r.data->>'address', '') AS address,
            COALESCE(r.data->>'category', '') AS category,
            COALESCE(r.data->'complete_address'->>'city', '') AS city,
            COALESCE(r.data->'complete_address'->>'state', '') AS state,
            COALESCE(r.data->>'review_rating', '') AS rating,
            COALESCE(r.data->>'review_count', '') AS reviews,
            COALESCE(ce.estimated_revenue, '') AS est_revenue,
            COALESCE(ce.revenue_confidence, '') AS rev_confidence,
            COALESCE(ce.estimated_headcount, '') AS est_headcount,
            COALESCE(ce.headcount_confidence, '') AS hc_confidence,
            COALESCE(ce.evidence_summary, '') AS evidence
        FROM results r
        LEFT JOIN company_evaluations ce ON ce.result_id = r.id
        WHERE {ev_where_sql}
        ORDER BY r.id
        LIMIT {ev_page_size} OFFSET {ev_offset}
    """, ev_params)

    if ev_df.empty:
        st.info("No companies match your filters.")
    else:
        st.dataframe(ev_df, use_container_width=True, hide_index=True, column_config={
            "id": st.column_config.NumberColumn("ID", width="small"),
            "company": st.column_config.TextColumn("Company", width="large"),
            "phone": "Phone",
            "website": st.column_config.LinkColumn("Website", width="medium"),
            "address": st.column_config.TextColumn("Address", width="medium"),
            "category": "Category",
            "city": "City",
            "state": "State",
            "rating": "⭐",
            "reviews": "Reviews",
            "est_revenue": st.column_config.TextColumn("Est. Revenue", width="medium"),
            "rev_confidence": "Rev. Conf.",
            "est_headcount": st.column_config.TextColumn("Est. Headcount", width="small"),
            "hc_confidence": "HC Conf.",
            "evidence": st.column_config.TextColumn("Evidence Summary", width="large"),
        })

    # ── CSV Export ─────────────────────────────────────────────────────
    st.divider()
    if st.button("📥 Export all evaluations to CSV", key="btn_ev_csv"):
        export_df = run_query("""
            SELECT
                r.id,
                COALESCE(r.data->>'title', '') AS company,
                COALESCE(r.data->>'phone', '') AS phone,
                COALESCE(r.data->>'web_site', '') AS website,
                COALESCE(r.data->>'address', '') AS address,
                COALESCE(r.data->>'category', '') AS category,
                COALESCE(r.data->'complete_address'->>'city', '') AS city,
                COALESCE(r.data->'complete_address'->>'state', '') AS state,
                COALESCE(r.data->>'review_rating', '') AS rating,
                COALESCE(r.data->>'review_count', '') AS reviews,
                COALESCE(ce.estimated_revenue, '') AS est_revenue,
                COALESCE(ce.revenue_confidence, '') AS rev_confidence,
                COALESCE(ce.estimated_headcount, '') AS est_headcount,
                COALESCE(ce.headcount_confidence, '') AS hc_confidence,
                COALESCE(ce.evidence_summary, '') AS evidence
            FROM results r
            LEFT JOIN company_evaluations ce ON ce.result_id = r.id
            ORDER BY r.id
        """)
        if not export_df.empty:
            csv_data = export_df.to_csv(index=False)
            st.download_button(
                "⬇️ Download CSV", csv_data,
                file_name=f"{_active_db()}_evaluations.csv",
                mime="text/csv",
            )
        else:
            st.info("No data to export.")

    # ── Confidence charts ──────────────────────────────────────────────
    if eval_total > 0:
        st.divider()
        conf_col1, conf_col2 = st.columns(2)
        with conf_col1:
            st.subheader("Revenue Confidence")
            rev_conf = run_query(
                "SELECT revenue_confidence AS confidence, count(*) AS cnt "
                "FROM company_evaluations GROUP BY 1 ORDER BY 2 DESC"
            )
            if not rev_conf.empty:
                colors = {"high": "#22c55e", "medium": "#eab308", "low": "#f97316", "none": "#a3a3a3"}
                fig_rc = px.pie(rev_conf, values="cnt", names="confidence",
                               color="confidence", color_discrete_map=colors, hole=0.4)
                fig_rc.update_layout(height=320, margin=dict(t=20, b=20))
                st.plotly_chart(fig_rc, use_container_width=True)
        with conf_col2:
            st.subheader("Headcount Confidence")
            hc_conf = run_query(
                "SELECT headcount_confidence AS confidence, count(*) AS cnt "
                "FROM company_evaluations GROUP BY 1 ORDER BY 2 DESC"
            )
            if not hc_conf.empty:
                colors = {"high": "#22c55e", "medium": "#eab308", "low": "#f97316", "none": "#a3a3a3"}
                fig_hc = px.pie(hc_conf, values="cnt", names="confidence",
                               color="confidence", color_discrete_map=colors, hole=0.4)
                fig_hc.update_layout(height=320, margin=dict(t=20, b=20))
                st.plotly_chart(fig_hc, use_container_width=True)

    # ── Launch evaluation worker ───────────────────────────────────────
    st.divider()
    st.subheader("🚀 Run Evaluator")
    st.caption("Queries ChromaDB for each company, extracts revenue & headcount via OpenAI.")

    ev_col1, ev_col2 = st.columns(2)
    with ev_col1:
        ev_batch = st.number_input("Batch size", min_value=1, max_value=100, value=10, key="ev_batch")
    with ev_col2:
        ev_poll = st.number_input("Poll interval (s)", min_value=5, max_value=300, value=30, key="ev_poll")

    evbtn1, evbtn2 = st.columns(2)
    with evbtn1:
        if st.button("📈 Start Evaluator (continuous)", key="btn_evaluator"):
            ev_cmd = (
                f'{VENV_PYTHON} worker_evaluate.py'
                f' --batch {ev_batch} --poll {ev_poll}'
                f' --chroma-dir runtime/chroma/chroma_smoke_db'
                f' --collection {COLLECTION_NAME}'
            )
            subprocess.Popen(ev_cmd, cwd=str(PROJECT_ROOT), shell=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            st.success(f"✅ Evaluator started (batch={ev_batch}, poll={ev_poll}s)")
    with evbtn2:
        if st.button("📈 Run Once (single pass)", key="btn_evaluator_once"):
            ev_cmd = (
                f'{VENV_PYTHON} worker_evaluate.py --once'
                f' --batch {ev_batch}'
                f' --chroma-dir runtime/chroma/chroma_smoke_db'
                f' --collection {COLLECTION_NAME}'
            )
            subprocess.Popen(ev_cmd, cwd=str(PROJECT_ROOT), shell=True,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            st.success("✅ Single evaluation pass launched — refresh in a moment to see results.")


# ── Sidebar footer ─────────────────────────────────────────────────────
st.sidebar.divider()
st.sidebar.caption(f"DB: `{_active_db()}` @ {_parsed_dsn}")
st.sidebar.caption(f"Chroma: `{COLLECTION_NAME}`")
try:
    _footer_count = run_scalar('SELECT count(*) FROM results')
    st.sidebar.caption(f"Companies: {_footer_count:,}")
except Exception:
    st.sidebar.caption("Companies: —")
if st.sidebar.button("🔄 Refresh"):
    st.rerun()
