import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useDb } from '../DbContext'
import { api } from '../api'
import {
  Loader2, Play, Square, Search, FileText, Layers,
  CheckCircle2, AlertCircle, Zap, ChevronDown,
  ChevronUp, MapPin, Sparkles, Link2, Database,
  Clock, Globe, Newspaper, Box, Activity,
} from 'lucide-react'

// ── Trades for the query generator ──────────────────────────────────
const TRADES = [
  'Construction Companies', 'General Contractors', 'Home Builders',
  'Custom Home Builders', 'Roofing Contractors', 'Concrete Contractors',
  'Remodelers', 'Excavating Contractors', 'Commercial Contractors',
  'Residential Contractors', 'Plumbing Contractors', 'Electrical Contractors',
  'HVAC Contractors', 'Painting Contractors', 'Landscaping Companies',
]

// ── Small metric chip ───────────────────────────────────────────────
function Chip({ icon: Icon, label, value, color = 'gray' }) {
  const cls = {
    blue:   'border-blue-500/30  text-blue-400',
    green:  'border-green-500/30 text-green-400',
    amber:  'border-amber-500/30 text-amber-400',
    purple: 'border-purple-500/30 text-purple-400',
    gray:   'border-gray-700     text-gray-400',
  }
  return (
    <div className={`rounded-lg border bg-gray-900/80 ${cls[color]} px-4 py-3`}>
      <div className="flex items-center gap-1.5 text-[11px] text-gray-500 mb-0.5">
        <Icon size={12} /> {label}
      </div>
      <div className="text-xl font-bold text-white">{value?.toLocaleString() ?? '—'}</div>
    </div>
  )
}

// =====================================================================
export default function PipelinePage() {
  const { activeDb } = useDb()
  const qc = useQueryClient()

  // ── Query-entry state ─────────────────────────────────────────────
  const [queries, setQueries]       = useState('')
  const [city, setCity]             = useState('')
  const [state, setState]           = useState('')
  const [trades, setTrades]         = useState([
    'Construction Companies', 'General Contractors', 'Home Builders',
  ])
  const [showGenerator, setShowGenerator] = useState(false)
  const [showAdvanced, setShowAdvanced]   = useState(false)

  // ── Pipeline config ───────────────────────────────────────────────
  const [batch, setBatch]     = useState(10)
  const [poll, setPoll]       = useState(20)
  const [newsTop, setNewsTop] = useState(10)

  // ── Queries ───────────────────────────────────────────────────────
  const { data: queryFiles } = useQuery({
    queryKey: ['queryFiles'],
    queryFn: () => api.getQueryFiles(),
  })

  // ── Enrichment / pipeline stats ───────────────────────────────────
  const { data: stats } = useQuery({
    queryKey: ['enrichmentStats', activeDb],
    queryFn: () => api.getEnrichmentStats(activeDb),
    enabled: !!activeDb,
    refetchInterval: 8_000,
  })

  const { data: procs } = useQuery({
    queryKey: ['workerProcesses'],
    queryFn: () => api.getWorkerProcesses(),
    refetchInterval: 5_000,
  })

  const { data: workers } = useQuery({
    queryKey: ['workerStatus'],
    queryFn: () => api.getWorkerStatus(),
    refetchInterval: 8_000,
  })

  const { data: scraperStatus } = useQuery({
    queryKey: ['scraperStatus'],
    queryFn: () => api.getScraperStatus(),
    refetchInterval: 10_000,
  })

  // ── Mutations ─────────────────────────────────────────────────────
  // Save queries → seed scraper → start scraper → start unified
  const runPipeline = useMutation({
    mutationFn: async () => {
      const lines = queries.trim().split('\n').filter(l => l.trim())
      if (lines.length === 0) throw new Error('Enter at least one search query')

      // 1. Save a query file
      const filename = `pipeline_${Date.now()}.txt`
      await api.saveQueryFile(filename, lines)

      // 2. Seed & start the scraper
      await api.startScraper(activeDb, {
        query_file: filename,
        concurrency: 4,
        depth: 1,
      })

      // 3. Start the unified enrichment pipeline
      await api.startUnified(activeDb, { batch, poll, news_top: newsTop })

      return { queries: lines.length, filename }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['workerProcesses'] })
      qc.invalidateQueries({ queryKey: ['scraperStatus'] })
      qc.invalidateQueries({ queryKey: ['queryFiles'] })
    },
  })

  const startEnrich = useMutation({
    mutationFn: () => api.startUnified(activeDb, { batch, poll, news_top: newsTop }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['workerProcesses'] }),
  })

  const stopPipeline = useMutation({
    mutationFn: async () => {
      await api.stopWorker(activeDb, 'unified').catch(() => {})
      await api.stopScraper(activeDb).catch(() => {})
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['workerProcesses'] })
      qc.invalidateQueries({ queryKey: ['scraperStatus'] })
    },
  })

  // ── Derived state ─────────────────────────────────────────────────
  const pipelineRunning = procs?.processes?.some(
    p => p.CommandLine?.includes('worker_unified')
  )
  const scraperRunning = scraperStatus?.containers?.some(
    c => c.status?.toLowerCase().includes('up')
  )
  const isRunning = pipelineRunning || scraperRunning

  const unifiedStatus = workers?.statuses?.find(s =>
    s._filename?.includes('unified') || s.worker === 'unified_pipeline'
  )
  const totals = unifiedStatus?.totals || {}

  const totalResults = stats?.total_results || 0
  const enriched    = stats?.results_enriched || 0
  const pending     = stats?.results_pending || 0
  const pct = totalResults > 0 ? Math.round((enriched / totalResults) * 100) : 0

  // ── Scraper logs (live, only when running) ─────────────────────────
  const { data: scraperLogs } = useQuery({
    queryKey: ['scraperLogs', activeDb],
    queryFn: () => api.getScraperLogs(activeDb, 30).catch(() => ({ logs: '' })),
    enabled: !!activeDb && isRunning,
    refetchInterval: 4_000,
  })

  // Parse scraper log into meaningful events
  const scraperEvents = useMemo(() => {
    if (!scraperLogs?.logs) return []
    return scraperLogs.logs
      .split('\n')
      .filter(l => l.trim())
      .map(line => {
        try {
          const j = JSON.parse(line)
          if (j.message === 'scrapemate stats') {
            return { type: 'stats', jobs: j.numOfJobsCompleted, failed: j.numOfJobsFailed, speed: j.speed, time: j.time }
          }
          if (j.message?.includes('exiting')) {
            return { type: 'exit', msg: 'Scraper finished — no more jobs', time: j.time }
          }
          if (j.message === 'starting scrapemate') {
            return { type: 'start', msg: 'Scraper started', time: j.time }
          }
          return null
        } catch {
          if (line.includes('INFO Downloading driver')) return { type: 'info', msg: 'Downloading browser…' }
          if (line.includes('Downloaded driver')) return { type: 'info', msg: 'Browser ready' }
          if (line.includes('Downloaded browsers')) return { type: 'info', msg: 'Browser setup complete' }
          return null
        }
      })
      .filter(Boolean)
  }, [scraperLogs?.logs])

  // Derive a human-readable phase
  const currentPhase = useMemo(() => {
    if (!isRunning) return null
    const scraperExited = scraperEvents.some(e => e.type === 'exit')
    const scraperHasStats = scraperEvents.filter(e => e.type === 'stats')
    const lastStat = scraperHasStats[scraperHasStats.length - 1]

    if (scraperRunning && (!lastStat || lastStat.jobs === 0)) {
      return { icon: Search, label: 'Searching Google Maps…', detail: 'Looking for companies matching your queries', color: 'blue' }
    }
    if (scraperRunning && lastStat?.jobs > 0) {
      return { icon: Search, label: `Found ${lastStat.jobs} companies so far`, detail: `Speed: ${lastStat.speed}`, color: 'blue' }
    }
    if (scraperExited && pipelineRunning && totals.companies_processed === 0) {
      return { icon: Clock, label: 'Scraper finished, enrichment starting…', detail: `${totalResults} companies to process`, color: 'amber' }
    }
    if (pipelineRunning && totals.companies_processed > 0) {
      const phaseParts = []
      if (totals.news_links > 0) phaseParts.push(`${totals.news_links} news articles`)
      if (totals.website_pages > 0) phaseParts.push(`${totals.website_pages} web pages`)
      if (totals.chunks_embedded > 0) phaseParts.push(`${totals.chunks_embedded} chunks embedded`)
      return { icon: Zap, label: `Processing company ${totals.companies_processed} of ${totalResults || '?'}`, detail: phaseParts.join(' • ') || 'Gathering data…', color: 'emerald' }
    }
    if (pipelineRunning) {
      return { icon: Clock, label: 'Waiting for companies…', detail: 'The scraper will feed results to the pipeline', color: 'gray' }
    }
    return { icon: Activity, label: 'Running…', detail: '', color: 'blue' }
  }, [isRunning, scraperRunning, pipelineRunning, scraperEvents, totals, totalResults])

  const queryCount = useMemo(
    () => queries.trim().split('\n').filter(l => l.trim()).length,
    [queries],
  )

  if (!activeDb) {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] text-gray-500 space-y-3">
        <Database size={40} className="text-gray-700" />
        <p>Select or create a database to get started.</p>
      </div>
    )
  }

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div className="max-w-3xl mx-auto space-y-6">

      {/* ── Header ─────────────────────────────────────────────── */}
      <div>
        <h2 className="text-2xl font-bold text-white">Pipeline</h2>
        <p className="text-sm text-gray-500 mt-1">
          Enter your searches, hit <span className="text-emerald-400 font-medium">Run</span>, and we'll find, research & organize leads for you.
        </p>
      </div>

      {/* ━━━━━━━━━ Step 1 — Search queries ━━━━━━━━━━━━━━━━━━━━━ */}
      <section className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Search size={16} className="text-blue-400" />
            <h3 className="text-sm font-semibold text-gray-200">
              What are you looking for?
            </h3>
          </div>

          {/* Quick-load a saved query file */}
          {queryFiles?.files?.length > 0 && (
            <select
              onChange={(e) => {
                if (!e.target.value) return
                api.getQueryFile(e.target.value).then(d => setQueries(d.lines.join('\n')))
                e.target.value = ''
              }}
              className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-400 focus:outline-none"
            >
              <option value="">Load saved queries…</option>
              {queryFiles.files.map(f => (
                <option key={f.name} value={f.name}>{f.name} ({f.queries})</option>
              ))}
            </select>
          )}
        </div>

        {/* Textarea */}
        <div className="px-5 py-4">
          <textarea
            value={queries}
            onChange={(e) => setQueries(e.target.value)}
            placeholder={`Type one search per line, e.g.\n\nConstruction companies in Charlotte NC\nHome builders in Raleigh NC\nRoofing contractors in Atlanta GA`}
            rows={5}
            className="w-full bg-gray-800/60 border border-gray-700 rounded-lg px-4 py-3 text-sm text-gray-200 placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-blue-500 resize-y"
          />
          {queryCount > 0 && queries.trim() && (
            <p className="text-xs text-gray-500 mt-1.5">
              {queryCount} {queryCount === 1 ? 'search' : 'searches'} ready
            </p>
          )}
        </div>

        {/* Query generator toggle */}
        <div className="border-t border-gray-800">
          <button
            onClick={() => setShowGenerator(!showGenerator)}
            className="w-full px-5 py-3 flex items-center gap-2 text-xs text-gray-500 hover:text-gray-300 transition-colors"
          >
            <Sparkles size={12} />
            <span>Generate queries for a city</span>
            {showGenerator ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
          </button>

          {showGenerator && (
            <div className="px-5 pb-4 space-y-3">
              <div className="flex gap-2">
                <div className="flex-1">
                  <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">City</label>
                  <input
                    value={city} onChange={(e) => setCity(e.target.value)}
                    placeholder="Charlotte"
                    className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                </div>
                <div className="w-24">
                  <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">State</label>
                  <input
                    value={state} onChange={(e) => setState(e.target.value)}
                    placeholder="NC"
                    className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                </div>
              </div>

              <div>
                <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">Trades</label>
                <div className="flex flex-wrap gap-1.5 mt-1.5">
                  {TRADES.map(t => (
                    <button
                      key={t}
                      onClick={() => setTrades(prev =>
                        prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]
                      )}
                      className={`px-2.5 py-1 rounded-full text-[11px] transition-colors ${
                        trades.includes(t)
                          ? 'bg-blue-600/30 text-blue-400 border border-blue-500/30'
                          : 'bg-gray-800 text-gray-500 border border-gray-700'
                      }`}
                    >
                      {t}
                    </button>
                  ))}
                </div>
              </div>

              <button
                onClick={() => {
                  const generated = trades.map(t => `${t} in ${city.trim()} ${state.trim()}`)
                  setQueries(prev => {
                    const existing = prev.trim()
                    return existing ? `${existing}\n${generated.join('\n')}` : generated.join('\n')
                  })
                }}
                disabled={!city.trim() || !state.trim() || trades.length === 0}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600/20 text-blue-400 border border-blue-500/30 rounded-lg text-sm hover:bg-blue-600/30 disabled:opacity-40 transition-colors"
              >
                <MapPin size={14} />
                Add {trades.length} {trades.length === 1 ? 'query' : 'queries'} to list
              </button>
            </div>
          )}
        </div>
      </section>

      {/* ━━━━━━━━━ Step 2 — Run / Status ━━━━━━━━━━━━━━━━━━━━━━ */}
      <section className={`rounded-xl border-2 p-6 transition-colors ${
        isRunning
          ? 'border-emerald-500/40 bg-emerald-950/20'
          : 'border-gray-800 bg-gray-900/60'
      }`}>
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            {isRunning ? (
              <div className="relative">
                <Zap size={22} className="text-emerald-400" />
                <span className="absolute -top-0.5 -right-0.5 w-2.5 h-2.5 bg-emerald-400 rounded-full animate-pulse" />
              </div>
            ) : (
              <Zap size={22} className="text-gray-600" />
            )}
            <div>
              <h3 className="text-lg font-semibold text-white">
                {isRunning ? 'Pipeline Running' : 'Ready to Run'}
              </h3>
              {isRunning && currentPhase && (
                <div className="flex items-center gap-2 mt-0.5">
                  <currentPhase.icon size={13} className={`text-${currentPhase.color}-400`} />
                  <p className="text-sm text-gray-300 font-medium">{currentPhase.label}</p>
                </div>
              )}
              {isRunning && currentPhase?.detail && (
                <p className="text-xs text-gray-500 mt-0.5">{currentPhase.detail}</p>
              )}
            </div>
          </div>

          <div className="flex items-center gap-2">
            {isRunning ? (
              <button
                onClick={() => stopPipeline.mutate()}
                disabled={stopPipeline.isPending}
                className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-red-600/20 text-red-400 border border-red-700/50 text-sm font-medium hover:bg-red-600/30 transition-colors disabled:opacity-50"
              >
                {stopPipeline.isPending
                  ? <Loader2 size={16} className="animate-spin" />
                  : <Square size={16} />}
                Stop
              </button>
            ) : (
              <div className="flex items-center gap-2">
                {totalResults > 0 && pending > 0 && (
                  <button
                    onClick={() => startEnrich.mutate()}
                    disabled={startEnrich.isPending}
                    className="flex items-center gap-2 px-4 py-2.5 rounded-lg bg-gray-800 text-gray-300 text-sm hover:bg-gray-700 transition-colors disabled:opacity-50"
                  >
                    {startEnrich.isPending
                      ? <Loader2 size={16} className="animate-spin" />
                      : <Zap size={16} />}
                    Enrich Only
                  </button>
                )}
                <button
                  onClick={() => runPipeline.mutate()}
                  disabled={runPipeline.isPending || (!queries.trim() && pending === 0)}
                  className="flex items-center gap-2 px-6 py-2.5 rounded-lg bg-emerald-600 text-white text-sm font-semibold hover:bg-emerald-500 transition-colors disabled:opacity-50 shadow-lg shadow-emerald-900/30"
                >
                  {runPipeline.isPending
                    ? <Loader2 size={16} className="animate-spin" />
                    : <Play size={16} />}
                  Run Pipeline
                </button>
              </div>
            )}
          </div>
        </div>

        {/* Progress bar */}
        {totalResults > 0 && (
          <div className="mb-2">
            <div className="flex justify-between text-xs text-gray-500 mb-1.5">
              <span>{enriched.toLocaleString()} of {totalResults.toLocaleString()} companies processed</span>
              <span className="font-mono">{pct}%</span>
            </div>
            <div className="w-full bg-gray-800 rounded-full h-2.5 overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-500 ${
                  isRunning ? 'bg-emerald-500' : 'bg-blue-500'
                }`}
                style={{ width: `${Math.max(pct, 1)}%` }}
              />
            </div>
          </div>
        )}

        {pending > 0 && (
          <p className="text-xs text-amber-400 mt-2">
            ⚡ {pending.toLocaleString()} companies still pending
          </p>
        )}
        {pending === 0 && totalResults > 0 && !isRunning && (
          <p className="text-xs text-emerald-400 mt-2 flex items-center gap-1">
            <CheckCircle2 size={12} /> All companies processed
          </p>
        )}

        {/* ── Live activity panel (only while running) ─────────── */}
        {isRunning && (
          <div className="mt-4 pt-4 border-t border-gray-800/60 space-y-4">

            {/* 4-column live counters */}
            <div className="grid grid-cols-4 gap-3">
              <div className="bg-gray-800/60 rounded-lg p-3 text-center">
                <div className="flex items-center justify-center gap-1.5 text-[10px] text-gray-500 mb-1">
                  <Search size={10} /> FOUND
                </div>
                <div className="text-xl font-bold text-blue-400">{totalResults || '—'}</div>
                <div className="text-[10px] text-gray-600">companies</div>
              </div>
              <div className="bg-gray-800/60 rounded-lg p-3 text-center">
                <div className="flex items-center justify-center gap-1.5 text-[10px] text-gray-500 mb-1">
                  <Newspaper size={10} /> NEWS
                </div>
                <div className="text-xl font-bold text-green-400">{totals.news_links || '—'}</div>
                <div className="text-[10px] text-gray-600">articles</div>
              </div>
              <div className="bg-gray-800/60 rounded-lg p-3 text-center">
                <div className="flex items-center justify-center gap-1.5 text-[10px] text-gray-500 mb-1">
                  <Globe size={10} /> PAGES
                </div>
                <div className="text-xl font-bold text-amber-400">{totals.docs_ok || '—'}</div>
                <div className="text-[10px] text-gray-600">fetched</div>
              </div>
              <div className="bg-gray-800/60 rounded-lg p-3 text-center">
                <div className="flex items-center justify-center gap-1.5 text-[10px] text-gray-500 mb-1">
                  <Box size={10} /> CHUNKS
                </div>
                <div className="text-xl font-bold text-purple-400">{totals.chunks_embedded || '—'}</div>
                <div className="text-[10px] text-gray-600">embedded</div>
              </div>
            </div>

            {/* Step-by-step pipeline phases */}
            <div className="space-y-1">
              <p className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold mb-2">Pipeline Steps</p>
              {[
                { icon: Search,    label: 'Find companies on Google Maps',      done: !scraperRunning && (totalResults > 0 || scraperEvents.some(e => e.type === 'exit')), active: scraperRunning },
                { icon: Newspaper, label: 'Discover news articles',              done: totals.news_links > 0 && !scraperRunning, active: pipelineRunning && totals.companies_processed > 0 && totals.news_links === 0 },
                { icon: Globe,     label: 'Fetch websites & documents',          done: totals.docs_ok > 0 && pending === 0, active: pipelineRunning && totals.news_links > 0 },
                { icon: Box,       label: 'Chunk & embed for search',            done: totals.chunks_embedded > 0 && pending === 0, active: pipelineRunning && totals.docs_ok > 0 },
              ].map((step, i) => (
                <div key={i} className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm ${
                  step.active ? 'bg-emerald-950/30 text-emerald-300' :
                  step.done   ? 'text-gray-500' :
                                'text-gray-600'
                }`}>
                  <div className="w-5 flex justify-center">
                    {step.active ? <Loader2 size={14} className="animate-spin text-emerald-400" /> :
                     step.done   ? <CheckCircle2 size={14} className="text-emerald-600" /> :
                                   <div className="w-2 h-2 rounded-full bg-gray-700" />}
                  </div>
                  <step.icon size={14} />
                  <span>{step.label}</span>
                  {step.active && <span className="ml-auto text-[10px] text-emerald-500 animate-pulse">in progress</span>}
                </div>
              ))}
            </div>

            {/* Scraper live log */}
            {scraperEvents.length > 0 && (
              <div>
                <p className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold mb-2">Scraper Log</p>
                <div className="bg-gray-950 rounded-lg border border-gray-800 p-3 max-h-32 overflow-y-auto font-mono text-[11px] text-gray-400 space-y-0.5">
                  {scraperEvents.map((e, i) => (
                    <div key={i} className={
                      e.type === 'exit' ? 'text-amber-400' :
                      e.type === 'stats' ? 'text-gray-500' :
                      e.type === 'start' ? 'text-emerald-500' :
                      'text-gray-500'
                    }>
                      {e.type === 'stats'
                        ? `📊 ${e.jobs} jobs done, ${e.failed} failed — ${e.speed}`
                        : e.type === 'exit'
                          ? `⏹ ${e.msg}`
                          : e.type === 'start'
                            ? `▶ ${e.msg}`
                            : `ℹ ${e.msg}`}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Errors */}
        {runPipeline.isError && (
          <p className="text-xs text-red-400 mt-3 flex items-center gap-1">
            <AlertCircle size={12} /> {runPipeline.error?.message || 'Failed to start'}
          </p>
        )}
        {runPipeline.isSuccess && !isRunning && (
          <p className="text-xs text-emerald-400 mt-3 flex items-center gap-1">
            <CheckCircle2 size={12} /> Pipeline started — status will update shortly
          </p>
        )}
      </section>

      {/* ━━━━━━━━━ Metrics ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */}
      {totalResults > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Chip icon={Database} label="Companies Found"   value={totalResults}           color="blue" />
          <Chip icon={Link2}    label="Link Candidates"   value={
            stats?.link_candidates_by_type
              ? Object.values(stats.link_candidates_by_type).reduce((a, b) => a + b, 0)
              : 0
          } color="green" />
          <Chip icon={FileText}  label="Documents"         value={stats?.total_documents}  color="amber" />
          <Chip icon={Layers}    label="Chunks Embedded"   value={stats?.total_chunks}     color="purple" />
        </div>
      )}

      {/* ━━━━━━━━━ Advanced settings (collapsed) ━━━━━━━━━━━━━━ */}
      <div className="bg-gray-900 rounded-lg border border-gray-800">
        <button
          onClick={() => setShowAdvanced(!showAdvanced)}
          className="w-full px-5 py-3 flex items-center justify-between text-xs text-gray-500 hover:text-gray-300 transition-colors"
        >
          <span>Advanced Settings</span>
          {showAdvanced ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
        </button>
        {showAdvanced && (
          <div className="px-5 pb-4 space-y-3 border-t border-gray-800 pt-3">
            <div className="grid grid-cols-3 gap-3">
              <div>
                <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">Batch Size</label>
                <input type="number" value={batch} onChange={(e) => setBatch(+e.target.value)} min={1} max={50}
                  className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500" />
                <p className="text-[10px] text-gray-600 mt-0.5">Companies per cycle</p>
              </div>
              <div>
                <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">Poll Interval</label>
                <input type="number" value={poll} onChange={(e) => setPoll(+e.target.value)} min={5} max={300}
                  className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500" />
                <p className="text-[10px] text-gray-600 mt-0.5">Seconds between cycles</p>
              </div>
              <div>
                <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">News per Company</label>
                <input type="number" value={newsTop} onChange={(e) => setNewsTop(+e.target.value)} min={1} max={50}
                  className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500" />
                <p className="text-[10px] text-gray-600 mt-0.5">Max news URLs to keep</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
