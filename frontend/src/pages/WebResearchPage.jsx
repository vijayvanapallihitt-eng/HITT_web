import { useState, useEffect, useRef } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { useDb } from '../DbContext'
import { api } from '../api'
import {
  Globe, Loader2, CheckCircle2, XCircle, ArrowLeft,
  Search as SearchIcon, FileText, AlertTriangle, ChevronDown, ChevronRight,
} from 'lucide-react'

const PHASE_ICONS = {
  batch: '📋',
  search: '🔍',
  search_done: '✅',
  crawl: '🌐',
  crawl_ok: '✅',
  crawl_skip: '⏭️',
  crawl_fail: '❌',
  done: '🏁',
  ingest: '📦',
  ingest_done: '📦',
  batch_done: '🎉',
}

export default function WebResearchPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const { activeDb } = useDb()

  // Get result IDs from navigation state or allow manual entry
  const passedIds = location.state?.resultIds || []
  const passedDb = location.state?.db || activeDb

  const [resultIds, setResultIds] = useState(passedIds)
  const [manualIds, setManualIds] = useState(passedIds.join(', '))
  const [maxResults, setMaxResults] = useState(10)
  const [maxPages, setMaxPages] = useState(8)
  const [searchSuffix, setSearchSuffix] = useState('construction')
  const [runIngest, setRunIngest] = useState(true)

  const [running, setRunning] = useState(false)
  const [logs, setLogs] = useState([])
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [elapsed, setElapsed] = useState(0)
  const [currentCompany, setCurrentCompany] = useState('')
  const [companySummaries, setCompanySummaries] = useState({})

  const abortRef = useRef(null)
  const logEndRef = useRef(null)
  const startTimeRef = useRef(null)
  const timerRef = useRef(null)

  // Auto-scroll log panel
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  // Elapsed timer
  useEffect(() => {
    if (running) {
      startTimeRef.current = Date.now()
      timerRef.current = setInterval(() => {
        setElapsed(Math.floor((Date.now() - startTimeRef.current) / 1000))
      }, 1000)
    } else {
      clearInterval(timerRef.current)
    }
    return () => clearInterval(timerRef.current)
  }, [running])

  // No auto-start — let user review/edit config before launching

  const startResearch = async () => {
    const ids = resultIds.length > 0
      ? resultIds
      : manualIds.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n))

    if (!ids.length) return

    setRunning(true)
    setLogs([])
    setResult(null)
    setError(null)
    setElapsed(0)
    setCurrentCompany('')
    setCompanySummaries({})

    const stream = api.runWebResearchStream(passedDb || activeDb, ids, {
      max_google_results: maxResults,
      max_crawl_pages: maxPages,
      search_suffix: searchSuffix,
      run_ingest: runIngest,
    })
    abortRef.current = stream.abort

    try {
      for await (const { event, data } of stream.events()) {
        if (event === 'progress') {
          setLogs(prev => [...prev, data])
          if (data.phase === 'batch' && data.detail?.company) {
            setCurrentCompany(data.detail.company)
          }
          if (data.phase === 'done' && data.detail) {
            setCompanySummaries(prev => ({
              ...prev,
              [data.detail.company || data.detail.result_id]: data.detail,
            }))
          }
        } else if (event === 'result') {
          setResult(data)
        } else if (event === 'error') {
          setError(data.message || 'Unknown error')
        }
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        setError(err.message)
      }
    } finally {
      setRunning(false)
      abortRef.current = null
    }
  }

  const cancelResearch = () => {
    abortRef.current?.()
    setRunning(false)
  }

  const fmtTime = (s) => {
    const m = Math.floor(s / 60)
    const sec = s % 60
    return m > 0 ? `${m}m ${sec}s` : `${sec}s`
  }

  return (
    <div className="space-y-5 max-w-5xl">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate('/companies')}
          className="p-1.5 rounded hover:bg-gray-800 text-gray-400 hover:text-gray-200 transition-colors"
        >
          <ArrowLeft size={18} />
        </button>
        <div>
          <h2 className="text-2xl font-bold text-white flex items-center gap-2">
            <Globe size={22} className="text-emerald-400" /> Web Research
          </h2>
          <p className="text-xs text-gray-500 mt-0.5">
            Google search + crawl4ai for selected companies • {passedDb || activeDb}
          </p>
        </div>
      </div>

      {/* Config panel — only if not auto-started */}
      {!running && !result && (
        <div className="bg-gray-900/60 border border-gray-800 rounded-lg p-4 space-y-3">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div>
              <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">
                Result IDs
              </label>
              <input
                type="text"
                value={manualIds}
                onChange={e => {
                  setManualIds(e.target.value)
                  setResultIds(e.target.value.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n)))
                }}
                className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
                placeholder="44, 272, 303…"
              />
            </div>
            <div>
              <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">
                Max Google Results
              </label>
              <input
                type="number"
                value={maxResults}
                onChange={e => setMaxResults(Number(e.target.value))}
                min={1} max={30}
                className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">
                Max Pages to Crawl
              </label>
              <input
                type="number"
                value={maxPages}
                onChange={e => setMaxPages(Number(e.target.value))}
                min={1} max={20}
                className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">
                Search Suffix
              </label>
              <input
                type="text"
                value={searchSuffix}
                onChange={e => setSearchSuffix(e.target.value)}
                className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
                placeholder="construction"
              />
            </div>
          </div>
          <div className="flex items-center gap-4">
            <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
              <input
                type="checkbox"
                checked={runIngest}
                onChange={e => setRunIngest(e.target.checked)}
                className="rounded border-gray-600 text-blue-500 focus:ring-blue-500"
              />
              Auto-embed after crawling
            </label>
            <button
              onClick={startResearch}
              disabled={resultIds.length === 0 && manualIds.trim().length === 0}
              className="flex items-center gap-1.5 px-4 py-2 rounded bg-emerald-600 text-white text-sm font-medium hover:bg-emerald-500 disabled:opacity-40 transition-colors"
            >
              <Globe size={14} /> Start Web Research ({resultIds.length || manualIds.split(',').filter(s => s.trim()).length} companies)
            </button>
          </div>
        </div>
      )}

      {/* Status bar */}
      {running && (
        <div className="flex items-center justify-between bg-gray-900/60 border border-gray-800 rounded-lg px-4 py-3">
          <div className="flex items-center gap-3">
            <Loader2 size={16} className="animate-spin text-emerald-400" />
            <span className="text-sm text-gray-200">
              {currentCompany ? `Researching: ${currentCompany}` : 'Starting…'}
            </span>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-500 font-mono">{fmtTime(elapsed)}</span>
            <button
              onClick={cancelResearch}
              className="px-3 py-1 rounded bg-red-600/20 text-red-400 text-xs hover:bg-red-600/40 transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="flex items-start gap-2 bg-red-900/20 border border-red-800/50 rounded-lg px-4 py-3">
          <AlertTriangle size={16} className="text-red-400 mt-0.5 flex-shrink-0" />
          <div className="text-sm text-red-300">{error}</div>
        </div>
      )}

      {/* Final result summary */}
      {result && (
        <div className="bg-emerald-900/20 border border-emerald-700/40 rounded-lg p-4 space-y-3">
          <div className="flex items-center gap-2 text-emerald-300 font-medium">
            <CheckCircle2 size={16} /> Research Complete
            <span className="text-xs text-gray-500 font-mono ml-auto">{fmtTime(elapsed)}</span>
          </div>
          <div className="grid grid-cols-3 gap-4 text-center">
            <div>
              <div className="text-2xl font-bold text-white">{result.companies_count}</div>
              <div className="text-xs text-gray-500">Companies</div>
            </div>
            <div>
              <div className="text-2xl font-bold text-emerald-400">{result.total_docs}</div>
              <div className="text-xs text-gray-500">Documents Stored</div>
            </div>
            <div>
              <div className="text-2xl font-bold text-gray-400">{result.total_skipped}</div>
              <div className="text-xs text-gray-500">Skipped / Irrelevant</div>
            </div>
          </div>

          {/* Per-company breakdown */}
          {result.summaries?.length > 0 && (
            <div className="mt-3 space-y-1">
              {result.summaries.map((s, i) => (
                <div key={i} className="flex items-center gap-2 text-xs text-gray-300 bg-gray-800/40 rounded px-3 py-1.5">
                  <span className="font-medium text-gray-200 min-w-[180px] truncate">{s.company}</span>
                  <span className="text-emerald-400">{s.docs} docs</span>
                  <span className="text-gray-500">•</span>
                  <span className="text-gray-400">{s.skipped} skipped</span>
                  <span className="text-gray-500">•</span>
                  <span className="text-gray-500">{s.links} links</span>
                </div>
              ))}
            </div>
          )}

          <button
            onClick={() => { setResult(null); setLogs([]) }}
            className="px-3 py-1.5 rounded bg-gray-800 text-gray-300 text-sm hover:bg-gray-700 transition-colors"
          >
            Run Again
          </button>
        </div>
      )}

      {/* Live log panel */}
      {(running || logs.length > 0) && (
        <div className="bg-gray-950 border border-gray-800 rounded-lg overflow-hidden">
          <div className="flex items-center justify-between px-3 py-2 bg-gray-900/50 border-b border-gray-800">
            <span className="text-xs text-gray-400 font-medium">Activity Log</span>
            <span className="text-[10px] text-gray-600">{logs.length} events</span>
          </div>
          <div className="max-h-[400px] overflow-y-auto p-2 space-y-0.5 font-mono text-xs">
            {logs.map((log, i) => (
              <div key={i} className="flex items-start gap-2 py-0.5">
                <span className="flex-shrink-0 w-5 text-center">
                  {PHASE_ICONS[log.phase] || '•'}
                </span>
                <span className={`
                  ${log.phase === 'crawl_ok' ? 'text-emerald-400' : ''}
                  ${log.phase === 'crawl_fail' ? 'text-red-400' : ''}
                  ${log.phase === 'crawl_skip' ? 'text-yellow-500' : ''}
                  ${log.phase === 'batch' ? 'text-blue-300 font-semibold' : ''}
                  ${log.phase === 'search' ? 'text-gray-400' : ''}
                  ${log.phase === 'search_done' ? 'text-emerald-300' : ''}
                  ${log.phase === 'done' ? 'text-emerald-300 font-semibold' : ''}
                  ${log.phase === 'batch_done' ? 'text-emerald-200 font-bold' : ''}
                  ${log.phase === 'ingest' || log.phase === 'ingest_done' ? 'text-purple-300' : ''}
                  ${!['crawl_ok','crawl_fail','crawl_skip','batch','search','search_done','done','batch_done','ingest','ingest_done'].includes(log.phase) ? 'text-gray-400' : ''}
                `}>
                  {log.message}
                </span>
              </div>
            ))}
            <div ref={logEndRef} />
          </div>
        </div>
      )}
    </div>
  )
}
