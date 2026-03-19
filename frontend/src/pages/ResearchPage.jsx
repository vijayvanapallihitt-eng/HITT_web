import { useState, useEffect, useRef, useCallback } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { useDb } from '../DbContext'
import { api } from '../api'
import {
  Search, Users, DollarSign, Building2, Globe, Linkedin,
  Loader2, Play, CheckCircle, AlertTriangle, User, ChevronDown, ChevronUp,
  ArrowRight, ExternalLink, Radio, Clock, Zap, Database, BrainCircuit,
} from 'lucide-react'

// Phase → icon / color mapping for progress steps
const PHASE_META = {
  start:      { icon: Zap,          color: 'text-blue-400',    label: 'Starting' },
  init:       { icon: Loader2,      color: 'text-gray-400',    label: 'Init' },
  search:     { icon: Search,       color: 'text-yellow-400',  label: 'Search' },
  linkedin:   { icon: Linkedin,     color: 'text-blue-400',    label: 'LinkedIn' },
  website:    { icon: Globe,        color: 'text-emerald-400', label: 'Website' },
  crawl_done: { icon: CheckCircle,  color: 'text-emerald-400', label: 'Crawled' },
  extract:    { icon: BrainCircuit, color: 'text-purple-400',  label: 'GPT' },
  saving:     { icon: Database,     color: 'text-orange-400',  label: 'Saving' },
  saved:      { icon: Database,     color: 'text-emerald-400', label: 'Saved' },
  done:       { icon: CheckCircle,  color: 'text-emerald-400', label: 'Done' },
}

export default function ResearchPage() {
  const { activeDb } = useDb()
  const queryClient = useQueryClient()

  const [searchParams] = useSearchParams()
  const [resultId, setResultId] = useState(searchParams.get('id') || '')
  const [companyName, setCompanyName] = useState('')
  const [companyWebsite, setCompanyWebsite] = useState('')
  const [fetchLinkedin, setFetchLinkedin] = useState(true)
  const [fetchWebsite, setFetchWebsite] = useState(true)
  const [maxPages, setMaxPages] = useState(8)
  const [mode, setMode] = useState(searchParams.get('id') ? 'id' : 'id')

  // Progress tracking state
  const [isRunning, setIsRunning] = useState(false)
  const [progressLog, setProgressLog] = useState([])   // [{phase, message, ts}]
  const [currentPhase, setCurrentPhase] = useState(null)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [startTime, setStartTime] = useState(null)
  const [elapsed, setElapsed] = useState(0)
  const abortRef = useRef(null)
  const logEndRef = useRef(null)

  // Company lookup
  const { data: companyInfo } = useQuery({
    queryKey: ['companyDetail', activeDb, resultId],
    queryFn: () => api.getCompanyDetail(activeDb, resultId),
    enabled: !!activeDb && !!resultId && mode === 'id' && /^\d+$/.test(resultId),
    retry: false,
  })

  // Existing results
  const { data: existingResults } = useQuery({
    queryKey: ['researchResults', activeDb, resultId],
    queryFn: () => api.getResearchResults(activeDb, parseInt(resultId)),
    enabled: !!activeDb && !!resultId && mode === 'id' && /^\d+$/.test(resultId) && !isRunning,
    retry: false,
  })

  // Elapsed timer
  useEffect(() => {
    if (!isRunning || !startTime) return
    const interval = setInterval(() => setElapsed(Date.now() - startTime), 200)
    return () => clearInterval(interval)
  }, [isRunning, startTime])

  // Auto-scroll progress log
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [progressLog])

  const formatElapsed = (ms) => {
    const s = Math.floor(ms / 1000)
    const m = Math.floor(s / 60)
    return m > 0 ? `${m}m ${s % 60}s` : `${s}s`
  }

  // ── Run research with SSE streaming ──
  const runResearch = useCallback(async () => {
    if (!activeDb || !resultId) return

    setIsRunning(true)
    setProgressLog([])
    setCurrentPhase(null)
    setResult(null)
    setError(null)
    setStartTime(Date.now())
    setElapsed(0)

    const stream = api.runResearchStream(activeDb, parseInt(resultId), {
      fetch_linkedin: fetchLinkedin,
      fetch_website: fetchWebsite,
      max_pages: maxPages,
    })
    abortRef.current = stream.abort

    try {
      for await (const { event, data } of stream.events()) {
        if (event === 'progress') {
          setCurrentPhase(data.phase)
          setProgressLog(prev => [...prev, {
            phase: data.phase,
            message: data.message,
            detail: data.detail || {},
            ts: data.ts,
          }])
        } else if (event === 'result') {
          setResult(data)
          queryClient.invalidateQueries(['researchResults'])
          queryClient.invalidateQueries(['companyDetail'])
        } else if (event === 'error') {
          setError(data.message || 'Research failed')
        }
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        setError(err.message || 'Connection lost')
      }
    } finally {
      setIsRunning(false)
      abortRef.current = null
    }
  }, [activeDb, resultId, fetchLinkedin, fetchWebsite, maxPages, queryClient])

  const handleCancel = () => {
    if (abortRef.current) {
      abortRef.current()
      setIsRunning(false)
      setProgressLog(prev => [...prev, { phase: 'cancel', message: 'Cancelled by user', ts: Date.now() / 1000 }])
    }
  }

  const displayName = mode === 'id' ? (companyInfo?.data?.title || `Result #${resultId}`) : companyName
  const displayWebsite = mode === 'id' ? (companyInfo?.data?.web_site || '') : companyWebsite

  if (!activeDb) return <div className="text-gray-500">Select a database.</div>

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Users className="text-blue-400" size={24} />
        <h2 className="text-2xl font-bold text-white">Research Agent</h2>
      </div>
      <p className="text-sm text-gray-400">
        Searches LinkedIn and company websites to find employee headcount, key personnel,
        and revenue signals. Results are saved to the company evaluation in <strong className="text-gray-300">{activeDb}</strong>.
      </p>

      {/* Mode toggle */}
      <div className="flex gap-2">
        <button onClick={() => setMode('id')}
          className={`px-4 py-2 rounded text-sm font-medium transition-colors ${mode === 'id' ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}>
          By Result ID
        </button>
        <button onClick={() => setMode('name')}
          className={`px-4 py-2 rounded text-sm font-medium transition-colors ${mode === 'name' ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}>
          By Company Name
        </button>
      </div>

      {/* Input form */}
      <div className="bg-gray-900/60 border border-gray-800 rounded-lg p-5 space-y-4">
        {mode === 'id' ? (
          <div>
            <label className="block text-xs text-gray-500 mb-1 uppercase tracking-wide">Result ID</label>
            <input type="number" value={resultId} onChange={(e) => setResultId(e.target.value)}
              placeholder="e.g. 111"
              className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500" />
            {companyInfo && (
              <p className="mt-2 text-sm text-gray-300">
                <Building2 size={14} className="inline mr-1 text-blue-400" />
                {companyInfo.data?.title}
                {companyInfo.data?.web_site && (
                  <span className="ml-3 text-gray-500">
                    <Globe size={12} className="inline mr-1" />{companyInfo.data.web_site}
                  </span>
                )}
              </p>
            )}
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-gray-500 mb-1 uppercase tracking-wide">Company Name</label>
              <input type="text" value={companyName} onChange={(e) => setCompanyName(e.target.value)}
                placeholder="e.g. Turner Construction"
                className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500" />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1 uppercase tracking-wide">Website (optional)</label>
              <input type="text" value={companyWebsite} onChange={(e) => setCompanyWebsite(e.target.value)}
                placeholder="e.g. turnerconstruction.com"
                className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500" />
            </div>
          </div>
        )}

        {/* Options */}
        <div className="flex flex-wrap items-center gap-4 pt-2 border-t border-gray-800">
          <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
            <input type="checkbox" checked={fetchLinkedin} onChange={(e) => setFetchLinkedin(e.target.checked)}
              className="rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500" />
            <Linkedin size={14} className="text-blue-400" />LinkedIn
          </label>
          <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
            <input type="checkbox" checked={fetchWebsite} onChange={(e) => setFetchWebsite(e.target.checked)}
              className="rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500" />
            <Globe size={14} className="text-emerald-400" />Website
          </label>
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-500">Max pages:</label>
            <input type="number" min={1} max={30} value={maxPages}
              onChange={(e) => setMaxPages(parseInt(e.target.value) || 8)}
              className="w-16 bg-gray-800 border border-gray-700 rounded px-2 py-1 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500" />
          </div>
        </div>

        {/* Run / Cancel buttons */}
        <div className="flex items-center gap-3">
          <button onClick={runResearch}
            disabled={isRunning || (mode === 'id' ? !resultId : !companyName)}
            className="flex items-center gap-2 px-5 py-2.5 rounded bg-blue-600 text-white font-medium text-sm hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            {isRunning ? (
              <><Loader2 size={16} className="animate-spin" />Researching…</>
            ) : (
              <><Play size={16} />Run Research Agent</>
            )}
          </button>
          {isRunning && (
            <button onClick={handleCancel}
              className="px-4 py-2.5 rounded bg-red-900/40 text-red-400 border border-red-800 text-sm hover:bg-red-900/60 transition-colors">
              Cancel
            </button>
          )}
          {isRunning && startTime && (
            <span className="flex items-center gap-1.5 text-sm text-gray-500">
              <Clock size={14} />{formatElapsed(elapsed)}
            </span>
          )}
        </div>

        {error && (
          <div className="flex items-center gap-2 text-red-400 text-sm bg-red-900/20 border border-red-800 rounded px-3 py-2">
            <AlertTriangle size={14} />{error}
          </div>
        )}
      </div>

      {/* ── Live Progress Panel ── */}
      {(isRunning || progressLog.length > 0) && (
        <ProgressPanel
          log={progressLog}
          currentPhase={currentPhase}
          isRunning={isRunning}
          elapsed={elapsed}
          logEndRef={logEndRef}
        />
      )}

      {/* ── Results ── */}
      {(result || (!isRunning && existingResults)) && (
        <ResultsPanel
          data={result || existingResults}
          company={displayName}
          isNew={!!result}
          resultId={resultId}
        />
      )}
    </div>
  )
}


// ---------------------------------------------------------------------------
// Progress Panel — live log of research steps
// ---------------------------------------------------------------------------

function ProgressPanel({ log, currentPhase, isRunning, elapsed, logEndRef }) {
  return (
    <div className="bg-gray-900/60 border border-gray-800 rounded-lg overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
        <div className="flex items-center gap-2">
          {isRunning ? (
            <Radio size={14} className="text-red-400 animate-pulse" />
          ) : (
            <CheckCircle size={14} className="text-emerald-400" />
          )}
          <h3 className="text-sm font-semibold text-gray-200">
            {isRunning ? 'Research in progress…' : 'Research complete'}
          </h3>
        </div>
        <span className="text-xs text-gray-500">{log.length} steps</span>
      </div>

      {/* Phase indicator bar */}
      <PhaseBar currentPhase={currentPhase} isRunning={isRunning} />

      {/* Scrollable log */}
      <div className="max-h-64 overflow-y-auto px-4 py-2 space-y-0.5 font-mono text-xs">
        {log.map((entry, i) => {
          const meta = PHASE_META[entry.phase] || PHASE_META.start
          const Icon = meta.icon
          return (
            <div key={i} className="flex items-start gap-2 py-0.5 group">
              <Icon size={12} className={`mt-0.5 flex-shrink-0 ${meta.color}`} />
              <span className={`${meta.color} font-semibold w-16 flex-shrink-0 uppercase text-[10px] mt-px`}>
                {meta.label}
              </span>
              <span className="text-gray-400 flex-1">{entry.message}</span>
              {entry.detail?.chars && (
                <span className="text-gray-600 flex-shrink-0">{Number(entry.detail.chars).toLocaleString()} chars</span>
              )}
            </div>
          )
        })}
        <div ref={logEndRef} />
      </div>
    </div>
  )
}


// Horizontal phase indicator
const PHASES_ORDER = ['search', 'linkedin', 'website', 'extract', 'saving', 'done']

function PhaseBar({ currentPhase, isRunning }) {
  const currentIdx = PHASES_ORDER.indexOf(currentPhase)

  return (
    <div className="px-4 py-2 border-b border-gray-800 flex items-center gap-1">
      {PHASES_ORDER.map((phase, i) => {
        const meta = PHASE_META[phase] || PHASE_META.start
        const isDone = i < currentIdx || (!isRunning && currentIdx >= 0)
        const isCurrent = phase === currentPhase || (PHASE_META[currentPhase]?.label === meta.label)
        const isActive = isDone || isCurrent

        return (
          <div key={phase} className="flex items-center gap-1">
            {i > 0 && (
              <div className={`w-4 h-px ${isDone ? 'bg-emerald-600' : 'bg-gray-700'}`} />
            )}
            <div className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] font-medium uppercase tracking-wider transition-all ${
              isCurrent && isRunning
                ? `${meta.color} bg-gray-800 ring-1 ring-gray-700`
                : isDone
                ? 'text-emerald-500 bg-emerald-900/20'
                : 'text-gray-600 bg-gray-800/40'
            }`}>
              {isCurrent && isRunning ? (
                <Loader2 size={10} className="animate-spin" />
              ) : isDone ? (
                <CheckCircle size={10} />
              ) : null}
              {meta.label}
            </div>
          </div>
        )
      })}
    </div>
  )
}


// ---------------------------------------------------------------------------
// Results Panel (unchanged from before)
// ---------------------------------------------------------------------------

function ResultsPanel({ data, company, isNew, resultId }) {
  const [showAllEmployees, setShowAllEmployees] = useState(false)
  const navigate = useNavigate()
  const employees = data.employees || []
  const displayEmployees = showAllEmployees ? employees : employees.slice(0, 10)

  return (
    <div className="space-y-4">
      {isNew && (
        <div className="bg-emerald-900/20 border border-emerald-800 rounded-lg p-4 space-y-3">
          <div className="flex items-center gap-2 text-emerald-400 text-sm font-medium">
            <CheckCircle size={16} />
            Research complete — results saved to company evaluation
          </div>
          <p className="text-xs text-gray-400">
            The headcount and revenue data now appears in the <strong className="text-gray-300">Companies</strong> table
            and on the <strong className="text-gray-300">Company Detail</strong> page.
          </p>
          {resultId && (
            <div className="flex flex-wrap gap-2 pt-1">
              <button onClick={() => navigate(`/companies/${resultId}`)}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded bg-emerald-600/20 text-emerald-400 border border-emerald-800 text-xs font-medium hover:bg-emerald-600/30 transition-colors">
                <ArrowRight size={12} />View Company Detail
              </button>
              <button onClick={() => navigate('/companies')}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded bg-gray-800 text-gray-300 border border-gray-700 text-xs font-medium hover:bg-gray-700 transition-colors">
                Back to Companies
              </button>
            </div>
          )}
        </div>
      )}

      {/* Stat cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Headcount" value={data.estimated_headcount || 'Unknown'} confidence={data.headcount_confidence} icon={<Users size={18} />} />
        <StatCard label="Employees Found" value={data.employee_count ?? employees.length} confidence={employees.length > 0 ? 'high' : 'none'} icon={<User size={18} />} />
        <StatCard label="Revenue" value={data.estimated_revenue || 'Unknown'} confidence={data.revenue_confidence} icon={<DollarSign size={18} />} />
        <StatCard label="Sources" value={`${data.search_hits ?? '?'} hits / ${data.pages_fetched ?? '?'} pages`} icon={<Search size={18} />} />
      </div>

      {/* Employee list */}
      {employees.length > 0 && (
        <div className="bg-gray-900/60 border border-gray-800 rounded-lg">
          <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-200">Employees Found ({employees.length})</h3>
            {employees.length > 10 && (
              <button onClick={() => setShowAllEmployees(!showAllEmployees)}
                className="flex items-center gap-1 text-xs text-blue-400 hover:text-blue-300">
                {showAllEmployees ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                {showAllEmployees ? 'Show less' : `Show all ${employees.length}`}
              </button>
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-900/80">
                <tr className="text-left text-xs text-gray-500 uppercase tracking-wider">
                  <th className="px-4 py-2">#</th>
                  <th className="px-4 py-2">Name</th>
                  <th className="px-4 py-2">Title</th>
                  <th className="px-4 py-2">Source</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-800/50">
                {displayEmployees.map((emp, i) => (
                  <tr key={i} className="hover:bg-gray-800/30">
                    <td className="px-4 py-2 text-gray-600 text-xs">{i + 1}</td>
                    <td className="px-4 py-2 text-gray-200 font-medium">{emp.name || '—'}</td>
                    <td className="px-4 py-2 text-gray-400">{emp.title || '—'}</td>
                    <td className="px-4 py-2">
                      {emp.url ? (
                        <a href={emp.url} target="_blank" rel="noopener noreferrer"
                          className="text-blue-400 hover:text-blue-300 text-xs">
                          {emp.source === 'linkedin' ? (
                            <span className="flex items-center gap-1"><Linkedin size={12} /> LinkedIn</span>
                          ) : (emp.source || 'link')}
                        </a>
                      ) : (
                        <span className="text-gray-600 text-xs">{emp.source || '—'}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Evidence summary */}
      {data.evidence_summary && (
        <div className="bg-gray-900/60 border border-gray-800 rounded-lg p-4">
          <h3 className="text-sm font-semibold text-gray-200 mb-2">Evidence Summary</h3>
          <p className="text-sm text-gray-400 leading-relaxed whitespace-pre-wrap">{data.evidence_summary}</p>
        </div>
      )}
    </div>
  )
}


function StatCard({ label, value, confidence, icon }) {
  const colorMap = { high: 'text-emerald-400', medium: 'text-yellow-400', low: 'text-orange-400', none: 'text-gray-500' }
  const badgeColorMap = {
    high: 'bg-emerald-900/40 text-emerald-400 border-emerald-800',
    medium: 'bg-yellow-900/40 text-yellow-400 border-yellow-800',
    low: 'bg-orange-900/40 text-orange-400 border-orange-800',
    none: 'bg-gray-800 text-gray-500 border-gray-700',
  }
  const valueColor = colorMap[confidence] || 'text-gray-200'
  const displayVal = value === 'Unknown' ? '—' : value

  return (
    <div className="bg-gray-900/60 border border-gray-800 rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs text-gray-500 uppercase tracking-wide">{label}</span>
        <span className="text-gray-600">{icon}</span>
      </div>
      <div className={`text-lg font-bold ${valueColor}`}>{displayVal}</div>
      {confidence && (
        <span className={`inline-block mt-1 px-2 py-0.5 rounded-full text-[10px] border ${badgeColorMap[confidence] || badgeColorMap.none}`}>
          {confidence}
        </span>
      )}
    </div>
  )
}
