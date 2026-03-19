import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { useDb } from '../DbContext'
import { api } from '../api'
import { Database, Link2, FileText, Layers, Loader2, AlertCircle, Zap, Play, ArrowRight, Activity } from 'lucide-react'

function StatCard({ icon: Icon, label, value, sub, color = 'blue' }) {
  const colors = {
    blue: 'from-blue-600/20 to-blue-600/5 border-blue-500/30',
    green: 'from-green-600/20 to-green-600/5 border-green-500/30',
    amber: 'from-amber-600/20 to-amber-600/5 border-amber-500/30',
    purple: 'from-purple-600/20 to-purple-600/5 border-purple-500/30',
  }
  return (
    <div className={`rounded-lg border bg-gradient-to-br p-4 ${colors[color]}`}>
      <div className="flex items-center gap-2 text-gray-400 text-xs mb-2">
        <Icon size={14} /> {label}
      </div>
      <div className="text-2xl font-bold text-white">{value?.toLocaleString() ?? '—'}</div>
      {sub && <div className="text-xs text-gray-500 mt-1">{sub}</div>}
    </div>
  )
}

export default function OverviewPage() {
  const { activeDb } = useDb()
  const navigate = useNavigate()

  const { data: stats, isLoading, error } = useQuery({
    queryKey: ['dbStats', activeDb],
    queryFn: () => api.getDatabaseStats(activeDb),
    enabled: !!activeDb,
    refetchInterval: 15_000,
  })

  const { data: workers } = useQuery({
    queryKey: ['workerProcesses'],
    queryFn: () => api.getWorkerProcesses(),
    refetchInterval: 10_000,
  })

  const { data: scraperStatus } = useQuery({
    queryKey: ['scraperStatus'],
    queryFn: () => api.getScraperStatus(),
    refetchInterval: 10_000,
  })

  const { data: collections } = useQuery({
    queryKey: ['collections'],
    queryFn: () => api.getCollections(),
  })

  const pipelineRunning = workers?.processes?.some(
    p => p.CommandLine?.includes('worker_unified') && p.CommandLine?.includes(activeDb)
  )
  const scraperRunning = scraperStatus?.containers?.some(
    c => c.status?.toLowerCase().includes('up') && c.name?.startsWith(`${activeDb}-scraper`)
  )
  const isRunning = pipelineRunning || scraperRunning

  // Other databases with active pipelines
  const otherRunningDbs = useMemo(() => {
    const workerDbs = (workers?.processes || [])
      .filter(p => p.CommandLine?.includes('worker_unified'))
      .map(p => { const m = p.CommandLine?.match(/\/([\w-]+?)(?:["']|\s|$)/); return m ? m[1] : null })
      .filter(Boolean)
    const scraperDbs = (scraperStatus?.containers || [])
      .filter(c => c.status?.toLowerCase().includes('up'))
      .map(c => c.name?.replace(/-scraper$/, ''))
      .filter(Boolean)
    const allDbs = new Set([...workerDbs, ...scraperDbs])
    allDbs.delete(activeDb)
    return [...allDbs]
  }, [workers?.processes, scraperStatus?.containers, activeDb])

  if (!activeDb) return <div className="text-gray-500">Select a database from the sidebar.</div>
  if (isLoading) return <div className="flex items-center gap-2 text-gray-400"><Loader2 className="animate-spin" size={16} /> Loading…</div>
  if (error) return <div className="flex items-center gap-2 text-red-400"><AlertCircle size={16} /> {error.message}</div>

  const tc = stats?.table_counts || {}
  const pending = stats?.pending || {}
  const jobs = stats?.gmaps_jobs || {}

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-white">Overview</h2>
        <p className="text-sm text-gray-500 mt-1">Database: <span className="text-blue-400 font-mono">{activeDb}</span></p>
      </div>

      {/* Primary metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard icon={Database} label="Scraped Results" value={tc.results} color="blue" />
        <StatCard icon={Link2} label="Link Candidates" value={tc.link_candidates} color="green" />
        <StatCard icon={FileText} label="Documents" value={tc.documents} color="amber" />
        <StatCard icon={Layers} label="Chunks Embedded" value={tc.document_chunks} color="purple" />
      </div>

      {/* Pipeline status — quick view */}
      <div className={`rounded-xl border-2 p-5 transition-colors ${
        isRunning
          ? 'border-emerald-500/40 bg-emerald-950/20'
          : 'border-gray-800 bg-gray-900/60'
      }`}>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {isRunning ? (
              <div className="relative">
                <Zap size={20} className="text-emerald-400" />
                <span className="absolute -top-0.5 -right-0.5 w-2.5 h-2.5 bg-emerald-400 rounded-full animate-pulse" />
              </div>
            ) : (
              <Zap size={20} className="text-gray-600" />
            )}
            <div>
              <h3 className="text-sm font-semibold text-white">
                {isRunning
                  ? 'Pipeline Running'
                  : 'Pipeline Idle'}
              </h3>
              <p className="text-xs text-gray-500">
                {pending.results_missing_link_discovery > 0
                  ? `${pending.results_missing_link_discovery} companies pending`
                  : tc.results > 0 ? 'All companies processed' : 'No data yet'}
              </p>
            </div>
          </div>
          <button
            onClick={() => navigate('/pipeline')}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-gray-800 text-gray-200 text-sm hover:bg-gray-700 transition-colors"
          >
            {isRunning
              ? 'View Pipeline'
              : 'Start Pipeline'}
            <ArrowRight size={14} />
          </button>
        </div>

        {/* Progress bar */}
        {tc.results > 0 && (
          <div className="mt-3">
            <div className="w-full bg-gray-800 rounded-full h-2 overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-500 ${
                  isRunning
                    ? 'bg-emerald-500' : 'bg-blue-500'
                }`}
                style={{
                  width: `${Math.max(
                    Math.round(((tc.results - (pending.results_missing_link_discovery || 0)) / tc.results) * 100),
                    1
                  )}%`
                }}
              />
            </div>
          </div>
        )}
      </div>

      {/* Other active pipelines */}
      {otherRunningDbs.length > 0 && (
        <div className="rounded-xl border border-amber-700/40 bg-amber-950/20 px-5 py-3 flex items-center gap-2">
          <Activity size={14} className="text-amber-400" />
          <span className="text-sm text-amber-300">
            Also running on: <span className="text-amber-400 font-medium">{otherRunningDbs.join(', ')}</span>
          </span>
          <button
            onClick={() => navigate('/pipeline')}
            className="ml-auto text-xs text-gray-400 hover:text-white transition-colors"
          >
            Manage →
          </button>
        </div>
      )}

      {/* Pending work + Workers */}
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Pending Work</h3>
          <dl className="space-y-2 text-sm">
            <div className="flex justify-between">
              <dt className="text-gray-500">Companies to process</dt>
              <dd className="text-amber-400 font-mono">{pending.results_missing_link_discovery?.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-gray-500">Links to fetch</dt>
              <dd className="text-amber-400 font-mono">{pending.link_candidates_pending_fetch?.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-gray-500">Docs to embed</dt>
              <dd className="text-amber-400 font-mono">{pending.documents_pending_chunk_embed?.toLocaleString()}</dd>
            </div>
          </dl>
        </div>

        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Workers</h3>
          {workers?.processes?.length > 0 ? (
            <ul className="space-y-2 text-xs">
              {workers.processes.map((p) => (
                <li key={p.ProcessId} className="text-gray-400">
                  <span className="inline-block w-2 h-2 rounded-full bg-green-500 mr-1.5" />
                  PID {p.ProcessId} — {p.CommandLine?.split(' ').slice(1, 3).join(' ')}
                </li>
              ))}
            </ul>
          ) : (
            <div className="text-gray-600 text-sm">No workers running</div>
          )}
        </div>
      </div>

      {/* Link candidate breakdown */}
      {stats?.link_candidates?.by_source_type && (
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Link Candidates by Source</h3>
          <div className="flex gap-6">
            {Object.entries(stats.link_candidates.by_source_type).map(([type, count]) => (
              <div key={type} className="text-center">
                <div className="text-xl font-bold text-white">{count.toLocaleString()}</div>
                <div className="text-xs text-gray-500">{type}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ChromaDB */}
      {collections?.collections?.length > 0 && (
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">ChromaDB Collections</h3>
          <div className="flex gap-6">
            {collections.collections.map((c) => (
              <div key={c.name} className="text-center">
                <div className="text-xl font-bold text-white">{c.count.toLocaleString()}</div>
                <div className="text-xs text-gray-500 font-mono">{c.name}</div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
