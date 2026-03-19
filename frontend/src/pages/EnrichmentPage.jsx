import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useDb } from '../DbContext'
import { api } from '../api'
import { Loader2 } from 'lucide-react'

export default function EnrichmentPage() {
  const { activeDb } = useDb()
  const qc = useQueryClient()

  const { data: stats, isLoading } = useQuery({
    queryKey: ['enrichmentStats', activeDb],
    queryFn: () => api.getEnrichmentStats(activeDb),
    enabled: !!activeDb,
    refetchInterval: 10_000,
  })

  const { data: workers } = useQuery({
    queryKey: ['workerStatus'],
    queryFn: () => api.getWorkerStatus(),
    refetchInterval: 10_000,
  })

  const { data: procs } = useQuery({
    queryKey: ['workerProcesses'],
    queryFn: () => api.getWorkerProcesses(),
    refetchInterval: 10_000,
  })

  const startEnricher = useMutation({
    mutationFn: () => api.startEnricher(activeDb),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['workerProcesses'] }),
  })

  const startIngester = useMutation({
    mutationFn: () => api.startIngester(activeDb),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['workerProcesses'] }),
  })

  const stopWorker = useMutation({
    mutationFn: (worker) => api.stopWorker(activeDb, worker),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['workerProcesses'] }),
  })

  if (!activeDb) return <div className="text-gray-500">Select a database.</div>
  if (isLoading) return <div className="flex items-center gap-2 text-gray-400"><Loader2 className="animate-spin" size={16} /> Loading…</div>

  const enricherRunning = procs?.processes?.some(p => p.CommandLine?.includes('worker_enrich'))
  const ingesterRunning = procs?.processes?.some(p => p.CommandLine?.includes('run_document_ingest'))

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-white">Enrichment Monitor</h2>
        <p className="text-sm text-gray-500 mt-1">Database: <span className="text-blue-400 font-mono">{activeDb}</span></p>
      </div>

      {/* Pipeline stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Total Results', value: stats?.total_results },
          { label: 'Enriched', value: stats?.results_enriched },
          { label: 'Pending', value: stats?.results_pending },
          { label: 'Documents', value: stats?.total_documents },
        ].map((m) => (
          <div key={m.label} className="bg-gray-900 rounded-lg border border-gray-800 p-4">
            <div className="text-[10px] uppercase tracking-widest text-gray-500">{m.label}</div>
            <div className="text-2xl font-bold text-white mt-1">{m.value?.toLocaleString() ?? '—'}</div>
          </div>
        ))}
      </div>

      {/* Pending detail */}
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Pending Work</h3>
          <dl className="space-y-2 text-sm">
            <div className="flex justify-between"><dt className="text-gray-500">News discovery</dt><dd className="text-amber-400 font-mono">{stats?.pending_news ?? 0}</dd></div>
            <div className="flex justify-between"><dt className="text-gray-500">Website discovery</dt><dd className="text-amber-400 font-mono">{stats?.pending_website ?? 0}</dd></div>
          </dl>
        </div>
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Documents by Status</h3>
          <dl className="space-y-2 text-sm">
            {stats?.documents_by_status && Object.entries(stats.documents_by_status).map(([st, cnt]) => (
              <div key={st} className="flex justify-between">
                <dt className="text-gray-500">{st}</dt>
                <dd className="text-gray-300 font-mono">{cnt.toLocaleString()}</dd>
              </div>
            ))}
          </dl>
        </div>
      </div>

      {/* Link candidate types */}
      {stats?.link_candidates_by_type && (
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Link Candidates by Type</h3>
          <div className="flex gap-8">
            {Object.entries(stats.link_candidates_by_type).map(([type, count]) => (
              <div key={type} className="text-center">
                <div className="text-xl font-bold text-white">{count.toLocaleString()}</div>
                <div className="text-xs text-gray-500">{type}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Worker controls */}
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-gray-300">Enricher Worker</h3>
            {enricherRunning ? (
              <span className="text-xs bg-green-500/20 text-green-400 px-2 py-0.5 rounded">Running</span>
            ) : (
              <span className="text-xs bg-gray-700/50 text-gray-500 px-2 py-0.5 rounded">Stopped</span>
            )}
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => startEnricher.mutate()}
              disabled={startEnricher.isPending}
              className="px-3 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-500 disabled:opacity-50"
            >
              {startEnricher.isPending ? 'Starting…' : 'Start Enricher'}
            </button>
            <button
              onClick={() => stopWorker.mutate('enricher')}
              disabled={stopWorker.isPending}
              className="px-3 py-1.5 bg-red-600/30 text-red-400 rounded text-sm hover:bg-red-600/50 disabled:opacity-50"
            >
              Stop
            </button>
          </div>
        </div>

        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-gray-300">Ingester Worker</h3>
            {ingesterRunning ? (
              <span className="text-xs bg-green-500/20 text-green-400 px-2 py-0.5 rounded">Running</span>
            ) : (
              <span className="text-xs bg-gray-700/50 text-gray-500 px-2 py-0.5 rounded">Stopped</span>
            )}
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => startIngester.mutate()}
              disabled={startIngester.isPending}
              className="px-3 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-500 disabled:opacity-50"
            >
              {startIngester.isPending ? 'Starting…' : 'Start Ingester'}
            </button>
            <button
              onClick={() => stopWorker.mutate('ingester')}
              disabled={stopWorker.isPending}
              className="px-3 py-1.5 bg-red-600/30 text-red-400 rounded text-sm hover:bg-red-600/50 disabled:opacity-50"
            >
              Stop
            </button>
          </div>
        </div>
      </div>

      {/* Worker status files */}
      {workers?.statuses?.length > 0 && (
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="text-sm font-semibold text-gray-300 mb-3">Worker Status Files</h3>
          <div className="space-y-3">
            {workers.statuses.map((s, i) => (
              <details key={i} className="border border-gray-800 rounded p-3">
                <summary className="text-sm cursor-pointer flex items-center gap-2">
                  <span className={`w-2 h-2 rounded-full ${
                    s.stage === 'idle' || s.stage === 'cycle_complete' ? 'bg-green-500' :
                    s.stage === 'failed' ? 'bg-red-500' : 'bg-amber-500'
                  }`} />
                  <span className="text-gray-300">{s._filename}</span>
                  <span className="text-gray-500 text-xs">— {s.stage} — {s.updated_at?.slice(0, 19)}</span>
                </summary>
                <pre className="mt-2 text-xs text-gray-500 overflow-auto max-h-60">{JSON.stringify(s, null, 2)}</pre>
              </details>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
