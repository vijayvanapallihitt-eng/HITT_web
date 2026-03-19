import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useDb } from '../DbContext'
import { api } from '../api'
import { Loader2, Plus, Trash2 } from 'lucide-react'

const TRADES = [
  'Construction Companies', 'General Contractors', 'Home Builders',
  'Custom Home Builders', 'Roofing Contractors', 'Concrete Contractors',
  'Remodelers', 'Excavating Contractors', 'Commercial Contractors',
  'Residential Contractors', 'Plumbing Contractors', 'Electrical Contractors',
  'HVAC Contractors', 'Painting Contractors', 'Landscaping Companies',
]

export default function ScraperPage() {
  const { activeDb } = useDb()
  const qc = useQueryClient()

  // Container status
  const { data: scraperStatus } = useQuery({
    queryKey: ['scraperStatus'],
    queryFn: () => api.getScraperStatus(),
    refetchInterval: 10_000,
  })

  // Query files
  const { data: queryFiles } = useQuery({
    queryKey: ['queryFiles'],
    queryFn: () => api.getQueryFiles(),
  })

  // Docker logs
  const [logTail, setLogTail] = useState(50)
  const fetchLogs = useQuery({
    queryKey: ['scraperLogs', activeDb, logTail],
    queryFn: () => api.getScraperLogs(activeDb, logTail),
    enabled: false,
  })

  // Scraper controls
  const [selectedQf, setSelectedQf] = useState('')
  const [concurrency, setConcurrency] = useState(4)
  const [depth, setDepth] = useState(1)

  const startScraper = useMutation({
    mutationFn: () => api.startScraper(activeDb, { query_file: selectedQf, concurrency, depth }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['scraperStatus'] }),
  })

  const stopScraper = useMutation({
    mutationFn: () => api.stopScraper(activeDb),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['scraperStatus'] }),
  })

  const seedScraper = useMutation({
    mutationFn: () => api.seedScraper(activeDb, selectedQf),
  })

  // Query generator
  const [genCity, setGenCity] = useState('')
  const [genState, setGenState] = useState('')
  const [genTrades, setGenTrades] = useState(['Construction Companies', 'General Contractors', 'Home Builders'])
  const [generated, setGenerated] = useState([])

  // New query file
  const [newQueries, setNewQueries] = useState('')
  const [newFilename, setNewFilename] = useState('custom_queries.txt')

  const saveQueries = useMutation({
    mutationFn: () => api.saveQueryFile(newFilename, newQueries.split('\n')),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['queryFiles'] }),
  })

  if (!activeDb) return <div className="text-gray-500">Select a database.</div>

  const qfNames = queryFiles?.files?.map(f => f.name) || []

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-white">Scraper Manager</h2>
        <p className="text-sm text-gray-500 mt-1">Database: <span className="text-blue-400 font-mono">{activeDb}</span> &nbsp;·&nbsp; Container: <span className="font-mono text-gray-400">{activeDb}-scraper</span></p>
      </div>

      {/* Docker containers */}
      <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
        <h3 className="text-sm font-semibold text-gray-300 mb-3">Docker Containers</h3>
        {scraperStatus?.containers?.length > 0 ? (
          <table className="w-full text-xs">
            <thead><tr className="text-left text-gray-500 uppercase tracking-wider">
              <th className="px-2 py-1">Name</th><th className="px-2 py-1">Status</th><th className="px-2 py-1">Created</th>
            </tr></thead>
            <tbody className="divide-y divide-gray-800/50">
              {scraperStatus.containers.map((c) => (
                <tr key={c.name}>
                  <td className="px-2 py-1.5 text-gray-300 font-mono">{c.name}</td>
                  <td className={`px-2 py-1.5 ${c.status?.includes('Up') ? 'text-green-400' : 'text-gray-500'}`}>{c.status}</td>
                  <td className="px-2 py-1.5 text-gray-500">{c.created}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="text-gray-600 text-sm">No scraper containers found.</p>
        )}
      </div>

      {/* Scraper controls */}
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 space-y-3">
          <h3 className="text-sm font-semibold text-gray-300">Launch Scraper</h3>
          <select value={selectedQf} onChange={(e) => setSelectedQf(e.target.value)}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200">
            <option value="">Select query file…</option>
            {qfNames.map(f => <option key={f} value={f}>{f}</option>)}
          </select>
          <div className="flex gap-2">
            <div>
              <label className="text-[10px] text-gray-500">Concurrency</label>
              <input type="number" value={concurrency} onChange={(e) => setConcurrency(+e.target.value)} min={1} max={16}
                className="w-full bg-gray-800 border border-gray-700 rounded px-2 py-1 text-sm text-gray-200" />
            </div>
            <div>
              <label className="text-[10px] text-gray-500">Depth</label>
              <input type="number" value={depth} onChange={(e) => setDepth(+e.target.value)} min={1} max={20}
                className="w-full bg-gray-800 border border-gray-700 rounded px-2 py-1 text-sm text-gray-200" />
            </div>
          </div>
          <div className="flex gap-2">
            <button onClick={() => seedScraper.mutate()} disabled={!selectedQf || seedScraper.isPending}
              className="px-3 py-1.5 bg-amber-600 text-white rounded text-sm hover:bg-amber-500 disabled:opacity-50">
              {seedScraper.isPending ? 'Seeding…' : 'Seed Only'}
            </button>
            <button onClick={() => startScraper.mutate()} disabled={!selectedQf || startScraper.isPending}
              className="px-3 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-500 disabled:opacity-50">
              {startScraper.isPending ? 'Starting…' : 'Seed & Start'}
            </button>
            <button onClick={() => stopScraper.mutate()} disabled={stopScraper.isPending}
              className="px-3 py-1.5 bg-red-600/30 text-red-400 rounded text-sm hover:bg-red-600/50 disabled:opacity-50">
              Stop
            </button>
          </div>
          {seedScraper.data && (
            <pre className="text-xs text-gray-500 mt-2 max-h-32 overflow-auto">{seedScraper.data.stdout || seedScraper.data.stderr}</pre>
          )}
          {startScraper.data && (
            <p className="text-xs text-green-400 mt-1">{startScraper.data.status === 'ok' ? '✅ Scraper started' : `❌ ${startScraper.data.stderr}`}</p>
          )}
        </div>

        {/* Docker logs */}
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 space-y-3">
          <h3 className="text-sm font-semibold text-gray-300">Container Logs</h3>
          <div className="flex gap-2 items-end">
            <div className="flex-1">
              <label className="text-[10px] text-gray-500">Tail lines</label>
              <input type="number" value={logTail} onChange={(e) => setLogTail(+e.target.value)} min={10} max={500}
                className="w-full bg-gray-800 border border-gray-700 rounded px-2 py-1 text-sm text-gray-200" />
            </div>
            <button onClick={() => fetchLogs.refetch()}
              className="px-3 py-1.5 bg-gray-700 text-gray-200 rounded text-sm hover:bg-gray-600">
              Fetch Logs
            </button>
          </div>
          {fetchLogs.data?.logs && (
            <pre className="text-xs text-gray-500 bg-gray-950 rounded p-2 max-h-60 overflow-auto">{fetchLogs.data.logs}</pre>
          )}
        </div>
      </div>

      {/* Query files */}
      <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
        <h3 className="text-sm font-semibold text-gray-300 mb-3">Query Files</h3>
        <div className="space-y-2">
          {queryFiles?.files?.map((qf) => (
            <details key={qf.name} className="border border-gray-800 rounded p-2">
              <summary className="text-sm text-gray-300 cursor-pointer">
                📄 {qf.name} — {qf.queries} queries
              </summary>
              <pre className="mt-2 text-xs text-gray-500 max-h-40 overflow-auto">{qf.preview?.join('\n')}{qf.queries > 10 ? `\n…and ${qf.queries - 10} more` : ''}</pre>
            </details>
          ))}
        </div>
      </div>

      {/* Add new queries */}
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 space-y-3">
          <h3 className="text-sm font-semibold text-gray-300">Add Queries</h3>
          <textarea
            value={newQueries}
            onChange={(e) => setNewQueries(e.target.value)}
            placeholder="One query per line…"
            rows={6}
            className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:outline-none"
          />
          <div className="flex gap-2 items-end">
            <input
              value={newFilename}
              onChange={(e) => setNewFilename(e.target.value)}
              className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200"
              placeholder="filename.txt"
            />
            <button onClick={() => saveQueries.mutate()} disabled={saveQueries.isPending}
              className="px-3 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-500 disabled:opacity-50">
              Save
            </button>
          </div>
        </div>

        {/* Query generator */}
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 space-y-3">
          <h3 className="text-sm font-semibold text-gray-300">Query Generator</h3>
          <div className="flex gap-2">
            <input value={genCity} onChange={(e) => setGenCity(e.target.value)} placeholder="City"
              className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200" />
            <input value={genState} onChange={(e) => setGenState(e.target.value)} placeholder="State"
              className="w-20 bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200" />
          </div>
          <div className="flex flex-wrap gap-1">
            {TRADES.map(t => (
              <button key={t}
                onClick={() => setGenTrades(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t])}
                className={`px-2 py-0.5 rounded text-[10px] ${genTrades.includes(t) ? 'bg-blue-600/30 text-blue-400' : 'bg-gray-800 text-gray-500'}`}>
                {t}
              </button>
            ))}
          </div>
          <button
            onClick={async () => {
              const res = await api.generateQueries(genCity, genState, genTrades)
              setGenerated(res.queries)
              setNewQueries(res.queries.join('\n'))
            }}
            disabled={!genCity || !genState}
            className="px-3 py-1.5 bg-gray-700 text-gray-200 rounded text-sm hover:bg-gray-600 disabled:opacity-50"
          >
            Generate
          </button>
          {generated.length > 0 && (
            <pre className="text-xs text-gray-500 max-h-32 overflow-auto">{generated.join('\n')}</pre>
          )}
        </div>
      </div>
    </div>
  )
}
