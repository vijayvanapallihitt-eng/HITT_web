import { useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useDb } from '../DbContext'
import { api } from '../api'
import { ArrowLeft, ExternalLink, Loader2, Users } from 'lucide-react'
import { useNavigate } from 'react-router-dom'

export default function CompanyDetailPage() {
  const { id } = useParams()
  const { activeDb } = useDb()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['companyDetail', activeDb, id],
    queryFn: () => api.getCompanyDetail(activeDb, id),
    enabled: !!activeDb && !!id,
  })

  if (isLoading) return <div className="flex items-center gap-2 text-gray-400"><Loader2 className="animate-spin" size={16} /> Loading…</div>
  if (error) return <div className="text-red-400">{error.message}</div>
  if (!data) return null

  const d = data.data || {}
  const addr = d.complete_address || {}

  return (
    <div className="space-y-6 max-w-5xl">
      <button onClick={() => navigate('/companies')} className="flex items-center gap-1 text-sm text-gray-400 hover:text-gray-200">
        <ArrowLeft size={14} /> Back to companies
      </button>

      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">{d.title || 'Unknown'}</h2>
          <p className="text-sm text-gray-500 mt-1">Result ID: {data.id} &nbsp;&middot;&nbsp; {d.category || 'No category'}</p>
        </div>
        <button
          onClick={() => navigate(`/research?id=${data.id}`)}
          className="flex items-center gap-2 px-4 py-2 rounded bg-blue-600/20 text-blue-400 border border-blue-800 text-sm font-medium hover:bg-blue-600/30 transition-colors"
        >
          <Users size={14} />
          Research Employees
        </button>
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Rating', value: d.review_rating ?? '—' },
          { label: 'Reviews', value: d.review_count ?? '—' },
          { label: 'Phone', value: d.phone || '—' },
          { label: 'Status', value: d.status || '—' },
        ].map((m) => (
          <div key={m.label} className="bg-gray-900 rounded-lg border border-gray-800 p-3">
            <div className="text-[10px] uppercase tracking-widest text-gray-500">{m.label}</div>
            <div className="text-lg font-semibold text-white mt-1">{m.value}</div>
          </div>
        ))}
      </div>

      {/* Details */}
      <div className="grid grid-cols-2 gap-6">
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 space-y-2 text-sm">
          <h3 className="font-semibold text-gray-300 mb-3">Contact & Location</h3>
          {d.address && <p className="text-gray-400"><span className="text-gray-500">Address:</span> {d.address}</p>}
          {(addr.city || addr.state) && (
            <p className="text-gray-400">
              <span className="text-gray-500">City/State:</span> {addr.city}, {addr.state} {addr.postal_code}
            </p>
          )}
          {d.web_site && (
            <p>
              <span className="text-gray-500">Website:</span>{' '}
              <a href={d.web_site.startsWith('http') ? d.web_site : `https://${d.web_site}`}
                 target="_blank" rel="noopener noreferrer" className="text-blue-400 hover:underline inline-flex items-center gap-1">
                {d.web_site} <ExternalLink size={12} />
              </a>
            </p>
          )}
          {d.emails?.length > 0 && (
            <p className="text-gray-400"><span className="text-gray-500">Emails:</span> {d.emails.join(', ')}</p>
          )}
          {d.categories?.length > 0 && (
            <p className="text-gray-400"><span className="text-gray-500">Categories:</span> {d.categories.join(', ')}</p>
          )}
        </div>

        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 space-y-2 text-sm">
          <h3 className="font-semibold text-gray-300 mb-3">Hours & Details</h3>
          {d.open_hours && typeof d.open_hours === 'object' && Object.keys(d.open_hours).length > 0 ? (
            Object.entries(d.open_hours).map(([day, times]) => (
              <p key={day} className="text-gray-400">
                <span className="text-gray-500 w-20 inline-block">{day}:</span>{' '}
                {Array.isArray(times) ? times.join(', ') : String(times)}
              </p>
            ))
          ) : (
            <p className="text-gray-600">No hours available</p>
          )}
          {d.description && <p className="text-gray-400 mt-3"><span className="text-gray-500">Description:</span> {d.description}</p>}
        </div>
      </div>

      {/* Evaluation */}
      {data.evaluation && (
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="font-semibold text-gray-300 mb-3">AI Evaluation</h3>
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div>
              <div className="text-gray-500 text-xs">Revenue</div>
              <div className="text-white font-mono">{data.evaluation.estimated_revenue}</div>
              <div className="text-gray-500 text-xs">Confidence: {data.evaluation.revenue_confidence}</div>
            </div>
            <div>
              <div className="text-gray-500 text-xs">Headcount</div>
              <div className="text-white font-mono">{data.evaluation.estimated_headcount}</div>
              <div className="text-gray-500 text-xs">Confidence: {data.evaluation.headcount_confidence}</div>
            </div>
            <div>
              <div className="text-gray-500 text-xs">Evidence</div>
              <div className="text-gray-400">{data.evaluation.evidence_summary}</div>
            </div>
          </div>
        </div>
      )}

      {/* Link Candidates */}
      {data.link_candidates?.length > 0 && (
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <h3 className="font-semibold text-gray-300 mb-3">
            Link Candidates ({data.link_candidates.length})
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left text-gray-500 uppercase tracking-wider">
                  <th className="px-2 py-1.5">ID</th>
                  <th className="px-2 py-1.5">Source</th>
                  <th className="px-2 py-1.5">Status</th>
                  <th className="px-2 py-1.5">URL</th>
                  <th className="px-2 py-1.5">Title</th>
                  <th className="px-2 py-1.5">Doc</th>
                  <th className="px-2 py-1.5">Chunks</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-800/50">
                {data.link_candidates.map((lc) => (
                  <tr key={lc.id} className="hover:bg-gray-800/30">
                    <td className="px-2 py-1.5 text-gray-500 font-mono">{lc.id}</td>
                    <td className="px-2 py-1.5">
                      <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                        lc.source_type === 'news' ? 'bg-blue-500/20 text-blue-400' : 'bg-green-500/20 text-green-400'
                      }`}>
                        {lc.source_type}
                      </span>
                    </td>
                    <td className="px-2 py-1.5 text-gray-400">{lc.discovery_status}</td>
                    <td className="px-2 py-1.5 max-w-xs truncate">
                      {lc.url_discovered ? (
                        <a href={lc.url_discovered} target="_blank" rel="noopener noreferrer"
                           className="text-blue-400 hover:underline">{lc.url_discovered}</a>
                      ) : '—'}
                    </td>
                    <td className="px-2 py-1.5 text-gray-400 max-w-xs truncate">{lc.title_discovered || '—'}</td>
                    <td className="px-2 py-1.5 text-gray-400">{lc.fetch_status || '—'}</td>
                    <td className="px-2 py-1.5 text-gray-400 font-mono">{lc.chunks ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Raw JSON */}
      <details className="bg-gray-900 rounded-lg border border-gray-800 p-4">
        <summary className="text-sm text-gray-400 cursor-pointer hover:text-gray-200">Raw JSONB Data</summary>
        <pre className="mt-3 text-xs text-gray-500 overflow-auto max-h-96">{JSON.stringify(d, null, 2)}</pre>
      </details>
    </div>
  )
}
