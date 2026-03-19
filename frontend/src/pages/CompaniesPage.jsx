import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { useDb } from '../DbContext'
import { api } from '../api'
import { Search, ChevronLeft, ChevronRight, ExternalLink, Loader2, ArrowUpDown, ArrowUp, ArrowDown, Newspaper, Globe, CheckSquare, Square, XCircle } from 'lucide-react'

export default function CompaniesPage() {
  const { activeDb } = useDb()
  const navigate = useNavigate()

  const [search, setSearch] = useState('')
  const [category, setCategory] = useState('')
  const [state, setState] = useState('')
  const [city, setCity] = useState('')
  const [enriched, setEnriched] = useState('')
  const [page, setPage] = useState(1)
  const [sortBy, setSortBy] = useState('id')
  const [sortDir, setSortDir] = useState('asc')
  const [selected, setSelected] = useState(new Set())

  // Filters
  const { data: filters } = useQuery({
    queryKey: ['companyFilters', activeDb],
    queryFn: () => api.getCompanyFilters(activeDb),
    enabled: !!activeDb,
  })

  // Companies
  const { data, isLoading } = useQuery({
    queryKey: ['companies', activeDb, search, category, state, city, enriched, page, sortBy, sortDir],
    queryFn: () =>
      api.getCompanies(activeDb, {
        search, category, state, city, enriched, page, page_size: 50,
        sort_by: sortBy, sort_dir: sortDir,
      }),
    enabled: !!activeDb,
    keepPreviousData: true,
  })

  const totalPages = data ? Math.ceil(data.total / data.page_size) : 1

  const handleSort = (col) => {
    if (sortBy === col) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(col)
      setSortDir('asc')
    }
    setPage(1)
  }

  const SortIcon = ({ col }) => {
    if (sortBy !== col) return <ArrowUpDown size={10} className="text-gray-700 ml-1 inline" />
    return sortDir === 'asc'
      ? <ArrowUp size={10} className="text-blue-400 ml-1 inline" />
      : <ArrowDown size={10} className="text-blue-400 ml-1 inline" />
  }

  // ── Selection helpers ─────────────────────────────────
  const pageIds = (data?.items || []).map(r => r.id)
  const allPageSelected = pageIds.length > 0 && pageIds.every(id => selected.has(id))

  const toggleOne = (id, e) => {
    e.stopPropagation()
    setSelected(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  const toggleAll = (e) => {
    e.stopPropagation()
    setSelected(prev => {
      const next = new Set(prev)
      if (allPageSelected) {
        pageIds.forEach(id => next.delete(id))
      } else {
        pageIds.forEach(id => next.add(id))
      }
      return next
    })
  }

  const clearSelection = () => setSelected(new Set())

  const launchWebResearch = () => {
    const ids = Array.from(selected)
    if (!ids.length) return
    // Navigate to web-research page with selected IDs in state
    navigate('/web-research', { state: { resultIds: ids, db: activeDb } })
  }

  if (!activeDb) return <div className="text-gray-500">Select a database.</div>

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold text-white">Companies</h2>

      {/* Filters */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <div className="relative col-span-2">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
          <input
            type="text"
            placeholder="Search company or address…"
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(1) }}
            className="w-full bg-gray-900 border border-gray-700 rounded pl-9 pr-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>
        <select value={category} onChange={(e) => { setCategory(e.target.value); setPage(1) }}
          className="bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500">
          <option value="">All categories</option>
          {filters?.categories?.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select value={state} onChange={(e) => { setState(e.target.value); setPage(1) }}
          className="bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500">
          <option value="">All states</option>
          {filters?.states?.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <select value={enriched} onChange={(e) => { setEnriched(e.target.value); setPage(1) }}
          className="bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500">
          <option value="">All enrichment</option>
          <option value="yes">Enriched</option>
          <option value="no">Not enriched</option>
        </select>
      </div>

      <div className="text-xs text-gray-500">
        {data?.total?.toLocaleString() ?? 0} companies found
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-gray-800">
        <table className="w-full text-sm">
          <thead className="bg-gray-900/80">
            <tr className="text-left text-xs text-gray-500 uppercase tracking-wider">
              <th className="px-2 py-2 w-8">
                <button onClick={toggleAll} className="text-gray-500 hover:text-gray-300">
                  {allPageSelected ? <CheckSquare size={14} className="text-blue-400" /> : <Square size={14} />}
                </button>
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('id')}>
                ID<SortIcon col="id" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('company')}>
                Company<SortIcon col="company" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('category')}>
                Category<SortIcon col="category" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('city')}>
                City<SortIcon col="city" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('state')}>
                State<SortIcon col="state" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('rating')}>
                ⭐<SortIcon col="rating" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('revenue')}>
                Revenue<SortIcon col="revenue" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('employees')}>
                Employees<SortIcon col="employees" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('news')}>
                📰 News<SortIcon col="news" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('links')}>
                Links<SortIcon col="links" />
              </th>
              <th className="px-3 py-2 cursor-pointer select-none hover:text-gray-300" onClick={() => handleSort('docs')}>
                Docs<SortIcon col="docs" />
              </th>
              <th className="px-3 py-2">Website</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800/50">
            {isLoading ? (
              <tr><td colSpan={13} className="px-3 py-8 text-center text-gray-500">
                <Loader2 className="animate-spin inline mr-2" size={14} /> Loading…
              </td></tr>
            ) : data?.items?.length === 0 ? (
              <tr><td colSpan={13} className="px-3 py-8 text-center text-gray-600">No results</td></tr>
            ) : (
              data?.items?.map((row) => (
                <tr
                  key={row.id}
                  onClick={() => navigate(`/companies/${row.id}`)}
                  className={`hover:bg-gray-800/50 cursor-pointer transition-colors ${selected.has(row.id) ? 'bg-blue-900/20' : ''}`}
                >
                  <td className="px-2 py-2" onClick={e => e.stopPropagation()}>
                    <button onClick={(e) => toggleOne(row.id, e)} className="text-gray-500 hover:text-gray-300">
                      {selected.has(row.id) ? <CheckSquare size={14} className="text-blue-400" /> : <Square size={14} />}
                    </button>
                  </td>
                  <td className="px-3 py-2 text-gray-500 font-mono text-xs">{row.id}</td>
                  <td className="px-3 py-2 text-gray-200 font-medium max-w-[220px] truncate">{row.company}</td>
                  <td className="px-3 py-2 text-gray-400 text-xs max-w-[140px] truncate">{row.category}</td>
                  <td className="px-3 py-2 text-gray-400">{row.city}</td>
                  <td className="px-3 py-2 text-gray-400">{row.state}</td>
                  <td className="px-3 py-2 text-amber-400">{row.rating || '—'}</td>
                  <td className="px-3 py-2 text-xs">
                    {row.estimated_revenue && row.estimated_revenue !== 'Unknown' ? (
                      <span className={row.revenue_confidence === 'high' ? 'text-emerald-400' : row.revenue_confidence === 'medium' ? 'text-yellow-400' : 'text-orange-400'}>
                        {row.estimated_revenue}
                      </span>
                    ) : <span className="text-gray-600">—</span>}
                  </td>
                  <td className="px-3 py-2 text-xs">
                    {row.estimated_headcount && row.estimated_headcount !== 'Unknown' ? (
                      <span className={row.headcount_confidence === 'high' ? 'text-emerald-400' : row.headcount_confidence === 'medium' ? 'text-yellow-400' : 'text-orange-400'}>
                        {row.estimated_headcount}
                      </span>
                    ) : <span className="text-gray-600">—</span>}
                  </td>
                  <td className="px-3 py-2 text-xs font-mono">
                    {row.news_count > 0 ? (
                      <span className="text-amber-400">{row.news_count}</span>
                    ) : <span className="text-gray-600">0</span>}
                  </td>
                  <td className="px-3 py-2 text-gray-400 font-mono">{row.links}</td>
                  <td className="px-3 py-2 text-gray-400 font-mono">{row.docs}</td>
                  <td className="px-3 py-2">
                    {row.website ? (
                      <a
                        href={row.website.startsWith('http') ? row.website : `https://${row.website}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        onClick={(e) => e.stopPropagation()}
                        className="text-blue-400 hover:text-blue-300"
                      >
                        <ExternalLink size={14} />
                      </a>
                    ) : '—'}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Selection action bar */}
      {selected.size > 0 && (
        <div className="flex items-center gap-3 px-4 py-3 rounded-lg bg-blue-900/30 border border-blue-700/40">
          <span className="text-sm text-blue-300 font-medium">
            {selected.size} selected
          </span>
          <button
            onClick={launchWebResearch}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded bg-emerald-600 text-white text-sm font-medium hover:bg-emerald-500 transition-colors"
          >
            <Globe size={14} /> Web Research
          </button>
          <button
            onClick={clearSelection}
            className="flex items-center gap-1 px-2 py-1.5 rounded text-gray-400 text-sm hover:text-gray-200 hover:bg-gray-800 transition-colors"
          >
            <XCircle size={14} /> Clear
          </button>
        </div>
      )}

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm">
        <button
          disabled={page <= 1}
          onClick={() => setPage((p) => p - 1)}
          className="flex items-center gap-1 px-3 py-1.5 rounded bg-gray-800 text-gray-300 disabled:opacity-30 hover:bg-gray-700 transition-colors"
        >
          <ChevronLeft size={14} /> Prev
        </button>
        <span className="text-gray-500">
          Page {page} of {totalPages}
        </span>
        <button
          disabled={page >= totalPages}
          onClick={() => setPage((p) => p + 1)}
          className="flex items-center gap-1 px-3 py-1.5 rounded bg-gray-800 text-gray-300 disabled:opacity-30 hover:bg-gray-700 transition-colors"
        >
          Next <ChevronRight size={14} />
        </button>
      </div>
    </div>
  )
}
