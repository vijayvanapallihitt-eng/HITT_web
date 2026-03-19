import { useState, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { api } from '../api'
import {
  Search, Loader2, Sparkles, ExternalLink, Building2,
  FileText, ChevronDown, ChevronUp, BookOpen, Hash,
} from 'lucide-react'

export default function VectorSearchPage() {
  const [query, setQuery] = useState('')
  const [nResults, setNResults] = useState(10)
  const [summarize, setSummarize] = useState(true)

  // Collection picker
  const { data: collections } = useQuery({
    queryKey: ['collections'],
    queryFn: () => api.getCollections(),
  })

  const [selectedCol, setSelectedCol] = useState('')
  const [selectedChromaDir, setSelectedChromaDir] = useState('')

  // Company filter
  const [companyFilter, setCompanyFilter] = useState('')
  const [companySearch, setCompanySearch] = useState('')

  const { data: companiesData } = useQuery({
    queryKey: ['vectorCompanies', selectedCol, selectedChromaDir],
    queryFn: () => {
      const colName = selectedCol.includes('::') ? selectedCol.split('::')[1] : selectedCol || undefined
      return api.getVectorCompanies(colName, selectedChromaDir)
    },
    enabled: true,
  })
  const allCompanies = companiesData?.companies || []
  const filteredCompanies = companySearch
    ? allCompanies.filter(c => c.toLowerCase().includes(companySearch.toLowerCase()))
    : allCompanies

  // Search mutation
  const searchMutation = useMutation({
    mutationFn: () => api.vectorSearch(
      query, nResults, selectedCol, selectedChromaDir, companyFilter, summarize,
    ),
  })

  const handleColChange = (e) => {
    const val = e.target.value
    setSelectedCol(val)
    const col = collections?.collections?.find(c => `${c.db}::${c.name}` === val)
    setSelectedChromaDir(col?.chroma_dir || '')
    setCompanyFilter('')
  }

  const handleSearch = () => {
    if (query.trim()) searchMutation.mutate()
  }

  return (
    <div className="space-y-6 max-w-5xl">
      <div className="flex items-center gap-3">
        <Search className="text-blue-400" size={24} />
        <h2 className="text-2xl font-bold text-white">Vector Search</h2>
      </div>
      <p className="text-sm text-gray-500">
        Semantic search across ingested company documents. Filter by company and get AI-generated answers with citations.
      </p>

      {/* Filters row */}
      <div className="bg-gray-900/60 border border-gray-800 rounded-lg p-4 space-y-3">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {/* Collection picker */}
          <div>
            <label className="block text-[10px] text-gray-500 uppercase tracking-wide mb-1">Collection</label>
            <select value={selectedCol} onChange={handleColChange}
              className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200">
              <option value="">Default collection</option>
              {collections?.collections?.map(c => (
                <option key={`${c.db}::${c.name}`} value={`${c.db}::${c.name}`}>
                  [{c.db}] {c.name} ({c.count.toLocaleString()})
                </option>
              ))}
            </select>
          </div>

          {/* Company filter */}
          <div>
            <label className="block text-[10px] text-gray-500 uppercase tracking-wide mb-1">
              Company Filter <span className="text-gray-600">({allCompanies.length} available)</span>
            </label>
            <div className="relative">
              <Building2 size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-500" />
              <input
                list="company-options"
                value={companyFilter}
                onChange={(e) => { setCompanyFilter(e.target.value); setCompanySearch(e.target.value) }}
                placeholder="All companies"
                className="w-full bg-gray-800 border border-gray-700 rounded pl-8 pr-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <datalist id="company-options">
                {filteredCompanies.slice(0, 50).map(c => (
                  <option key={c} value={c} />
                ))}
              </datalist>
              {companyFilter && (
                <button onClick={() => { setCompanyFilter(''); setCompanySearch('') }}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300 text-xs">
                  ✕
                </button>
              )}
            </div>
          </div>

          {/* Options */}
          <div className="flex items-end gap-3">
            <div>
              <label className="block text-[10px] text-gray-500 uppercase tracking-wide mb-1">Results</label>
              <input type="number" value={nResults} onChange={(e) => setNResults(+e.target.value)} min={1} max={50}
                className="w-20 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200" />
            </div>
            <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer pb-2">
              <input type="checkbox" checked={summarize} onChange={(e) => setSummarize(e.target.checked)}
                className="rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500" />
              <Sparkles size={14} className="text-purple-400" />AI Answer
            </label>
          </div>
        </div>

        {/* Search box */}
        <div className="flex gap-3">
          <div className="relative flex-1">
            <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" />
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              placeholder="Ask a question about your companies…"
              className="w-full bg-gray-800 border border-gray-700 rounded-lg pl-10 pr-4 py-3 text-sm text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
          </div>
          <button
            onClick={handleSearch}
            disabled={!query.trim() || searchMutation.isPending}
            className="px-5 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-500 disabled:opacity-50 flex items-center gap-2"
          >
            {searchMutation.isPending ? <Loader2 className="animate-spin" size={14} /> : <Search size={14} />}
            Search
          </button>
        </div>
      </div>

      {/* AI Answer panel */}
      {searchMutation.data?.answer && (
        <AnswerPanel answer={searchMutation.data.answer} query={searchMutation.data.query} />
      )}

      {/* Results */}
      {searchMutation.data?.results && (
        <ResultsList
          results={searchMutation.data.results}
          query={searchMutation.data.query}
          companyFilter={searchMutation.data.company_filter}
        />
      )}

      {searchMutation.data?.error && (
        <div className="text-red-400 text-sm bg-red-900/20 border border-red-800 rounded px-4 py-3">
          {searchMutation.data.error}
        </div>
      )}
    </div>
  )
}


// ---------------------------------------------------------------------------
// AI Answer Panel
// ---------------------------------------------------------------------------

function AnswerPanel({ answer, query }) {
  const [showCitations, setShowCitations] = useState(true)

  return (
    <div className="bg-gradient-to-br from-purple-900/20 to-blue-900/20 border border-purple-800/50 rounded-lg overflow-hidden">
      <div className="px-5 py-3 border-b border-purple-800/40 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Sparkles size={16} className="text-purple-400" />
          <h3 className="text-sm font-semibold text-purple-300">AI Answer</h3>
        </div>
        <span className="text-[10px] text-gray-500 font-mono">{answer.model}</span>
      </div>
      <div className="px-5 py-4">
        <p className="text-sm text-gray-200 leading-relaxed whitespace-pre-wrap">{answer.text}</p>
      </div>

      {/* Citations */}
      {answer.citations?.length > 0 && (
        <div className="border-t border-purple-800/40">
          <button
            onClick={() => setShowCitations(!showCitations)}
            className="w-full px-5 py-2 flex items-center justify-between text-xs text-gray-400 hover:bg-white/5 transition-colors"
          >
            <span className="flex items-center gap-1.5">
              <BookOpen size={12} />
              {answer.citations.length} sources cited
            </span>
            {showCitations ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
          </button>
          {showCitations && (
            <div className="px-5 pb-3 space-y-1.5">
              {answer.citations.map((cite) => (
                <div key={cite.index} className="flex items-start gap-2 text-xs">
                  <span className="flex-shrink-0 w-5 h-5 rounded bg-purple-800/40 text-purple-400 flex items-center justify-center font-mono text-[10px]">
                    {cite.index}
                  </span>
                  <div className="min-w-0 flex-1">
                    <span className="text-gray-300 font-medium">{cite.title || cite.company || 'Untitled'}</span>
                    {cite.company && cite.title && (
                      <span className="text-gray-600 ml-1.5">· {cite.company}</span>
                    )}
                    {cite.source_type && (
                      <span className={`ml-1.5 px-1.5 py-0.5 rounded text-[9px] font-medium uppercase ${
                        cite.source_type === 'news'
                          ? 'bg-amber-900/30 text-amber-400'
                          : 'bg-blue-900/30 text-blue-400'
                      }`}>{cite.source_type}</span>
                    )}
                    {cite.url && (
                      <a href={cite.url} target="_blank" rel="noopener noreferrer"
                        className="ml-1.5 text-blue-400/60 hover:text-blue-400 inline-flex items-center gap-0.5">
                        <ExternalLink size={9} />
                      </a>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}


// ---------------------------------------------------------------------------
// Results List
// ---------------------------------------------------------------------------

function ResultsList({ results, query, companyFilter }) {
  const [expanded, setExpanded] = useState({})

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-xs text-gray-500">
          {results.length} chunks for "<span className="text-gray-300">{query}</span>"
          {companyFilter && (
            <span className="ml-2 px-2 py-0.5 rounded bg-blue-900/30 text-blue-400 text-[10px]">
              <Building2 size={10} className="inline mr-1" />{companyFilter}
            </span>
          )}
        </p>
      </div>

      {results.map((r, i) => (
        <div key={r.id} className="bg-gray-900/60 rounded-lg border border-gray-800 p-4 hover:border-gray-700 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <span className="text-[10px] font-mono text-gray-600 bg-gray-800 px-1.5 py-0.5 rounded">
                <Hash size={9} className="inline" />{i + 1}
              </span>
              <span className="text-[10px] font-mono text-gray-600">
                dist: {r.distance?.toFixed(4)}
              </span>
              {r.metadata?.source_type && (
                <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium uppercase ${
                  r.metadata.source_type === 'news'
                    ? 'bg-amber-900/30 text-amber-400'
                    : 'bg-blue-900/30 text-blue-400'
                }`}>{r.metadata.source_type}</span>
              )}
            </div>
            {r.metadata?.company && (
              <span className="text-xs bg-blue-500/15 text-blue-400 px-2 py-0.5 rounded flex items-center gap-1">
                <Building2 size={10} />{r.metadata.company}
              </span>
            )}
          </div>

          {r.metadata?.page_title && (
            <p className="text-xs text-gray-400 font-medium mb-1.5">{r.metadata.page_title}</p>
          )}

          <p className="text-sm text-gray-300 leading-relaxed">
            {expanded[r.id] ? r.document : r.document?.slice(0, 300)}
            {r.document?.length > 300 && (
              <button
                onClick={() => setExpanded(prev => ({ ...prev, [r.id]: !prev[r.id] }))}
                className="ml-1 text-blue-400 hover:text-blue-300 text-xs"
              >
                {expanded[r.id] ? 'show less' : '…show more'}
              </button>
            )}
          </p>

          {r.metadata?.url_fetched && (
            <div className="mt-2">
              <a href={r.metadata.url_fetched} target="_blank" rel="noopener noreferrer"
                className="text-[10px] text-blue-400/60 hover:text-blue-400 flex items-center gap-1 truncate max-w-lg">
                <ExternalLink size={9} />{r.metadata.url_fetched}
              </a>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}
