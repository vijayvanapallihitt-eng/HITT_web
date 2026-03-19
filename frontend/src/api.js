/**
 * Thin wrapper around fetch() that prepends /api and handles JSON.
 */
const BASE = '/api'

async function request(path, options = {}) {
  const url = `${BASE}${path}`
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`${res.status}: ${text}`)
  }
  return res.json()
}

export const api = {
  // ── Databases ─────────────────────────────────────────
  getDatabases: () => request('/databases'),
  createDatabase: (name) => request('/databases', { method: 'POST', body: JSON.stringify({ name }) }),
  deleteDatabase: (name) => request(`/databases/${name}`, { method: 'DELETE' }),
  getDatabaseStats: (name) => request(`/databases/${name}/stats`),

  // ── Companies ─────────────────────────────────────────
  getCompanies: (db, params = {}) => {
    const qs = new URLSearchParams({ db, ...params }).toString()
    return request(`/companies?${qs}`)
  },
  getCompanyFilters: (db) => request(`/companies/filters?db=${db}`),
  getCompanyDetail: (db, id) => request(`/companies/${id}?db=${db}`),

  // ── Enrichment ────────────────────────────────────────
  getEnrichmentStats: (db) => request(`/enrichment/stats?db=${db}`),

  // ── Documents ─────────────────────────────────────────
  getDocuments: (db, params = {}) => {
    const qs = new URLSearchParams({ db, ...params }).toString()
    return request(`/documents?${qs}`)
  },

  // ── Vectors ───────────────────────────────────────────
  getCollections: () => request('/vectors/collections'),
  getVectorCompanies: (collection, chromaDir) => {
    const params = new URLSearchParams()
    if (collection) params.set('collection', collection)
    if (chromaDir) params.set('chroma_dir', chromaDir)
    return request(`/vectors/companies?${params}`)
  },
  vectorSearch: (query, nResults = 10, collection, chromaDir, company, summarize = false) => {
    const body = { query, n_results: nResults, summarize }
    if (collection) {
      const name = collection.includes('::') ? collection.split('::')[1] : collection
      body.collection = name
    }
    if (chromaDir) body.chroma_dir = chromaDir
    if (company) body.company = company
    return request('/vectors/search', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  // ── Scraper ───────────────────────────────────────────
  getScraperStatus: () => request('/scraper/status'),
  seedScraper: (db, queryFile) =>
    request('/scraper/seed', { method: 'POST', body: JSON.stringify({ db, query_file: queryFile }) }),
  startScraper: (db, opts = {}) =>
    request('/scraper/start', { method: 'POST', body: JSON.stringify({ db, ...opts }) }),
  stopScraper: (db) =>
    request('/scraper/stop', { method: 'POST', body: JSON.stringify({ db }) }),
  stopAllScrapers: () =>
    request('/scraper/stop-all', { method: 'POST' }),
  getScraperLogs: (db, tail = 50) => request(`/scraper/logs?db=${db}&tail=${tail}`),

  // ── Workers ───────────────────────────────────────────
  getWorkerStatus: () => request('/workers/status'),
  getWorkerProcesses: () => request('/workers/processes'),
  startEnricher: (db, opts = {}) =>
    request('/workers/enricher/start', { method: 'POST', body: JSON.stringify({ db, ...opts }) }),
  startIngester: (db, opts = {}) =>
    request('/workers/ingester/start', { method: 'POST', body: JSON.stringify({ db, ...opts }) }),
  startUnified: (db, opts = {}) =>
    request('/workers/unified/start', { method: 'POST', body: JSON.stringify({ db, ...opts }) }),
  stopWorker: (db, worker) =>
    request('/workers/stop', { method: 'POST', body: JSON.stringify({ db, worker }) }),
  stopAllWorkers: () =>
    request('/workers/stop-all', { method: 'POST' }),

  // ── Queries ───────────────────────────────────────────
  getQueryFiles: () => request('/queries'),
  getQueryFile: (name) => request(`/queries/${name}`),
  saveQueryFile: (filename, queries) =>
    request('/queries', { method: 'POST', body: JSON.stringify({ filename, queries }) }),
  generateQueries: (city, state, trades) =>
    request('/queries/generate', { method: 'POST', body: JSON.stringify({ city, state, trades }) }),

  // ── Research Agent ────────────────────────────────────
  runResearch: (db, result_id, opts = {}) =>
    request('/research/run', { method: 'POST', body: JSON.stringify({ db, result_id, ...opts }) }),
  runResearchByName: (db, company, website = '', opts = {}) =>
    request('/research/run-by-name', { method: 'POST', body: JSON.stringify({ db, company, website, ...opts }) }),
  getResearchResults: (db, result_id) =>
    request(`/research/results?db=${db}&result_id=${result_id}`),

  /**
   * Run web research (Google search + crawl4ai) with SSE progress streaming.
   * Returns { abort, events() } — same pattern as runResearchStream.
   */
  runWebResearchStream: (db, resultIds, opts = {}) => {
    const controller = new AbortController()
    const body = JSON.stringify({
      db,
      result_ids: resultIds,
      ...opts,
    })

    const promise = fetch(`${BASE}/web-research/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      signal: controller.signal,
    })

    return {
      abort: () => controller.abort(),
      async *events() {
        const res = await promise
        if (!res.ok) {
          const text = await res.text()
          throw new Error(`${res.status}: ${text}`)
        }
        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''

        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })

          const lines = buffer.split('\n')
          buffer = lines.pop()

          let currentEvent = 'message'
          for (const line of lines) {
            if (line.startsWith('event: ')) {
              currentEvent = line.slice(7).trim()
            } else if (line.startsWith('data: ')) {
              try {
                const data = JSON.parse(line.slice(6))
                yield { event: currentEvent, data }
              } catch { /* ignore non-JSON */ }
              currentEvent = 'message'
            }
          }
        }
      },
    }
  },

  /**
   * Run research with SSE progress streaming.
   * Returns an object with { eventSource, abort } where eventSource is
   * a ReadableStream wrapper you subscribe to via onProgress / onResult / onError.
   */
  runResearchStream: (db, result_id, opts = {}) => {
    const controller = new AbortController()
    const body = JSON.stringify({ db, result_id, ...opts })

    const promise = fetch(`${BASE}/research/run-stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      signal: controller.signal,
    })

    return {
      abort: () => controller.abort(),
      /** Async generator that yields parsed SSE events */
      async *events() {
        const res = await promise
        if (!res.ok) {
          const text = await res.text()
          throw new Error(`${res.status}: ${text}`)
        }
        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''

        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })

          // Parse SSE frames from buffer
          const lines = buffer.split('\n')
          buffer = lines.pop() // keep incomplete line

          let currentEvent = 'message'
          for (const line of lines) {
            if (line.startsWith('event: ')) {
              currentEvent = line.slice(7).trim()
            } else if (line.startsWith('data: ')) {
              const raw = line.slice(6)
              try {
                const data = JSON.parse(raw)
                yield { event: currentEvent, data }
              } catch {
                // ignore non-JSON data lines
              }
              currentEvent = 'message'
            }
            // ignore comment lines (: keepalive) and empty lines
          }
        }
      },
    }
  },

  // ── Health ────────────────────────────────────────────
  health: () => request('/health'),
}
