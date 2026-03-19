import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useDb } from '../DbContext'
import { api } from '../api'
import { Plus, Trash2, Loader2, Database } from 'lucide-react'

function DatabaseCard({ db, isActive, setActiveDb, onDelete }) {
  const { data: stats } = useQuery({
    queryKey: ['dbStats', db.name],
    queryFn: () => api.getDatabaseStats(db.name),
    staleTime: 30_000,
  })

  const tc = stats?.table_counts || {}
  const pending = stats?.pending || {}

  return (
    <div className={`bg-gray-900 rounded-lg border p-4 transition-colors ${isActive ? 'border-blue-500/50' : 'border-gray-800'}`}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-3">
          <Database size={16} className={isActive ? 'text-blue-400' : 'text-gray-500'} />
          <h3 className="font-semibold text-gray-200 font-mono">{db.name}</h3>
          {isActive && <span className="text-[10px] bg-blue-600/30 text-blue-400 px-2 py-0.5 rounded">Active</span>}
        </div>
        <div className="flex gap-2">
          {!isActive && (
            <button
              onClick={() => setActiveDb(db.name)}
              className="px-3 py-1 bg-gray-800 text-gray-300 rounded text-xs hover:bg-gray-700"
            >
              Switch to
            </button>
          )}
          <button
            onClick={() => { if (confirm(`Delete database "${db.name}"?`)) onDelete(db.name) }}
            className="px-2 py-1 text-red-400/50 hover:text-red-400 rounded text-xs hover:bg-red-600/10"
          >
            <Trash2 size={14} />
          </button>
        </div>
      </div>

      <div className="grid grid-cols-4 gap-3 text-sm">
        <div>
          <div className="text-[10px] text-gray-500 uppercase">Results</div>
          <div className="text-white font-mono">{(tc.results ?? db.results)?.toLocaleString()}</div>
        </div>
        <div>
          <div className="text-[10px] text-gray-500 uppercase">Links</div>
          <div className="text-white font-mono">{(tc.link_candidates ?? db.link_candidates)?.toLocaleString()}</div>
        </div>
        <div>
          <div className="text-[10px] text-gray-500 uppercase">Documents</div>
          <div className="text-white font-mono">{(tc.documents ?? db.documents)?.toLocaleString()}</div>
        </div>
        <div>
          <div className="text-[10px] text-gray-500 uppercase">Chunks</div>
          <div className="text-white font-mono">{(tc.document_chunks ?? db.document_chunks)?.toLocaleString()}</div>
        </div>
      </div>

      {pending.results_missing_link_discovery > 0 && (
        <div className="mt-2 text-xs text-amber-400">
          ⚠ {pending.results_missing_link_discovery} results pending enrichment
        </div>
      )}
    </div>
  )
}

export default function DatabasesPage() {
  const { databases, activeDb, setActiveDb, refresh } = useDb()
  const qc = useQueryClient()
  const [newName, setNewName] = useState('')

  const createDb = useMutation({
    mutationFn: () => api.createDatabase(newName),
    onSuccess: () => { setNewName(''); refresh(); qc.invalidateQueries({ queryKey: ['databases'] }) },
  })

  const deleteDb = useMutation({
    mutationFn: (name) => api.deleteDatabase(name),
    onSuccess: () => { refresh(); qc.invalidateQueries({ queryKey: ['databases'] }) },
  })

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-white">Databases</h2>

      {/* Create new */}
      <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
        <h3 className="text-sm font-semibold text-gray-300 mb-3">Create New Database</h3>
        <div className="flex gap-3">
          <input
            value={newName}
            onChange={(e) => setNewName(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, ''))}
            placeholder="my_new_database"
            className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 font-mono focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
          <button
            onClick={() => createDb.mutate()}
            disabled={!newName.trim() || createDb.isPending}
            className="px-4 py-2 bg-blue-600 text-white rounded text-sm font-medium hover:bg-blue-500 disabled:opacity-50 flex items-center gap-2"
          >
            {createDb.isPending ? <Loader2 className="animate-spin" size={14} /> : <Plus size={14} />}
            Create
          </button>
        </div>
        {createDb.isSuccess && <p className="text-green-400 text-xs mt-2">✅ Database created!</p>}
        {createDb.isError && <p className="text-red-400 text-xs mt-2">{createDb.error.message}</p>}
      </div>

      {/* Database list */}
      <div className="space-y-4">
        {databases.map((db) => (
          <DatabaseCard
            key={db.name}
            db={db}
            isActive={db.name === activeDb}
            setActiveDb={setActiveDb}
            onDelete={(name) => deleteDb.mutate(name)}
          />
        ))}
      </div>
    </div>
  )
}
