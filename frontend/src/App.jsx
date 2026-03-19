import { useState } from 'react'
import { Routes, Route, NavLink, Navigate } from 'react-router-dom'
import { DbProvider, useDb } from './DbContext'
import OverviewPage from './pages/OverviewPage'
import CompaniesPage from './pages/CompaniesPage'
import CompanyDetailPage from './pages/CompanyDetailPage'
import PipelinePage from './pages/PipelinePage'
import DatabasesPage from './pages/DatabasesPage'
import VectorSearchPage from './pages/VectorSearchPage'
import { api } from './api'
import {
  LayoutDashboard,
  Building2,
  Workflow,
  MessageCircleQuestion,
  Plus,
  Settings,
  ChevronDown,
  Loader2,
} from 'lucide-react'

const NAV = [
  { to: '/overview',   label: 'Dashboard',  icon: LayoutDashboard },
  { to: '/pipeline',   label: 'Pipeline',   icon: Workflow },
  { to: '/companies',  label: 'Results',    icon: Building2 },
  { to: '/search',     label: 'Ask',        icon: MessageCircleQuestion },
]

function Sidebar() {
  const { databases, activeDb, setActiveDb, refresh } = useDb()
  const [showNewDb, setShowNewDb] = useState(false)
  const [newName, setNewName] = useState('')
  const [creating, setCreating] = useState(false)

  const handleCreate = async () => {
    if (!newName.trim()) return
    setCreating(true)
    try {
      await api.createDatabase(newName.trim().toLowerCase())
      setNewName('')
      setShowNewDb(false)
      refresh()
    } catch (e) {
      alert(e.message)
    } finally {
      setCreating(false)
    }
  }

  return (
    <aside className="w-56 bg-gray-900 border-r border-gray-800 flex flex-col h-screen sticky top-0">
      <div className="px-4 py-5 border-b border-gray-800">
        <h1 className="text-lg font-bold tracking-tight text-white">Broker</h1>
        <p className="text-xs text-gray-500 mt-0.5">Lead Pipeline</p>
      </div>

      {/* Database selector */}
      <div className="px-3 py-3 border-b border-gray-800">
        <label className="text-[10px] uppercase tracking-widest text-gray-500 font-semibold">
          Database
        </label>
        <select
          value={activeDb}
          onChange={(e) => setActiveDb(e.target.value)}
          className="mt-1 w-full bg-gray-800 border border-gray-700 rounded text-sm px-2 py-1.5 text-gray-200 focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          {databases.map((db) => (
            <option key={db.name} value={db.name}>
              {db.name} ({db.results?.toLocaleString() ?? 0})
            </option>
          ))}
        </select>

        {/* Create new database inline */}
        {showNewDb ? (
          <div className="mt-2 space-y-1.5">
            <input
              value={newName}
              onChange={(e) => setNewName(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, ''))}
              placeholder="new_database_name"
              className="w-full bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 font-mono focus:outline-none focus:ring-1 focus:ring-blue-500"
              onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
              autoFocus
            />
            <div className="flex gap-1">
              <button
                onClick={handleCreate}
                disabled={!newName.trim() || creating}
                className="flex-1 px-2 py-1 bg-blue-600 text-white rounded text-[10px] hover:bg-blue-500 disabled:opacity-50 flex items-center justify-center gap-1"
              >
                {creating ? <Loader2 size={10} className="animate-spin" /> : <Plus size={10} />}
                Create
              </button>
              <button
                onClick={() => { setShowNewDb(false); setNewName('') }}
                className="px-2 py-1 bg-gray-800 text-gray-400 rounded text-[10px] hover:bg-gray-700"
              >
                Cancel
              </button>
            </div>
          </div>
        ) : (
          <button
            onClick={() => setShowNewDb(true)}
            className="mt-1.5 w-full flex items-center justify-center gap-1 px-2 py-1 text-[10px] text-gray-500 hover:text-gray-300 hover:bg-gray-800 rounded transition-colors"
          >
            <Plus size={10} /> New Database
          </button>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-2 py-4 space-y-1 overflow-y-auto">
        {NAV.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `flex items-center gap-2.5 px-3 py-2.5 rounded-lg text-sm transition-colors ${
                isActive
                  ? 'bg-blue-600/20 text-blue-400 font-medium'
                  : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800'
              }`
            }
          >
            <Icon size={18} />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* Footer with settings link */}
      <div className="px-3 py-3 border-t border-gray-800">
        <NavLink
          to="/settings"
          className={({ isActive }) =>
            `flex items-center gap-2 px-3 py-2 rounded-lg text-xs transition-colors ${
              isActive
                ? 'bg-gray-800 text-gray-200'
                : 'text-gray-600 hover:text-gray-400 hover:bg-gray-800/50'
            }`
          }
        >
          <Settings size={14} />
          Settings
        </NavLink>
        <div className="px-3 mt-2 text-[10px] text-gray-700">
          Broker Pipeline v0.3
        </div>
      </div>
    </aside>
  )
}

export default function App() {
  return (
    <DbProvider>
      <div className="flex min-h-screen">
        <Sidebar />
        <main className="flex-1 p-6 overflow-y-auto">
          <Routes>
            <Route path="/" element={<Navigate to="/pipeline" replace />} />
            <Route path="/overview" element={<OverviewPage />} />
            <Route path="/pipeline" element={<PipelinePage />} />
            <Route path="/companies" element={<CompaniesPage />} />
            <Route path="/companies/:id" element={<CompanyDetailPage />} />
            <Route path="/search" element={<VectorSearchPage />} />
            <Route path="/settings" element={<DatabasesPage />} />
            {/* Redirect old routes */}
            <Route path="/enrichment" element={<Navigate to="/pipeline" replace />} />
            <Route path="/scraper" element={<Navigate to="/pipeline" replace />} />
            <Route path="/databases" element={<Navigate to="/settings" replace />} />
            <Route path="/research" element={<Navigate to="/pipeline" replace />} />
          </Routes>
        </main>
      </div>
    </DbProvider>
  )
}
