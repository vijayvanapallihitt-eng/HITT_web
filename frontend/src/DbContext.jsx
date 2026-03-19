import { createContext, useContext, useState, useEffect } from 'react'
import { api } from './api'

const DbContext = createContext(null)

export function DbProvider({ children }) {
  const [databases, setDatabases] = useState([])
  const [activeDb, setActiveDb] = useState(() => localStorage.getItem('broker_active_db') || '')
  const [loading, setLoading] = useState(true)

  const refresh = async () => {
    try {
      const dbs = await api.getDatabases()
      setDatabases(dbs)
      const dbNames = dbs.map(d => d.name)
      // Validate cached selection still exists; fall back to first DB
      if (activeDb && !dbNames.includes(activeDb)) {
        console.warn(`Cached DB '${activeDb}' no longer exists, switching to '${dbNames[0]}'`)
        setActiveDb(dbNames[0] || '')
      } else if (!activeDb && dbs.length > 0) {
        setActiveDb(dbs[0].name)
      }
    } catch (e) {
      console.error('Failed to load databases:', e)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [])

  useEffect(() => {
    if (activeDb) localStorage.setItem('broker_active_db', activeDb)
  }, [activeDb])

  return (
    <DbContext.Provider value={{ databases, activeDb, setActiveDb, loading, refresh }}>
      {children}
    </DbContext.Provider>
  )
}

export function useDb() {
  const ctx = useContext(DbContext)
  if (!ctx) throw new Error('useDb must be used within <DbProvider>')
  return ctx
}
