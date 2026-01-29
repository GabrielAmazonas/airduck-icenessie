'use client'

import { useState, useEffect } from 'react'

interface SearchResult {
  id: string
  content: string
  score: number
  metadata?: Record<string, unknown>
}

interface IndexStatus {
  total_documents: number
  last_updated: string | null
  status: string
  index_file?: string
  has_vector_index?: boolean
}

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:80'

export default function Home() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchResult[]>([])
  const [status, setStatus] = useState<IndexStatus | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  
  // Index document state
  const [showIndexForm, setShowIndexForm] = useState(false)
  const [indexing, setIndexing] = useState(false)
  const [indexSuccess, setIndexSuccess] = useState<string | null>(null)
  const [docId, setDocId] = useState('')
  const [docContent, setDocContent] = useState('')
  const [docMetadata, setDocMetadata] = useState('')

  useEffect(() => {
    fetchStatus()
  }, [])

  const fetchStatus = async () => {
    try {
      const response = await fetch(`${API_URL}/status`)
      if (response.ok) {
        const data = await response.json()
        setStatus(data)
      }
    } catch (err) {
      console.error('Failed to fetch status:', err)
    }
  }

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!query.trim()) return

    setLoading(true)
    setError(null)

    try {
      const response = await fetch(`${API_URL}/search`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query, top_k: 10 }),
      })

      if (!response.ok) {
        throw new Error('Search failed')
      }

      const data = await response.json()
      setResults(data)
    } catch (err) {
      setError('Failed to perform search. Please try again.')
      console.error('Search error:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleIndexDocument = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!docId.trim() || !docContent.trim()) return

    setIndexing(true)
    setError(null)
    setIndexSuccess(null)

    try {
      let metadata = null
      if (docMetadata.trim()) {
        try {
          metadata = JSON.parse(docMetadata)
        } catch {
          setError('Invalid JSON in metadata field')
          setIndexing(false)
          return
        }
      }

      const response = await fetch(`${API_URL}/index`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          id: docId,
          content: docContent,
          metadata: metadata,
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || 'Indexing failed')
      }

      const data = await response.json()
      setIndexSuccess(`Document "${data.id}" indexed successfully!`)
      
      // Reset form
      setDocId('')
      setDocContent('')
      setDocMetadata('')
      
      // Refresh status
      fetchStatus()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to index document')
      console.error('Index error:', err)
    } finally {
      setIndexing(false)
    }
  }

  const generateDocId = () => {
    setDocId(`doc-${Date.now().toString(36)}`)
  }

  return (
    <main className="min-h-screen">
      {/* Header */}
      <header className="glass border-b border-dark-700/50 sticky top-0 z-50">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-primary-500 to-accent-500 flex items-center justify-center shadow-lg shadow-primary-500/20">
              <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </div>
            <h1 className="text-xl font-semibold tracking-tight">
              <span className="gradient-text">RAG</span> Search
            </h1>
          </div>
          
          {/* Status and Add Document Button */}
          <div className="flex items-center gap-4">
            {status && (
              <div className="flex items-center gap-2 text-sm text-dark-400">
                <span className={`w-2 h-2 rounded-full ${status.status === 'ready' || status.has_vector_index ? 'bg-emerald-400 animate-pulse-soft' : 'bg-amber-400'}`} />
                <span>{status.total_documents.toLocaleString()} documents</span>
              </div>
            )}
            <button
              onClick={() => setShowIndexForm(!showIndexForm)}
              className={`btn-secondary flex items-center gap-2 ${showIndexForm ? 'bg-dark-700 border-primary-500' : ''}`}
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
              </svg>
              Add Document
            </button>
          </div>
        </div>
      </header>

      {/* Index Document Panel */}
      {showIndexForm && (
        <div className="border-b border-dark-700/50 bg-dark-900/80 backdrop-blur-sm animate-slide-up">
          <div className="max-w-4xl mx-auto px-6 py-8">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-lg font-medium text-dark-200">Index New Document</h2>
              <button
                onClick={() => {
                  setShowIndexForm(false)
                  setIndexSuccess(null)
                }}
                className="text-dark-500 hover:text-dark-300 transition-colors"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            {/* Success Message */}
            {indexSuccess && (
              <div className="mb-6 p-4 bg-emerald-900/30 border border-emerald-700/50 rounded-xl text-emerald-300 flex items-center gap-3 animate-fade-in">
                <svg className="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
                {indexSuccess}
              </div>
            )}

            <form onSubmit={handleIndexDocument} className="space-y-4">
              {/* Document ID */}
              <div>
                <label className="block text-sm font-medium text-dark-400 mb-2">
                  Document ID
                </label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={docId}
                    onChange={(e) => setDocId(e.target.value)}
                    placeholder="unique-document-id"
                    className="search-input flex-1"
                    required
                  />
                  <button
                    type="button"
                    onClick={generateDocId}
                    className="btn-secondary whitespace-nowrap"
                  >
                    Generate ID
                  </button>
                </div>
              </div>

              {/* Content */}
              <div>
                <label className="block text-sm font-medium text-dark-400 mb-2">
                  Content
                </label>
                <textarea
                  value={docContent}
                  onChange={(e) => setDocContent(e.target.value)}
                  placeholder="Enter the document content to be indexed..."
                  rows={4}
                  className="search-input resize-none"
                  required
                />
              </div>

              {/* Metadata (Optional) */}
              <div>
                <label className="block text-sm font-medium text-dark-400 mb-2">
                  Metadata <span className="text-dark-600">(optional, JSON format)</span>
                </label>
                <textarea
                  value={docMetadata}
                  onChange={(e) => setDocMetadata(e.target.value)}
                  placeholder='{"category": "tutorial", "author": "John Doe"}'
                  rows={2}
                  className="search-input resize-none font-mono text-sm"
                />
              </div>

              {/* Error message */}
              {error && !loading && (
                <div className="p-4 bg-red-900/30 border border-red-700/50 rounded-xl text-red-300 animate-fade-in">
                  {error}
                </div>
              )}

              {/* Submit Button */}
              <div className="flex justify-end gap-3 pt-2">
                <button
                  type="button"
                  onClick={() => {
                    setShowIndexForm(false)
                    setIndexSuccess(null)
                    setError(null)
                  }}
                  className="btn-secondary"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={indexing || !docId.trim() || !docContent.trim()}
                  className="btn-primary disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
                >
                  {indexing ? (
                    <>
                      <svg className="animate-spin w-4 h-4" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                      </svg>
                      Indexing...
                    </>
                  ) : (
                    <>
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
                      </svg>
                      Index Document
                    </>
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Hero Section */}
      <section className="relative py-20 px-6">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-5xl md:text-6xl font-bold mb-6 tracking-tight animate-fade-in">
            Search your <span className="gradient-text">knowledge base</span>
          </h2>
          <p className="text-xl text-dark-400 mb-12 max-w-2xl mx-auto animate-slide-up" style={{ animationDelay: '0.1s' }}>
            Powered by DuckDB vector search. Find exactly what you need with semantic understanding.
          </p>

          {/* Search Form */}
          <form onSubmit={handleSearch} className="animate-slide-up" style={{ animationDelay: '0.2s' }}>
            <div className="relative flex gap-3 max-w-3xl mx-auto">
              <div className="relative flex-1">
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Ask anything about your documents..."
                  className="search-input pr-12"
                />
                <svg 
                  className="absolute right-4 top-1/2 -translate-y-1/2 w-5 h-5 text-dark-500"
                  fill="none" 
                  stroke="currentColor" 
                  viewBox="0 0 24 24"
                >
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
              </div>
              <button
                type="submit"
                disabled={loading || !query.trim()}
                className="btn-primary disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
              >
                {loading ? (
                  <>
                    <svg className="animate-spin w-5 h-5" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                    </svg>
                    Searching
                  </>
                ) : (
                  'Search'
                )}
              </button>
            </div>
          </form>

          {/* Error message */}
          {error && !showIndexForm && (
            <div className="mt-6 p-4 bg-red-900/30 border border-red-700/50 rounded-xl text-red-300 max-w-2xl mx-auto animate-fade-in">
              {error}
            </div>
          )}
        </div>
      </section>

      {/* Results Section */}
      <section className="px-6 pb-20">
        <div className="max-w-4xl mx-auto">
          {results.length > 0 && (
            <>
              <div className="flex items-center justify-between mb-6">
                <h3 className="text-lg font-medium text-dark-300">
                  Found <span className="text-primary-400">{results.length}</span> results
                </h3>
              </div>

              <div className="space-y-4">
                {results.map((result, index) => (
                  <article
                    key={result.id}
                    className="result-card animate-slide-up"
                    style={{ animationDelay: `${index * 0.05}s` }}
                  >
                    <div className="flex items-start justify-between gap-4 mb-3">
                      <span className="text-xs font-mono text-dark-500 bg-dark-800 px-2 py-1 rounded">
                        {result.id}
                      </span>
                      <span className="text-xs text-primary-400 font-medium">
                        Score: {(result.score * 100).toFixed(1)}%
                      </span>
                    </div>
                    <p className="text-dark-200 leading-relaxed">
                      {result.content}
                    </p>
                    {result.metadata && Object.keys(result.metadata).length > 0 && (
                      <div className="mt-4 pt-4 border-t border-dark-700/50">
                        <div className="flex flex-wrap gap-2">
                          {Object.entries(result.metadata).map(([key, value]) => (
                            <span
                              key={key}
                              className="text-xs px-2 py-1 bg-dark-800/80 text-dark-400 rounded-md"
                            >
                              {key}: {String(value)}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                  </article>
                ))}
              </div>
            </>
          )}

          {/* Empty state */}
          {results.length === 0 && !loading && (
            <div className="text-center py-16">
              <div className="w-20 h-20 mx-auto mb-6 rounded-2xl bg-dark-800/50 flex items-center justify-center">
                <svg className="w-10 h-10 text-dark-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <h4 className="text-lg font-medium text-dark-400 mb-2">Ready to search</h4>
              <p className="text-dark-500 max-w-sm mx-auto">
                Enter a query above to search through your indexed documents, or click "Add Document" to index new content.
              </p>
            </div>
          )}
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-dark-800 py-8 px-6">
        <div className="max-w-6xl mx-auto flex items-center justify-between text-sm text-dark-500">
          <span>RAG Stack • DuckDB + FastAPI + Next.js</span>
          <span>Powered by vector search</span>
        </div>
      </footer>
    </main>
  )
}
