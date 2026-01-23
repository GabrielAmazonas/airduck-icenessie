import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'RAG Search Engine',
  description: 'Vector search powered by DuckDB and FastAPI',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
      </head>
      <body className="font-sans antialiased" style={{ fontFamily: "'Space Grotesk', system-ui, sans-serif" }}>
        {children}
      </body>
    </html>
  )
}




