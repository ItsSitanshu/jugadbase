'use client';

import React from 'react';

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white">
      {/* Custom Comforta Font */}
      <style jsx global>{`
        @import url('https://fonts.googleapis.com/css2?family=Comforta:wght@300;400;500;600;700&display=swap');
        
        :root {
          --dark: #102B55;
          --mid: #174A7E;
          --light: #22699D;
          --lbg: #202124;
          --dbg: #121212;
        }
        
        * {
          font-family: 'Comforta', cursive;
        }
        
        .gradient-text {
          background: linear-gradient(135deg, #22699D 0%, #174A7E 50%, #102B55 100%);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
        }
        
        .card-hover {
          transition: all 0.3s ease;
          backdrop-filter: blur(10px);
        }
        
        .card-hover:hover {
          transform: translateY(-5px);
          box-shadow: 0 20px 40px rgba(34, 105, 157, 0.3);
        }
        
        .pulse-glow {
          animation: pulse-glow 2s ease-in-out infinite alternate;
        }
        
        @keyframes pulse-glow {
          from {
            box-shadow: 0 0 20px rgba(34, 105, 157, 0.4);
          }
          to {
            box-shadow: 0 0 30px rgba(34, 105, 157, 0.6), 0 0 40px rgba(23, 74, 126, 0.3);
          }
        }
        
        .bg-custom-dark { background-color: var(--dark); }
        .bg-custom-mid { background-color: var(--mid); }
        .bg-custom-light { background-color: var(--light); }
        .bg-custom-lbg { background-color: var(--lbg); }
        .bg-custom-dbg { background-color: var(--dbg); }
        
        .text-custom-light { color: var(--light); }
        .text-custom-mid { color: var(--mid); }
        
        .border-custom-light { border-color: var(--light); }
        .border-custom-mid { border-color: var(--mid); }
      `}</style>

      {/* Header */}
      <header className="relative z-10 px-8 py-6">
        <nav className="flex items-center justify-between max-w-7xl mx-auto">
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 bg-custom-light rounded-lg flex items-center justify-center pulse-glow">
              <span className="text-white font-bold text-xl">J</span>
            </div>
            <h1 className="text-2xl font-bold gradient-text">JugadBase</h1>
          </div>
          <div className="hidden md:flex items-center space-x-6">
            <a href="#features" className="text-gray-300 hover:text-custom-light transition-colors">Features</a>
            <a href="#docs" className="text-gray-300 hover:text-custom-light transition-colors">Docs</a>
            <a href="#pricing" className="text-gray-300 hover:text-custom-light transition-colors">Pricing</a>
            <button className="bg-custom-light hover:bg-custom-mid px-6 py-2 rounded-full transition-colors font-medium">
              Get Started
            </button>
          </div>
        </nav>
      </header>

      {/* Hero Section */}
      <main className="relative z-10 px-8 py-20">
        <div className="max-w-6xl mx-auto text-center">
          <div className="mb-8">
            <h2 className="text-6xl md:text-7xl font-bold mb-6 leading-tight">
              The Open Source
              <br />
              <span className="gradient-text">Backend-as-a-Service</span>
            </h2>
            <p className="text-xl md:text-2xl text-gray-300 mb-8 max-w-3xl mx-auto leading-relaxed">
              Build faster with JugadBase - the powerful, developer-friendly alternative to traditional BaaS solutions. 
              Full-stack development made simple.
            </p>
          </div>

          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center mb-16">
            <button className="bg-custom-light hover:bg-custom-mid px-8 py-4 rounded-full text-lg font-semibold transition-all transform hover:scale-105 pulse-glow">
              Start Building Now
            </button>
            <button className="border-2 border-custom-light text-custom-light hover:bg-custom-light hover:text-white px-8 py-4 rounded-full text-lg font-semibold transition-all transform hover:scale-105">
              View Documentation
            </button>
          </div>

          {/* Feature Cards */}
          <div className="grid md:grid-cols-3 gap-8 mt-20">
            <div className="card-hover bg-custom-lbg bg-opacity-50 p-8 rounded-2xl border border-custom-mid">
              <div className="w-16 h-16 bg-custom-light rounded-2xl flex items-center justify-center mb-6 mx-auto">
                <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 1.79 4 4 4h8c2.21 0 4-1.79 4-4V7c0-2.21-1.79-4-4-4H8c-2.21 0-4 1.79-4 4z" />
                </svg>
              </div>
              <h3 className="text-2xl font-bold mb-4 text-custom-light">Database</h3>
              <p className="text-gray-300 leading-relaxed">
                Powerful PostgreSQL database with real-time subscriptions, row-level security, and automatic API generation.
              </p>
            </div>

            <div className="card-hover bg-custom-lbg bg-opacity-50 p-8 rounded-2xl border border-custom-mid">
              <div className="w-16 h-16 bg-custom-mid rounded-2xl flex items-center justify-center mb-6 mx-auto">
                <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                </svg>
              </div>
              <h3 className="text-2xl font-bold mb-4 text-custom-light">Authentication</h3>
              <p className="text-gray-300 leading-relaxed">
                Complete auth system with social logins, magic links, and advanced user management out of the box.
              </p>
            </div>

            <div className="card-hover bg-custom-lbg bg-opacity-50 p-8 rounded-2xl border border-custom-mid">
              <div className="w-16 h-16 bg-custom-dark rounded-2xl flex items-center justify-center mb-6 mx-auto">
                <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M9 19l3 3m0 0l3-3m-3 3V10" />
                </svg>
              </div>
              <h3 className="text-2xl font-bold mb-4 text-custom-light">Storage</h3>
              <p className="text-gray-300 leading-relaxed">
                Scalable file storage with automatic image optimization, CDN distribution, and fine-grained access controls.
              </p>
            </div>
          </div>

          {/* Code Preview */}
          <div className="mt-20 bg-custom-dbg rounded-2xl p-8 text-left max-w-4xl mx-auto border border-custom-mid">
            <div className="flex items-center mb-6">
              <div className="flex space-x-2">
                <div className="w-3 h-3 bg-red-500 rounded-full"></div>
                <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
                <div className="w-3 h-3 bg-green-500 rounded-full"></div>
              </div>
              <span className="ml-4 text-gray-400 font-medium">Get started in seconds</span>
            </div>
            <pre className="text-custom-light text-sm md:text-base overflow-x-auto">
              <code>{`import { createClient } from 'jugadbase-js'

const jugadbase = createClient(
  'https://your-project.jugadbase.co',
  'your-anon-key'
)

// Insert data
const { data, error } = await jugadbase
  .from('users')
  .insert([
    { name: 'John Doe', email: 'john@example.com' }
  ])

// Real-time subscriptions
jugadbase
  .channel('users')
  .on('postgres_changes', 
    { event: '*', schema: 'public', table: 'users' },
    (payload) => console.log('Change received!', payload)
  )
  .subscribe()`}</code>
            </pre>
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="relative z-10 px-8 py-12 border-t border-custom-mid">
        <div className="max-w-6xl mx-auto">
          <div className="flex flex-col md:flex-row justify-between items-center">
            <div className="flex items-center space-x-3 mb-4 md:mb-0">
              <div className="w-8 h-8 bg-custom-light rounded-lg flex items-center justify-center">
                <span className="text-white font-bold">J</span>
              </div>
              <span className="text-xl font-bold gradient-text">JugadBase</span>
            </div>
            <div className="flex flex-wrap gap-6 text-center md:text-left">
              <a href="#" className="text-gray-400 hover:text-custom-light transition-colors flex items-center gap-2">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.746 0 3.332.477 4.5 1.253v13C19.832 18.477 18.246 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
                </svg>
                Documentation
              </a>
              <a href="#" className="text-gray-400 hover:text-custom-light transition-colors flex items-center gap-2">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
                </svg>
                GitHub
              </a>
              <a href="#" className="text-gray-400 hover:text-custom-light transition-colors flex items-center gap-2">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8h2a2 2 0 012 2v6a2 2 0 01-2 2h-2m-4-6v6m-4 0h8m-8 0H9a2 2 0 01-2-2V10a2 2 0 012-2h2m4 0V6a2 2 0 00-2-2H9a2 2 0 00-2 2v2" />
                </svg>
                Community
              </a>
            </div>
          </div>
          <div className="mt-8 pt-8 border-t border-custom-mid text-center text-gray-400">
            <p>&copy; 2024 JugadBase. Built with ❤️ for developers.</p>
          </div>
        </div>
      </footer>

      {/* Background Elements */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-custom-light rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-pulse"></div>
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-custom-mid rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-pulse" style={{ animationDelay: '2s' }}></div>
        <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 w-96 h-96 bg-custom-dark rounded-full mix-blend-multiply filter blur-xl opacity-10 animate-pulse" style={{ animationDelay: '4s' }}></div>
      </div>
    </div>
  );
}