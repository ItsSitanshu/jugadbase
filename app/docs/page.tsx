'use client';

import Navbar from '@/app/ui/Navbar';
import Link from 'next/link';

const DocsHome = () => {
  const cardData = [
    { title: 'Introduction to Jugadbase', content: 'Learn about Jugad-DB, its core concepts, and how it compares to other solutions', url: 'docs/intro' },
    { title: 'Authentication & Authorization', content: 'Understand how to implement user authentication, authorization, and security', url: 'docs/auth' },
    { title: 'SQL-Like Database', content: 'Learn about the sql-like database that powers Jugadbase ', url: 'docs/real-time' },
    { title: 'Real-Time Database', content: 'Discover how to implement real-time features', url: 'docs/real-time' },
    { title: 'Storage & File Management', content: 'Learn how to use Jugadbase for managing and serving large files like images and documents', url: 'docs/storage' },
    { title: 'API & Endpoints', content: 'API overview, interact with your projects easily', url: 'docs/api' },
  ];  

  return (
    <div className="min-h-screen min-w-screen bg-[var(--background)]">
      <Navbar />
      <div className="flex flex-row w-full flex-wrap gap-10">
        {cardData.map((card, index) => (
          <div key={index} className="w-2/5 p-5 h-1/4 bg-[var(--secondary)]">
            <Link href={`/${card.url}`}>
              <h3>{card.title}</h3>
              <p>{card.content}</p>
            </Link>
          </div>
        ))}
      </div>
    </div>
  );
};

export default DocsHome;
