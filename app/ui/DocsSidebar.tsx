'use client';

import { useState, useEffect } from "react";

const DocsSidebar: React.FC = () => {
  const [toc, setToc] = useState<any>(null);
  const [hoveredItem, setHoveredItem] = useState<string | null>(null);

  const fetchTOC = async () => {
    try {
      const response = await fetch('https://raw.githubusercontent.com/ItsSitanshu/jugadbase/main/docs/config.json');
      if (!response.ok) throw new Error('Failed to fetch TOC');
      
      const data = await response.json();
      setToc(data);
    } catch (error) {
      console.error(error);
    }
  };

  useEffect(() => {
    fetchTOC();
  }, []);

  const handleMouseEnter = (key: string) => {
    setTimeout(() => {
      setHoveredItem(key);
    }, 500); 
  };

  const handleMouseLeave = () => {
    setHoveredItem(null);
  };

  const renderTOC = (section: any, depth: number = 0) => (
    <ul className={`${depth > 0 ? "pl-4" : ""}`}>
      {Object.entries(section).map(([key, value]: [string, any]) => (
        <li 
          key={key} 
          className="mb-1 relative"
          onMouseEnter={() => handleMouseEnter(key)}
          onMouseLeave={handleMouseLeave}
        >
          <a href={`/docs/${value.file}`} className="hover:underline font-fira ">
            {value.title}
          </a>
          {value.description && hoveredItem === key && (
            <div className="absolute left-full ml-2 top-[0.2rem] bg-[var(--tertiary)] text-[var(--foreground)] text-xs p-2 rounded shadow-md w-64">
              {value.description}
            </div>
          )}
          {value.subsections && renderTOC(value.subsections, depth + 1)}
        </li>
      ))}
    </ul>
  );

  return (
    <aside className="toc bg-[var(--background)] text-white p-4 mr-6">
      <h2 className="text-xl text-[var(--tertiary)] font-bold ">Documentation</h2>
      {toc ? renderTOC(toc) : <p>Loading...</p>}
    </aside>
  );
}

export default DocsSidebar;
