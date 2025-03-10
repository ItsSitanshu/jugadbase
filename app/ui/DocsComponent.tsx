'use client';

import { useEffect, useState } from 'react';
import { unified } from 'unified';

import ReactMarkdown from 'react-markdown';
import rehypeSlug from 'rehype-slug';
import rehypeAutolinkHeadings from 'rehype-autolink-headings';
import remarkParse from 'remark-parse';
import rehypePrism from 'rehype-prism-plus';
import 'prismjs/themes/prism-twilight.css';

import Navbar from '@/app/ui/Navbar';

import './DocsMarkdown.css';

interface Heading {
  level: number;
  text: string;
  id: string;
}

const DocsComponent = ({ page, subpage, subsubpage }: { page?: string; subpage?: string; subsubpage?: string }) => {
  const [content, setContent] = useState<string>('');
  const [headings, setHeadings] = useState<Heading[]>([]);

  const fetchMarkdown = async (url: string) => {
    try {
      const response = await fetch(url);
      if (response.ok) {
        const text = await response.text();
        setContent(text);
        extractHeadings(text);
      } else {
        setContent('Error loading content.');
      }
    } catch (error) {
      setContent('Error loading content.');
    }
  };

  useEffect(() => {
    let url = 'https://raw.githubusercontent.com/ItsSitanshu/jugadbase/refs/heads/main/docs/index.md';

    if (page && subpage && subsubpage) {
      url = `https://raw.githubusercontent.com/ItsSitanshu/jugadbase/refs/heads/main/docs/${page}/${subpage}/${subsubpage}.md`;
    } else if (page && subpage) {
      url = `https://raw.githubusercontent.com/ItsSitanshu/jugadbase/refs/heads/main/docs/${page}/${subpage}.md`;
    } else if (page) {
      url = `https://raw.githubusercontent.com/ItsSitanshu/jugadbase/refs/heads/main/docs/${page}/index.md`;
    }

    fetchMarkdown(url);
  }, [page, subpage, subsubpage]);

  const extractHeadings = (markdown: string) => {
    const processor = unified().use(remarkParse);
    const tree = processor.parse(markdown);
    const headings: Heading[] = [];

    const visit = (node: any) => {
      if (node.type === 'heading') {
        const text = node.children.map((child: any) => child.value).join('');
        const id = text.toLowerCase().replace(/\s+/g, '-');
        if (node.depth === 2 || node.depth === 3 || node.depth === 4) {
          headings.push({ level: node.depth, text, id });
        }
      }

      if (node.children) {
        node.children.forEach(visit);
      }
    };

    visit(tree);
    setHeadings(headings);
  };

  return (
    <div className="min-h-screen min-w-screen bg-[var(--background)]">
      <Navbar />
      <div className="flex flex-row w-full h-full bg-[var(--background)] justify-center mt-5">
        <div className="h-full w-1/2 bg-[var(--background)] text-[var(--foreground)]" >
          <ReactMarkdown 
            children={content}
            rehypePlugins={[rehypeSlug, rehypeAutolinkHeadings, rehypePrism]} 
            components={{
              h1: ({ node, ...props }) => (
                <div className="relative w-full">
                  <h1 className="absolute left-[-1.8rem] top-[0.2rem] text-3xl text-white/20 font-fira">#</h1>
                  <h1 className="text-4xl font-bold border-[var(--secondary)] mb-2 font-fira" {...props} />
                </div>

              ),
              h2: ({ node, ...props }) => (
                <h2 className="text-2xl py-1 mb-2 hover:text-[var(--secondary)] transition-all duration-300 font-fira" {...props} />
              ),
              p: ({ node, ...props }) => (
                <p className="leading-relaxed font-fira" {...props} />
              ),
              strong: ({ node, ...props}) => (
                <strong className='font-fira' {...props}/>
              ),
              a: ({ node, ...props }) => (
                <a className="text-[var(--tertiary)] underline hover:text-secondary transition-colors font-fira" {...props} />
              ),
              ul: ({ node, ...props }) => (
                <ul className="list-['-_'] pl-5 space-y-2 font-fira" {...props} />
              ),
              ol: ({ node, ...props }) => (
                <ol className="list-[upper-roman] pl-5 font-fira" {...props} />
              ),
              li: ({ node, ...props }) => (
                <li className="font-fira " {...props} />
              ),
              blockquote: ({ node, ...props }) => (
                <blockquote className="border-l-4 border-tertiary pl-4 italic font-fira" {...props} />
              ),
              code: ({ node, ...props }) => (
                <code className="rounded px-1 bg-light-background font-fira" {...props} />
              ),
              pre: ({ node, ...props }) => (
                <pre className="p-3 rounded overflow-x-auto bg-dark-background font-fira" {...props} />
              ),
            }}
          />
        </div>
        {/* {page && (
          <aside className="toc">
            <p>On this document</p>
            <ul>
              {headings.map((heading) => (
                <li key={heading.id} className={`level-${heading.level - 1}`}>
                  <a href={`#${heading.id}`}>{heading.text}</a>
                </li>
              ))}
            </ul>
          </aside>
        )} */}
      </div>
    </div>
  );
};

export default DocsComponent;