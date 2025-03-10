'use client';

import { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import rehypeSlug from 'rehype-slug';
import rehypeAutolinkHeadings from 'rehype-autolink-headings';
import remarkParse from 'remark-parse';
import { unified } from 'unified';
import rehypePrism from 'rehype-prism-plus';
import 'prismjs/themes/prism-twilight.css';

import Navbar from '@/app/ui/Navbar';

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
    <>
      <Navbar />
      <div className="doc-wrapper">
        <div className="content-wrapper">
          <div className="markdown-content">
            <ReactMarkdown children={content} rehypePlugins={[rehypeSlug, rehypeAutolinkHeadings, rehypePrism]} />
          </div>
        </div>
        {page && (
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
        )}
      </div>
    </>
  );
};

export default DocsComponent;