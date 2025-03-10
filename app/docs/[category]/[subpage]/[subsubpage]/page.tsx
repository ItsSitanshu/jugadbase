'use client';

import DocsComponent from '@/app/ui/DocsComponent';

const DocsSubsubpage = ({ params }: { params: { category: string; subpage: string; subsubpage: string } }) => {
  return <DocsComponent page={params.category} subpage={params.subpage} subsubpage={params.subsubpage} />;
};

export default DocsSubsubpage;