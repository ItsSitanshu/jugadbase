'use client';

import DocsComponent from '@/app/ui/DocsComponent';

const DocsSubpage = ({ params }: { params: { category: string; subpage: string } }) => {
  return <DocsComponent page={params.category} subpage={params.subpage} />;
};

export default DocsSubpage;