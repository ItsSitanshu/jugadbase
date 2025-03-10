'use client';

import React, { useEffect, useState } from 'react';
import DocsComponent from '@/app/ui/DocsComponent';

const DocsCategory = ({ params }: { params: { category: string } }) => {
  const [category, setCategory] = useState<string | undefined>(undefined);

  useEffect(() => {
    const fetchParams = async () => {
      const unwrappedParams = await params;
      setCategory(unwrappedParams.category);
    };

    fetchParams();
  }, [params]);

  if (!category) {
    return <div>Loading...</div>; 
  }

  return <DocsComponent page={category} />;
};

export default DocsCategory;
