'use client';

import Image from 'next/image';
import { useRouter } from 'next/navigation';
import { useState } from 'react';

const Navbar: React.FC = () => {
  const router = useRouter();

  return (
    <div
      className="flex flex-row justify-between px-64 py-2 h-18 border-b-1 border-white/20 bg-[var(--background)]"
      // style={{ background: 'linear-gradient(to bottom, var(--light-background), var(--dark-background)'}}
    >
      <div className="flex flex-row items-center h-full gap-3">
        <Image src={'/brand_logo.svg'} width={256} height={256} className='h-12 w-12' alt="logo"/>
        <h1 className='tracking-widest text-white font-bold text-3xl'>jugadbase</h1>
      </div>
      <div className="flex flex-row items-center h-full gap-2">
        <button type="button"
          className="bg-[var(--secondary)] border border-[var(--tertiary)] px-2 py-1 rounded-md
          hover:bg-[var(--tertiary)] hover:cursor-pointer transition-all duration-300 "
        >
          <h1 className='text-white font-medium pt-0.5'>Get Started</h1>
        </button>
        <Image 
          src={require('@/app/assets/icons/docs.svg')}
          width={256} height={256} 
          className='h-8 w-8 hover:cursor-pointer'
          alt="docs"
          onClick={() => router.push('/docs')}
        />
        <Image 
          src={require('@/app/assets/icons/github.svg')}
          width={256} height={256} 
          className='h-8 w-8 hover:cursor-pointer'
          alt="github"
          onClick={() => window.open('https://github.com/itssitanshu/jugadbase', '_blank')}
        />
      </div>
    </div>
  );
}

export default Navbar;