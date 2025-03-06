'use client';

import Image from "next/image";

import Hero from "@/app/ui/Hero";
import Navbar from "@/app/ui/Navbar";

export default function Home() {
  return (
    <div className="overflow-hidden">
      <Navbar/>
      <Hero/>
    </div>      
  );
}
