import type { Metadata } from "next";
import { Josefin_Sans, Comfortaa, Fira_Code } from "next/font/google";
import "./globals.css";

const JosefinSans = Josefin_Sans({
  variable: "--font-jose-sans",
  subsets: ["latin"],
});

const ComfortaaFont = Comfortaa({
  variable: "--font-comforta",
  subsets: ["latin"],
});

const FiraCodeFont = Fira_Code({
  variable: "--font-fira",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Jugadbase",
  description: "engineering, the jugad way",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <link rel="icon" type="image/svg+xml" href="/brand_logo.svg" />
      </head>
      <body
        className={`${JosefinSans.variable} ${ComfortaaFont.variable} ${FiraCodeFont.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
