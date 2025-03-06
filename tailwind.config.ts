import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/ui/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "#ffffff",
        foreground: "#171717",
        primary: "#102B55",
        secondary: "#174A7E",
        tertiary: "#22699D",
        lightBackground: "#202124",
        darkBackground: "#121212",
      },
    }
  },
  plugins: [],
};

export default config;