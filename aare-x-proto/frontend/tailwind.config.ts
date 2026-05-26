import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "#0A0B0D",
        surface: "#111318",
        border: "#1E2028",
        cyanAccent: "#00D4FF",
        greenAccent: "#00FF88",
        amberAccent: "#FFB800",
        crimsonAccent: "#FF4444",
      },
      fontFamily: {
        mono: ["JetBrains Mono", "Fira Code", "ui-monospace", "monospace"],
      },
    },
  },
  plugins: [],
};
export default config;
