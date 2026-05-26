import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "AARE-X v2 Orchestrator Platform Console",
  description: "Next-gen Adaptive Retrieval Execution Console for distributed technical docs databases.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-background text-slate-200 antialiased min-h-screen relative overflow-x-hidden selection:bg-cyanAccent selection:text-black">
        <div className="terminal-overlay absolute inset-0 z-50 pointer-events-none opacity-[0.03]" />
        {children}
      </body>
    </html>
  );
}
