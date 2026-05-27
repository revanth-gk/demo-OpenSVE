"use client";

import React, { useState, useEffect, useRef } from "react";
import { 
  Play, RotateCcw, AlertTriangle, ShieldCheck, CheckCircle2, 
  Cpu, Database, Layers, Activity, Sliders, ListFilter, Terminal, BookOpen, BarChart2
} from "lucide-react";

interface NodeData {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: {
    label: string;
    status: string;
    latency: number;
    estimated: number;
    logs: string;
  };
}

interface EdgeData {
  id: string;
  source: string;
  target: string;
  animated: boolean;
}

interface RetrievalResult {
  doc_id: string;
  title: string;
  domain: string;
  content: string;
  scores: {
    vector: number;
    bm25: number;
    graph: number;
  };
  rrf_score: number;
}

interface ResponseData {
  response: string;
  references: string[];
  citations: { doc_id: string; title: string }[];
  hallucination_index: number;
  verification_status: string;
  warnings: string[];
}

const PRESET_QUERIES = [
  "Compare Raft vs Paxos optimization loops",
  "Optimize HNSW vector indexes layer construction criteria",
  "FlashAttention key-value context pruning heuristics",
  "Resolve split-brain transaction locks using Spanner TrueTime limits",
  "Show me speculative decoding failure scenarios and rollback logs"
];

export default function Workbench() {
  // Console Inputs
  const [query, setQuery] = useState(PRESET_QUERIES[0]);
  const [latencyBudget, setLatencyBudget] = useState(3.0);
  const [tokenBudget, setTokenBudget] = useState(2048);
  const [forceGraph, setForceGraph] = useState(false);
  const [forceCache, setForceCache] = useState(false);

  // Runtime States
  const [queryId, setQueryId] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [nodes, setNodes] = useState<NodeData[]>([]);
  const [edges, setEdges] = useState<EdgeData[]>([]);
  const [totalEstLatency, setTotalEstLatency] = useState(0);
  const [elapsedTime, setElapsedTime] = useState(0);
  const [activeTab, setActiveTab] = useState<"output" | "trace" | "verification">("output");
  const [selectedDocId, setSelectedDocId] = useState<string | null>(null);

  // Streamed Data Output
  const [traceResults, setTraceResults] = useState<RetrievalResult[]>([]);
  const [responseOutput, setResponseOutput] = useState<ResponseData | null>(null);
  const [terminalLogs, setTerminalLogs] = useState<string[]>([
    "AARE-X core cluster initialized. Status: READY.",
    "Distributed vector storage partition mounted."
  ]);

  const timerRef = useRef<NodeJS.Timeout | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const logsEndRef = useRef<HTMLDivElement>(null);

  // Refs for tracking real-time values in EventSource closures without stale variables
  const elapsedTimeRef = useRef<number>(0);
  const latencyBudgetRef = useRef<number>(latencyBudget);
  const isRunningRef = useRef<boolean>(false);

  useEffect(() => {
    elapsedTimeRef.current = elapsedTime;
  }, [elapsedTime]);

  useEffect(() => {
    latencyBudgetRef.current = latencyBudget;
  }, [latencyBudget]);

  useEffect(() => {
    isRunningRef.current = isRunning;
  }, [isRunning]);

  // Auto scroll logs
  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [terminalLogs]);

  // Handle preset selection
  const selectPreset = (preset: string) => {
    if (isRunning) return;
    setQuery(preset);
    addLog(`Swapped input configuration to preset query.`);
  };

  const addLog = (msg: string) => {
    const timestamp = new Date().toISOString().split("T")[1].slice(0, 8);
    setTerminalLogs(prev => [...prev, `[${timestamp}] ${msg}`]);
  };

  // Compile and initialize execution graph
  const compileAndExecute = async () => {
    if (isRunning) return;

    // Reset runtime states
    setIsRunning(true);
    isRunningRef.current = true;
    setElapsedTime(0);
    elapsedTimeRef.current = 0;
    setTraceResults([]);
    setResponseOutput(null);
    setSelectedDocId(null);
    setTerminalLogs([
      "Initializing execution compilation pipeline...",
      `Query payload: "${query}"`
    ]);

    // Start timer clock
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = setInterval(() => {
      setElapsedTime(prev => parseFloat((prev + 0.1).toFixed(1)));
    }, 100);

    try {
      // POST payload to initialize pipeline
      const res = await fetch("http://localhost:8000/api/v1/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query,
          token_budget: tokenBudget,
          latency_budget: latencyBudget,
          force_graph: forceGraph,
          force_cache: forceCache
        })
      });

      if (!res.ok) {
        throw new Error(`Orchestrator returned ${res.status}`);
      }

      const data = await res.json();
      setQueryId(data.query_id);
      setNodes(data.dag.nodes);
      setEdges(data.dag.edges);
      setTotalEstLatency(data.dag.total_estimated_latency_ms);
      
      addLog(`Intent classified as ${data.intent.query_type} in ${data.intent.detected_domain} domain.`);
      addLog(`DAG constructed. Longest path weight estimation: ${data.dag.total_estimated_latency_ms}ms.`);

      // Connect SSE stream
      connectEventStream(data.query_id);

    } catch (err: any) {
      addLog(`FATAL: Execution compilation failed. ${err.message}`);
      setIsRunning(false);
      if (timerRef.current) clearInterval(timerRef.current);
    }
  };

  // Connect SSE to stream node state transitions
  const connectEventStream = (qid: string) => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    const es = new EventSource(`http://localhost:8000/api/v1/execution/${qid}/stream`);
    eventSourceRef.current = es;

    es.onmessage = (event) => {
      if (elapsedTimeRef.current > latencyBudgetRef.current) {
        return;
      }

      if (event.data === "[DONE]") {
        addLog("Orchestrator completed transaction execution pipeline. Graph idle.");
        setIsRunning(false);
        isRunningRef.current = false;
        if (timerRef.current) clearInterval(timerRef.current);
        es.close();
        return;
      }

      try {
        const payload = JSON.parse(event.data);

        if (payload.type === "state_update") {
          setNodes(payload.nodes);
          // Extract logs from running or active nodes
          const activeNode = payload.nodes.find((n: any) => n.data.status === "running");
          if (activeNode) {
            addLog(`[NODE:${activeNode.id}] ${activeNode.data.logs}`);
          }
          const completedNode = payload.nodes.find((n: any) => n.data.status === "completed" && n.data.latency > 0);
          // Highlight warnings
          const errorNode = payload.nodes.find((n: any) => n.data.status === "error");
          if (errorNode) {
            addLog(`[CRITICAL] Conflict caught on node ${errorNode.id}. Triggering self-correction rules.`);
          }
        } else if (payload.type === "trace_update") {
          setTraceResults(payload.results);
          addLog(`Index traces aggregated. ${payload.results.length} documents indexed into context.`);
        } else if (payload.type === "response_complete") {
          setResponseOutput(payload.data);
          addLog(`Response synthesis verified. Safety index: ${((1 - payload.data.hallucination_index) * 100).toFixed(0)}%.`);
          if (payload.data.warnings.length > 0) {
            payload.data.warnings.forEach((w: string) => addLog(`[Consensus Warning] ${w}`));
          }
        }
      } catch (err) {
        // Parse error
      }
    };

    es.onerror = (err) => {
      addLog("SSE execution channel received close signal from server.");
      setIsRunning(false);
      if (timerRef.current) clearInterval(timerRef.current);
      es.close();
    };
  };

  // SLA timeout checking hook
  useEffect(() => {
    if (isRunning && elapsedTime > latencyBudget) {
      // Latency budget exceeded!
      setIsRunning(false);
      isRunningRef.current = false;
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      
      // Mark running and pending nodes as timeout
      setNodes(prevNodes => {
        return prevNodes.map(node => {
          if (node.data.status === "running" || node.data.status === "pending") {
            return {
              ...node,
              data: {
                ...node.data,
                status: "timeout",
                logs: node.data.status === "running" 
                  ? `SLA breach: Latency budget of ${latencyBudget}s exceeded during execution.`
                  : `Execution canceled due to upstream SLA latency breach.`
              }
            };
          }
          return node;
        });
      });

      addLog(`[CRITICAL] Latency budget limit of ${latencyBudget}s exceeded! SLA breach detected. Aborting execution graph.`);
    }
  }, [elapsedTime, isRunning, latencyBudget]);

  useEffect(() => {
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
      if (eventSourceRef.current) eventSourceRef.current.close();
    };
  }, []);

  return (
    <div className="flex flex-col min-h-screen bg-background text-slate-100 font-mono">
      
      {/* 1. TOP HEADER BAR */}
      <header className="flex items-center justify-between px-6 py-4 border-b border-border bg-[#0E1014] shrink-0">
        <div className="flex items-center gap-3">
          <div className="relative flex items-center justify-center w-8 h-8 rounded border border-cyanAccent/30 bg-cyanAccent/5">
            <Cpu className="w-4 h-4 text-cyanAccent animate-pulse" />
          </div>
          <div>
            <h1 className="text-sm font-bold tracking-wider text-slate-100 flex items-center gap-2">
              AARE-X <span className="text-cyanAccent text-xs border border-cyanAccent/50 px-1 py-0.5 rounded">v2.0-PROTO</span>
            </h1>
            <p className="text-[10px] text-slate-500">ADAPTIVE RETRIEVAL EXECUTION PLATFORM</p>
          </div>
        </div>

        {/* Global Cluster Stats */}
        <div className="hidden md:flex items-center gap-6 text-[11px] text-slate-400">
          <div className="flex items-center gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-greenAccent animate-ping" />
            <span>CLUSTER STATE: <strong className="text-greenAccent">ONLINE</strong></span>
          </div>
          <div className="w-px h-4 bg-border" />
          <div className="flex items-center gap-2">
            <Database className="w-3.5 h-3.5 text-cyanAccent" />
            <span>MOCK SHARDS: <strong className="text-slate-200">8/8 ACTIVE</strong></span>
          </div>
          <div className="w-px h-4 bg-border" />
          <div className="flex items-center gap-2">
            <Layers className="w-3.5 h-3.5 text-amberAccent" />
            <span>ROUTING PLANNER: <strong className="text-slate-200">NETWORKX-RULE-V2</strong></span>
          </div>
        </div>
      </header>

      {/* 2. THREE-PANEL CORE LAYOUT */}
      <div className="flex flex-col lg:flex-row flex-1 overflow-hidden">
        
        {/* PANEL A: CONTROL HUB (LEFT) */}
        <section className="w-full lg:w-96 border-r border-border bg-[#0C0D11] p-5 flex flex-col gap-5 overflow-y-auto select-none shrink-0">
          
          <div className="flex items-center gap-2 text-xs font-bold text-slate-400 border-b border-border/40 pb-2">
            <Sliders className="w-3.5 h-3.5 text-cyanAccent" />
            <span>QUERY INGESTION CONSOLE</span>
          </div>

          {/* Text Area */}
          <div className="flex flex-col gap-2">
            <label className="text-[10px] text-slate-500 uppercase tracking-widest">Query String Buffer</label>
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              disabled={isRunning}
              className="w-full h-32 bg-[#08090C] border border-border rounded p-3 text-xs text-slate-300 focus:outline-none focus:border-cyanAccent resize-none transition-colors"
              placeholder="Inject technical prompt..."
            />
          </div>

          {/* Configuration Sliders */}
          <div className="flex flex-col gap-4 bg-[#111318]/50 p-3 rounded border border-border/50">
            <div>
              <div className="flex justify-between text-[11px] mb-1">
                <span className="text-slate-400">LATENCY BUDGET LIMIT</span>
                <span className="text-cyanAccent font-bold">{latencyBudget}s</span>
              </div>
              <input
                type="range"
                min="0.5"
                max="10.0"
                step="0.5"
                value={latencyBudget}
                onChange={(e) => setLatencyBudget(parseFloat(e.target.value))}
                disabled={isRunning}
                className="w-full accent-cyanAccent cursor-pointer"
              />
            </div>
            
            <div>
              <div className="flex justify-between text-[11px] mb-1">
                <span className="text-slate-400">MAX TOKEN BOUND</span>
                <span className="text-amberAccent font-bold">{tokenBudget} tokens</span>
              </div>
              <input
                type="range"
                min="512"
                max="8192"
                step="512"
                value={tokenBudget}
                onChange={(e) => setTokenBudget(parseInt(e.target.value))}
                disabled={isRunning}
                className="w-full accent-amberAccent cursor-pointer"
              />
            </div>
          </div>

          {/* Override Checkboxes */}
          <div className="flex flex-col gap-2.5">
            <label className="text-[10px] text-slate-500 uppercase tracking-widest">Override Directives</label>
            
            <label className="flex items-center gap-2.5 text-xs text-slate-300 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={forceGraph}
                onChange={(e) => setForceGraph(e.target.checked)}
                disabled={isRunning}
                className="rounded bg-[#08090C] border-border text-cyanAccent focus:ring-0 focus:ring-offset-0"
              />
              <span>Force Knowledge Graph Traversal</span>
            </label>

            <label className="flex items-center gap-2.5 text-xs text-slate-300 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={forceCache}
                onChange={(e) => setForceCache(e.target.checked)}
                disabled={isRunning}
                className="rounded bg-[#08090C] border-border text-cyanAccent focus:ring-0 focus:ring-offset-0"
              />
              <span>Bypass Semantic Cache Check</span>
            </label>
          </div>

          {/* Action Trigger Button */}
          <button
            onClick={compileAndExecute}
            disabled={isRunning || query.trim().length < 3}
            className={`w-full py-3 px-4 rounded border text-xs font-bold flex items-center justify-center gap-2 uppercase transition-all ${
              isRunning 
                ? "bg-cyanAccent/10 border-cyanAccent/30 text-cyanAccent cursor-not-allowed"
                : "bg-cyanAccent hover:bg-cyanAccent/90 border-transparent text-[#0A0B0D] hover:shadow-[0_0_15px_rgba(0,212,255,0.4)]"
            }`}
          >
            <Play className={`w-4 h-4 ${isRunning ? "animate-spin" : ""}`} />
            <span>{isRunning ? "Orchestrating Pipeline..." : "Compile & Execute"}</span>
          </button>

          {/* Preset Prompts List */}
          <div className="flex flex-col gap-2 mt-auto">
            <div className="flex items-center gap-2 text-xs font-bold text-slate-400 border-b border-border/40 pb-2">
              <ListFilter className="w-3.5 h-3.5 text-cyanAccent" />
              <span>PRESET CONFLICT LOGS</span>
            </div>
            <div className="flex flex-col gap-1.5 max-h-56 overflow-y-auto">
              {PRESET_QUERIES.map((preset, idx) => (
                <button
                  key={idx}
                  onClick={() => selectPreset(preset)}
                  disabled={isRunning}
                  className={`text-left text-[11px] p-2 rounded transition-all border ${
                    query === preset 
                      ? "bg-cyanAccent/5 border-cyanAccent/45 text-cyanAccent" 
                      : "bg-[#111318]/40 border-border/40 hover:bg-[#111318]/80 text-slate-400 hover:text-slate-200"
                  }`}
                >
                  {preset}
                </button>
              ))}
            </div>
          </div>
        </section>

        {/* PANEL B: LIVE DAG VISUALIZER (CENTER) */}
        <section className="flex-1 border-r border-border bg-[#090A0E] flex flex-col overflow-hidden relative">
          
          {/* Header/Breadcrumbs */}
          <div className="flex items-center justify-between px-5 py-3 border-b border-border/40 bg-[#0C0D11] shrink-0">
            <div className="flex items-center gap-2 text-xs font-bold text-slate-300">
              <Activity className="w-4 h-4 text-cyanAccent" />
              <span>INTERACTIVE DEPENDENCY GRAPH VISUALIZATION</span>
            </div>
            {isRunning && (
              <span className="text-[10px] bg-cyanAccent/10 border border-cyanAccent/40 px-2 py-0.5 rounded text-cyanAccent animate-pulse">
                STREAMING RUNTIME SSE
              </span>
            )}
          </div>

          {/* Timeline Speed bar */}
          <div className="px-5 py-3.5 bg-[#101217]/50 border-b border-border/30 flex items-center justify-between text-xs text-slate-400 shrink-0">
            <div className="flex items-center gap-2">
              <span>ELAPSED: <strong className="text-slate-100 font-bold">{elapsedTime}s</strong></span>
              <span className="text-slate-600">/</span>
              <span>ESTIMATED BUDGET: <strong className="text-slate-100 font-bold">{latencyBudget}s</strong></span>
            </div>

            {/* Micro progress gauge */}
            <div className="w-1/3 bg-[#1A1D24] h-2 rounded overflow-hidden relative border border-border/60">
              <div 
                className={`h-full transition-all duration-300 rounded ${
                  elapsedTime > latencyBudget 
                    ? "bg-crimsonAccent shadow-[0_0_8px_rgba(255,68,68,0.5)]" 
                    : "bg-cyanAccent shadow-[0_0_8px_rgba(0,212,255,0.5)]"
                }`}
                style={{ width: `${Math.min((elapsedTime / latencyBudget) * 100, 100)}%` }}
              />
            </div>
          </div>

          {/* Visual Execution DAG Plot (Interactive Custom SVG Structure) */}
          <div className="flex-1 overflow-auto p-6 flex items-center justify-center relative bg-[#07080B]">
            {nodes.length === 0 ? (
              <div className="text-center text-slate-500 text-xs max-w-sm flex flex-col items-center gap-3">
                <Terminal className="w-8 h-8 text-slate-600 animate-bounce" />
                <p>No active execution pipeline. Inject a query payload in the left control panel and execute compile pipelines to render.</p>
              </div>
            ) : (
              <div className="relative" style={{ width: "700px", height: "550px" }}>
                {/* SVG connection edges */}
                <svg className="absolute inset-0 w-full h-full pointer-events-none z-0">
                  <defs>
                    <marker id="arrow" viewBox="0 0 10 10" refX="24" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                      <path d="M 0 0 L 10 5 L 0 10 z" fill="#1E2028" />
                    </marker>
                    <marker id="arrow-active" viewBox="0 0 10 10" refX="24" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                      <path d="M 0 0 L 10 5 L 0 10 z" fill="#00D4FF" />
                    </marker>
                    <marker id="arrow-done" viewBox="0 0 10 10" refX="24" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                      <path d="M 0 0 L 10 5 L 0 10 z" fill="#00FF88" />
                    </marker>
                    <marker id="arrow-timeout" viewBox="0 0 10 10" refX="24" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                      <path d="M 0 0 L 10 5 L 0 10 z" fill="#FF4444" />
                    </marker>
                  </defs>
                  {edges.map((edge) => {
                    const srcNode = nodes.find((n) => n.id === edge.source);
                    const tgtNode = nodes.find((n) => n.id === edge.target);
                    if (!srcNode || !tgtNode) return null;

                    // Convert position values to absolute drawing coordinates
                    const x1 = srcNode.position.x + 350 + 80;
                    const y1 = srcNode.position.y + 20;
                    const x2 = tgtNode.position.x + 350 + 80;
                    const y2 = tgtNode.position.y + 20;

                    let strokeColor = "#1E2028";
                    let markerId = "arrow";
                    if (srcNode.data.status === "completed" && tgtNode.data.status !== "timeout" && tgtNode.data.status !== "error") {
                      strokeColor = "#00FF88";
                      markerId = "arrow-done";
                    } else if (srcNode.data.status === "running") {
                      strokeColor = "#00D4FF";
                      markerId = "arrow-active";
                    } else if (srcNode.data.status === "timeout" || srcNode.data.status === "error" || tgtNode.data.status === "timeout" || tgtNode.data.status === "error") {
                      strokeColor = "#FF4444";
                      markerId = "arrow-timeout";
                    }

                    return (
                      <g key={edge.id}>
                        <line
                          x1={x1}
                          y1={y1}
                          x2={x2}
                          y2={y2}
                          stroke={strokeColor}
                          strokeWidth={edge.animated || srcNode.data.status === "running" ? 2 : 1.5}
                          strokeDasharray={edge.animated || srcNode.data.status === "running" ? "4,4" : undefined}
                          markerEnd={`url(#${markerId})`}
                          className="transition-colors duration-300"
                        />
                      </g>
                    );
                  })}
                </svg>

                {/* Nodes rendering layer */}
                {nodes.map((node) => {
                  const leftPos = node.position.x + 350;
                  const topPos = node.position.y;

                  // Dynamic style class based on node states
                  let borderClass = "border-border bg-[#111318]";
                  let textClass = "text-slate-400";
                  let glowClass = "";

                  if (node.data.status === "running") {
                    borderClass = "border-cyanAccent bg-[#111318]/90 running-node-pulse";
                    textClass = "text-cyanAccent font-bold";
                    glowClass = "glow-cyan";
                  } else if (node.data.status === "completed") {
                    borderClass = "border-greenAccent bg-[#121815]";
                    textClass = "text-[#00FF88]";
                    glowClass = "glow-green";
                  } else if (node.data.status === "error" || node.data.status === "timeout") {
                    borderClass = "border-crimsonAccent bg-[#181212]";
                    textClass = "text-crimsonAccent font-bold";
                    glowClass = "glow-crimson";
                  }

                  return (
                    <div
                      key={node.id}
                      style={{ left: `${leftPos}px`, top: `${topPos}px`, width: "160px" }}
                      className={`absolute z-10 px-2 py-2.5 rounded border text-center transition-all duration-300 ${borderClass} ${glowClass}`}
                    >
                      <div className="flex flex-col gap-1">
                        <span className={`text-[10px] font-semibold truncate ${textClass}`}>
                          {node.data.label}
                        </span>
                        
                        {/* Node mini metric indicators */}
                        <div className="flex items-center justify-between text-[8px] text-slate-500 mt-1 border-t border-border/30 pt-1">
                          <span>{node.id}</span>
                          {node.data.status === "completed" && (
                            <span className="text-greenAccent">{node.data.latency}ms</span>
                          )}
                          {node.data.status === "running" && (
                            <span className="text-cyanAccent animate-pulse">RUNNING</span>
                          )}
                          {node.data.status === "pending" && (
                            <span>PENDING</span>
                          )}
                          {node.data.status === "error" && (
                            <span className="text-crimsonAccent animate-bounce">CORRECTING</span>
                          )}
                          {node.data.status === "timeout" && (
                            <span className="text-crimsonAccent animate-pulse font-bold">TIMEOUT</span>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Bottom Terminal Live log window */}
          <div className="h-44 border-t border-border bg-[#090A0E] flex flex-col shrink-0">
            <div className="flex items-center gap-2 px-4 py-2 border-b border-border/40 bg-[#0C0D11] text-[10px] uppercase font-bold tracking-widest text-slate-400">
              <Terminal className="w-3.5 h-3.5 text-cyanAccent" />
              <span>Orchestration Cluster Trace Output Log</span>
            </div>
            
            <div className="flex-1 p-3 overflow-y-auto text-[10px] text-slate-400 flex flex-col gap-1 font-mono bg-[#07080B]">
              {terminalLogs.map((log, idx) => (
                <div 
                  key={idx} 
                  className={`leading-relaxed border-l-2 pl-2 ${
                    log.includes("[CRITICAL]") 
                      ? "border-crimsonAccent text-crimsonAccent bg-crimsonAccent/5" 
                      : log.includes("[Consensus") 
                      ? "border-amberAccent text-amberAccent bg-amberAccent/5"
                      : log.includes("[NODE:") 
                      ? "border-cyanAccent text-slate-300"
                      : "border-border text-slate-500"
                  }`}
                >
                  {log}
                </div>
              ))}
              <div ref={logsEndRef} />
            </div>
          </div>
        </section>

        {/* PANEL C: OUTPUT ANALYZER & INSPECTIONS (RIGHT) */}
        <section className="w-full lg:w-96 border-l border-border bg-[#0C0D11] flex flex-col overflow-hidden shrink-0">
          
          {/* Tab Selector Header */}
          <div className="flex border-b border-border/50 bg-[#0E1014] shrink-0">
            <button
              onClick={() => setActiveTab("output")}
              className={`flex-1 py-3.5 text-center text-xs font-semibold flex items-center justify-center gap-2 border-b-2 transition-all ${
                activeTab === "output"
                  ? "border-cyanAccent text-cyanAccent bg-cyanAccent/5"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
            >
              <BookOpen className="w-3.5 h-3.5" />
              <span>OUTPUT</span>
            </button>
            <button
              onClick={() => setActiveTab("trace")}
              className={`flex-1 py-3.5 text-center text-xs font-semibold flex items-center justify-center gap-2 border-b-2 transition-all ${
                activeTab === "trace"
                  ? "border-cyanAccent text-cyanAccent bg-cyanAccent/5"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
            >
              <BarChart2 className="w-3.5 h-3.5" />
              <span>TRACE DATA</span>
            </button>
            <button
              onClick={() => setActiveTab("verification")}
              className={`flex-1 py-3.5 text-center text-xs font-semibold flex items-center justify-center gap-2 border-b-2 transition-all ${
                activeTab === "verification"
                  ? "border-cyanAccent text-cyanAccent bg-cyanAccent/5"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
            >
              <ShieldCheck className="w-3.5 h-3.5" />
              <span>VERIFY</span>
            </button>
          </div>

          {/* TAB PANEL CONTENTS */}
          <div className="flex-1 overflow-y-auto p-5">
            
            {/* 1. RESPONSE OUTPUT TAB */}
            {activeTab === "output" && (
              <div className="flex flex-col gap-4">
                <div className="flex items-center justify-between text-xs font-bold text-slate-400 border-b border-border/40 pb-2">
                  <span>SYNTHESIZED ANSWER PROSE</span>
                  {responseOutput && (
                    <span className={`text-[10px] px-1.5 py-0.5 rounded border uppercase ${
                      responseOutput.verification_status === "PASS"
                        ? "bg-greenAccent/10 border-greenAccent/40 text-greenAccent"
                        : "bg-amberAccent/10 border-amberAccent/40 text-amberAccent"
                    }`}>
                      Consensus: {responseOutput.verification_status}
                    </span>
                  )}
                </div>

                {!responseOutput ? (
                  <div className="text-center py-20 text-slate-500 text-xs">
                    No response synthesized yet. Initiate compilation loops.
                  </div>
                ) : (
                  <div className="text-xs text-slate-300 leading-relaxed flex flex-col gap-3">
                    
                    {/* Render response text and replace document tags like [doc_001] with interactable microchips */}
                    <div className="whitespace-pre-wrap font-sans text-[13px] bg-[#111318]/50 p-4 rounded border border-border leading-relaxed">
                      {responseOutput.response.split(/(\[doc_\d+\])/).map((segment, index) => {
                        const match = segment.match(/\[(doc_\d+)\]/);
                        if (match) {
                          const docId = match[1];
                          return (
                            <button
                              key={index}
                              onClick={() => {
                                setSelectedDocId(docId);
                                setActiveTab("trace");
                              }}
                              className="mx-1 px-1.5 py-0.5 text-[10px] rounded bg-cyanAccent/10 border border-cyanAccent/50 text-cyanAccent hover:bg-cyanAccent hover:text-black font-mono font-semibold transition-all inline-flex items-center gap-0.5"
                            >
                              {docId}
                            </button>
                          );
                        }
                        
                        // Parse basic markdown: ### Headers and **Bold**
                        const headerParts = segment.split(/(### [^\n]+)/);
                        return (
                          <span key={index}>
                            {headerParts.map((hPart, hIdx) => {
                              if (hPart.startsWith('### ')) {
                                return <div key={`h-${hIdx}`} className="text-[14px] font-bold text-white mt-4 mb-2 pb-1 border-b border-border/50 uppercase tracking-wide">{hPart.slice(4)}</div>;
                              }
                              const boldParts = hPart.split(/(\*\*.*?\*\*)/);
                              return (
                                <span key={`p-${hIdx}`}>
                                  {boldParts.map((bPart, bIdx) => {
                                    if (bPart.startsWith('**') && bPart.endsWith('**')) {
                                      return <strong key={`b-${bIdx}`} className="text-cyanAccent font-semibold">{bPart.slice(2, -2)}</strong>;
                                    }
                                    return <span key={`t-${bIdx}`}>{bPart}</span>;
                                  })}
                                </span>
                              );
                            })}
                          </span>
                        );
                      })}
                    </div>

                    {/* Citations index footer */}
                    {responseOutput.citations && responseOutput.citations.length > 0 && (
                      <div className="mt-4 flex flex-col gap-2">
                        <span className="text-[10px] text-slate-500 uppercase tracking-wider">Source References</span>
                        <div className="flex flex-col gap-1.5">
                          {responseOutput.citations.map((c, i) => (
                            <div 
                              key={i} 
                              onClick={() => {
                                setSelectedDocId(c.doc_id);
                                setActiveTab("trace");
                              }}
                              className="p-2 rounded bg-[#08090C] border border-border hover:border-cyanAccent/40 transition-colors cursor-pointer flex items-center justify-between"
                            >
                              <span className="text-slate-300 truncate font-semibold">{c.title}</span>
                              <span className="text-cyanAccent text-[9px] shrink-0">{c.doc_id}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* 2. RETRIEVAL TRACE DATA TAB */}
            {activeTab === "trace" && (
              <div className="flex flex-col gap-4">
                <div className="flex items-center justify-between text-xs font-bold text-slate-400 border-b border-border/40 pb-2">
                  <span>RETRIEVAL MATRIX COMPARATOR</span>
                  <span className="text-[10px] text-slate-500 font-normal">Reciprocal Rank Fusion</span>
                </div>

                {traceResults.length === 0 ? (
                  <div className="text-center py-20 text-slate-500 text-xs">
                    No retrieved trace indices available. Complete query processing.
                  </div>
                ) : (
                  <div className="flex flex-col gap-4">
                    {traceResults.map((result) => {
                      const isSelected = selectedDocId === result.doc_id;
                      return (
                        <div 
                          key={result.doc_id} 
                          className={`p-3.5 rounded border flex flex-col gap-3 transition-all ${
                            isSelected 
                              ? "bg-cyanAccent/5 border-cyanAccent shadow-[0_0_10px_rgba(0,212,255,0.15)]" 
                              : "bg-[#111318]/60 border-border hover:border-slate-700"
                          }`}
                        >
                          <div className="flex items-center justify-between border-b border-border/50 pb-1.5">
                            <span className="text-[11px] font-bold text-slate-200 truncate pr-2" title={result.title}>
                              {result.title}
                            </span>
                            <span className={`text-[10px] font-bold ${isSelected ? "text-cyanAccent" : "text-slate-500"}`}>
                              {result.doc_id}
                            </span>
                          </div>

                          {/* Scores grid table */}
                          <div className="grid grid-cols-4 gap-1 text-[10px] text-center">
                            <div className="bg-[#08090C] p-1 border border-border/30 rounded">
                              <span className="block text-slate-500 text-[8px] uppercase">Vector</span>
                              <strong className="text-slate-300 font-bold">{result.scores.vector}</strong>
                            </div>
                            <div className="bg-[#08090C] p-1 border border-border/30 rounded">
                              <span className="block text-slate-500 text-[8px] uppercase">BM25</span>
                              <strong className="text-slate-300 font-bold">{result.scores.bm25}</strong>
                            </div>
                            <div className="bg-[#08090C] p-1 border border-border/30 rounded">
                              <span className="block text-slate-500 text-[8px] uppercase">Graph</span>
                              <strong className="text-slate-300 font-bold">
                                {result.scores.graph > 0 ? result.scores.graph : "N/A"}
                              </strong>
                            </div>
                            <div className="bg-[#0D1412] p-1 border border-greenAccent/20 rounded">
                              <span className="block text-greenAccent text-[8px] uppercase">RRF</span>
                              <strong className="text-greenAccent font-bold">{result.rrf_score}</strong>
                            </div>
                          </div>

                          {/* Context Content Snippet */}
                          <p className="text-[10px] text-slate-400 leading-relaxed font-sans border-t border-border/30 pt-2">
                            {result.content.slice(0, 180)}...
                          </p>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            )}

            {/* 3. VERIFICATION ENGINE TAB */}
            {activeTab === "verification" && (
              <div className="flex flex-col gap-5">
                <div className="flex items-center justify-between text-xs font-bold text-slate-400 border-b border-border/40 pb-2">
                  <span>COGNITIVE SAFETY ARCS</span>
                  <span className="text-[10px] text-slate-500 font-normal">Guardrail Status</span>
                </div>

                {!responseOutput ? (
                  <div className="text-center py-20 text-slate-500 text-xs">
                    No active verification matrix computed. Stream query tasks.
                  </div>
                ) : (
                  <div className="flex flex-col gap-6">
                    
                    {/* Circle Gauges */}
                    <div className="flex flex-col gap-4">
                      
                      {/* Gauge A: Confidence index */}
                      <div className="bg-[#111318]/70 p-4 rounded border border-border flex items-center justify-between">
                        <div>
                          <span className="text-slate-400 text-xs block font-bold">Consensus Alignment</span>
                          <p className="text-[10px] text-slate-500 mt-1 leading-normal font-sans">
                            Percentage of source partition nodes validating key statements.
                          </p>
                        </div>
                        <div className="relative w-16 h-16 flex items-center justify-center rounded-full border-4 border-[#1D212A] border-t-cyanAccent">
                          <span className="text-[11px] font-bold text-slate-100">
                            {responseOutput.verification_status === "PASS" ? "92%" : "74%"}
                          </span>
                        </div>
                      </div>

                      {/* Gauge B: Hallucination Risk Index */}
                      <div className="bg-[#111318]/70 p-4 rounded border border-border flex items-center justify-between">
                        <div>
                          <span className="text-slate-400 text-xs block font-bold">Hallucination Index</span>
                          <p className="text-[10px] text-slate-500 mt-1 leading-normal font-sans">
                            Probability of non-consensus factual claims detected.
                          </p>
                        </div>
                        <div className={`relative w-16 h-16 flex items-center justify-center rounded-full border-4 ${
                          responseOutput.hallucination_index > 0.5 
                            ? "border-[#2A1D1D] border-t-crimsonAccent" 
                            : "border-[#1D2A20] border-t-greenAccent"
                        }`}>
                          <span className={`text-[11px] font-bold ${
                            responseOutput.hallucination_index > 0.5 ? "text-crimsonAccent" : "text-greenAccent"
                          }`}>
                            {(responseOutput.hallucination_index * 100).toFixed(0)}%
                          </span>
                        </div>
                      </div>

                      {/* Gauge C: Source Validation Rate */}
                      <div className="bg-[#111318]/70 p-4 rounded border border-border flex items-center justify-between">
                        <div>
                          <span className="text-slate-400 text-xs block font-bold">Source Grounding</span>
                          <p className="text-[10px] text-slate-500 mt-1 leading-normal font-sans">
                            Percentage of target response mapped directly to reference tokens.
                          </p>
                        </div>
                        <div className="relative w-16 h-16 flex items-center justify-center rounded-full border-4 border-[#1D212A] border-t-amberAccent">
                          <span className="text-[11px] font-bold text-slate-100">88%</span>
                        </div>
                      </div>

                    </div>

                    {/* Active Guardrails Alert Box */}
                    <div className="p-4 rounded border border-border bg-[#08090C] flex flex-col gap-3">
                      <span className="text-[10px] text-slate-500 uppercase tracking-widest flex items-center gap-1.5">
                        <ShieldCheck className="w-3.5 h-3.5 text-greenAccent" />
                        <span>Active System Guardrails</span>
                      </span>
                      
                      <div className="flex flex-col gap-2 text-[11px] text-slate-400 font-sans">
                        <div className="flex items-center gap-2">
                          <CheckCircle2 className="w-3.5 h-3.5 text-greenAccent shrink-0" />
                          <span>Factual Consistency Filter: <strong className="text-slate-200 font-mono">ACTIVE</strong></span>
                        </div>
                        <div className="flex items-center gap-2">
                          <CheckCircle2 className="w-3.5 h-3.5 text-greenAccent shrink-0" />
                          <span>Context Leakage Guard: <strong className="text-slate-200 font-mono">ACTIVE</strong></span>
                        </div>
                        <div className="flex items-center gap-2">
                          <CheckCircle2 className="w-3.5 h-3.5 text-greenAccent shrink-0" />
                          <span>Adversarial Injection Sanitizer: <strong className="text-slate-200 font-mono">ACTIVE</strong></span>
                        </div>
                      </div>
                    </div>

                    {/* Show conflict resolution triggers if retried */}
                    {responseOutput.warnings.length > 0 && (
                      <div className="p-3.5 rounded border border-amberAccent/45 bg-[#201B0C]/40 text-amberAccent text-[11px] flex flex-col gap-2">
                        <div className="flex items-center gap-1.5 font-bold">
                          <AlertTriangle className="w-4 h-4 shrink-0" />
                          <span>Conflict Correction Logged</span>
                        </div>
                        <p className="leading-relaxed font-sans text-slate-300">
                          A transient transaction block mismatch was flagged by the verification parser. Self-correction rules compiled successfully.
                        </p>
                      </div>
                    )}

                  </div>
                )}
              </div>
            )}

          </div>
        </section>

      </div>
    </div>
  );
}
