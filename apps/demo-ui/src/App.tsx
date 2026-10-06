import React, { useState, useEffect } from 'react';
import { io, Socket } from 'socket.io-client';
import {
  Activity,
  CheckCircle2,
  XCircle,
  Clock,
  AlertTriangle,
  Server,
  Mail,
  Layers,
  RefreshCw,
  Send,
  Zap,
} from 'lucide-react';

interface ActivationStepInfo {
  status: 'OK' | 'FAILED';
  at: string;
  details?: any;
}

interface ActivationHistoryItem {
  eventType: string;
  at: string;
  details?: any;
}

interface Activation {
  id: string;
  customerId: string;
  planId: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
  simulateFailure: string;
  steps: {
    billing?: ActivationStepInfo;
    provisioning?: ActivationStepInfo;
  };
  history: ActivationHistoryItem[];
  createdAt: string;
  updatedAt: string;
}

const API_BASE = 'http://localhost:3000';

export default function App() {
  const [activations, setActivations] = useState<Activation[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [customerId, setCustomerId] = useState<string>('C-1234');
  const [planId, setPlanId] = useState<string>('FLOW-FULL');
  const [simulateFailure, setSimulateFailure] = useState<'none' | 'billing' | 'provisioning'>('none');
  const [loading, setLoading] = useState<boolean>(false);
  const [wsConnected, setWsConnected] = useState<boolean>(false);

  // Fetch initial list
  const loadActivations = async () => {
    try {
      const res = await fetch(`${API_BASE}/activations`);
      if (res.ok) {
        const data = await res.json();
        setActivations(data);
        if (data.length > 0 && !selectedId) {
          setSelectedId(data[0].id);
        }
      }
    } catch (err) {
      console.error('Failed to load activations', err);
    }
  };

  useEffect(() => {
    loadActivations();

    // Setup Socket.IO connection
    const socket: Socket = io(API_BASE, {
      transports: ['websocket', 'polling'],
    });

    socket.on('connect', () => {
      setWsConnected(true);
    });

    socket.on('disconnect', () => {
      setWsConnected(false);
    });

    socket.on('activation:created', (newAct: Activation) => {
      setActivations((prev) => [newAct, ...prev.filter((a) => a.id !== newAct.id)]);
      setSelectedId(newAct.id);
    });

    socket.on('activation:update', (updatedAct: Activation) => {
      setActivations((prev) => {
        const index = prev.findIndex((a) => a.id === updatedAct.id);
        if (index >= 0) {
          const next = [...prev];
          next[index] = updatedAct;
          return next;
        }
        return [updatedAct, ...prev];
      });
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  const handleContractPlan = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    try {
      const res = await fetch(`${API_BASE}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerId,
          planId,
          simulateFailure,
        }),
      });

      if (res.status === 202) {
        const data = await res.json();
        setSelectedId(data.activationId);
        // Randomize customerId for next test convenience
        setCustomerId(`C-${Math.floor(1000 + Math.random() * 9000)}`);
      }
    } catch (err) {
      console.error('Error submitting activation:', err);
    } finally {
      setLoading(false);
    }
  };

  const selectedActivation = activations.find((a) => a.id === selectedId);

  const getStatusBadge = (status: Activation['status']) => {
    switch (status) {
      case 'PENDING':
        return (
          <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-yellow-950/60 text-yellow-400 border border-yellow-800/60">
            <Clock className="w-3.5 h-3.5 animate-spin" /> PENDING
          </span>
        );
      case 'IN_PROGRESS':
        return (
          <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-blue-950/60 text-blue-400 border border-blue-800/60">
            <RefreshCw className="w-3.5 h-3.5 animate-spin" /> IN PROGRESS
          </span>
        );
      case 'ACTIVE':
        return (
          <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-950/60 text-emerald-400 border border-emerald-800/60">
            <CheckCircle2 className="w-3.5 h-3.5" /> ACTIVE
          </span>
        );
      case 'FAILED':
        return (
          <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-rose-950/60 text-rose-400 border border-rose-800/60">
            <XCircle className="w-3.5 h-3.5" /> FAILED
          </span>
        );
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
      {/* Top Navbar */}
      <header className="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-50 px-6 py-3.5 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-indigo-600 rounded-lg shadow-md shadow-indigo-500/20">
            <Zap className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-base font-bold tracking-tight text-white flex items-center gap-2">
              POC Activación de Servicios
              <span className="text-xs px-2 py-0.5 rounded bg-indigo-900/50 text-indigo-300 font-mono border border-indigo-700/50">
                Kafka + PostgreSQL
              </span>
            </h1>
            <p className="text-xs text-slate-400">Arquitectura Orientada a Eventos · UADER 2026</p>
          </div>
        </div>

        <div className="flex items-center gap-4 text-xs">
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-slate-800 border border-slate-700">
            <span
              className={`w-2.5 h-2.5 rounded-full ${
                wsConnected ? 'bg-emerald-400 animate-pulse' : 'bg-rose-500'
              }`}
            />
            <span className="text-slate-300">
              WebSocket: {wsConnected ? 'En vivo' : 'Desconectado'}
            </span>
          </div>

          <a
            href="http://localhost:8080"
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition border border-slate-700"
          >
            <Server className="w-3.5 h-3.5 text-orange-400" />
            Kafka UI
          </a>

          <a
            href="http://localhost:8025"
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition border border-slate-700"
          >
            <Mail className="w-3.5 h-3.5 text-blue-400" />
            Mailhog
          </a>
        </div>
      </header>

      {/* Main Grid Content */}
      <main className="flex-1 max-w-7xl w-full mx-auto p-6 grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Left Column: Form & Activation Selector (5 cols) */}
        <section className="lg:col-span-5 flex flex-col gap-6">
          {/* Form Card (RF-01) */}
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl">
            <div className="flex items-center gap-2 mb-4 pb-3 border-b border-slate-800">
              <Layers className="w-5 h-5 text-indigo-400" />
              <h2 className="text-sm font-semibold tracking-wide uppercase text-slate-200">
                1. Contratar Plan (RF-01)
              </h2>
            </div>

            <form onSubmit={handleContractPlan} className="space-y-4">
              <div>
                <label className="block text-xs font-medium text-slate-300 mb-1">
                  Cliente ID (Partición por clave)
                </label>
                <input
                  type="text"
                  value={customerId}
                  onChange={(e) => setCustomerId(e.target.value)}
                  required
                  placeholder="ej. C-1234"
                  className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-indigo-500"
                />
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-300 mb-1">Plan</label>
                <select
                  value={planId}
                  onChange={(e) => setPlanId(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-indigo-500"
                >
                  <option value="FLOW-FULL">FLOW FULL (TV + 300 Megas)</option>
                  <option value="FIBRA-500M">Fibra Óptica 500M</option>
                  <option value="GIGA-1000">Giga Simétrico 1000M</option>
                </select>
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-300 mb-1">
                  Simulación de Fallo Forzado
                </label>
                <div className="grid grid-cols-3 gap-2">
                  <button
                    type="button"
                    onClick={() => setSimulateFailure('none')}
                    className={`py-2 px-2 text-xs font-medium rounded-lg border transition ${
                      simulateFailure === 'none'
                        ? 'bg-emerald-950/60 border-emerald-600 text-emerald-300 shadow-sm'
                        : 'bg-slate-950 border-slate-800 text-slate-400 hover:border-slate-700'
                    }`}
                  >
                    Ninguno (OK)
                  </button>
                  <button
                    type="button"
                    onClick={() => setSimulateFailure('billing')}
                    className={`py-2 px-2 text-xs font-medium rounded-lg border transition ${
                      simulateFailure === 'billing'
                        ? 'bg-rose-950/60 border-rose-600 text-rose-300 shadow-sm'
                        : 'bg-slate-950 border-slate-800 text-slate-400 hover:border-slate-700'
                    }`}
                  >
                    Fallo Billing
                  </button>
                  <button
                    type="button"
                    onClick={() => setSimulateFailure('provisioning')}
                    className={`py-2 px-2 text-xs font-medium rounded-lg border transition ${
                      simulateFailure === 'provisioning'
                        ? 'bg-rose-950/60 border-rose-600 text-rose-300 shadow-sm'
                        : 'bg-slate-950 border-slate-800 text-slate-400 hover:border-slate-700'
                    }`}
                  >
                    Fallo Provisioning
                  </button>
                </div>
              </div>

              <button
                type="submit"
                disabled={loading}
                className="w-full flex items-center justify-center gap-2 py-2.5 px-4 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white font-semibold text-sm transition shadow-lg shadow-indigo-600/30 disabled:opacity-50"
              >
                {loading ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <Send className="w-4 h-4" />
                )}
                Publicar Activación (POST /activations)
              </button>
            </form>
          </div>

          {/* Activations List */}
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl flex-1 flex flex-col">
            <div className="flex items-center justify-between mb-3 pb-2 border-b border-slate-800">
              <h2 className="text-sm font-semibold tracking-wide uppercase text-slate-200">
                Activaciones ({activations.length})
              </h2>
              <button
                onClick={loadActivations}
                className="text-xs text-indigo-400 hover:text-indigo-300 flex items-center gap-1"
              >
                <RefreshCw className="w-3 h-3" /> Actualizar
              </button>
            </div>

            <div className="overflow-y-auto max-h-[340px] space-y-2 pr-1">
              {activations.length === 0 ? (
                <p className="text-xs text-slate-500 text-center py-6">
                  No hay activaciones aún. Crea una arriba.
                </p>
              ) : (
                activations.map((item) => (
                  <button
                    key={item.id}
                    onClick={() => setSelectedId(item.id)}
                    className={`w-full text-left p-3 rounded-lg border transition flex items-center justify-between ${
                      item.id === selectedId
                        ? 'bg-indigo-950/50 border-indigo-600'
                        : 'bg-slate-950/60 border-slate-800 hover:border-slate-700'
                    }`}
                  >
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-xs font-bold text-white">
                          {item.id}
                        </span>
                        <span className="text-xs text-slate-400">({item.customerId})</span>
                      </div>
                      <p className="text-xs text-slate-500 mt-0.5">{item.planId}</p>
                    </div>
                    <div>{getStatusBadge(item.status)}</div>
                  </button>
                ))
              )}
            </div>
          </div>
        </section>

        {/* Right Column: Live Event Timeline (RF-08) (7 cols) */}
        <section className="lg:col-span-7 bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl flex flex-col">
          <div className="flex items-center justify-between pb-4 border-b border-slate-800 mb-6">
            <div className="flex items-center gap-2.5">
              <Activity className="w-5 h-5 text-indigo-400" />
              <h2 className="text-sm font-semibold tracking-wide uppercase text-slate-200">
                Línea de Tiempo en Vivo (RF-08)
              </h2>
            </div>
            {selectedActivation && (
              <div className="flex items-center gap-2">
                <span className="text-xs text-slate-400">ID:</span>
                <span className="font-mono text-xs font-bold text-indigo-300 bg-indigo-950/60 px-2 py-0.5 rounded border border-indigo-800/50">
                  {selectedActivation.id}
                </span>
                {getStatusBadge(selectedActivation.status)}
              </div>
            )}
          </div>

          {!selectedActivation ? (
            <div className="flex-1 flex flex-col items-center justify-center text-slate-500">
              <Activity className="w-12 h-12 mb-3 stroke-1 text-slate-600" />
              <p className="text-sm">Selecciona o crea una activación para ver su línea de tiempo.</p>
            </div>
          ) : (
            <div className="flex flex-col gap-6 flex-1">
              {/* Service Steps Summary Bar */}
              <div className="grid grid-cols-2 gap-4 bg-slate-950/80 p-4 rounded-xl border border-slate-800/80">
                {/* Billing Step */}
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-slate-800 border border-slate-700">
                    {selectedActivation.steps?.billing?.status === 'OK' ? (
                      <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                    ) : selectedActivation.steps?.billing?.status === 'FAILED' ? (
                      <XCircle className="w-5 h-5 text-rose-400" />
                    ) : (
                      <Clock className="w-5 h-5 text-yellow-400 animate-spin" />
                    )}
                  </div>
                  <div>
                    <h4 className="text-xs font-medium text-slate-300">billing-service</h4>
                    <p className="text-xs text-slate-500 font-mono">
                      {selectedActivation.steps?.billing?.status || 'Esperando evento...'}
                    </p>
                  </div>
                </div>

                {/* Provisioning Step */}
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-slate-800 border border-slate-700">
                    {selectedActivation.steps?.provisioning?.status === 'OK' ? (
                      <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                    ) : selectedActivation.steps?.provisioning?.status === 'FAILED' ? (
                      <XCircle className="w-5 h-5 text-rose-400" />
                    ) : (
                      <Clock className="w-5 h-5 text-yellow-400 animate-spin" />
                    )}
                  </div>
                  <div>
                    <h4 className="text-xs font-medium text-slate-300">provisioning-service</h4>
                    <p className="text-xs text-slate-500 font-mono">
                      {selectedActivation.steps?.provisioning?.status || 'Esperando evento...'}
                    </p>
                  </div>
                </div>
              </div>

              {/* Timeline Items */}
              <div className="flex-1 overflow-y-auto space-y-4 relative pl-6 before:content-[''] before:absolute before:left-2.5 before:top-2 before:bottom-2 before:w-0.5 before:bg-slate-800">
                {selectedActivation.history?.map((item, idx) => {
                  const isSuccess =
                    item.eventType === 'ActivationCompleted' ||
                    item.eventType === 'BillingAccountCreated' ||
                    item.eventType === 'ProvisioningCompleted';
                  const isFail =
                    item.eventType === 'ActivationFailed' ||
                    item.eventType === 'BillingFailed' ||
                    item.eventType === 'ProvisioningFailed' ||
                    item.eventType === 'BillingAccountCancelled';

                  return (
                    <div key={idx} className="relative group">
                      {/* Node Bullet */}
                      <span
                        className={`absolute -left-6 top-1.5 w-3 h-3 rounded-full border-2 bg-slate-950 ${
                          isSuccess
                            ? 'border-emerald-400 bg-emerald-400/20'
                            : isFail
                            ? 'border-rose-400 bg-rose-400/20'
                            : 'border-indigo-400 bg-indigo-400/20'
                        }`}
                      />

                      <div className="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 hover:border-slate-700 transition">
                        <div className="flex items-center justify-between mb-1">
                          <span
                            className={`font-mono text-xs font-semibold ${
                              isSuccess
                                ? 'text-emerald-400'
                                : isFail
                                ? 'text-rose-400'
                                : 'text-indigo-400'
                            }`}
                          >
                            {item.eventType}
                          </span>
                          <span className="text-[11px] text-slate-500 font-mono">
                            {new Date(item.at).toLocaleTimeString()}
                          </span>
                        </div>

                        {item.details && (
                          <pre className="text-[11px] font-mono text-slate-400 bg-slate-900/90 p-2 rounded mt-2 overflow-x-auto border border-slate-800">
                            {JSON.stringify(item.details, null, 2)}
                          </pre>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}
