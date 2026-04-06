import React, { useMemo, useState } from 'react';
import { motion } from 'framer-motion';
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Building2,
  ChevronDown,
  ClipboardList,
  Clock3,
  Download,
  FileText,
  Filter,
  FolderKanban,
  LayoutGrid,
  MapPinned,
  Sparkles,
  TrendingDown,
  TrendingUp,
  TriangleAlert,
  Users,
} from 'lucide-react';

const sidebarGroups = [
  {
    label: 'Operacional',
    items: [
      { icon: LayoutGrid, label: 'Dashboard' },
      { icon: ClipboardList, label: 'Entradas' },
      { icon: Clock3, label: 'Histórico' },
      { icon: Download, label: 'Resultados' },
    ],
  },
  {
    label: 'Análise',
    items: [
      { icon: BarChart3, label: 'Painel Gerencial', active: true },
      { icon: FileText, label: 'Institucional' },
      { icon: Activity, label: 'Ocorrências' },
    ],
  },
  {
    label: 'Cadastros',
    items: [
      { icon: FolderKanban, label: 'Contratos' },
      { icon: Building2, label: 'Núcleos' },
      { icon: MapPinned, label: 'Localidades' },
      { icon: Users, label: 'Clientes' },
    ],
  },
];

const trendData = [118, 146, 133, 182, 171, 190, 214, 208, 223, 236, 221, 244, 258, 249];
const compareBars = [62, 74, 88, 94, 71, 97, 83];
const compareBarsPrev = [54, 69, 76, 82, 66, 74, 77];

const services = [
  { name: 'Prolongamento de Rede de Água', code: 'rede_agua', value: 160, color: 'from-blue-400 to-blue-300' },
  { name: 'Instalação de hidrômetros', code: 'hidrometro', value: 84, color: 'from-sky-400 to-cyan-300' },
  { name: 'Ligações intradomiciliares', code: 'intradomiciliar', value: 61, color: 'from-indigo-300 to-blue-200' },
  { name: 'Execução de interligação', code: 'interligacao', value: 42, color: 'from-cyan-300 to-teal-200' },
  { name: 'Instalação de caixas UMA', code: 'caixa_uma', value: 19, color: 'from-slate-300 to-slate-100' },
];

const nuclei = [
  { name: 'Mississipi', value: 39, delta: '+12%' },
  { name: 'Savoy', value: 31, delta: '+8%' },
  { name: 'Oeste 1', value: 24, delta: '+6%' },
  { name: 'Leste 3', value: 17, delta: '-2%' },
  { name: 'Centro Expandido', value: 12, delta: '+4%' },
];

const occurrences = [
  { name: 'Mensagem com 2 núcleos', value: 4 },
  { name: 'Serviço não mapeado', value: 2 },
  { name: 'Equipe não padronizada', value: 2 },
  { name: 'Município ausente', value: 1 },
];

const executiveCards = [
  {
    label: 'Volume consolidado',
    value: '181',
    delta: '+18%',
    helper: 'vs. período anterior',
    tone: 'blue',
    icon: TrendingUp,
  },
  {
    label: 'Itens com alerta',
    value: '9',
    delta: '-12%',
    helper: 'queda na recorrência',
    tone: 'amber',
    icon: TriangleAlert,
  },
  {
    label: 'Qualidade de mapeamento',
    value: '100%',
    delta: '+4pp',
    helper: 'classificação concluída',
    tone: 'emerald',
    icon: Sparkles,
  },
  {
    label: 'Núcleos ativos',
    value: '7',
    delta: '+1',
    helper: 'base em produção',
    tone: 'violet',
    icon: Building2,
  },
];

function SidebarItem({ icon: Icon, label, active }) {
  return (
    <button
      className={`flex w-full items-center gap-3 rounded-2xl px-4 py-3 text-left transition-all ${
        active
          ? 'border border-blue-400/35 bg-[linear-gradient(180deg,rgba(34,99,191,0.28),rgba(24,39,74,0.32))] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]'
          : 'text-slate-300 hover:bg-white/5 hover:text-white'
      }`}
    >
      <Icon size={17} className={active ? 'text-blue-300' : 'text-slate-400'} />
      <span className="text-[15px] font-medium">{label}</span>
    </button>
  );
}

function ShellCard({ title, subtitle, action, children, className = '' }) {
  return (
    <section
      className={`rounded-[30px] border border-white/10 bg-[linear-gradient(180deg,rgba(14,20,33,0.94),rgba(8,12,22,0.98))] p-6 shadow-[0_16px_48px_rgba(0,0,0,0.28)] ${className}`}
    >
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-[28px] font-semibold tracking-[-0.03em] text-white">{title}</h3>
          {subtitle ? <p className="mt-1 text-sm text-slate-400">{subtitle}</p> : null}
        </div>
        {action ? <div>{action}</div> : null}
      </div>
      <div className="mt-6">{children}</div>
    </section>
  );
}

function ExecutiveKpi({ item }) {
  const Icon = item.icon;
  const toneMap = {
    blue: 'from-blue-500/20 to-blue-500/0 text-blue-200 border-blue-400/20',
    amber: 'from-amber-500/18 to-amber-500/0 text-amber-200 border-amber-400/20',
    emerald: 'from-emerald-500/18 to-emerald-500/0 text-emerald-200 border-emerald-400/20',
    violet: 'from-violet-500/18 to-violet-500/0 text-violet-200 border-violet-400/20',
  };

  return (
    <div className={`rounded-[28px] border bg-[linear-gradient(180deg,rgba(12,18,30,0.96),rgba(7,12,22,0.98))] p-5 shadow-[0_16px_40px_rgba(0,0,0,0.24)] ${toneMap[item.tone]}`}>
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-[12px] font-semibold uppercase tracking-[0.24em] text-slate-400">{item.label}</div>
          <div className="mt-3 text-5xl font-semibold tracking-[-0.05em] text-white">{item.value}</div>
        </div>
        <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-white/10 bg-white/5">
          <Icon size={18} />
        </div>
      </div>
      <div className="mt-5 flex items-center justify-between gap-3">
        <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1.5 text-xs font-semibold text-white">
          {item.delta}
        </span>
        <span className="text-sm text-slate-400">{item.helper}</span>
      </div>
    </div>
  );
}

function AreaTrendChart({ values }) {
  const width = 760;
  const height = 220;
  const padding = 18;
  const max = Math.max(...values) * 1.08;
  const min = Math.min(...values) * 0.9;

  const points = values
    .map((value, index) => {
      const x = padding + (index / (values.length - 1)) * (width - padding * 2);
      const y = height - padding - ((value - min) / (max - min || 1)) * (height - padding * 2);
      return `${x},${y}`;
    })
    .join(' ');

  const linePoints = values.map((value, index) => {
    const x = padding + (index / (values.length - 1)) * (width - padding * 2);
    const y = height - padding - ((value - min) / (max - min || 1)) * (height - padding * 2);
    return { x, y, value };
  });

  const areaPath = `${points} ${width - padding},${height - padding} ${padding},${height - padding}`;

  return (
    <div className="rounded-[28px] border border-white/8 bg-[linear-gradient(180deg,rgba(6,14,26,0.96),rgba(8,15,27,0.98))] p-5">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[18px] font-semibold text-white">Tendência de volume consolidado</div>
          <div className="mt-1 text-sm text-slate-400">Evolução diária do recorte atual com leitura mais compacta e premium.</div>
        </div>
        <div className="rounded-full border border-blue-400/20 bg-blue-500/10 px-3 py-1 text-xs font-semibold text-blue-200">+18% no recorte</div>
      </div>
      <div className="mt-5 overflow-hidden rounded-[24px] border border-white/6 bg-[linear-gradient(180deg,rgba(255,255,255,0.02),rgba(255,255,255,0.01))] p-4">
        <svg viewBox={`0 0 ${width} ${height}`} className="h-[220px] w-full">
          <defs>
            <linearGradient id="areaFill" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgba(59,130,246,0.42)" />
              <stop offset="100%" stopColor="rgba(59,130,246,0.02)" />
            </linearGradient>
            <linearGradient id="lineStroke" x1="0" x2="1" y1="0" y2="0">
              <stop offset="0%" stopColor="#38bdf8" />
              <stop offset="100%" stopColor="#3b82f6" />
            </linearGradient>
          </defs>

          {[0, 1, 2, 3].map((g) => (
            <line
              key={g}
              x1={padding}
              x2={width - padding}
              y1={padding + g * ((height - padding * 2) / 3)}
              y2={padding + g * ((height - padding * 2) / 3)}
              stroke="rgba(148,163,184,0.14)"
              strokeDasharray="4 10"
            />
          ))}

          <polygon points={areaPath} fill="url(#areaFill)" />
          <polyline points={points} fill="none" stroke="url(#lineStroke)" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />

          {linePoints.map((point, index) => (
            <g key={index}>
              <circle cx={point.x} cy={point.y} r="4.5" fill="#0b1220" stroke="#60a5fa" strokeWidth="2.5" />
            </g>
          ))}
        </svg>

        <div className="mt-3 grid grid-cols-7 gap-2 text-xs font-medium text-slate-500">
          {['08/03', '09/03', '10/03', '11/03', '12/03', '13/03', '14/03'].map((day) => (
            <div key={day}>{day}</div>
          ))}
        </div>
      </div>
    </div>
  );
}

function CompareBarsCard() {
  const bars = compareBars.map((value, index) => ({
    current: value,
    previous: compareBarsPrev[index],
    label: ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'][index],
  }));
  const max = Math.max(...compareBars, ...compareBarsPrev);

  return (
    <div className="rounded-[28px] border border-white/8 bg-[linear-gradient(180deg,rgba(6,14,26,0.96),rgba(8,15,27,0.98))] p-5">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[18px] font-semibold text-white">Comparativo com período anterior</div>
          <div className="mt-1 text-sm text-slate-400">Leitura rápida da variação de execuções por dia no recorte.</div>
        </div>
        <div className="flex items-center gap-3 text-xs font-semibold">
          <span className="flex items-center gap-2 text-blue-200"><span className="h-2.5 w-2.5 rounded-full bg-blue-400" />Atual</span>
          <span className="flex items-center gap-2 text-slate-300"><span className="h-2.5 w-2.5 rounded-full bg-slate-500" />Anterior</span>
        </div>
      </div>

      <div className="mt-6 flex h-[266px] items-end gap-4 rounded-[24px] border border-white/6 bg-[linear-gradient(180deg,rgba(255,255,255,0.02),rgba(255,255,255,0.01))] px-5 pb-5 pt-6">
        {bars.map((bar) => (
          <div key={bar.label} className="flex flex-1 flex-col items-center gap-3">
            <div className="flex h-[180px] items-end gap-2">
              <div className="w-4 rounded-full bg-slate-600/80" style={{ height: `${(bar.previous / max) * 180}px` }} />
              <div className="w-4 rounded-full bg-[linear-gradient(180deg,#60a5fa,#2563eb)] shadow-[0_8px_18px_rgba(37,99,235,0.35)]" style={{ height: `${(bar.current / max) * 180}px` }} />
            </div>
            <div className="text-xs font-semibold text-slate-400">{bar.label}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function DonutQualityCard() {
  const percent = 100;
  const circumference = 2 * Math.PI * 60;
  const offset = circumference - (percent / 100) * circumference;

  return (
    <div className="rounded-[28px] border border-white/8 bg-[linear-gradient(180deg,rgba(8,16,28,0.98),rgba(7,12,22,0.98))] p-5">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[18px] font-semibold text-white">Mapeado x não mapeado</div>
          <div className="mt-1 text-sm text-slate-400">Visual mais compacto para qualidade de classificação.</div>
        </div>
        <div className="rounded-full border border-emerald-400/20 bg-emerald-500/10 px-3 py-1 text-xs font-semibold text-emerald-200">14 itens mapeados</div>
      </div>

      <div className="mt-6 grid gap-5 lg:grid-cols-[0.95fr_1.05fr] lg:items-center">
        <div className="flex items-center justify-center">
          <div className="relative flex h-[220px] w-[220px] items-center justify-center rounded-full bg-[radial-gradient(circle_at_center,rgba(15,23,42,0.85),rgba(2,6,23,0.95))] shadow-[inset_0_0_24px_rgba(255,255,255,0.03)]">
            <svg viewBox="0 0 160 160" className="h-[180px] w-[180px] -rotate-90">
              <circle cx="80" cy="80" r="60" fill="none" stroke="rgba(148,163,184,0.16)" strokeWidth="16" />
              <circle
                cx="80"
                cy="80"
                r="60"
                fill="none"
                stroke="url(#donutStroke)"
                strokeWidth="16"
                strokeLinecap="round"
                strokeDasharray={circumference}
                strokeDashoffset={offset}
              />
              <defs>
                <linearGradient id="donutStroke" x1="0" x2="1" y1="0" y2="1">
                  <stop offset="0%" stopColor="#34d399" />
                  <stop offset="100%" stopColor="#22d3ee" />
                </linearGradient>
              </defs>
            </svg>
            <div className="absolute inset-0 flex flex-col items-center justify-center">
              <div className="text-[42px] font-semibold tracking-[-0.05em] text-white">100%</div>
              <div className="mt-1 text-xs font-semibold uppercase tracking-[0.24em] text-slate-400">Conforme</div>
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <div className="rounded-[24px] border border-white/8 bg-white/[0.03] p-4">
            <div className="flex items-center justify-between gap-3 text-sm font-medium text-white">
              <span>Classificação concluída</span>
              <span>14</span>
            </div>
            <div className="mt-3 h-3 rounded-full bg-slate-800">
              <div className="h-3 rounded-full bg-[linear-gradient(90deg,#34d399,#22d3ee)]" style={{ width: '100%' }} />
            </div>
          </div>

          <div className="rounded-[24px] border border-white/8 bg-white/[0.03] p-4">
            <div className="flex items-center justify-between gap-3 text-sm font-medium text-white">
              <span>Não mapeado</span>
              <span>0</span>
            </div>
            <div className="mt-3 h-3 rounded-full bg-slate-800">
              <div className="h-3 rounded-full bg-slate-600" style={{ width: '4%' }} />
            </div>
          </div>

          <div className="rounded-[24px] border border-blue-400/12 bg-blue-500/5 p-4 text-sm leading-6 text-slate-300">
            O bloco deixa de ocupar meia tela com pouca informação e passa a funcionar como indicador visual de qualidade.
          </div>
        </div>
      </div>
    </div>
  );
}

function HorizontalRank({ title, subtitle, items, accent = 'blue' }) {
  const max = Math.max(...items.map((item) => item.value));
  const accentMap = {
    blue: 'from-blue-400 to-blue-300',
    cyan: 'from-cyan-400 to-cyan-300',
    emerald: 'from-emerald-400 to-teal-300',
  };

  return (
    <div className="rounded-[28px] border border-white/8 bg-[linear-gradient(180deg,rgba(8,16,28,0.98),rgba(7,12,22,0.98))] p-5">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-[18px] font-semibold text-white">{title}</div>
          <div className="mt-1 text-sm text-slate-400">{subtitle}</div>
        </div>
        <button className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs font-semibold text-slate-300">Top 5</button>
      </div>

      <div className="mt-5 space-y-4">
        {items.map((item, index) => (
          <div key={item.name} className={`rounded-[22px] border p-4 ${index === 0 ? 'border-blue-400/20 bg-blue-500/[0.06]' : 'border-white/6 bg-white/[0.03]'}`}>
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-[16px] font-semibold text-white">{item.name}</div>
                {'code' in item ? <div className="mt-1 text-sm text-slate-500">{item.code}</div> : null}
                {'delta' in item ? <div className="mt-1 text-sm text-slate-500">{item.delta}</div> : null}
              </div>
              <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs font-semibold text-slate-200">{item.value}</div>
            </div>
            <div className="mt-4 flex items-center gap-3">
              <div className="h-2.5 flex-1 overflow-hidden rounded-full bg-slate-800">
                <div
                  className={`h-2.5 rounded-full bg-[linear-gradient(90deg,var(--tw-gradient-stops))] ${'color' in item ? item.color : accentMap[accent]}`}
                  style={{ width: `${(item.value / max) * 100}%` }}
                />
              </div>
              <div className="w-12 text-right text-sm font-medium text-slate-400">{Math.round((item.value / max) * 100)}%</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function EmptyStateCard() {
  return (
    <div className="rounded-[28px] border border-dashed border-white/12 bg-[linear-gradient(180deg,rgba(8,14,25,0.98),rgba(6,10,19,0.98))] p-6">
      <div className="flex h-14 w-14 items-center justify-center rounded-2xl border border-white/8 bg-white/[0.03] text-slate-300">
        <AlertTriangle size={20} />
      </div>
      <div className="mt-5 text-[22px] font-semibold tracking-[-0.03em] text-white">Base pequena para tendência detalhada</div>
      <div className="mt-2 max-w-[46ch] text-sm leading-6 text-slate-400">
        Quando houver pouco dado, o painel mostra um estado inteligente com resumo e orientação, em vez de deixar um gráfico grande vazio.
      </div>
      <div className="mt-5 grid gap-3 sm:grid-cols-3">
        {[
          ['Mensagens processadas', '2'],
          ['Execuções', '14'],
          ['Com alerta', '2'],
        ].map(([label, value]) => (
          <div key={label} className="rounded-2xl border border-white/8 bg-white/[0.03] p-4">
            <div className="text-[12px] font-semibold uppercase tracking-[0.22em] text-slate-500">{label}</div>
            <div className="mt-2 text-3xl font-semibold tracking-[-0.04em] text-white">{value}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function FilterBar() {
  return (
    <div className="mt-6 rounded-[28px] border border-white/10 bg-[linear-gradient(180deg,rgba(10,16,28,0.96),rgba(8,12,21,0.98))] px-5 py-4 shadow-[0_12px_32px_rgba(0,0,0,0.24)]">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
        <div className="flex items-center gap-3">
          <div className="flex h-11 w-11 items-center justify-center rounded-2xl border border-white/10 bg-white/[0.04] text-blue-300">
            <Filter size={18} />
          </div>
          <div>
            <div className="text-lg font-semibold text-white">Filtros gerenciais</div>
            <div className="text-sm text-slate-400">Contexto analítico mais compacto, com menos ruído antes dos gráficos.</div>
          </div>
        </div>
        <div className="flex flex-wrap gap-3 xl:justify-end">
          {['Contrato Sabesp Oeste', 'Obra 13/03 a 14/03/2026', 'Comparativo semanal'].map((filter) => (
            <button key={filter} className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm font-medium text-slate-200 transition hover:border-blue-400/25 hover:text-white">
              {filter}
            </button>
          ))}
          <button className="flex items-center gap-2 rounded-full border border-blue-400/20 bg-blue-500/10 px-4 py-2 text-sm font-semibold text-blue-200">
            Expandir
            <ChevronDown size={16} />
          </button>
        </div>
      </div>
    </div>
  );
}

export default function App() {
  const [mode, setMode] = useState('rich');

  const quickSummary = useMemo(
    () => [
      '2 processamentos no período filtrado, com 2 alertas ativos.',
      'Núcleo mais recorrente: Mississipi. Equipe líder: Xavier.',
      'Mapeamento estável em 100%, sem itens órfãos no recorte.',
      'Maior ganho veio do volume consolidado nas frentes de rede de água.',
    ],
    []
  );

  return (
    <div className="min-h-screen bg-[#060b14] text-white">
      <div className="flex min-h-screen">
        <aside className="hidden w-[300px] shrink-0 border-r border-white/6 bg-[linear-gradient(180deg,#08101c_0%,#07101a_100%)] xl:block">
          <div className="flex h-full flex-col p-6">
            <div className="rounded-[28px] border border-blue-400/20 bg-[linear-gradient(180deg,rgba(10,20,35,0.95),rgba(7,13,23,0.98))] p-5 shadow-[0_18px_44px_rgba(0,0,0,0.28)]">
              <div className="text-[12px] font-semibold uppercase tracking-[0.26em] text-blue-200">Sistema SaaS</div>
              <h2 className="mt-4 text-[56px] font-semibold leading-[0.9] tracking-[-0.07em] text-white">Gestão<br />Contratual</h2>
              <p className="mt-4 text-[15px] leading-7 text-slate-400">Painel web com navegação por módulos operacionais, analíticos e cadastrais.</p>
            </div>

            <div className="mt-8 flex-1 space-y-6 overflow-y-auto pr-1">
              {sidebarGroups.map((group) => (
                <div key={group.label}>
                  <div className="mb-3 px-2 text-[12px] font-semibold uppercase tracking-[0.24em] text-slate-500">{group.label}</div>
                  <div className="space-y-2">
                    {group.items.map((item) => (
                      <SidebarItem key={item.label} icon={item.icon} label={item.label} active={item.active} />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </aside>

        <main className="min-w-0 flex-1 px-6 py-6 xl:px-10">
          <motion.header
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.24 }}
            className="rounded-[34px] border border-blue-400/20 bg-[linear-gradient(90deg,rgba(8,18,34,0.95),rgba(28,94,173,0.72))] px-6 py-6 shadow-[0_22px_54px_rgba(6,18,38,0.42)]"
          >
            <div className="flex flex-col gap-5 2xl:flex-row 2xl:items-start 2xl:justify-between">
              <div className="max-w-[860px]">
                <div className="text-[12px] font-semibold uppercase tracking-[0.26em] text-blue-100/90">Leitura executiva</div>
                <h1 className="mt-3 text-[58px] font-semibold leading-[0.92] tracking-[-0.07em] text-white">Painel gerencial</h1>
                <p className="mt-4 max-w-[850px] text-[17px] leading-8 text-blue-50/90">
                  Direção visual refinada para os gráficos: mais densidade útil, menos espaço ocioso e hierarquia executiva mais clara para leitura rápida.
                </p>
                <div className="mt-5 flex flex-wrap gap-3">
                  {['KPIs executivos', 'Tendência diária', 'Comparativo inteligente', 'Fallback elegante'].map((pill) => (
                    <span key={pill} className="rounded-full border border-white/12 bg-white/10 px-4 py-2 text-sm font-semibold text-white/95 backdrop-blur-sm">
                      {pill}
                    </span>
                  ))}
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-2 xl:w-[430px]">
                {[
                  ['Processamentos', '2', 'Mensagens consideradas no recorte'],
                  ['Mapeado', '100%', 'Itens classificados com sucesso'],
                  ['Execuções', '14', 'Registros consolidados no período'],
                  ['Ocorrências', '4', 'Impactos operacionais registrados'],
                ].map(([label, value, helper]) => (
                  <div key={label} className="rounded-[24px] border border-white/12 bg-white/[0.12] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
                    <div className="text-[12px] font-semibold uppercase tracking-[0.24em] text-blue-100/80">{label}</div>
                    <div className="mt-2 text-[58px] font-semibold leading-none tracking-[-0.07em] text-white">{value}</div>
                    <div className="mt-2 text-sm text-blue-50/80">{helper}</div>
                  </div>
                ))}
              </div>
            </div>
          </motion.header>

          <FilterBar />

          <section className="mt-6 grid gap-5 xl:grid-cols-4">
            {executiveCards.map((item) => (
              <ExecutiveKpi key={item.label} item={item} />
            ))}
          </section>

          <section className="mt-6 grid gap-5 xl:grid-cols-[1.18fr_0.82fr]">
            <ShellCard title="Resumo executivo" subtitle="A leitura inicial continua textual, mas conectada com a hierarquia dos gráficos abaixo.">
              <div className="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
                <div className="rounded-[26px] border border-white/8 bg-white/[0.03] p-5">
                  <div className="text-[18px] font-semibold text-white">Leitura rápida</div>
                  <ul className="mt-4 space-y-3">
                    {quickSummary.map((item) => (
                      <li key={item} className="flex items-start gap-3 text-[16px] leading-7 text-slate-200">
                        <span className="mt-2 h-2.5 w-2.5 rounded-full bg-blue-400" />
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </div>
                <div className="rounded-[26px] border border-white/8 bg-white/[0.03] p-5">
                  <div className="text-[18px] font-semibold text-white">Contexto do recorte</div>
                  <div className="mt-4 flex flex-wrap gap-3">
                    {['Obra: 13/03/2026 a 14/03/2026', 'Processamento: 24/03 01:05', 'Runs válidas: 2', 'Volume total: 181'].map((tag) => (
                      <span key={tag} className="rounded-full border border-blue-400/20 bg-blue-500/10 px-3 py-1.5 text-sm font-semibold text-blue-200">
                        {tag}
                      </span>
                    ))}
                  </div>
                  <div className="mt-5 rounded-[22px] border border-white/8 bg-white/[0.02] p-4 text-sm leading-6 text-slate-400">
                    Comparativo ativo entre 08/03/2026 a 14/03/2026 e o período imediatamente anterior.
                  </div>
                </div>
              </div>
            </ShellCard>

            <ShellCard title="Modo de leitura" subtitle="O mockup inclui visão cheia e fallback para base pequena.">
              <div className="grid gap-3">
                <button
                  onClick={() => setMode('rich')}
                  className={`rounded-[24px] border px-4 py-4 text-left transition ${
                    mode === 'rich'
                      ? 'border-blue-400/30 bg-blue-500/10'
                      : 'border-white/8 bg-white/[0.03] hover:border-blue-400/20 hover:bg-white/[0.05]'
                  }`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[16px] font-semibold text-white">Visão principal</div>
                      <div className="mt-1 text-sm text-slate-400">Mostra o painel com densidade analítica completa.</div>
                    </div>
                    <TrendingUp className="text-blue-300" size={18} />
                  </div>
                </button>

                <button
                  onClick={() => setMode('empty')}
                  className={`rounded-[24px] border px-4 py-4 text-left transition ${
                    mode === 'empty'
                      ? 'border-blue-400/30 bg-blue-500/10'
                      : 'border-white/8 bg-white/[0.03] hover:border-blue-400/20 hover:bg-white/[0.05]'
                  }`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[16px] font-semibold text-white">Fallback elegante</div>
                      <div className="mt-1 text-sm text-slate-400">Simula o estado de pouca base sem deixar gráficos vazios.</div>
                    </div>
                    <TrendingDown className="text-slate-300" size={18} />
                  </div>
                </button>
              </div>
            </ShellCard>
          </section>

          <section className="mt-6 grid gap-5 xl:grid-cols-[1.14fr_0.86fr]">
            <AreaTrendChart values={trendData} />
            <CompareBarsCard />
          </section>

          <section className="mt-6 grid gap-5 xl:grid-cols-[0.9fr_1.1fr]">
            <DonutQualityCard />
            {mode === 'rich' ? (
              <HorizontalRank
                title="Serviços mais recorrentes"
                subtitle="Ranking por volume consolidado com leitura premium e compacta."
                items={services}
                accent="blue"
              />
            ) : (
              <EmptyStateCard />
            )}
          </section>

          <section className="mt-6 grid gap-5 xl:grid-cols-2">
            <HorizontalRank
              title="Núcleos com mais processamentos"
              subtitle="Concentração operacional por núcleo com foco em comparação rápida."
              items={nuclei}
              accent="cyan"
            />
            <HorizontalRank
              title="Ocorrências mais recorrentes"
              subtitle="Principais impactos operacionais do recorte em barras horizontais compactas."
              items={occurrences}
              accent="emerald"
            />
          </section>
        </main>
      </div>
    </div>
  );
}
