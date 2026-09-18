/*
 * ArixAI Precision Content + Query Algorithm
 * File: api/algorithm.js
 *
 * Purpose
 * - This module is intentionally NOT the crawler/orchestrator.
 * - It creates precise, query-aware search variants and turns candidate URLs
 *   into validated, real source page content as quickly as possible.
 * - It can be imported directly by api/crawler.js to avoid an extra HTTP hop.
 * - It also exposes a standalone GET/POST endpoint for testing/integration.
 *
 * Core contract
 * - Search snippets are evidence for discovery/ranking only; they are never
 *   emitted as pageContent when requireRealContent=true.
 * - Final content must come from a live source page, a trusted structured
 *   content payload supplied by a search provider, a real PDF, or a real
 *   transcript/reader response.
 * - Relevance is graded instead of using a brittle one-shot threshold.
 * - Hard rejection is reserved for clearly unsafe, blocked, wrong-type, or
 *   demonstrably unrelated candidates.
 * - The algorithm is bounded, concurrent, cache-aware, and fail-safe.
 *
 * Integration from crawler.js
 *   import {
 *     analyzeQuery,
 *     buildPreciseQueries,
 *     rankCandidates,
 *     enrichCandidates,
 *     isRealSourceContent,
 *   } from './algorithm.js';
 */

export const runtime = 'edge';
export const config = { runtime: 'edge' };
export const maxDuration = 300;

const VERSION = 'arix-content-algorithm-1.0.0';

const MAX_RESULTS = 40;
const MAX_QUERY_LEN = 700;
const MAX_CANDIDATES = 180;
const MAX_PAGE_BYTES = 800_000;
const MAX_TEXT_CHARS = 30_000;
const MAX_SEARCH_CONTENT_CHARS = 30_000;

const DEFAULT_BUDGET_MS = 8_800;
const DEFAULT_PAGE_TIMEOUT_MS = 1_900;
const DEFAULT_READER_TIMEOUT_MS = 2_200;
const DEFAULT_TAVILY_TIMEOUT_MS = 2_400;
const DEFAULT_SEARCH_TIMEOUT_MS = 1_700;

const DISCOVERY_CONCURRENCY = 18;
const CONTENT_CONCURRENCY = 30;
const FALLBACK_CONCURRENCY = 18;
const MAX_TAVILY_KEYS = 12;
const MAX_TAVILY_CALLS = 2;
const TAVILY_RESULTS_PER_CALL = 20;

const SEARCH_CACHE_TTL_MS = 7_500;
const CONTENT_CACHE_TTL_MS = 180_000;
const CACHE_MAX = 120;

const SEARCH_CACHE = new Map();
const CONTENT_CACHE = new Map();

const BLOCKED_HOSTS = [
  'google-analytics.com', 'googletagmanager.com', 'googlesyndication.com',
  'googleadservices.com', 'doubleclick.net', 'gstatic.com', 'googleapis.com',
  'facebook.net', 'connect.facebook.net', 'scorecardresearch.com',
  'pixel.wp.com', 'adsrvr.org', 'amazon-adsystem.com', 'taboola.com',
  'outbrain.com', 'segment.io', 'hotjar.com', 'clarity.ms',
];

const BLOCKED_EXT = /\.(?:js|mjs|cjs|css|map|png|jpe?g|gif|webp|avif|svg|ico|bmp|tiff?|woff2?|woff|ttf|otf|eot|mp3|wav|m4a|mp4|webm|mov|avi|zip|rar|7z|exe|dmg)(?:$|[?#])/i;
const DATA_PATH = /(?:^|[\/_-])(analytics|gtag|ga4|collect|pixel|beacon|tracking|tracker|telemetry|consent|ads?)(?:[\/_-]|$)/i;
const ARTICLE_PATH = /(?:article|articles|story|stories|news|post|posts|blog|blogs|report|reports|press[-_]?release|explained|timeline|history)/i;

const GOV_DOMAINS = [
  'gov.in', 'nic.in', 'mygov.in', 'india.gov.in', 'pib.gov.in', 'mca.gov.in',
  'gst.gov.in', 'incometax.gov.in', 'msme.gov.in', 'education.gov.in', 'meity.gov.in',
  'rbi.org.in', 'sebi.gov.in', 'supremecourt.gov.in', 'indiacode.nic.in',
];

const TRUSTED_DOMAINS = [
  'who.int', 'un.org', 'nasa.gov', 'oecd.org', 'worldbank.org', 'imf.org',
  'w3.org', 'ietf.org', 'mozilla.org', 'developer.mozilla.org',
];

const STOPWORDS = new Set([
  'a', 'an', 'the', 'and', 'or', 'of', 'to', 'in', 'on', 'for', 'with', 'from',
  'by', 'as', 'at', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'this',
  'that', 'these', 'those', 'what', 'when', 'where', 'how', 'why', 'who', 'which',
  'about', 'into', 'near', 'over', 'under', 'than', 'then', 'during', 'through',
  'latest', 'current', 'recent', 'today', 'news', 'update', 'updates', 'please',
  'show', 'find', 'give', 'tell', 'me', 'can', 'you', 'i', 'we', 'it', 'its',
  'their', 'our', 'your', 'my', 'more', 'information', 'info', 'details',
]);

const SYNONYM_GROUPS = [
  ['car', 'cars', 'automobile', 'automobiles', 'vehicle', 'vehicles', 'motorcar', 'motorcars'],
  ['history', 'historical', 'timeline', 'origins', 'origin', 'evolution', 'development', 'heritage'],
  ['india', 'indian'],
  ['price', 'prices', 'cost', 'costs', 'rate', 'rates', 'pricing', 'priced'],
  ['law', 'laws', 'legal', 'legislation', 'act', 'acts', 'regulation', 'regulations', 'rule', 'rules'],
  ['policy', 'policies', 'framework', 'initiative', 'initiatives', 'program', 'programs', 'programme', 'programmes'],
  ['company', 'companies', 'firm', 'firms', 'business', 'businesses', 'corporation', 'corporations'],
  ['founder', 'founders', 'created', 'creator', 'cofounder', 'co-founder', 'originator'],
  ['population', 'people', 'residents', 'inhabitants', 'demographics'],
  ['economy', 'economic', 'economics', 'gdp', 'market', 'markets'],
  ['education', 'school', 'schools', 'student', 'students', 'curriculum', 'syllabus'],
  ['research', 'study', 'studies', 'paper', 'papers', 'report', 'reports', 'analysis'],
  ['news', 'latest', 'recent', 'breaking', 'announcement', 'announced', 'today', 'current'],
  ['review', 'reviews', 'comparison', 'compare', 'versus', 'vs'],
  ['guide', 'guides', 'tutorial', 'tutorials', 'manual', 'documentation', 'docs'],
  ['technology', 'technologies', 'tech', 'technical'],
  ['electric', 'ev', 'electricity', 'battery-powered'],
  ['manufacturing', 'manufacture', 'production', 'factory', 'factories'],
];

const SYNONYM_INDEX = new Map();
for (const group of SYNONYM_GROUPS) {
  const canonical = group[0];
  for (const word of group) SYNONYM_INDEX.set(word, canonical);
}

function env(name) {
  try {
    if (typeof process !== 'undefined' && process.env) return process.env[name];
  } catch {}
  return undefined;
}

function getEnvKeys(prefix, max = MAX_TAVILY_KEYS) {
  const keys = [];
  const base = env(prefix);
  if (base) keys.push(base.trim());
  for (let i = 2; i <= max; i++) {
    const key = env(`${prefix}_${i}`);
    if (key && key.trim() && !keys.includes(key.trim())) keys.push(key.trim());
  }
  return keys;
}

function nowIso() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function left(deadline) { return Math.max(0, deadline - Date.now()); }

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, Number(n) || 0));
}

function truncate(value, max) {
  const s = String(value ?? '').trim();
  return s.length <= max ? s : `${s.slice(0, Math.max(0, max - 1))}…`;
}

function normalizeText(value) {
  return String(value || '')
    .normalize('NFKC')
    .replace(/[^\p{L}\p{N}+#&'./:-]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function decodeHtml(value = '') {
  return String(value)
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => {
      const n = parseInt(h, 16); return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : _;
    })
    .replace(/&#(\d+);/g, (_, d) => {
      const n = parseInt(d, 10); return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : _;
    });
}

function stripTags(html = '') {
  return decodeHtml(String(html)
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi, ' ')
    .replace(/<template\b[^>]*>[\s\S]*?<\/template>/gi, ' ')
    .replace(/<nav\b[^>]*>[\s\S]*?<\/nav>/gi, ' ')
    .replace(/<footer\b[^>]*>[\s\S]*?<\/footer>/gi, ' ')
    .replace(/<form\b[^>]*>[\s\S]*?<\/form>/gi, ' ')
    .replace(/<[^>]+>/g, ' '));
}

function tokenize(value) {
  const words = normalizeText(value)
    .replace(/https?:\/\/\S+/g, ' ')
    .split(/\s+/)
    .map(x => x.trim())
    .filter(Boolean);
  return words;
}

function canonicalToken(token) {
  const t = String(token || '').toLowerCase();
  return SYNONYM_INDEX.get(t) || t.replace(/(?:ies|ing|ed|s)$/i, m => {
    if (m === 'ies') return 'y';
    if (m === 'ing' || m === 'ed') return '';
    return '';
  });
}

function semanticTokens(value) {
  return [...new Set(tokenize(value)
    .filter(t => t.length > 1 && !STOPWORDS.has(t))
    .map(canonicalToken)
    .filter(Boolean))];
}

function hasWholePhrase(text, phrase) {
  const a = normalizeText(text);
  const b = normalizeText(phrase);
  return Boolean(b) && a.includes(b);
}

function titleSimilarity(a, b) {
  const aa = new Set(semanticTokens(a));
  const bb = new Set(semanticTokens(b));
  if (!aa.size || !bb.size) return 0;
  let hits = 0;
  for (const token of aa) if (bb.has(token)) hits++;
  return hits / Math.max(aa.size, bb.size);
}

function jaccard(a, b) {
  const aa = new Set(a), bb = new Set(b);
  if (!aa.size || !bb.size) return 0;
  let intersection = 0;
  for (const x of aa) if (bb.has(x)) intersection++;
  return intersection / (aa.size + bb.size - intersection);
}

function safeUrl(url) {
  try {
    const u = new URL(String(url || ''));
    if (!/^https?:$/i.test(u.protocol)) return false;
    const h = u.hostname.toLowerCase();
    if (!h || h === 'localhost' || h === '0.0.0.0' || h.endsWith('.localhost')) return false;
    if (/^(127\.|10\.|192\.168\.|169\.254\.)/.test(h)) return false;
    if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(h)) return false;
    if (h === '::1' || h.startsWith('fc') || h.startsWith('fd')) return false;
    return true;
  } catch { return false; }
}

function normalizedHost(url) {
  try { return new URL(url).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; }
}

function normalizedUrl(url) {
  try {
    const u = new URL(url);
    u.hash = '';
    for (const k of [...u.searchParams.keys()]) {
      if (/^(?:utm_|gclid|fbclid|dclid|msclkid|ref|referrer|cmpid|source|src)$/i.test(k)) u.searchParams.delete(k);
    }
    return u.href;
  } catch { return String(url || ''); }
}

function urlBlocked(url) {
  if (!safeUrl(url)) return true;
  if (BLOCKED_EXT.test(String(url))) return true;
  const h = normalizedHost(url);
  if (BLOCKED_HOSTS.some(d => h === d || h.endsWith(`.${d}`))) return true;
  try { if (DATA_PATH.test(new URL(url).pathname)) return true; } catch {}
  return false;
}

function isGov(url) {
  const h = normalizedHost(url);
  return GOV_DOMAINS.some(d => h === d || h.endsWith(`.${d}`));
}

function isTrusted(url) {
  const h = normalizedHost(url);
  return TRUSTED_DOMAINS.some(d => h === d || h.endsWith(`.${d}`)) || /\.edu(?:\.|$)/i.test(h) || /\.ac\.(?:in|uk|jp|nz)$/i.test(h);
}

function isDoc(url) {
  return /\.(?:pdf|doc|docx|xls|xlsx|ppt|pptx)(?:[?#]|$)/i.test(String(url || '')) || /(?:^|[/?_-])(pdf|docs?|documentation)(?:[/?_-]|$)/i.test(String(url || ''));
}

function isVideo(url) {
  const h = normalizedHost(url);
  return h === 'youtube.com' || h.endsWith('.youtube.com') || h === 'youtu.be' || h.includes('vimeo.com') || /\.(?:mp4|webm|mov)(?:[?#]|$)/i.test(String(url || ''));
}

function safeInteger(value, fallback, min, max) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? Math.min(max, Math.max(min, n)) : fallback;
}

function cacheGet(cache, key, ttl) {
  const item = cache.get(key);
  if (!item) return null;
  if (Date.now() - item.at > ttl) { cache.delete(key); return null; }
  return item.value;
}

function cacheSet(cache, key, value) {
  cache.set(key, { at: Date.now(), value });
  while (cache.size > CACHE_MAX) cache.delete(cache.keys().next().value);
}

async function mapConcurrent(items, limit, worker) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return [];
  const out = new Array(list.length);
  let cursor = 0;
  const n = Math.max(1, Math.min(limit, list.length));
  const runners = Array.from({ length: n }, async () => {
    while (true) {
      const index = cursor++;
      if (index >= list.length) return;
      try { out[index] = await worker(list[index], index); }
      catch (error) { out[index] = { __error: error?.message || 'WORKER_FAILED' }; }
    }
  });
  await Promise.all(runners);
  return out;
}

function detectDateIntent(query) {
  const q = normalizeText(query);
  if (/\b(today|right now|current|latest|breaking|just in)\b/.test(q)) return { kind: 'live', words: ['today', 'latest', 'current'] };
  if (/\b(this week|last 7 days)\b/.test(q)) return { kind: 'week', words: ['this week', 'recent'] };
  if (/\b(last 30 days|this month)\b/.test(q)) return { kind: 'month', words: ['recent', 'this month'] };
  const year = q.match(/\b(19\d{2}|20\d{2})\b/);
  if (year) return { kind: 'year', year: Number(year[1]), words: [year[1]] };
  return { kind: 'none', words: [] };
}

function detectExplicitType(query, requestedType, mode) {
  const explicit = normalizeText(requestedType || '').replace(/\s+/g, '');
  if (['news'].includes(explicit) || /\bnews\b/.test(normalizeText(query)) && mode === 'news') return 'news';
  if (['video', 'videos'].includes(explicit) || (mode === 'video')) return 'video';
  if (['doc', 'docs', 'document', 'documents', 'pdf'].includes(explicit) || mode === 'doc') return 'doc';
  if (['gov', 'government', 'official'].includes(explicit) || mode === 'gov') return 'gov';
  return '';
}

function extractConceptGroups(query) {
  const raw = semanticTokens(query);
  const groups = [];
  const matched = new Set();
  for (const group of SYNONYM_GROUPS) {
    const canonical = group[0];
    if (raw.includes(canonical)) {
      groups.push({ id: canonical, terms: group, core: canonical });
      matched.add(canonical);
    }
  }

  const nonStop = raw.filter(x => !matched.has(x) && x.length >= 4);
  for (const token of nonStop.slice(0, 8)) {
    groups.push({ id: token, terms: [token], core: token });
  }

  return groups.slice(0, 12);
}

function extractNamedAnchors(query) {
  const words = String(query || '').trim().split(/\s+/).filter(Boolean);
  const anchors = [];
  for (let i = 0; i < words.length; i++) {
    if (/^[A-Z][\p{L}\d&.-]*(?:\s+[A-Z][\p{L}\d&.-]*){0,3}$/u.test(words[i])) {
      anchors.push(words[i]);
    }
  }
  const quoted = [...String(query || '').matchAll(/"([^"]{3,120})"/g)].map(m => m[1]);
  return [...new Set([...quoted, ...anchors])].slice(0, 8);
}

export function analyzeQuery(query, options = {}) {
  const clean = truncate(String(query || '').trim(), MAX_QUERY_LEN);
  const requestedType = String(options.type || '').toLowerCase();
  const mode = String(options.mode || 'auto').toLowerCase();
  const type = detectExplicitType(clean, requestedType, mode);
  const dateIntent = detectDateIntent(clean);
  const concepts = extractConceptGroups(clean);
  const tokens = semanticTokens(clean);
  const anchors = extractNamedAnchors(clean);
  const q = normalizeText(clean);
  const wantsOfficial = /\b(official|government|govt|ministry|notification|circular|law|act|rule|regulation|scheme|tax|gst|income tax|rbi|sebi|mca|policy)\b/i.test(q);
  const wantsAcademic = /\b(research|study|paper|academic|journal|thesis|literature|evidence)\b/i.test(q);
  const wantsHistory = concepts.some(x => x.id === 'history');
  const wantsComparison = concepts.some(x => x.id === 'review');
  const wantsGuide = concepts.some(x => x.id === 'guide');
  const explicitNews = type === 'news';
  const explicitVideo = type === 'video';
  const explicitDoc = type === 'doc';
  const explicitGov = type === 'gov';
  return {
    query: clean,
    type: type || 'web',
    requestedType,
    mode,
    tokens,
    concepts,
    anchors,
    dateIntent,
    flags: {
      wantsOfficial: wantsOfficial || explicitGov,
      wantsAcademic,
      wantsHistory,
      wantsComparison,
      wantsGuide,
      explicitNews,
      explicitVideo,
      explicitDoc,
      explicitGov,
    },
  };
}

function choosePrimaryTerms(plan) {
  const groups = plan.concepts.filter(x => x.id !== 'latest' && x.id !== 'news');
  return groups.slice(0, 6).map(x => x.core);
}

export function buildPreciseQueries(query, options = {}) {
  const plan = options.plan || analyzeQuery(query, options);
  const base = plan.query;
  const set = new Set();
  const add = q => {
    const x = String(q || '').replace(/\s+/g, ' ').trim();
    if (x && x.length >= 3 && x.length <= 500) set.add(x);
  };

  // 1. Preserve the user's exact wording as the primary intent.
  add(base);

  const meaningful = plan.tokens.filter(x => x.length >= 3);
  if (meaningful.length >= 2) add(`"${meaningful.slice(0, 8).join(' ')}"`);

  // 2. Force the important concept groups to stay together. This prevents
  //    broad lexical search from silently drifting to a related-but-wrong topic.
  const primary = choosePrimaryTerms(plan);
  if (primary.length >= 2) add(primary.join(' '));
  if (primary.length >= 3) add(`${primary.slice(0, 4).join(' ')} overview`);

  // 3. Intent-specific precision variants; only activated when actually relevant.
  if (plan.flags.wantsHistory) {
    add(`${base} timeline origins evolution`);
    add(`${base} historical overview milestones`);
  }
  if (plan.flags.wantsOfficial) {
    add(`${base} official source`);
    add(`${base} official document`);
    add(`${base} site:gov.in`);
  }
  if (plan.flags.wantsAcademic) {
    add(`${base} research paper evidence`);
    add(`${base} academic study`);
  }
  if (plan.flags.wantsComparison) add(`${base} comparison differences`);
  if (plan.flags.wantsGuide) add(`${base} official documentation guide`);
  if (plan.flags.explicitNews) {
    add(`${base} latest news`);
    add(`${base} recent developments`);
  }
  if (plan.flags.explicitDoc) {
    add(`${base} filetype:pdf`);
    add(`${base} official PDF`);
  }
  if (plan.flags.explicitVideo) add(`${base} video YouTube`);
  if (plan.dateIntent.kind !== 'none') add(`${base} ${plan.dateIntent.words.join(' ')}`);

  // 4. Geography/entity-preserving variants.
  if (plan.anchors.length) add(`${plan.anchors.slice(0, 4).join(' ')} ${primary.slice(0, 4).join(' ')}`);

  return [...set].slice(0, 8);
}

function typeFits(candidate, plan) {
  const explicit = plan.type;
  if (!explicit || explicit === 'web') return true;
  if (explicit === 'gov') return isGov(candidate.url) || candidate.type === 'gov';
  if (explicit === 'doc') return isDoc(candidate.url) || candidate.type === 'doc';
  if (explicit === 'video') return isVideo(candidate.url) || candidate.type === 'video';
  if (explicit === 'news') return candidate.type === 'news' || ARTICLE_PATH.test(String(candidate.url || ''));
  return true;
}

function countConceptCoverage(text, concepts) {
  const tokenSet = new Set(semanticTokens(text));
  if (!concepts.length) return { ratio: 1, hits: [], missing: [] };
  const hits = [];
  const missing = [];
  for (const concept of concepts) {
    const canonical = SYNONYM_INDEX.get(concept.core) || concept.core;
    if (tokenSet.has(canonical) || concept.terms.some(t => tokenSet.has(canonicalToken(t)))) hits.push(concept.id);
    else missing.push(concept.id);
  }
  return { ratio: hits.length / concepts.length, hits, missing };
}

function relevanceDetails(candidate, plan) {
  const title = String(candidate.title || '');
  const snippet = String(candidate.snippet || candidate.content || '');
  const body = String(candidate.pageContent || candidate.extractedText || candidate.rawContent || candidate.content || '');
  const combined = `${title} ${snippet} ${body}`;
  const queryTokens = plan.tokens;
  const titleTokens = semanticTokens(title);
  const bodyTokens = semanticTokens(body);
  const snippetTokens = semanticTokens(snippet);
  const conceptTitle = countConceptCoverage(title, plan.concepts);
  const conceptBody = countConceptCoverage(body, plan.concepts);
  const conceptCombined = countConceptCoverage(combined, plan.concepts);
  const titleCov = queryTokens.length ? queryTokens.filter(t => titleTokens.includes(t)).length / queryTokens.length : 1;
  const bodyCov = queryTokens.length ? queryTokens.filter(t => bodyTokens.includes(t)).length / queryTokens.length : 1;
  const snippetCov = queryTokens.length ? queryTokens.filter(t => snippetTokens.includes(t)).length / queryTokens.length : 1;
  const phraseExact = plan.query.length >= 6 ? hasWholePhrase(combined, plan.query) : false;
  const exactAnchorHits = plan.anchors.filter(anchor => normalizeText(combined).includes(normalizeText(anchor))).length;

  let score = 20;
  score += titleCov * 22;
  score += bodyCov * 18;
  score += snippetCov * 8;
  score += conceptTitle.ratio * 13;
  score += conceptBody.ratio * 14;
  score += plan.concepts.length ? conceptCombined.ratio * 10 : 0;
  score += phraseExact ? 8 : 0;
  score += exactAnchorHits * 3;

  const semanticScore = Number(candidate.semanticSearchScore ?? candidate.score ?? candidate.relevance ?? NaN);
  if (Number.isFinite(semanticScore)) score += clamp(semanticScore, 0, 1) * 12;

  const host = normalizedHost(candidate.url);
  if (isGov(candidate.url) && plan.flags.wantsOfficial) score += 7;
  if (isTrusted(candidate.url) && (plan.flags.wantsOfficial || plan.flags.wantsAcademic)) score += 4;
  if (ARTICLE_PATH.test(String(candidate.url || '')) && (plan.flags.wantsHistory || plan.flags.explicitNews || plan.flags.wantsAcademic)) score += 2;

  const mismatchTerms = [];
  if (plan.flags.wantsHistory && !conceptCombined.hits.includes('history')) {
    if (/\bnew cars?|upcoming cars?|car prices?|buy a car|best cars?\b/i.test(combined)) {
      score -= 12;
      mismatchTerms.push('current-shopping-content-without-history');
    }
  }
  if (plan.flags.explicitNews && !conceptCombined.hits.includes('news')) score -= 8;
  if (plan.flags.explicitVideo && !isVideo(candidate.url)) score -= 10;
  if (plan.flags.explicitDoc && !isDoc(candidate.url)) score -= 8;
  if (plan.flags.explicitGov && !isGov(candidate.url)) score -= 7;

  const hardTopicMiss = plan.concepts.length >= 2 && conceptCombined.ratio < 0.34;
  const coreMiss = plan.concepts.length > 0 && conceptCombined.hits.length === 0;
  const titleBodyConflict = titleCov >= 0.55 && bodyCov < 0.18;
  const acceptable = !coreMiss && !hardTopicMiss && score >= 24 && !titleBodyConflict;

  return {
    score: Number(clamp(score, 0, 100).toFixed(2)),
    titleCoverage: Number(titleCov.toFixed(3)),
    bodyCoverage: Number(bodyCov.toFixed(3)),
    snippetCoverage: Number(snippetCov.toFixed(3)),
    conceptCoverage: Number(conceptCombined.ratio.toFixed(3)),
    titleConceptCoverage: Number(conceptTitle.ratio.toFixed(3)),
    bodyConceptCoverage: Number(conceptBody.ratio.toFixed(3)),
    phraseExact,
    exactAnchorHits,
    hits: conceptCombined.hits,
    missing: conceptCombined.missing,
    mismatchTerms,
    acceptable,
    band: score >= 75 ? 'excellent' : score >= 58 ? 'strong' : score >= 42 ? 'usable' : score >= 28 ? 'weak-match' : 'poor',
    host,
  };
}

export function rankCandidates(candidates, queryOrPlan, options = {}) {
  const plan = typeof queryOrPlan === 'string' ? analyzeQuery(queryOrPlan, options) : (queryOrPlan || analyzeQuery('', options));
  const list = Array.isArray(candidates) ? candidates.slice(0, MAX_CANDIDATES) : [];
  const ranked = [];
  for (let index = 0; index < list.length; index++) {
    const original = list[index] || {};
    const candidate = {
      ...original,
      url: normalizedUrl(original.url || original.link || original.sourceUrl || ''),
      title: truncate(original.title || original.name || '', 500),
      snippet: truncate(original.snippet || original.description || original.content || '', 3000),
    };
    if (urlBlocked(candidate.url)) continue;
    if (!typeFits(candidate, plan)) continue;
    const details = relevanceDetails(candidate, plan);
    if (!details.acceptable && !options.keepWeak) continue;
    ranked.push({ ...candidate, _relevance: details, _sourceIndex: index });
  }

  ranked.sort((a, b) => {
    const ds = (b._relevance.score || 0) - (a._relevance.score || 0);
    if (Math.abs(ds) > 0.01) return ds;
    const ac = Number(a.semanticSearchScore ?? -1), bc = Number(b.semanticSearchScore ?? -1);
    if (bc !== ac) return bc - ac;
    return (b.title || '').length - (a.title || '').length;
  });

  return ranked;
}

function diversify(ranked, count, plan) {
  const selected = [];
  const domainCount = new Map();
  const groupCount = new Map();
  const pool = [...ranked];
  while (selected.length < count && pool.length) {
    let bestIndex = 0;
    let best = -Infinity;
    for (let i = 0; i < pool.length; i++) {
      const r = pool[i];
      const domain = normalizedHost(r.url);
      const domains = domainCount.get(domain) || 0;
      const relevance = r._relevance?.score || 0;
      const conceptHits = r._relevance?.hits || [];
      const repeatPenalty = Math.min(9, domains * 2.2);
      const novelty = conceptHits.reduce((sum, x) => sum + 1 / (1 + (groupCount.get(x) || 0)), 0);
      const adjusted = relevance - repeatPenalty + novelty;
      if (adjusted > best) { best = adjusted; bestIndex = i; }
    }
    const chosen = pool.splice(bestIndex, 1)[0];
    selected.push(chosen);
    const d = normalizedHost(chosen.url);
    domainCount.set(d, (domainCount.get(d) || 0) + 1);
    for (const g of chosen._relevance?.hits || []) groupCount.set(g, (groupCount.get(g) || 0) + 1);
  }
  return selected;
}

function parseJsonLd(html) {
  const out = [];
  const blocks = html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>[\s\S]*?<\/script>/gi) || [];
  for (const block of blocks.slice(0, 24)) {
    const raw = block.replace(/^<script[^>]*>/i, '').replace(/<\/script>$/i, '').trim();
    try {
      const value = JSON.parse(raw);
      const visit = obj => {
        if (!obj || typeof obj !== 'object') return;
        if (Array.isArray(obj)) { obj.forEach(visit); return; }
        if (obj.articleBody) out.push({ kind: 'articleBody', value: String(obj.articleBody) });
        if (obj.headline) out.push({ kind: 'headline', value: String(obj.headline) });
        if (obj.name) out.push({ kind: 'name', value: String(obj.name) });
        if (obj.description) out.push({ kind: 'description', value: String(obj.description) });
        if (obj.datePublished) out.push({ kind: 'datePublished', value: String(obj.datePublished) });
        if (obj.dateModified) out.push({ kind: 'dateModified', value: String(obj.dateModified) });
        Object.values(obj).forEach(visit);
      };
      visit(value);
    } catch {}
  }
  return out;
}

function parseTitle(html) {
  return decodeHtml((String(html).match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [, ''])[1]).replace(/\s+/g, ' ').trim();
}

function parseMeta(html, name) {
  const escaped = String(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const a = new RegExp(`<meta[^>]+(?:name|property)=["']${escaped}["'][^>]*content=["']([\\s\\S]*?)["'][^>]*>`, 'i');
  const b = new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+(?:name|property)=["']${escaped}["'][^>]*>`, 'i');
  return decodeHtml((String(html).match(a) || String(html).match(b) || [, ''])[1]).trim();
}

function parseCanonical(html, baseUrl) {
  const m = String(html).match(/<link[^>]+(?:rel=["'][^"']*canonical[^"']*["'][^>]*href|href=["']([^"']+)["'][^>]*rel=["'][^"']*canonical[^"']*)/i);
  const href = m?.[1] || (String(html).match(/<link[^>]+rel=["'][^"']*canonical[^"']*["'][^>]*href=["']([^"']+)["']/i) || [, ''])[1];
  try { return href ? new URL(decodeHtml(href), baseUrl).href : null; } catch { return null; }
}

function parsePublishedAt(html, jsonLd) {
  const values = [
    parseMeta(html, 'article:published_time'),
    parseMeta(html, 'datePublished'),
    parseMeta(html, 'date'),
    parseMeta(html, 'pubdate'),
    (String(html).match(/<time[^>]+datetime=["']([^"']+)["']/i) || [, ''])[1],
    ...jsonLd.filter(x => /date/i.test(x.kind)).map(x => x.value),
  ].filter(Boolean);
  for (const value of values) {
    const t = Date.parse(value);
    if (!Number.isNaN(t)) return new Date(t).toISOString();
  }
  return null;
}

function cleanContent(text) {
  const lines = String(text || '')
    .replace(/\[[^\]]{1,200}\]\([^)]{0,600}\)/g, ' ')
    .replace(/!\[[^\]]*\]\([^)]*\)/g, ' ')
    .replace(/\b(?:skip to content|accept cookies|cookie settings|privacy settings|sign in|log in|subscribe|menu|search this site|enable javascript)\b/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return truncate(lines, MAX_TEXT_CHARS);
}

function textDensity(htmlBlock) {
  const html = String(htmlBlock || '');
  const text = cleanContent(stripTags(html));
  const markup = Math.max(1, html.length);
  return text.length / markup;
}

function extractParagraphs(block) {
  const rows = [];
  const re = /<(?:h1|h2|h3|h4|p|blockquote)\b[^>]*>([\s\S]*?)<\/(?:h1|h2|h3|h4|p|blockquote)>/gi;
  for (const m of String(block || '').matchAll(re)) {
    const t = cleanContent(stripTags(m[1]));
    if (t.length >= 35) rows.push(t);
  }
  return [...new Set(rows)].slice(0, 220).join(' ');
}

function candidateContainers(html) {
  const out = [];
  const s = String(html || '');
  const add = (source, match, bonus = 0) => {
    if (!match) return;
    const block = match[1] || '';
    if (block.length < 300) return;
    const text = cleanContent(stripTags(block));
    if (text.length < 250) return;
    out.push({ source, block, text, bonus, density: textDensity(block) });
  };

  for (const tag of ['article', 'main']) {
    const re = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'gi');
    for (const m of s.matchAll(re)) add(tag, m, tag === 'article' ? 9 : 5);
  }
  for (const m of s.matchAll(/<[^>]+(?:id|class)=["'][^"']*(?:article|story|post|entry|content|main|body|text)[^"']*["'][^>]*>([\s\S]*?)<\/[a-z0-9]+>/gi)) add('semantic-container', m, 4);
  for (const m of s.matchAll(/<[^>]+role=["']main["'][^>]*>([\s\S]*?)<\/[a-z0-9]+>/gi)) add('role-main', m, 6);

  const body = s.match(/<body\b[^>]*>([\s\S]*?)<\/body>/i);
  add('body', body, 0);
  return out;
}

function extractBestContent(html, requestedTitle = '', queryPlan = null) {
  const jsonLd = parseJsonLd(html);
  const structured = jsonLd.filter(x => x.kind === 'articleBody').map(x => cleanContent(x.value)).filter(x => x.length >= 350);
  const title = parseTitle(html);
  if (structured.length) {
    const best = structured.sort((a, b) => b.length - a.length)[0];
    const sim = titleSimilarity(requestedTitle || title, title);
    return { content: best, method: 'jsonld-articlebody', title, titleSimilarity: sim, publishedAt: parsePublishedAt(html, jsonLd), sourceQuality: 1 };
  }

  const containers = candidateContainers(html);
  const scored = containers.map(c => {
    const paragraphText = extractParagraphs(c.block);
    const content = paragraphText.length >= 300 ? paragraphText : c.text;
    const sim = titleSimilarity(requestedTitle || title, title);
    const queryCoverage = queryPlan ? countConceptCoverage(`${title} ${content}`, queryPlan.concepts).ratio : 1;
    const articleSignal = c.source === 'article' ? 5 : 0;
    const score = c.bonus + articleSignal + sim * 12 + queryCoverage * 10 + Math.min(8, c.density * 20) + Math.min(6, content.length / 2500);
    return { ...c, paragraphText, content, score, titleSimilarity: sim, queryCoverage };
  }).sort((a, b) => b.score - a.score);

  const best = scored[0];
  if (best) {
    return {
      content: cleanContent(best.content),
      method: best.source === 'article' ? 'direct-html-article' : best.source === 'main' ? 'direct-html-main' : 'direct-html-semantic',
      title,
      titleSimilarity: best.titleSimilarity,
      publishedAt: parsePublishedAt(html, jsonLd),
      sourceQuality: best.source === 'article' ? 0.96 : 0.9,
    };
  }

  const stripped = cleanContent(stripTags(html));
  return {
    content: stripped,
    method: 'direct-html-body',
    title,
    titleSimilarity: titleSimilarity(requestedTitle || title, title),
    publishedAt: parsePublishedAt(html, jsonLd),
    sourceQuality: 0.65,
  };
}

function bytesToLatin1(bytes) {
  let out = '';
  const step = 8192;
  for (let i = 0; i < bytes.length; i += step) out += String.fromCharCode(...bytes.subarray(i, Math.min(i + step, bytes.length)));
  return out;
}

function decodePdfLiteral(raw) {
  return String(raw || '')
    .replace(/\\([nrtbf\\()])/g, (_, c) => ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '\\': '\\', '(': '(', ')': ')' })[c] || c)
    .replace(/\\([0-7]{1,3})/g, (_, oct) => String.fromCharCode(parseInt(oct, 8)));
}

function decodePdfHex(raw) {
  const hex = String(raw || '').replace(/[^0-9a-f]/gi, '');
  if (!hex) return '';
  const even = hex.length % 2 ? `${hex}0` : hex;
  const bytes = new Uint8Array(even.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(even.slice(i * 2, i * 2 + 2), 16);
  try {
    if (bytes.length >= 2 && bytes[0] === 0xfe && bytes[1] === 0xff) return new TextDecoder('utf-16be').decode(bytes.slice(2));
  } catch {}
  return new TextDecoder('utf-8', { fatal: false }).decode(bytes);
}

function extractPdfStrings(stream) {
  const out = [];
  let i = 0;
  while (i < stream.length) {
    if (stream[i] === '(') {
      let depth = 1, j = i + 1, escaped = false;
      for (; j < stream.length; j++) {
        const ch = stream[j];
        if (escaped) { escaped = false; continue; }
        if (ch === '\\') { escaped = true; continue; }
        if (ch === '(') depth++;
        else if (ch === ')') { depth--; if (depth === 0) break; }
      }
      if (j < stream.length) {
        const text = decodePdfLiteral(stream.slice(i + 1, j));
        if (text.trim()) out.push(text);
        i = j + 1; continue;
      }
    }
    if (stream[i] === '<' && stream[i + 1] !== '<') {
      const j = stream.indexOf('>', i + 1);
      if (j > i) {
        const text = decodePdfHex(stream.slice(i + 1, j));
        if (text.trim()) out.push(text);
        i = j + 1; continue;
      }
    }
    i++;
  }
  return out.join(' ');
}

async function inflateDeflate(bytes, deadline) {
  if (typeof DecompressionStream === 'undefined' || left(deadline) < 500) return null;
  const timeout = Math.min(1200, Math.max(350, left(deadline) - 100));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const ds = new DecompressionStream('deflate');
    const writer = ds.writable.getWriter();
    await writer.write(bytes);
    await writer.close();
    const ab = await new Response(ds.readable).arrayBuffer();
    if (controller.signal.aborted) return null;
    return new Uint8Array(ab);
  } catch { return null; }
  finally { clearTimeout(timer); }
}

async function extractPdfText(bytes, deadline) {
  const raw = bytesToLatin1(bytes);
  const pieces = [];
  const re = /<<(?:[\s\S]{0,7000}?)>>\s*stream\r?\n([\s\S]*?)\r?\nendstream/g;
  let m;
  while ((m = re.exec(raw)) && pieces.length < 120) {
    if (left(deadline) < 450) break;
    const dictEnd = m[0].indexOf('stream');
    const dict = dictEnd >= 0 ? m[0].slice(0, dictEnd) : '';
    const payloadRelative = m[0].indexOf(m[1]);
    const payloadStart = m.index + Math.max(0, payloadRelative);
    const payloadEnd = payloadStart + m[1].length;
    const source = bytes.slice(Math.max(0, payloadStart), Math.min(bytes.length, payloadEnd));
    if (/\/FlateDecode/i.test(dict)) {
      const inflated = await inflateDeflate(source, deadline);
      if (inflated) pieces.push(bytesToLatin1(inflated));
    } else pieces.push(m[1]);
  }
  return cleanContent(pieces.map(extractPdfStrings).join(' '));
}

function isJavaScript(text, contentType = '') {
  if (/javascript|ecmascript/i.test(String(contentType))) return true;
  const s = String(text || '').slice(0, 5000).trim();
  return /^(?:!function|function\s+|\(function|window\.|document\.|(?:var|let|const)\s+[A-Za-z_$]|[A-Za-z_$][\w$]*\s*=\s*function)/i.test(s) || /(?:google-analytics|googletagmanager|dataLayer\.push|gtag\()/i.test(s);
}

export function validateSourceContent(content, result, plan, metadata = {}) {
  const text = cleanContent(content);
  if (text.length < 350) return { valid: false, reason: 'TOO_SHORT', length: text.length, targetMatched: false, targetSimilarity: 0 };
  if (isJavaScript(text, metadata.contentType)) return { valid: false, reason: 'SCRIPT_CONTENT', length: text.length, targetMatched: false, targetSimilarity: 0 };

  const pageTitle = String(metadata.title || result?.title || '');
  const sim = titleSimilarity(result?.title || '', pageTitle);
  const coverage = countConceptCoverage(`${pageTitle} ${text}`, plan.concepts);
  const directQueryCoverage = plan.tokens.length
    ? plan.tokens.filter(t => semanticTokens(text).includes(t)).length / plan.tokens.length
    : 1;

  const boilerplateTokens = tokenize(text).filter(x => /^(menu|search|login|subscribe|privacy|cookie|home|facebook|twitter|instagram|newsletter|copyright)$/i.test(x)).length;
  const totalTokens = Math.max(1, tokenize(text).length);
  const boilerplateRatio = boilerplateTokens / totalTokens;
  const mismatch = plan.flags.wantsHistory && !coverage.hits.includes('history') && /\bnew cars?|upcoming cars?|car prices?|buy a car\b/i.test(text.slice(0, 8000));

  const targetMatched =
    sim >= 0.45 ||
    (coverage.ratio >= 0.55 && directQueryCoverage >= 0.18) ||
    (plan.concepts.length <= 2 && coverage.ratio >= 0.5 && directQueryCoverage >= 0.25);

  const quality = clamp(
    0.35 +
      Math.min(0.25, text.length / 20_000) +
      sim * 0.2 +
      coverage.ratio * 0.18 +
      Math.min(0.1, directQueryCoverage * 0.1) -
      Math.min(0.15, boilerplateRatio * 0.8) -
      (mismatch ? 0.2 : 0),
    0,
    1,
  );

  const valid = targetMatched && !mismatch && quality >= 0.48;
  return {
    valid,
    reason: valid ? 'VALIDATED' : mismatch ? 'TARGET_MISMATCH' : !targetMatched ? 'TARGET_NOT_MATCHED' : 'LOW_CONTENT_QUALITY',
    length: text.length,
    targetMatched,
    targetSimilarity: Number(sim.toFixed(3)),
    conceptCoverage: Number(coverage.ratio.toFixed(3)),
    queryCoverage: Number(directQueryCoverage.toFixed(3)),
    boilerplateRatio: Number(boilerplateRatio.toFixed(3)),
    quality: Number(quality.toFixed(3)),
  };
}

export function isRealSourceContent(result) {
  const status = String(result?.contentStatus || '').toLowerCase();
  const method = String(result?.contentMethod || '').toLowerCase();
  const content = String(result?.pageContent || result?.extractedText || '').trim();
  if (!content || content.length < 350) return false;
  if (['snippet', 'search-snippet', 'metadata', 'metadata-fallback'].some(x => status === x || method === x)) return false;
  if (/snippet|search evidence|query context/i.test(content.slice(0, 1200)) && method !== 'tavily-raw-content') return false;
  if (urlBlocked(result?.contentSourceUrl || result?.url)) return false;
  return ['full', 'reader', 'alternate', 'tavily-raw-content', 'jsonld', 'direct'].includes(status) || ['tavily-raw-content', 'direct-html-article', 'direct-html-main', 'direct-html-semantic', 'direct-html-body', 'jsonld-articlebody', 'jina-reader', 'alternate-page', 'youtube-captions', 'direct-pdf-text'].includes(method);
}

function readLimitBytes(value, max) {
  const reader = value?.body?.getReader?.();
  if (!reader) return value?.arrayBuffer ? value.arrayBuffer().then(ab => new Uint8Array(ab).slice(0, max)) : Promise.resolve(new Uint8Array());
  return (async () => {
    const chunks = [];
    let total = 0;
    try {
      while (total < max) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;
        const room = max - total;
        const chunk = value.byteLength > room ? value.slice(0, room) : value;
        chunks.push(chunk);
        total += chunk.byteLength;
      }
    } finally { try { await reader.cancel(); } catch {} }
    const bytes = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
    return bytes;
  })();
}

function readLimit(value, max) {
  const r = value?.body?.getReader?.();
  if (!r) return value?.text ? value.text().then(x => x.slice(0, max)) : Promise.resolve('');
  return (async () => {
    const chunks = [];
    let total = 0;
    try {
      while (total < max) {
        const { done, value } = await r.read();
        if (done) break;
        if (!value) continue;
        const room = max - total;
        const chunk = value.byteLength > room ? value.slice(0, room) : value;
        chunks.push(chunk);
        total += chunk.byteLength;
      }
    } finally { try { await r.cancel(); } catch {} }
    const bytes = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { bytes.set(c, off); off += c.length; }
    return new TextDecoder('utf-8', { fatal: false }).decode(bytes);
  })();
}

async function fetchUrl(url, timeout, deadline, headers = {}) {
  const available = Math.min(timeout, Math.max(200, left(deadline) - 100));
  if (available < 200) throw new Error('BUDGET_EXHAUSTED');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), available);
  try {
    return await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'user-agent': 'Mozilla/5.0 (compatible; ArixAI-ContentAlgorithm/1.0; +https://lexis-ai-chatini.vercel.app/)',
        accept: 'text/html,application/xhtml+xml,application/pdf,text/plain;q=0.9,*/*;q=0.2',
        'accept-language': 'en-IN,en;q=0.9',
        ...headers,
      },
    });
  } finally { clearTimeout(timer); }
}

async function fetchDirectContent(candidate, plan, deadline) {
  const url = normalizedUrl(candidate.url);
  if (urlBlocked(url)) return { ...candidate, _contentFailure: 'BLOCKED_URL' };
  try {
    const cached = cacheGet(CONTENT_CACHE, url, CONTENT_CACHE_TTL_MS);
    if (cached && isRealSourceContent(cached)) return { ...candidate, ...cached, cachedContent: true };

    const res = await fetchUrl(url, DEFAULT_PAGE_TIMEOUT_MS, deadline);
    const finalUrl = normalizedUrl(res.url || url);
    const type = res.headers.get('content-type') || '';
    if (!res.ok) throw new Error(`HTTP_${res.status}`);

    if (/application\/pdf/i.test(type) || /\.pdf(?:[?#]|$)/i.test(finalUrl)) {
      const pdfBytes = await readLimitBytes(res, MAX_PAGE_BYTES);
      const text = await extractPdfText(pdfBytes, deadline);
      const validation = validateSourceContent(text, candidate, plan, { contentType: type, title: candidate.title });
      if (!validation.valid) throw new Error(`PDF_${validation.reason}`);
      const result = {
        url: finalUrl,
        domain: normalizedHost(finalUrl),
        pageContent: text,
        extractedText: text,
        contentStatus: 'full',
        contentMethod: 'direct-pdf-text',
        contentSourceUrl: finalUrl,
        contentLength: text.length,
        contentConfidence: validation.quality,
        contentTargetMatched: true,
        contentTitleSimilarity: validation.targetSimilarity,
        validatedBy: 'algorithm',
        verificationMethod: 'direct-fetch',
        httpStatus: res.status,
        contentType: type,
        publishedAt: candidate.publishedAt || null,
      };
      cacheSet(CONTENT_CACHE, url, result);
      return { ...candidate, ...result };
    }

    if (!/html|xhtml|text\//i.test(type)) throw new Error(`NON_PAGE_CONTENT_${type}`);
    const html = await readLimit(res, MAX_PAGE_BYTES);
    if (!html) throw new Error('EMPTY_PAGE');
    const extracted = extractBestContent(html, candidate.title, plan);
    const canonical = parseCanonical(html, finalUrl);
    const sourceUrl = safeUrl(canonical || finalUrl) && !urlBlocked(canonical || finalUrl) ? (canonical || finalUrl) : finalUrl;
    const description = parseMeta(html, 'description') || parseMeta(html, 'og:description') || candidate.snippet || '';
    const validation = validateSourceContent(extracted.content, candidate, plan, {
      title: extracted.title,
      contentType: type,
    });
    if (!validation.valid) throw new Error(`HTML_${validation.reason}`);

    const result = {
      url: sourceUrl,
      domain: normalizedHost(sourceUrl),
      title: extracted.title || candidate.title,
      snippet: truncate(description, 1200),
      pageContent: extracted.content,
      extractedText: extracted.content,
      contentStatus: 'full',
      contentMethod: extracted.method,
      contentSourceUrl: sourceUrl,
      contentLength: extracted.content.length,
      contentConfidence: validation.quality,
      contentTargetMatched: true,
      contentTitleSimilarity: validation.targetSimilarity,
      contentConceptCoverage: validation.conceptCoverage,
      validatedBy: 'algorithm',
      verificationMethod: 'direct-fetch',
      httpStatus: res.status,
      contentType: type,
      publishedAt: extracted.publishedAt || candidate.publishedAt || null,
    };
    cacheSet(CONTENT_CACHE, url, result);
    return { ...candidate, ...result };
  } catch (error) {
    return { ...candidate, _contentFailure: error?.message || 'DIRECT_FETCH_FAILED' };
  }
}

function readerUrls(url) {
  try {
    const u = new URL(url);
    return [
      `https://r.jina.ai/http://${u.host}${u.pathname}${u.search}`,
      `https://r.jina.ai/https://${u.host}${u.pathname}${u.search}`,
    ];
  } catch { return []; }
}

function markdownToText(value) {
  return cleanContent(String(value || '')
    .replace(/^URL Source:\s*.*$/gmi, ' ')
    .replace(/^Title:\s*.*$/gmi, ' ')
    .replace(/^Published Time:\s*.*$/gmi, ' ')
    .replace(/^Markdown Content:\s*/gmi, ' ')
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/\[([^\]]+)\]\((?:https?:\/\/|\/)[^)]*\)/g, '$1')
    .replace(/^#{1,6}\s*/gm, '')
    .replace(/[*_~`]/g, ' '));
}

async function readerContent(candidate, plan, deadline) {
  if (urlBlocked(candidate.url) || left(deadline) < 650) return { ...candidate, _contentFailure: 'READER_BUDGET' };
  for (const readerUrl of readerUrls(candidate.url)) {
    if (left(deadline) < 600) break;
    try {
      const res = await fetchUrl(readerUrl, DEFAULT_READER_TIMEOUT_MS, deadline, { accept: 'text/plain,text/markdown;q=0.95,*/*;q=0.1' });
      if (!res.ok) continue;
      const raw = await readLimit(res, MAX_PAGE_BYTES);
      const text = markdownToText(raw);
      const titleMatch = raw.match(/^Title:\s*(.+)$/im)?.[1]?.trim() || candidate.title;
      const validation = validateSourceContent(text, candidate, plan, { title: titleMatch, contentType: 'text/markdown' });
      if (!validation.valid) continue;
      return {
        ...candidate,
        pageContent: text,
        extractedText: text,
        contentStatus: 'reader',
        contentMethod: 'jina-reader',
        contentSourceUrl: candidate.url,
        contentLength: text.length,
        contentConfidence: validation.quality,
        contentTargetMatched: true,
        contentTitleSimilarity: validation.targetSimilarity,
        contentConceptCoverage: validation.conceptCoverage,
        validatedBy: 'algorithm',
        verificationMethod: 'jina-reader',
        contentType: 'text/markdown',
      };
    } catch {}
  }
  return { ...candidate, _contentFailure: 'READER_FAILED' };
}

async function recoverContent(candidate, plan, deadline) {
  if (isRealSourceContent(candidate)) return candidate;
  if (left(deadline) < 500) return candidate;
  const direct = await fetchDirectContent(candidate, plan, deadline);
  if (isRealSourceContent(direct)) return direct;
  return readerContent(direct, plan, deadline);
}

function normalizeProviderCandidate(item, provider = 'unknown') {
  const url = normalizedUrl(item?.url || item?.link || item?.href || '');
  return {
    title: truncate(item?.title || item?.name || '', 500),
    url,
    snippet: truncate(item?.snippet || item?.description || item?.content || '', 2500),
    source: provider,
    type: item?.type || 'web',
    publishedAt: item?.publishedAt || item?.published_at || null,
    semanticSearchScore: item?.score ?? item?.relevanceScore ?? item?.semanticSearchScore ?? null,
    rawContent: truncate(item?.raw_content || item?.rawContent || '', MAX_SEARCH_CONTENT_CHARS),
    pageContent: truncate(item?.pageContent || '', MAX_SEARCH_CONTENT_CHARS),
    contentStatus: item?.contentStatus || '',
    contentMethod: item?.contentMethod || '',
    contentSourceUrl: item?.contentSourceUrl || '',
    contentConfidence: item?.contentConfidence ?? null,
    publisherUrl: item?.publisherUrl || null,
  };
}

function dedupeCandidates(candidates) {
  const map = new Map();
  for (const raw of candidates) {
    const c = normalizeProviderCandidate(raw, raw?.source || 'candidate');
    if (!c.url || urlBlocked(c.url)) continue;
    const key = normalizedUrl(c.url);
    const existing = map.get(key);
    if (!existing) { map.set(key, c); continue; }
    const merged = {
      ...existing,
      ...c,
      title: c.title || existing.title,
      snippet: (c.snippet || '').length > (existing.snippet || '').length ? c.snippet : existing.snippet,
      source: existing.source === 'candidate' ? c.source : existing.source,
    };
    if (isRealSourceContent(c)) Object.assign(merged, c);
    map.set(key, merged);
  }
  return [...map.values()];
}

async function tavilySearch(query, apiKey, deadline, searchVariantIndex = 0) {
  if (!apiKey || left(deadline) < 700) return { ok: false, results: [], error: 'NO_BUDGET' };
  const timeout = Math.min(DEFAULT_TAVILY_TIMEOUT_MS, Math.max(600, left(deadline) - 120));
  const planTopic = /\b(latest|today|recent|breaking|news)\b/i.test(query) ? 'news' : 'general';
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch('https://api.tavily.com/search', {
      method: 'POST',
      signal: controller.signal,
      headers: { 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({
        api_key: apiKey,
        query,
        topic: planTopic,
        search_depth: searchVariantIndex === 0 ? 'advanced' : 'basic',
        max_results: TAVILY_RESULTS_PER_CALL,
        include_answer: false,
        include_raw_content: true,
        include_images: false,
      }),
    });
    if (!response.ok) throw new Error(`TAVILY_HTTP_${response.status}`);
    const data = await response.json();
    const rows = Array.isArray(data?.results) ? data.results : [];
    return {
      ok: true,
      answer: data?.answer || null,
      results: rows.map(x => normalizeProviderCandidate(x, 'tavily')).filter(x => x.url),
    };
  } catch (error) {
    return { ok: false, results: [], error: error?.message || 'TAVILY_FAILED' };
  } finally { clearTimeout(timer); }
}

async function gatherTavily(queryVariants, deadline) {
  const keys = getEnvKeys('TAVILY_API_KEY').slice(0, MAX_TAVILY_CALLS);
  if (!keys.length || !queryVariants.length || left(deadline) < 900) return { results: [], calls: 0, succeeded: 0 };
  const calls = keys.map((key, i) => tavilySearch(queryVariants[Math.min(i, queryVariants.length - 1)], key, deadline, i));
  const rows = await Promise.allSettled(calls);
  const results = [];
  let succeeded = 0;
  for (const row of rows) {
    if (row.status !== 'fulfilled') continue;
    if (row.value?.ok) succeeded++;
    results.push(...(row.value?.results || []));
  }
  return { results: dedupeCandidates(results), calls: keys.length, succeeded };
}

function materializeSuppliedContent(candidate, plan) {
  const supplied = candidate.pageContent || candidate.rawContent || '';
  if (!supplied || supplied.length < 350) return null;
  const method = candidate.rawContent ? 'tavily-raw-content' : candidate.contentMethod || 'supplied-content';
  const validation = validateSourceContent(supplied, candidate, plan, {
    title: candidate.title,
    contentType: method === 'tavily-raw-content' ? 'text/plain' : 'text/plain',
  });
  if (!validation.valid) return null;
  return {
    ...candidate,
    pageContent: cleanContent(supplied),
    extractedText: cleanContent(supplied),
    contentStatus: method === 'tavily-raw-content' ? 'tavily-raw-content' : 'full',
    contentMethod: method,
    contentSourceUrl: candidate.url,
    contentLength: cleanContent(supplied).length,
    contentConfidence: validation.quality,
    contentTargetMatched: true,
    contentTitleSimilarity: validation.targetSimilarity,
    contentConceptCoverage: validation.conceptCoverage,
    validatedBy: 'algorithm-supplied-content-check',
  };
}

function chooseRelevantForContent(ranked, count, plan) {
  const high = ranked.filter(r => r._relevance.score >= 42);
  const medium = ranked.filter(r => r._relevance.score >= 28);
  let pool = high.length >= Math.min(count, 8) ? [...high, ...medium.filter(x => !high.includes(x))] : medium;
  if (!pool.length) pool = ranked.filter(r => r._relevance.score >= 20);
  if (!pool.length) pool = ranked.slice(0, Math.max(count * 2, count));
  const unique = [];
  const seen = new Set();
  for (const r of pool) {
    const key = normalizedUrl(r.url);
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(r);
    if (unique.length >= Math.min(MAX_CANDIDATES, Math.max(count * 2, 24))) break;
  }
  return unique;
}

export async function enrichCandidates(candidates, queryOrPlan, options = {}) {
  const started = Date.now();
  const budgetMs = safeInteger(options.budgetMs, DEFAULT_BUDGET_MS, 1200, 9_000);
  const deadline = started + budgetMs;
  const count = safeInteger(options.count, 10, 1, MAX_RESULTS);
  const plan = typeof queryOrPlan === 'string' ? analyzeQuery(queryOrPlan, options) : (queryOrPlan || analyzeQuery('', options));
  const requireRealContent = options.requireRealContent == null ? true : String(options.requireRealContent).toLowerCase() !== 'false';

  let deduped = dedupeCandidates(candidates || []);
  let ranked = rankCandidates(deduped, plan, { keepWeak: true });

  // Content already supplied by Tavily or a trusted upstream source is the fastest lane.
  const supplied = [];
  const needFetch = [];
  for (const r of chooseRelevantForContent(ranked, count, plan)) {
    const ready = materializeSuppliedContent(r, plan);
    if (ready) supplied.push(ready);
    else needFetch.push(r);
  }

  // Direct live page fetches happen concurrently for the entire relevant pool.
  const fetchTarget = Math.min(40, Math.max(count + 12, 24));
  const directTargets = needFetch.slice(0, fetchTarget);
  const directRows = await mapConcurrent(directTargets, CONTENT_CONCURRENCY, r => fetchDirectContent(r, plan, deadline));
  let enriched = [...supplied, ...directRows.filter(r => isRealSourceContent(r))];

  // Fast recovery wave only for candidates that were relevant but blocked/empty/JS-only.
  if (enriched.length < count && left(deadline) > 900) {
    const already = new Set(enriched.map(x => normalizedUrl(x.url)));
    const recovery = directRows.filter(r => !already.has(normalizedUrl(r.url)) && !isRealSourceContent(r));
    const recoveryRows = await mapConcurrent(recovery.slice(0, Math.min(40, count + 12)), FALLBACK_CONCURRENCY, r => readerContent(r, plan, deadline));
    enriched.push(...recoveryRows.filter(r => isRealSourceContent(r)));
  }

  // If page access was poor and Tavily keys exist, do one precise backfill search
  // using the algorithm-generated variants. This is additive, not a replacement.
  const queryVariants = buildPreciseQueries(plan.query, { plan });
  let tavilyMeta = { calls: 0, succeeded: 0 };
  if (enriched.length < count && left(deadline) > 850 && getEnvKeys('TAVILY_API_KEY').length) {
    const tv = await gatherTavily(queryVariants.slice(0, 2), deadline);
    tavilyMeta = { calls: tv.calls, succeeded: tv.succeeded };
    const extraRanked = rankCandidates(tv.results, plan, { keepWeak: true });
    const existing = new Set(enriched.map(x => normalizedUrl(x.url)));
    const extraTargets = chooseRelevantForContent(extraRanked.filter(x => !existing.has(normalizedUrl(x.url))), count - enriched.length, plan);
    const extraRows = await mapConcurrent(extraTargets, CONTENT_CONCURRENCY, async r => {
      const ready = materializeSuppliedContent(r, plan);
      if (ready) return ready;
      return fetchDirectContent(r, plan, deadline);
    });
    enriched.push(...extraRows.filter(r => isRealSourceContent(r)));
  }

  const finalRanked = rankCandidates(enriched, plan, { keepWeak: false });
  const diversified = diversify(finalRanked, count, plan);
  const final = diversified.slice(0, count).map((r, i) => {
    const { _relevance, _sourceIndex, ...source } = r;
    const content = cleanContent(source.pageContent || source.extractedText || '');
    return {
      rank: i + 1,
      title: truncate(source.title || 'Untitled', 300),
      url: source.url,
      domain: normalizedHost(source.url),
      type: source.type || (isDoc(source.url) ? 'doc' : isVideo(source.url) ? 'video' : isGov(source.url) ? 'gov' : 'web'),
      source: source.source || 'algorithm',
      snippet: truncate(source.snippet || '', 1200),
      publishedAt: source.publishedAt || null,
      pageContent: content,
      extractedText: content,
      contentAvailable: Boolean(content),
      contentStatus: source.contentStatus || 'full',
      contentMethod: source.contentMethod || 'algorithm',
      contentLength: content.length,
      contentConfidence: Number(source.contentConfidence ?? _relevance.score / 100).toFixed(2),
      contentSourceUrl: source.contentSourceUrl || source.url,
      contentTargetMatched: Boolean(source.contentTargetMatched),
      contentTitleSimilarity: Number(source.contentTitleSimilarity ?? _relevance.titleCoverage).toFixed(3),
      contentConceptCoverage: Number(source.contentConceptCoverage ?? _relevance.conceptCoverage).toFixed(3),
      relevanceScore: Math.round(_relevance.score),
      relevanceBand: _relevance.band,
      relevance: {
        titleCoverage: _relevance.titleCoverage,
        bodyCoverage: _relevance.bodyCoverage,
        conceptCoverage: _relevance.conceptCoverage,
        matchedConcepts: _relevance.hits,
        missingConcepts: _relevance.missing,
        exactPhrase: _relevance.phraseExact,
        mismatchTerms: _relevance.mismatchTerms,
      },
      verified: true,
      validatedBy: source.validatedBy || 'algorithm',
      verificationMethod: source.verificationMethod || null,
      httpStatus: source.httpStatus || null,
      contentType: source.contentType || null,
      contentError: null,
      contentForAI: `SOURCE_URL: ${source.contentSourceUrl || source.url}\nTITLE: ${truncate(source.title || '', 300)}\nCONTENT_STATUS: ${source.contentStatus || 'full'}\n\n${content}`,
    };
  });

  const result = {
    ok: true,
    version: VERSION,
    query: plan.query,
    requestedResults: count,
    returnedResults: final.length,
    queryPlan: {
      preciseQueries: queryVariants,
      concepts: plan.concepts.map(x => x.id),
      anchors: plan.anchors,
      type: plan.type,
      dateIntent: plan.dateIntent,
      flags: plan.flags,
    },
    contentPolicy: requireRealContent
      ? 'Every final source contains validated source-page content; snippets are discovery-only and are never promoted to pageContent.'
      : 'Fallback content may be permitted by caller.',
    tavily: tavilyMeta,
    latencyMs: Date.now() - started,
    results: final,
    warnings: [],
  };

  if (requireRealContent && final.length < count) result.warnings.push(`Only ${final.length} relevant sources produced validated live content within the algorithm budget; ${count} requested.`);
  if (!final.length) result.warnings.push('No candidate met the content and relevance contract within the available budget. No fake page content was emitted.');
  return result;
}

export async function runAlgorithm(input = {}) {
  const started = Date.now();
  const query = truncate(String(input.query || input.q || '').trim(), MAX_QUERY_LEN);
  if (!query) throw new Error('MISSING_QUERY');
  const count = safeInteger(input.count ?? input.limit, 10, 1, MAX_RESULTS);
  const options = { ...input, count };
  const plan = analyzeQuery(query, options);
  const key = JSON.stringify({ query, count, type: options.type || '', mode: options.mode || 'auto', requireRealContent: options.requireRealContent ?? true });
  const cached = cacheGet(SEARCH_CACHE, key, SEARCH_CACHE_TTL_MS);
  if (cached) return { ...cached, cached: true, latencyMs: Date.now() - started, generatedAt: nowIso() };

  let candidates = Array.isArray(input.candidates)
    ? input.candidates
    : Array.isArray(input.sources)
      ? input.sources
      : [];

  let discovery = { calls: 0, succeeded: 0 };
  if (!candidates.length && left(started + DEFAULT_BUDGET_MS) > 1_000) {
    const variants = buildPreciseQueries(query, { plan });
    const tv = await gatherTavily(variants.slice(0, 2), started + DEFAULT_BUDGET_MS);
    candidates = tv.results;
    discovery = { calls: tv.calls, succeeded: tv.succeeded };
  }

  const output = await enrichCandidates(candidates, plan, options);
  output.generatedAt = nowIso();
  output.discovery = discovery;
  output.latencyMs = Date.now() - started;
  cacheSet(SEARCH_CACHE, key, output);
  return output;
}

function corsHeaders() {
  return {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store, no-transform',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type, authorization, x-arix-search-key',
    'x-arix-algorithm-version': VERSION,
  };
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), { status, headers: corsHeaders() });
}

async function readInput(req) {
  const url = new URL(req.url);
  if (req.method === 'GET') return Object.fromEntries(url.searchParams.entries());
  const raw = await req.text();
  if (raw.length > 100_000) throw new Error('REQUEST_BODY_TOO_LARGE');
  if (!raw) return {};
  try { return JSON.parse(raw); } catch { return Object.fromEntries(new URLSearchParams(raw).entries()); }
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') return jsonResponse({ ok: true, version: VERSION });
  if (!['GET', 'POST'].includes(req.method)) return jsonResponse({ ok: false, version: VERSION, error: 'METHOD_NOT_ALLOWED' }, 405);
  try {
    const input = await readInput(req);
    return jsonResponse(await runAlgorithm(input));
  } catch (error) {
    return jsonResponse({ ok: false, version: VERSION, error: error?.message || 'ALGORITHM_FAILED', latencyMs: null }, error?.message === 'MISSING_QUERY' ? 400 : 500);
  }
}

// Minimal internal invariants used by local tests and by future crawler integration.
export const ALGORITHM_CONTRACT = Object.freeze({
  version: VERSION,
  maxResults: MAX_RESULTS,
  realContentMinimumChars: 350,
  defaultBudgetMs: DEFAULT_BUDGET_MS,
  contentConcurrency: CONTENT_CONCURRENCY,
  hardRelevanceFloor: 24,
  searchCacheTtlMs: SEARCH_CACHE_TTL_MS,
  contentCacheTtlMs: CONTENT_CACHE_TTL_MS,
});
