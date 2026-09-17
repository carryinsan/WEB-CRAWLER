/*
 * ArixAI Live Web Search / Crawler
 * Vercel Edge Function — single-file, dependency-free, ESM.
 *
 * File: api/crawler.js
 *
 * Compatibility:
 * - Preserves the existing GET/POST request contract and response fields.
 * - Preserves mode/type/count/deep/verify/ai/commonCrawl controls.
 * - Adds smarter freshness/date handling, publisher URL resolution,
 *   source diversity, deadline-aware execution, and streaming heartbeats.
 *
 * The deadline strategy is deliberately not a timeout bypass or restriction evasion:
 * it uses Vercel-supported streaming to start the response immediately and keep the
 * stream active while expensive optional work completes, while every upstream fetch
 * still has a bounded timeout and the search pipeline has a hard safety budget.
 */

export const runtime = 'edge';
export const config = { runtime: 'edge' };

const VERSION = 'arix-crawler-1.0.0';
const MAX_RESULTS = 40;
const DEFAULT_RESULTS = 10;
const MAX_QUERY_LEN = 500;
const SEARCH_TIMEOUT_MS = 4500;
const PAGE_TIMEOUT_MS = 5000;
const MAX_PAGE_BYTES = 800_000;
const MAX_TEXT_CHARS = 9000;
const DEFAULT_VERIFY = 8;
const DEEP_VERIFY = 12;
const MAX_ENGINE_REQUESTS = 12;

// Supported Edge streaming allows the function to keep a response stream active.
// Keep the internal work budget conservative so the function still fails safely.
const STREAM_HEARTBEAT_MS = 4000;
const SEARCH_WORK_BUDGET_MS = 105_000;
const AI_PLAN_TIMEOUT_MS = 3500;
const AI_RERANK_TIMEOUT_MS = 4500;
const NEWS_RESOLVE_TIMEOUT_MS = 1800;
const COMMON_CRAWL_TIMEOUT_MS = 3000;
const MAX_NEWS_RESOLVES = 20;
const MAX_CC_LOOKUPS = 4;
const NORMAL_VERIFY_CAP = 6;
const DEEP_VERIFY_CAP = 10;

const USER_AGENT =
  'Mozilla/5.0 (compatible; ArixAI-LiveSearch/1.1; +https://lexis-ai-chatini.vercel.app/)';

const COMMON_CRAWL_INDEXES = [
  'CC-MAIN-2026-34',
  'CC-MAIN-2026-30',
  'CC-MAIN-2026-21',
];

const GOV_DOMAINS = [
  'gov.in',
  'nic.in',
  'mygov.in',
  'india.gov.in',
  'pib.gov.in',
  'mca.gov.in',
  'gst.gov.in',
  'incometax.gov.in',
  'msme.gov.in',
  'education.gov.in',
  'meity.gov.in',
  'rbi.org.in',
  'sebi.gov.in',
  'supremecourt.gov.in',
  'indiacode.nic.in',
];

const TRUSTED_INTERNATIONAL = [
  'nasa.gov',
  'who.int',
  'un.org',
  'europa.eu',
  'oecd.org',
  'worldbank.org',
  'imf.org',
  'ietf.org',
  'w3.org',
  'mozilla.org',
  'developer.mozilla.org',
];

const NEWS_QUERY_HINTS = [
  'news',
  'latest',
  'today',
  'recent',
  'breaking',
  'update',
  'updates',
  'happened',
  'announced',
  'announcement',
  'this week',
  'yesterday',
];

const VIDEO_QUERY_HINTS = [
  'video',
  'videos',
  'watch',
  'youtube',
  'interview',
  'podcast',
  'explained',
  'tutorial',
];

const DOC_QUERY_HINTS = [
  'pdf',
  'documentation',
  'docs',
  'manual',
  'report',
  'paper',
  'research paper',
  'whitepaper',
  'specification',
  'spec',
];

const MONTHS = {
  january: 0, jan: 0,
  february: 1, feb: 1,
  march: 2, mar: 2,
  april: 3, apr: 3,
  may: 4,
  june: 5, jun: 5,
  july: 6, jul: 6,
  august: 7, aug: 7,
  september: 8, sep: 8, sept: 8,
  october: 9, oct: 9,
  november: 10, nov: 10,
  december: 11, dec: 11,
};

const STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'what', 'when', 'where', 'how',
  'why', 'who', 'which', 'about', 'into', 'near', 'over', 'under', 'latest', 'news',
  'today', 'recent', 'current', 'update', 'updates', 'august', 'september', 'october',
  'january', 'february', 'march', 'april', 'june', 'july', 'november', 'december',
  '2025', '2026', '2027', '2028',
]);

const HTML_ENTITY_RE = /&(#x?[0-9a-f]+|[a-z][a-z0-9]+);/gi;

function decodeHtml(value = '') {
  return String(value)
    .replace(HTML_ENTITY_RE, (_, code) => {
      const c = code.toLowerCase();
      const map = {
        amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ',
        ndash: '–', mdash: '—', hellip: '…', laquo: '«', raquo: '»',
        rsquo: '’', lsquo: '‘', rdquo: '”', ldquo: '“', bull: '•',
      };
      if (map[c] != null) return map[c];
      if (c.startsWith('#x')) {
        const n = parseInt(c.slice(2), 16);
        return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : _;
      }
      if (c.startsWith('#')) {
        const n = parseInt(c.slice(1), 10);
        return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : _;
      }
      return _;
    })
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function stripTags(html = '') {
  return decodeHtml(
    String(html)
      .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
      .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
      .replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
  );
}

function absoluteUrl(raw, base = 'https://example.com/') {
  try {
    return new URL(decodeHtml(raw), base).href;
  } catch {
    return null;
  }
}

function cleanUrl(raw, base) {
  const u = absoluteUrl(raw, base);
  if (!u) return null;
  try {
    const parsed = new URL(u);
    parsed.hash = '';
    for (const key of [...parsed.searchParams.keys()]) {
      if (/^(utm_|gclid$|fbclid$|ref$|referrer$|cmpid$|src$)/i.test(key)) {
        parsed.searchParams.delete(key);
      }
    }
    return parsed.href;
  } catch {
    return null;
  }
}

function hostname(url) {
  try { return new URL(url).hostname.toLowerCase(); } catch { return ''; }
}

function isGovUrl(url) {
  const h = hostname(url).replace(/^www\./, '');
  return GOV_DOMAINS.some(d => h === d || h.endsWith(`.${d}`));
}

function isTrustedInternational(url) {
  const h = hostname(url).replace(/^www\./, '');
  return TRUSTED_INTERNATIONAL.some(d => h === d || h.endsWith(`.${d}`));
}

function isLikelyVideoUrl(url) {
  const h = hostname(url);
  return h.includes('youtube.com') || h.includes('youtu.be') || h.includes('vimeo.com') ||
    /\.(mp4|webm|mov)(\?|$)/i.test(url);
}

function isDocUrl(url) {
  return /\.(pdf|docx?|xlsx?|pptx?)(\?|$)/i.test(url) || /\b(pdf|docs?|documentation)\b/i.test(url);
}

function inferType(result) {
  if (result.type) return result.type;
  if (isGovUrl(result.url)) return 'gov';
  if (isLikelyVideoUrl(result.url)) return 'video';
  if (isDocUrl(result.url)) return 'doc';
  return 'web';
}

function nowIso() { return new Date().toISOString(); }

function truncate(s, n) {
  const v = String(s || '').trim();
  return v.length <= n ? v : v.slice(0, Math.max(0, n - 1)) + '…';
}

function safeInteger(value, fallback, min, max) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function remainingMs(deadline) {
  return Math.max(0, deadline - Date.now());
}

function boundedTimeout(requested, deadline, floor = 250) {
  const remaining = remainingMs(deadline);
  if (remaining <= floor) return 0;
  return Math.min(requested, Math.max(floor, remaining - 150));
}

async function fetchText(url, {
  timeout = SEARCH_TIMEOUT_MS,
  headers = {},
  maxBytes = MAX_PAGE_BYTES,
  deadline = Date.now() + timeout,
} = {}) {
  const effectiveTimeout = boundedTimeout(timeout, deadline, 250);
  if (!effectiveTimeout) throw new Error('BUDGET_EXHAUSTED');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), effectiveTimeout);
  try {
    const res = await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'user-agent': USER_AGENT,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.5',
        'accept-language': 'en-IN,en;q=0.9',
        ...headers,
      },
    });
    if (!res.ok) throw new Error(`HTTP_${res.status}`);
    const reader = res.body?.getReader?.();
    if (!reader) return await res.text();
    const chunks = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) break;
      chunks.push(value);
    }
    const bytes = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
    let offset = 0;
    for (const c of chunks) { bytes.set(c, offset); offset += c.byteLength; }
    return new TextDecoder('utf-8', { fatal: false }).decode(bytes);
  } finally {
    clearTimeout(timer);
  }
}

async function fetchResponse(url, { timeout = SEARCH_TIMEOUT_MS, headers = {}, deadline = Date.now() + timeout } = {}) {
  const effectiveTimeout = boundedTimeout(timeout, deadline, 250);
  if (!effectiveTimeout) throw new Error('BUDGET_EXHAUSTED');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), effectiveTimeout);
  try {
    return await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'user-agent': USER_AGENT,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.5',
        'accept-language': 'en-IN,en;q=0.9',
        ...headers,
      },
    });
  } finally {
    clearTimeout(timer);
  }
}

function parseTitleFromHtml(html) {
  return decodeHtml((html.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [,''])[1]);
}

function parseMeta(html, name) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`<meta[^>]+(?:name|property)=["']${escaped}["'][^>]*content=["']([\\s\\S]*?)["'][^>]*>`, 'i');
  const re2 = new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+(?:name|property)=["']${escaped}["'][^>]*>`, 'i');
  return decodeHtml((html.match(re) || html.match(re2) || [,''])[1]);
}

function extractVisibleText(html) {
  const main = html.match(/<(?:main|article|body)\b[^>]*>([\s\S]*?)<\/(?:main|article|body)>/i);
  const source = main ? main[1] : html;
  const text = stripTags(source)
    .replace(/\b(function|var|const|let)\s+[^;]{0,120};?/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
  return truncate(text, MAX_TEXT_CHARS);
}

function parseDateCandidate(html) {
  const candidates = [
    parseMeta(html, 'article:published_time'),
    parseMeta(html, 'article:modified_time'),
    parseMeta(html, 'datePublished'),
    parseMeta(html, 'dateModified'),
    parseMeta(html, 'pubdate'),
    parseMeta(html, 'date'),
    parseMeta(html, 'parsely-pub-date'),
    parseMeta(html, 'dc.date'),
    ((html.match(/<time[^>]+datetime=["']([^"']+)["'][^>]*>/i) || [,''])[1]),
  ].filter(Boolean);
  for (const c of candidates) {
    const t = Date.parse(c);
    if (!Number.isNaN(t)) return new Date(t).toISOString();
  }
  return null;
}

function parseCanonical(html, baseUrl) {
  const raw = (html.match(/<link[^>]+rel=["'][^"']*canonical[^"']*["'][^>]*href=["']([^"']+)["'][^>]*>/i) || [,''])[1] ||
    (html.match(/<link[^>]+href=["']([^"']+)["'][^>]*rel=["'][^"']*canonical[^"']*["'][^>]*>/i) || [,''])[1];
  return raw ? cleanUrl(raw, baseUrl) : null;
}

function parseBing(html) {
  const out = [];
  const blocks = html.match(/<li[^>]+class=["'][^"']*b_algo[^"']*["'][\s\S]*?<\/li>/gi) || [];
  for (const block of blocks) {
    const m = block.match(/<h2[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/i);
    if (!m) continue;
    const url = cleanUrl(m[1], 'https://www.bing.com/');
    if (!url) continue;
    const snippet = stripTags((block.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [,''])[1]);
    out.push({ title: stripTags(m[2]), url, snippet, source: 'bing', type: 'web' });
  }
  return out;
}

function parseMojeek(html) {
  const out = [];
  const anchors = [...html.matchAll(/<a[^>]+class=["'](?:ob|title|result)[^"']*["'][^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)];
  for (const m of anchors) {
    const url = cleanUrl(m[1], 'https://www.mojeek.com/');
    if (!url || /mojeek\.com\/search/i.test(url)) continue;
    out.push({ title: stripTags(m[2]), url, snippet: '', source: 'mojeek', type: 'web' });
  }
  return out.slice(0, 20);
}

function parseYahoo(html) {
  const out = [];
  const blocks = html.match(/<div[^>]+class=["'][^"']*comp[^"']*["'][\s\S]*?<\/div>/gi) || [];
  for (const block of blocks) {
    const m = block.match(/<h3[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/i);
    if (!m) continue;
    const url = cleanUrl(m[1], 'https://search.yahoo.com/');
    if (!url || /search\.yahoo\.com\/search/i.test(url)) continue;
    const snippet = stripTags((block.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [,''])[1]);
    out.push({ title: stripTags(m[2]), url, snippet, source: 'yahoo', type: 'web' });
  }
  return out.slice(0, 20);
}

function parseGoogleWeb(html, source = 'google', forcedType = 'web') {
  const out = [];
  const seen = new Set();
  const anchors = [...html.matchAll(/<a[^>]+href=["'](?:\/url\?q=|)([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)];
  for (const m of anchors) {
    let raw = m[1];
    if (raw.startsWith('http://') || raw.startsWith('https://')) {
      // direct URL
    } else if (raw.startsWith('/url?q=')) {
      raw = raw.slice(7);
    } else {
      continue;
    }
    const url = cleanUrl(raw, 'https://www.google.com/');
    if (!url || /google\.(com|co\.in)\/search/i.test(url)) continue;
    const title = stripTags(m[2]);
    if (!title || title.length < 3) continue;
    const key = normalizedKey(url);
    if (seen.has(key)) continue;
    seen.add(key);
    if (forcedType === 'video' && !/youtube\.com|youtu\.be|vimeo\.com/i.test(url)) continue;
    out.push({ title, url, snippet: '', source, type: forcedType });
    if (out.length >= 20) break;
  }
  return out;
}

function parseDuckDuckGo(html) {
  const out = [];
  const anchors = [...html.matchAll(/<a[^>]+class=["'][^"']*result__a[^"']*["'][^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)];
  for (const m of anchors) {
    const url = cleanUrl(m[1], 'https://html.duckduckgo.com/');
    if (!url) continue;
    const idx = m.index || 0;
    const tail = html.slice(idx, idx + 5000);
    const snippet = stripTags((tail.match(/class=["'][^"']*result__snippet[^"']*[^>]*>([\s\S]*?)(?:<\/a>|<\/span>|<\/div>)/i) || [,''])[1]);
    out.push({ title: stripTags(m[2]), url, snippet, source: 'duckduckgo', type: 'web' });
  }
  return out;
}

function parseGoogleNewsRss(xml) {
  const out = [];
  const items = xml.match(/<item>[\s\S]*?<\/item>/gi) || [];
  for (const item of items) {
    const title = stripTags((item.match(/<title>([\s\S]*?)<\/title>/i) || [,''])[1]);
    const link = stripTags((item.match(/<link>([\s\S]*?)<\/link>/i) || [,''])[1]);
    const pubDate = stripTags((item.match(/<pubDate>([\s\S]*?)<\/pubDate>/i) || [,''])[1]);
    const desc = stripTags((item.match(/<description>([\s\S]*?)<\/description>/i) || [,''])[1]);
    const sourceMatch = item.match(/<source[^>]*url=["']([^"']+)["'][^>]*>([\s\S]*?)<\/source>/i) ||
      item.match(/<source[^>]*>([\s\S]*?)<\/source>/i);
    const sourceName = stripTags(sourceMatch?.[2] || sourceMatch?.[1] || '');
    const sourceUrl = sourceMatch?.[1] ? cleanUrl(sourceMatch[1], 'https://news.google.com/') : null;
    const url = cleanUrl(link, 'https://news.google.com/');
    if (!title || !url) continue;
    out.push({
      title,
      url,
      snippet: desc,
      publishedAt: Number.isNaN(Date.parse(pubDate)) ? null : new Date(pubDate).toISOString(),
      source: sourceName ? `google-news:${sourceName}` : 'google-news',
      publisherUrl: sourceUrl,
      type: 'news',
    });
  }
  return out;
}

function extractYouTubeTitle(window) {
  const run = window.match(/"title":\{"runs":\[\{"text":"((?:\\.|[^"\\])*)"/i);
  if (run?.[1]) return decodeHtml(run[1].replace(/\\"/g, '"').replace(/\\u0026/g, '&'));
  const simple = window.match(/"title":\{"simpleText":"((?:\\.|[^"\\])*)"/i);
  if (simple?.[1]) return decodeHtml(simple[1].replace(/\\"/g, '"').replace(/\\u0026/g, '&'));
  return '';
}

function parseYoutube(html) {
  const out = [];
  const seen = new Set();
  const rendererMatches = [...html.matchAll(/"videoRenderer":\{[\s\S]*?"videoId":"([\w-]{6,20})"[\s\S]*?\}/g)];
  for (const m of rendererMatches) {
    const id = m[1];
    if (seen.has(id)) continue;
    seen.add(id);
    const idx = m.index || 0;
    const window = html.slice(Math.max(0, idx - 200), Math.min(html.length, idx + 5000));
    out.push({
      title: extractYouTubeTitle(window) || `YouTube video ${id}`,
      url: `https://www.youtube.com/watch?v=${id}`,
      snippet: 'YouTube video result',
      source: 'youtube',
      type: 'video',
    });
    if (out.length >= 20) break;
  }
  if (!out.length) {
    const matches = [...html.matchAll(/"videoId":"([\w-]{6,20})"/g)];
    for (const m of matches) {
      const id = m[1];
      if (seen.has(id)) continue;
      seen.add(id);
      const idx = m.index || 0;
      const window = html.slice(Math.max(0, idx - 500), Math.min(html.length, idx + 2500));
      out.push({
        title: extractYouTubeTitle(window) || `YouTube video ${id}`,
        url: `https://www.youtube.com/watch?v=${id}`,
        snippet: 'YouTube video result',
        source: 'youtube',
        type: 'video',
      });
      if (out.length >= 20) break;
    }
  }
  return out;
}

function queryIntent(query, requestedType) {
  const q = query.toLowerCase();
  const hasAny = xs => xs.some(x => q.includes(x));
  const mixed = requestedType === 'mixed' || requestedType === 'all';
  const wantsNews = mixed || requestedType === 'news' || hasAny(NEWS_QUERY_HINTS);
  const wantsVideo = mixed || requestedType === 'video' || hasAny(VIDEO_QUERY_HINTS);
  const wantsDocs = mixed || requestedType === 'doc' || hasAny(DOC_QUERY_HINTS);
  const wantsGov = mixed || requestedType === 'gov' || /\b(india|indian|government|govt|ministry|scheme|gst|income tax|mca|rbi|sebi|law|act|notification|circular|policy)\b/i.test(q);
  return {
    type: requestedType || (wantsVideo ? 'video' : wantsNews ? 'news' : wantsDocs ? 'doc' : 'web'),
    wantsNews,
    wantsVideo,
    wantsGov,
    wantsDocs,
  };
}

function parseDateIntent(query) {
  const q = query.toLowerCase();
  const now = new Date();
  const latest = /\b(latest|today|current|recent|breaking|just in|this week|yesterday)\b/i.test(q);

  const monthPattern = new RegExp(`\\b(${Object.keys(MONTHS).join('|')})\\s+(20\\d{2})\\b`, 'i');
  const monthMatch = q.match(monthPattern);
  if (monthMatch) {
    const month = MONTHS[monthMatch[1].toLowerCase()];
    const year = Number(monthMatch[2]);
    const start = new Date(Date.UTC(year, month, 1));
    const end = new Date(Date.UTC(year, month + 1, 1));
    return {
      kind: 'explicit-month',
      start: start.toISOString(),
      end: end.toISOString(),
      label: `${monthMatch[1]} ${year}`,
      latest,
    };
  }

  const isoMonth = q.match(/\b(20\d{2})[-\/]([01]\d)\b/);
  if (isoMonth) {
    const year = Number(isoMonth[1]);
    const month = Number(isoMonth[2]) - 1;
    const start = new Date(Date.UTC(year, month, 1));
    const end = new Date(Date.UTC(year, month + 1, 1));
    return { kind: 'explicit-month', start: start.toISOString(), end: end.toISOString(), label: `${year}-${String(month + 1).padStart(2, '0')}`, latest };
  }

  const yearMatch = q.match(/\b(20\d{2})\b/);
  if (yearMatch && /(latest|news|update|events|during|in)\b/i.test(q)) {
    const year = Number(yearMatch[1]);
    const start = new Date(Date.UTC(year, 0, 1));
    const end = new Date(Date.UTC(year + 1, 0, 1));
    return { kind: 'explicit-year', start: start.toISOString(), end: end.toISOString(), label: String(year), latest };
  }

  if (/\btoday\b/i.test(q)) {
    const start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    return { kind: 'today', start: start.toISOString(), end: now.toISOString(), label: 'today', latest: true };
  }

  if (/\byesterday\b/i.test(q)) {
    const end = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const start = new Date(end.getTime() - 86400000);
    return { kind: 'yesterday', start: start.toISOString(), end: end.toISOString(), label: 'yesterday', latest: true };
  }

  if (/\bthis week\b/i.test(q)) {
    return { kind: 'recent-window', start: new Date(Date.now() - 7 * 86400000).toISOString(), end: now.toISOString(), label: 'last 7 days', latest: true };
  }

  if (/\blast\s+30\s+days?\b/i.test(q)) {
    return { kind: 'recent-window', start: new Date(Date.now() - 30 * 86400000).toISOString(), end: now.toISOString(), label: 'last 30 days', latest: true };
  }

  if (latest) {
    return { kind: 'latest', start: new Date(Date.now() - 30 * 86400000).toISOString(), end: now.toISOString(), label: 'recent', latest: true };
  }

  return { kind: 'none', start: null, end: null, label: 'any', latest: false };
}

function buildSearchQueries(query, intent, mode, dateIntent) {
  const qs = new Set([query]);
  if (intent.wantsNews) qs.add(`${query} latest news`);
  if (dateIntent.kind === 'explicit-month' || dateIntent.kind === 'explicit-year') {
    qs.add(`${query} after:${dateIntent.start.slice(0, 10)} before:${dateIntent.end.slice(0, 10)}`);
  }
  if (mode === 'deep') {
    qs.add(`${query} latest update`);
    qs.add(`${query} official source`);
  }
  if (intent.wantsGov || mode === 'gov') {
    qs.add(`${query} site:gov.in`);
    qs.add(`${query} site:nic.in`);
    qs.add(`${query} site:india.gov.in`);
  }
  if (intent.wantsDocs || mode === 'doc') qs.add(`${query} filetype:pdf`);
  return [...qs].slice(0, 6);
}

function buildEngineUrls(q, intent) {
  const encoded = encodeURIComponent(q);
  const urls = [
    { provider: 'bing', type: 'web', url: `https://www.bing.com/search?q=${encoded}&count=20&setlang=en-IN&cc=in` },
    { provider: 'duckduckgo', type: 'web', url: `https://html.duckduckgo.com/html/?q=${encoded}&kl=in-en` },
    { provider: 'mojeek', type: 'web', url: `https://www.mojeek.com/search?q=${encoded}` },
    { provider: 'yahoo', type: 'web', url: `https://search.yahoo.com/search?p=${encoded}` },
    { provider: 'google', type: 'web', url: `https://www.google.com/search?q=${encoded}&num=20&hl=en&gl=in` },
  ];
  if (intent.wantsNews) {
    urls.push({ provider: 'google-news', type: 'news', url: `https://news.google.com/rss/search?q=${encoded}&hl=en-IN&gl=IN&ceid=IN:en` });
  }
  if (intent.wantsVideo) {
    urls.push({ provider: 'youtube', type: 'video', url: `https://www.youtube.com/results?search_query=${encoded}&hl=en-IN` });
    urls.push({ provider: 'google-video', type: 'video', url: `https://www.google.com/search?q=${encodeURIComponent(`site:youtube.com ${q}`)}&num=20&hl=en&gl=in` });
  }
  return urls;
}

function prioritizeEngineRequests(requests, maxRequests) {
  // Provider-first round-robin: one query cannot monopolize the entire request cap,
  // and a single flaky engine cannot crowd out independent search surfaces.
  const byProvider = new Map();
  const providerOrder = [];
  for (const req of requests) {
    const provider = req.provider || 'unknown';
    if (!byProvider.has(provider)) {
      byProvider.set(provider, []);
      providerOrder.push(provider);
    }
    byProvider.get(provider).push(req);
  }
  const out = [];
  let cursor = 0;
  while (out.length < maxRequests && providerOrder.length) {
    let added = false;
    for (const provider of providerOrder) {
      const bucket = byProvider.get(provider);
      if (cursor < bucket.length && out.length < maxRequests) {
        out.push(bucket[cursor]);
        added = true;
      }
    }
    if (!added) break;
    cursor += 1;
  }
  return out;
}

async function discoverOne(engine, deadline) {
  try {
    const text = await fetchText(engine.url, {
      timeout: SEARCH_TIMEOUT_MS,
      maxBytes: 600_000,
      deadline,
    });
    let results = [];
    if (engine.provider === 'bing') results = parseBing(text);
    else if (engine.provider === 'duckduckgo') results = parseDuckDuckGo(text);
    else if (engine.provider === 'mojeek') results = parseMojeek(text);
    else if (engine.provider === 'yahoo') results = parseYahoo(text);
    else if (engine.provider === 'google') results = parseGoogleWeb(text);
    else if (engine.provider === 'google-video') results = parseGoogleWeb(text, 'google-video', 'video');
    else if (engine.provider === 'google-news') results = parseGoogleNewsRss(text);
    else if (engine.provider === 'youtube') results = parseYoutube(text);
    return { provider: engine.provider, results, ok: true };
  } catch (error) {
    return { provider: engine.provider, results: [], ok: false, error: error?.message || 'FETCH_FAILED' };
  }
}

function normalizedKey(url) {
  try {
    const u = new URL(url);
    const normalizedPath = u.pathname.replace(/\/{2,}/g, '/').replace(/\/$/, '');
    return `${u.hostname.toLowerCase().replace(/^www\./, '')}${normalizedPath}${u.search}`;
  } catch {
    return String(url || '').toLowerCase();
  }
}

function extractSearchTerms(query) {
  return query.toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .split(/\s+/)
    .filter(x => x.length > 2 && !STOPWORDS.has(x));
}

function scoreDate(publishedAt, dateIntent) {
  if (!publishedAt) return 0;
  const t = Date.parse(publishedAt);
  if (Number.isNaN(t)) return 0;
  if (dateIntent.start && dateIntent.end) {
    const start = Date.parse(dateIntent.start);
    const end = Date.parse(dateIntent.end);
    if (t >= start && t < end) return 16;
    if (dateIntent.kind === 'explicit-month' || dateIntent.kind === 'explicit-year') return -18;
  }
  if (dateIntent.latest) {
    const ageDays = Math.max(0, (Date.now() - t) / 86400000);
    if (ageDays <= 1) return 12;
    if (ageDays <= 7) return 9;
    if (ageDays <= 30) return 5;
    if (ageDays <= 90) return 0;
    if (ageDays <= 365) return -4;
    return -10;
  }
  return 0;
}

function scoreResult(r, query, intent, dateIntent) {
  const qTerms = extractSearchTerms(query);
  const title = (r.title || '').toLowerCase();
  const snippet = (r.snippet || '').toLowerCase();
  const url = (r.url || '').toLowerCase();
  const body = (r.extractedText || '').toLowerCase();
  let score = 0;
  let matched = 0;

  for (const token of qTerms) {
    if (title.includes(token)) { score += 5.0; matched += 1; }
    if (snippet.includes(token)) score += 1.5;
    if (body.includes(token)) score += 0.5;
    if (url.includes(token)) score += 0.5;
  }

  if (qTerms.length) score += Math.min(7, (matched / qTerms.length) * 7);
  if (intent.wantsGov && isGovUrl(r.url)) score += 10;
  if (intent.wantsNews && r.type === 'news') score += 8;
  if (intent.wantsVideo && r.type === 'video') score += 8;
  if (intent.wantsDocs && isDocUrl(r.url)) score += 6;
  if (isTrustedInternational(r.url)) score += 2.5;
  if (r.verified) score += 3;
  if (r.httpStatus === 200) score += 1;
  score += scoreDate(r.publishedAt, dateIntent);

  if (/login|signin|subscribe|advertis|cookie|enable javascript/i.test(`${title} ${snippet}`)) score -= 1.5;
  if (/^news\.google\.com$/i.test(hostname(r.url))) score -= 5;
  if (url.length > 220) score -= 0.25;

  return score;
}

function dedupeResults(results) {
  const map = new Map();
  for (const r of results) {
    if (!r?.url) continue;
    const key = normalizedKey(r.url);
    const existing = map.get(key);
    if (!existing ||
        (r.verified && !existing.verified) ||
        ((r.snippet || '').length > (existing.snippet || '').length)) {
      map.set(key, { ...existing, ...r });
    }
  }
  return [...map.values()];
}

function domainTrust(url) {
  const h = hostname(url).replace(/^www\./, '');
  if (!h) return 0.2;
  if (isGovUrl(url)) return 1.0;
  if (isTrustedInternational(url)) return 0.95;
  if (/\.edu(?:\.|$)/i.test(h)) return 0.95;
  if (/\.ac\.(?:in|uk|jp|nz)$/i.test(h)) return 0.95;
  if (/wikipedia\.org$/i.test(h)) return 0.8;
  if (/youtube\.com$|youtu\.be$/i.test(h)) return 0.65;
  if (/news\.google\.com$/i.test(h)) return 0.45;
  return 0.6;
}

function safeHttpUrl(url) {
  try {
    const u = new URL(url);
    if (!['http:', 'https:'].includes(u.protocol)) return false;
    const h = u.hostname.toLowerCase();
    if (!h || h === 'localhost' || h.endsWith('.localhost') || h === '0.0.0.0') return false;
    if (/^(127\.|10\.|192\.168\.|169\.254\.)/.test(h)) return false;
    if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(h)) return false;
    if (h === '::1' || h.startsWith('fc') || h.startsWith('fd')) return false;
    return true;
  } catch {
    return false;
  }
}

async function readBodyText(res, maxBytes, deadline) {
  const reader = res.body?.getReader?.();
  if (!reader) return await res.text();
  const chunks = [];
  let total = 0;
  while (true) {
    if (remainingMs(deadline) <= 100) break;
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) break;
    chunks.push(value);
  }
  const bytes = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
  let off = 0;
  for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
  return new TextDecoder().decode(bytes);
}

async function resolvePublisherUrl(result, deadline) {
  if (!/^news\.google\.com$/i.test(hostname(result.url))) return result;
  if (!/^\/rss\/articles\//i.test(new URL(result.url).pathname)) return result;
  try {
    const timeout = boundedTimeout(NEWS_RESOLVE_TIMEOUT_MS, deadline, 350);
    if (!timeout) return { ...result, verificationError: 'BUDGET_EXHAUSTED' };
    const res = await fetchResponse(result.url, {
      timeout,
      deadline,
      headers: { accept: 'text/html,application/xhtml+xml;q=0.8,*/*;q=0.1' },
    });
    const finalUrl = cleanUrl(res.url || result.url, result.url);
    try { await res.body?.cancel?.(); } catch {}
    if (finalUrl && hostname(finalUrl) && !/^news\.google\.com$/i.test(hostname(finalUrl))) {
      return { ...result, url: finalUrl, domain: hostname(finalUrl), publisherResolved: true };
    }
    return { ...result, publisherResolved: false };
  } catch (error) {
    return { ...result, publisherResolved: false, resolutionError: error?.message || 'RESOLVE_FAILED' };
  }
}

async function enrichResult(result, deadline) {
  if (!safeHttpUrl(result.url)) return { ...result, verified: false, verificationError: 'UNSAFE_URL' };
  try {
    const remaining = remainingMs(deadline);
    if (remaining < 500) return { ...result, verified: false, verificationError: 'BUDGET_EXHAUSTED' };
    const res = await fetchResponse(result.url, {
      timeout: Math.min(PAGE_TIMEOUT_MS, Math.max(700, remaining - 150)),
      deadline,
      headers: { accept: 'text/html,application/xhtml+xml;q=0.9,text/plain;q=0.8,*/*;q=0.2' },
    });
    const contentType = res.headers.get('content-type') || '';
    if (!res.ok) {
      try { await res.body?.cancel?.(); } catch {}
      return {
        ...result,
        verified: false,
        httpStatus: res.status,
        contentType,
        domain: hostname(res.url || result.url),
        trust: domainTrust(res.url || result.url),
      };
    }

    if (!/text\/html|application\/xhtml|text\/plain|application\/json|application\/pdf/i.test(contentType)) {
      try { await res.body?.cancel?.(); } catch {}
      return {
        ...result,
        verified: true,
        httpStatus: res.status,
        contentType,
        domain: hostname(res.url || result.url),
        trust: domainTrust(res.url || result.url),
      };
    }

    const text = await readBodyText(res, MAX_PAGE_BYTES, deadline);
    const finalUrl = cleanUrl(res.url || result.url, result.url) || result.url;
    if (/application\/pdf/i.test(contentType)) {
      return {
        ...result,
        url: finalUrl,
        domain: hostname(finalUrl),
        verified: true,
        httpStatus: res.status,
        contentType,
        trust: domainTrust(finalUrl),
        extractedText: null,
      };
    }

    if (/text\/html|application\/xhtml/i.test(contentType) || /<html\b/i.test(text)) {
      const canonical = parseCanonical(text, finalUrl);
      const canonicalUrl = canonical || finalUrl;
      const title = parseTitleFromHtml(text) || result.title;
      const description = parseMeta(text, 'description') || parseMeta(text, 'og:description') || result.snippet;
      const publishedAt = parseDateCandidate(text) || result.publishedAt || null;
      const bodyText = extractVisibleText(text);
      return {
        ...result,
        url: canonicalUrl,
        title: truncate(title, 300),
        snippet: truncate(description || result.snippet, 1000),
        publishedAt,
        extractedText: bodyText,
        verified: true,
        httpStatus: res.status,
        contentType,
        domain: hostname(canonicalUrl),
        trust: domainTrust(canonicalUrl),
      };
    }

    return {
      ...result,
      url: finalUrl,
      verified: true,
      httpStatus: res.status,
      contentType,
      domain: hostname(finalUrl),
      trust: domainTrust(finalUrl),
      extractedText: truncate(text, MAX_TEXT_CHARS),
    };
  } catch (error) {
    return {
      ...result,
      verified: false,
      verificationError: error?.message || 'ENRICH_FAILED',
      domain: hostname(result.url),
      trust: domainTrust(result.url),
    };
  }
}

async function resolveTopNews(results, deadline) {
  const candidates = results.filter(r => r.type === 'news').slice(0, MAX_NEWS_RESOLVES);
  if (!candidates.length || remainingMs(deadline) < 1000) return results;
  const resolved = await Promise.all(candidates.map(r => resolvePublisherUrl(r, deadline)));
  const byKey = new Map(candidates.map((r, i) => [normalizedKey(r.url), resolved[i]]));
  return results.map(r => byKey.get(normalizedKey(r.url)) || r);
}

function selectVerificationCandidates(results, count) {
  const out = [];
  const usedDomains = new Set();
  const usedUrls = new Set();
  const ranked = [...results].sort((a, b) => (b._score || 0) - (a._score || 0));
  for (const r of ranked) {
    if (out.length >= count) break;
    const key = normalizedKey(r.url);
    const domain = hostname(r.url);
    if (!safeHttpUrl(r.url) || usedUrls.has(key)) continue;
    if (!usedDomains.has(domain) || out.length >= Math.max(2, Math.ceil(count * 0.6))) {
      out.push(r);
      usedUrls.add(key);
      usedDomains.add(domain);
    }
  }
  return out;
}

function applyDateConstraint(results, dateIntent, requestedCount, warnings) {
  if (!dateIntent.start || !dateIntent.end) return results;
  const start = Date.parse(dateIntent.start);
  const end = Date.parse(dateIntent.end);
  const inRange = results.filter(r => {
    const t = r.publishedAt ? Date.parse(r.publishedAt) : NaN;
    return !Number.isNaN(t) && t >= start && t < end;
  });
  const exact = dateIntent.kind === 'explicit-month' || dateIntent.kind === 'explicit-year';
  const threshold = Math.min(5, Math.max(2, Math.ceil(requestedCount / 8)));
  if (exact && inRange.length >= threshold) {
    warnings.push(`Applied requested date window: ${dateIntent.label}.`);
    return inRange;
  }
  if (exact) {
    warnings.push(`Requested date window ${dateIntent.label} had only ${inRange.length} dated result(s); nearby dates were retained as fallback.`);
  }
  return results;
}

function freshnessBand(publishedAt) {
  if (!publishedAt) return 'unknown';
  const ageH = Math.max(0, (Date.now() - Date.parse(publishedAt)) / 36e5);
  if (ageH <= 24) return 'last_24h';
  if (ageH <= 168) return 'last_7d';
  if (ageH <= 720) return 'last_30d';
  if (ageH <= 8760) return 'last_year';
  return 'older';
}

function diversifyAndSelect(results, count, intent) {
  if (results.length <= count) return results;
  const pool = [...results].sort((a, b) => (b._score || 0) - (a._score || 0));
  const selected = [];
  const domainCounts = new Map();
  const typeCounts = new Map();
  const targetNewsShare = intent.wantsNews && !intent.wantsVideo && !intent.wantsDocs ? 0.72 : 0.5;

  while (selected.length < count && pool.length) {
    let bestIndex = 0;
    let bestAdjusted = -Infinity;
    for (let i = 0; i < pool.length; i++) {
      const r = pool[i];
      const domain = hostname(r.url);
      const type = r.type || 'web';
      const dPenalty = Math.min(6, (domainCounts.get(domain) || 0) * 2.25);
      const tCount = typeCounts.get(type) || 0;
      let tPenalty = Math.min(4, tCount * 0.7);
      if (intent.wantsNews && type === 'news') {
        const maxNewsBeforeDiversifying = Math.ceil(count * targetNewsShare);
        if (tCount < maxNewsBeforeDiversifying) tPenalty *= 0.35;
      }
      const adjusted = (r._score || 0) - dPenalty - tPenalty;
      if (adjusted > bestAdjusted) {
        bestAdjusted = adjusted;
        bestIndex = i;
      }
    }
    const [chosen] = pool.splice(bestIndex, 1);
    selected.push(chosen);
    const domain = hostname(chosen.url);
    const type = chosen.type || 'web';
    domainCounts.set(domain, (domainCounts.get(domain) || 0) + 1);
    typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
  }
  return selected;
}

function parseCount(value) {
  return safeInteger(value, DEFAULT_RESULTS, 1, MAX_RESULTS);
}

function extractJson(text) {
  if (!text) return null;
  try { return JSON.parse(text); } catch {}
  const m = String(text).match(/\{[\s\S]*\}/);
  if (!m) return null;
  try { return JSON.parse(m[0]); } catch { return null; }
}

async function callGroq(messages, maxTokens = 1600, timeoutMs = 7000, deadline = Date.now() + timeoutMs) {
  const key = typeof process !== 'undefined' ? process.env?.GROQ_API_KEY : undefined;
  if (!key) return null;
  const timeout = boundedTimeout(timeoutMs, deadline, 500);
  if (!timeout) return null;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: 'openai/gpt-oss-20b',
        messages,
        temperature: 0.1,
        max_completion_tokens: maxTokens,
      }),
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.choices?.[0]?.message?.content || null;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function aiPlan(query, mode, count, dateIntent, deadline) {
  const timeout = boundedTimeout(AI_PLAN_TIMEOUT_MS, deadline, 600);
  if (!timeout) return null;
  const text = await callGroq([
    {
      role: 'system',
      content: [
        'You are a search-routing planner for a web search engine.',
        'Return only JSON.',
        'Do not fabricate websites, URLs, or facts.',
        'Create concise query variants for public web search engines.',
        'Prefer official/primary sources when appropriate.',
        'Respect explicit date windows exactly when present.',
      ].join(' '),
    },
    {
      role: 'user',
      content: JSON.stringify({
        query,
        mode,
        requestedResults: count,
        dateIntent,
        schema: {
          intent: 'web|news|video|gov|doc|mixed',
          queries: ['query 1', 'query 2', 'query 3'],
          mustPreferOfficial: true,
          freshness: 'live|recent|any',
        },
      }),
    },
  ], 900, timeout, deadline);
  const data = extractJson(text);
  if (!data) return null;
  return {
    intent: data.intent || 'web',
    queries: Array.isArray(data.queries) ? data.queries.filter(Boolean).slice(0, 6) : [],
    mustPreferOfficial: Boolean(data.mustPreferOfficial),
    freshness: ['live', 'recent', 'any'].includes(data.freshness) ? data.freshness : 'any',
  };
}

async function aiRerank(query, results, mode, dateIntent, deadline) {
  if (!results.length || remainingMs(deadline) < 1200) return null;
  const payload = results.slice(0, 24).map((r, i) => ({
    id: i,
    title: truncate(r.title, 220),
    url: r.url,
    domain: r.domain || hostname(r.url),
    type: r.type,
    snippet: truncate(r.snippet, 500),
    publishedAt: r.publishedAt || null,
    verified: Boolean(r.verified),
    trust: r.trust || 0,
  }));
  const text = await callGroq([
    {
      role: 'system',
      content: [
        'You are a web-search reranker.',
        'Never invent evidence.',
        'Rank only the provided result IDs.',
        'Prefer direct/primary sources, exact query match, requested-date match, freshness when requested, and verified pages.',
        'Penalize duplicates and irrelevant old results.',
        'Return JSON only.',
      ].join(' '),
    },
    {
      role: 'user',
      content: JSON.stringify({
        query,
        mode,
        dateIntent,
        results: payload,
        output: { order: [0, 1], confidence: 0.0 },
      }),
    },
  ], 1400, AI_RERANK_TIMEOUT_MS, deadline);
  const data = extractJson(text);
  if (!data || !Array.isArray(data.order)) return null;
  return data.order
    .map(x => Number(x))
    .filter(Number.isInteger)
    .filter(x => x >= 0 && x < results.length);
}

async function groqBrowserFallback(query, count, deadline) {
  const key = typeof process !== 'undefined' ? process.env?.GROQ_API_KEY : undefined;
  if (!key || remainingMs(deadline) < 3000) return [];
  const timeout = boundedTimeout(8000, deadline, 900);
  if (!timeout) return [];
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: 'openai/gpt-oss-20b',
        messages: [{
          role: 'user',
          content: `Search the live web for: ${query}. Return up to ${Math.min(20, count)} distinct high-quality results as JSON in exactly this shape: {"results":[{"title":"...","url":"https://...","snippet":"...","type":"web|news|video|gov|doc","publishedAt":"ISO-or-null"}]}. Only include URLs actually found through browser search. Do not invent URLs. Prefer primary or official sources when appropriate.`,
        }],
        tool_choice: 'required',
        tools: [{ type: 'browser_search' }],
        temperature: 0.1,
        max_completion_tokens: 2200,
      }),
    });
    if (!res.ok) return [];
    const data = await res.json();
    const outputText = data?.choices?.[0]?.message?.content || '';
    const parsed = extractJson(outputText);
    if (!parsed?.results || !Array.isArray(parsed.results)) return [];
    return parsed.results.slice(0, 20).map(r => ({
      title: truncate(r.title || '', 300),
      url: cleanUrl(r.url, 'https://www.google.com/'),
      snippet: truncate(r.snippet || '', 1200),
      source: 'groq-browser',
      type: ['web', 'news', 'video', 'gov', 'doc'].includes(r.type) ? r.type : 'web',
      publishedAt: r.publishedAt ? safeIsoDate(r.publishedAt) : null,
    })).filter(r => r.url && safeHttpUrl(r.url));
  } catch {
    return [];
  } finally {
    clearTimeout(timer);
  }
}

function safeIsoDate(value) {
  const t = Date.parse(String(value || ''));
  return Number.isNaN(t) ? null : new Date(t).toISOString();
}

async function commonCrawlLookup(url, deadline) {
  if (!safeHttpUrl(url) || remainingMs(deadline) < 800) return null;
  const encoded = encodeURIComponent(url);
  for (const index of COMMON_CRAWL_INDEXES) {
    if (remainingMs(deadline) < 700) break;
    try {
      const ccUrl = `https://index.commoncrawl.org/${index}-index?url=${encoded}&output=json&filter=status:200&limit=3`;
      const body = await fetchText(ccUrl, {
        timeout: COMMON_CRAWL_TIMEOUT_MS,
        maxBytes: 80_000,
        deadline,
        headers: { accept: 'application/json,text/plain;q=0.8,*/*;q=0.1' },
      });
      const rows = body.trim().split('\n').filter(Boolean).map(x => {
        try { return JSON.parse(x); } catch { return null; }
      }).filter(Boolean);
      if (rows.length) {
        return {
          index,
          capturedAt: rows[0].timestamp || null,
          digest: rows[0].digest || null,
          status: rows[0].status || null,
          records: rows.slice(0, 3),
        };
      }
    } catch {
      // Continue to the next index.
    }
  }
  return null;
}

async function performSearch(input, started, deadline) {
  const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
  const count = parseCount(input.count ?? input.limit ?? DEFAULT_RESULTS);
  const mode = String(input.mode || 'auto').toLowerCase();
  const requestedType = String(input.type || '').toLowerCase();
  const deep = String(input.deep ?? 'false').toLowerCase() === 'true';
  const useAi = String(input.ai ?? input.useAi ?? 'auto').toLowerCase();
  const verifyRequested = input.verify == null ? true : String(input.verify).toLowerCase() !== 'false';
  const useCc = input.commonCrawl == null ? false : String(input.commonCrawl).toLowerCase() === 'true';

  if (!query) return { ok: false, error: 'MISSING_QUERY', message: 'Provide query in ?query=... or a JSON body with {"query":"..."}.' };
  if (query.length < 2) return { ok: false, error: 'QUERY_TOO_SHORT' };

  const baseIntent = queryIntent(query, requestedType || null);
  const dateIntent = parseDateIntent(query);
  const autoNeedsAi = deep || count > 10 || query.length > 120 ||
    baseIntent.wantsNews || baseIntent.wantsVideo || baseIntent.wantsGov ||
    baseIntent.wantsDocs || mode === 'deep' || mode === 'gov' || mode === 'doc';
  const shouldAiPlan = useAi === 'true' || (useAi === 'auto' && autoNeedsAi);

  // First wave is deterministic and starts immediately. The AI planner runs in parallel.
  const baseQueries = buildSearchQueries(query, baseIntent, mode, dateIntent);
  let providerStats = {};
  let discovered = [];
  let ai = null;
  const warnings = [];
  const internalFlags = {
    streamMode: true,
    partialDueToBudget: false,
    publisherResolutionAttempted: 0,
    publisherResolutionSucceeded: 0,
    verificationSucceeded: 0,
    commonCrawlPerformed: 0,
  };

  let aiPromise = Promise.resolve(null);
  if (shouldAiPlan && remainingMs(deadline) > 1800) {
    aiPromise = aiPlan(query, mode, count, dateIntent, deadline);
  }

  const firstRequests = [];
  for (let i = 0; i < baseQueries.length; i++) {
    for (const engine of buildEngineUrls(baseQueries[i], baseIntent)) {
      firstRequests.push({ ...engine, __queryIndex: i });
    }
  }
  const firstSelected = prioritizeEngineRequests(firstRequests, MAX_ENGINE_REQUESTS);
  let totalEngineRequests = firstSelected.length;
  const firstResponses = await Promise.allSettled(firstSelected.map(e => discoverOne(e, deadline)));
  for (const entry of firstResponses) {
    if (entry.status !== 'fulfilled') continue;
    const item = entry.value;
    providerStats[item.provider] = providerStats[item.provider] || { ok: 0, failed: 0, results: 0 };
    if (item.ok) providerStats[item.provider].ok += 1;
    else providerStats[item.provider].failed += 1;
    providerStats[item.provider].results += item.results.length;
    discovered.push(...item.results);
  }

  ai = await aiPromise;
  const aiQueries = ai?.queries?.length ? [...new Set([query, ...ai.queries])].slice(0, 6) : [];
  const plannedQueries = [...new Set([...baseQueries, ...aiQueries])].slice(0, 6);

  // Second wave: spend remaining budget on AI-guided variants, without exceeding the original 12 request cap.
  if (aiQueries.length && remainingMs(deadline) > 3500 && firstSelected.length < MAX_ENGINE_REQUESTS) {
    const remainingSlots = MAX_ENGINE_REQUESTS - firstSelected.length;
    const aiRequests = [];
    const startIndex = baseQueries.length;
    for (let i = 0; i < aiQueries.length; i++) {
      for (const engine of buildEngineUrls(aiQueries[i], baseIntent)) {
        aiRequests.push({ ...engine, __queryIndex: startIndex + i });
      }
    }
    const selectedAi = prioritizeEngineRequests(aiRequests, remainingSlots);
    totalEngineRequests += selectedAi.length;
    const aiResponses = await Promise.allSettled(selectedAi.map(e => discoverOne(e, deadline)));
    for (const entry of aiResponses) {
      if (entry.status !== 'fulfilled') continue;
      const item = entry.value;
      providerStats[item.provider] = providerStats[item.provider] || { ok: 0, failed: 0, results: 0 };
      if (item.ok) providerStats[item.provider].ok += 1;
      else providerStats[item.provider].failed += 1;
      providerStats[item.provider].results += item.results.length;
      discovered.push(...item.results);
    }
  }

  discovered = discovered
    .filter(r => r?.url && safeHttpUrl(r.url))
    .map(r => ({ ...r, type: inferType(r), domain: hostname(r.url) }));
  discovered = dedupeResults(discovered);

  for (const r of discovered) {
    r._score = scoreResult(r, query, baseIntent, dateIntent) + (r.verified ? 2 : 0) + domainTrust(r.url);
  }

  if (!discovered.length && (useAi === 'true' || useAi === 'auto') && remainingMs(deadline) > 3000) {
    const fallback = await groqBrowserFallback(query, count, deadline);
    if (fallback.length) {
      discovered = dedupeResults(fallback.map(r => ({ ...r, domain: hostname(r.url) })));
      for (const r of discovered) r._score = scoreResult(r, query, baseIntent, dateIntent) + domainTrust(r.url);
    }
  }

  discovered.sort((a, b) => (b._score || 0) - (a._score || 0));

  // Google News returns wrapper URLs. Resolve them to the actual publisher before verification.
  if (discovered.some(r => r.type === 'news') && remainingMs(deadline) > 2200) {
    internalFlags.publisherResolutionAttempted = Math.min(MAX_NEWS_RESOLVES, discovered.filter(r => r.type === 'news').length);
    const beforeKeys = new Set(discovered.filter(r => r.type === 'news').map(r => normalizedKey(r.url)));
    discovered = await resolveTopNews(discovered, deadline);
    internalFlags.publisherResolutionSucceeded = discovered.filter(r => r.publisherResolved).length;
    if (internalFlags.publisherResolutionSucceeded > 0) {
      warnings.push(`Resolved ${internalFlags.publisherResolutionSucceeded} Google News result(s) to publisher URLs before verification.`);
    }
    void beforeKeys;
  }

  for (const r of discovered) {
    r.domain = hostname(r.url);
    r._score = scoreResult(r, query, baseIntent, dateIntent) + (r.verified ? 2 : 0) + domainTrust(r.url);
  }

  discovered = applyDateConstraint(discovered, dateIntent, count, warnings);
  discovered.sort((a, b) => (b._score || 0) - (a._score || 0));

  const requestedVerifyCount = verifyRequested ? Math.min(discovered.length, deep ? DEEP_VERIFY : DEFAULT_VERIFY) : 0;
  const verifyCap = deep ? DEEP_VERIFY_CAP : NORMAL_VERIFY_CAP;
  const verifyCount = Math.min(requestedVerifyCount, verifyCap);
  let verificationPerformed = 0;

  if (verifyCount > 0 && remainingMs(deadline) > 1500) {
    const candidates = selectVerificationCandidates(discovered, verifyCount);
    verificationPerformed = candidates.length;
    const verified = await Promise.all(candidates.map(r => enrichResult(r, deadline)));
    const byKey = new Map(verified.map(x => [normalizedKey(x.url), x]));
    for (let i = 0; i < discovered.length; i++) {
      const key = normalizedKey(discovered[i].url);
      if (byKey.has(key)) discovered[i] = byKey.get(key);
    }
    internalFlags.verificationSucceeded = verified.filter(r => r.verified).length;
    for (const r of discovered) {
      r.domain = hostname(r.url);
      r._score = scoreResult(r, query, baseIntent, dateIntent) + (r.verified ? 3 : 0) + (r.trust || domainTrust(r.url));
    }
    discovered.sort((a, b) => (b._score || 0) - (a._score || 0));
  } else if (verifyRequested) {
    warnings.push('Verification was requested but the safe execution budget was nearly exhausted.');
  }

  let aiOrder = null;
  const shouldAiRerank = useAi === 'true' || (useAi === 'auto' && (deep || count > 10 || discovered.length > 15 || Boolean(ai)));
  if (shouldAiRerank && discovered.length > 1 && remainingMs(deadline) > 1700) {
    aiOrder = await aiRerank(query, discovered, mode, dateIntent, deadline);
    if (aiOrder?.length) {
      const ordered = [];
      const used = new Set();
      for (const idx of aiOrder) {
        if (!used.has(idx)) { ordered.push(discovered[idx]); used.add(idx); }
      }
      for (let i = 0; i < discovered.length; i++) if (!used.has(i)) ordered.push(discovered[i]);
      discovered = ordered;
    }
  }

  // Historical Common Crawl enrichment is optional and always comes after live evidence.
  if (useCc && discovered.length && remainingMs(deadline) > 1600) {
    const ccCount = Math.min(MAX_CC_LOOKUPS, discovered.length);
    const rows = await Promise.all(discovered.slice(0, ccCount).map(async r => ({
      url: r.url,
      cc: await commonCrawlLookup(r.url, deadline),
    })));
    internalFlags.commonCrawlPerformed = rows.filter(x => x.cc).length;
    const ccMap = new Map(rows.map(x => [x.url, x.cc]));
    for (const r of discovered) r.commonCrawl = ccMap.get(r.url) || null;
  } else if (useCc) {
    warnings.push('Common Crawl was requested but skipped because live search/verification was prioritized.');
  }

  if (remainingMs(deadline) < 1200) internalFlags.partialDueToBudget = true;
  if (internalFlags.partialDueToBudget) {
    warnings.push('Returned the best available live evidence before the crawler safety deadline.');
  }

  // Diversity is applied after relevance/freshness so the first result remains evidence-driven.
  discovered = diversifyAndSelect(discovered, count, {
    ...baseIntent,
    wantsNews: baseIntent.wantsNews,
  });

  const finalResults = discovered.slice(0, count).map((r, i) => {
    const relevanceBase = scoreResult(r, query, baseIntent, dateIntent);
    const relevanceScore = Math.max(0, Math.min(100, Math.round(50 + relevanceBase * 2.25)));
    return {
      rank: i + 1,
      title: truncate(r.title || 'Untitled', 300),
      url: r.url,
      domain: r.domain || hostname(r.url),
      type: r.type,
      source: r.source,
      snippet: truncate(r.snippet || '', 1200),
      publishedAt: r.publishedAt || null,
      freshness: freshnessBand(r.publishedAt),
      verified: Boolean(r.verified),
      httpStatus: r.httpStatus || null,
      contentType: r.contentType || null,
      trust: Number((r.trust || domainTrust(r.url)).toFixed(2)),
      relevanceScore,
      publisherResolved: Boolean(r.publisherResolved),
      extractedText: r.extractedText || null,
      commonCrawl: r.commonCrawl || null,
    };
  });

  if (!finalResults.some(r => r.verified) && verifyRequested && finalResults.length) {
    warnings.push('No selected result was successfully page-verified; discovery results are still real live search results.');
  }

  return {
    ok: true,
    version: VERSION,
    query,
    requestedResults: count,
    returnedResults: finalResults.length,
    mode,
    intent: {
      ...baseIntent,
      type: requestedType || ai?.intent || baseIntent.type,
      wantsNews: baseIntent.wantsNews || ai?.intent === 'news',
      wantsVideo: baseIntent.wantsVideo || ai?.intent === 'video',
      wantsGov: baseIntent.wantsGov || ai?.intent === 'gov' || mode === 'gov',
      wantsDocs: baseIntent.wantsDocs || ai?.intent === 'doc' || mode === 'doc',
    },
    generatedAt: nowIso(),
    latencyMs: Date.now() - started,
    keylessCoreSearch: true,
    groqUsed: Boolean(ai || aiOrder?.length),
    providers: providerStats,
    searchPlan: {
      queryVariants: plannedQueries,
      engineRequests: totalEngineRequests,
      verificationRequested: verifyRequested,
      verificationPerformed,
      verificationSucceeded: internalFlags.verificationSucceeded,
      commonCrawlEnabled: useCc,
      commonCrawlPerformed: internalFlags.commonCrawlPerformed,
      publisherResolutionAttempted: internalFlags.publisherResolutionAttempted,
      publisherResolutionSucceeded: internalFlags.publisherResolutionSucceeded,
      dateIntent,
      streamed: true,
    },
    results: finalResults,
    warnings: [
      'Core discovery is keyless but depends on public web surfaces that may rate-limit or block automated requests.',
      'This endpoint is not a substitute for an internet-scale index; use a provider or your own persistent index for very high volume.',
      'The response uses Vercel-supported streaming heartbeats so long searches can continue without waiting silently at the Edge gateway.',
      ...warnings,
    ],
  };
}

function withCors(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': 'content-type, authorization, x-arix-search-key',
      'x-arix-crawler-version': VERSION,
    },
  });
}

function streamCors(task) {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      let closed = false;
      const safeEnqueue = value => {
        if (closed) return;
        try { controller.enqueue(encoder.encode(value)); } catch { closed = true; }
      };

      // JSON permits whitespace before the object. One immediate byte starts the
      // response, and periodic whitespace keeps the stream active while upstream
      // search and verification work continues.
      safeEnqueue(' ');
      const heartbeat = setInterval(() => safeEnqueue('\n'), STREAM_HEARTBEAT_MS);

      Promise.resolve()
        .then(task)
        .then(result => {
          clearInterval(heartbeat);
          safeEnqueue(JSON.stringify(result, null, 2));
          try { controller.close(); } catch {}
          closed = true;
        })
        .catch(error => {
          clearInterval(heartbeat);
          safeEnqueue(JSON.stringify({
            ok: false,
            version: VERSION,
            error: 'SEARCH_FAILED',
            message: error?.message || 'Unexpected crawler error',
          }, null, 2));
          try { controller.close(); } catch {}
          closed = true;
        });
    },
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store, no-transform',
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': 'content-type, authorization, x-arix-search-key',
      'x-arix-crawler-version': VERSION,
      'x-arix-search-stream': '1',
      'x-arix-stream-heartbeat-ms': String(STREAM_HEARTBEAT_MS),
    },
  });
}

async function readInput(req) {
  const url = new URL(req.url);
  if (req.method === 'GET') return Object.fromEntries(url.searchParams.entries());
  const raw = await req.text();
  if (!raw) return {};
  if (raw.length > 64_000) throw new Error('REQUEST_BODY_TOO_LARGE');
  try { return JSON.parse(raw); } catch { return Object.fromEntries(new URLSearchParams(raw).entries()); }
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') return withCors({ ok: true, version: VERSION });
  if (!['GET', 'POST'].includes(req.method)) {
    return withCors({ ok: false, error: 'METHOD_NOT_ALLOWED', message: 'Use GET or POST.' }, 405);
  }

  try {
    const input = await readInput(req);
    const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
    if (!query) {
      return withCors({
        ok: false,
        error: 'MISSING_QUERY',
        message: 'Provide query in ?query=... or a JSON body with {"query":"..."}.',
      }, 400);
    }
    if (query.length < 2) return withCors({ ok: false, error: 'QUERY_TOO_SHORT' }, 400);

    const started = Date.now();
    const deadline = started + SEARCH_WORK_BUDGET_MS;
    return streamCors(() => performSearch(input, started, deadline));
  } catch (error) {
    return withCors({
      ok: false,
      version: VERSION,
      error: error?.message === 'REQUEST_BODY_TOO_LARGE' ? 'REQUEST_BODY_TOO_LARGE' : 'SEARCH_FAILED',
      message: error?.message || 'Unexpected crawler error',
    }, error?.message === 'REQUEST_BODY_TOO_LARGE' ? 413 : 500);
  }
}
