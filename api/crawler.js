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

const VERSION = 'arix-crawler-1.2.0';
const MAX_RESULTS = 40;
const DEFAULT_RESULTS = 10;
const MAX_QUERY_LEN = 500;
const SEARCH_TIMEOUT_MS = 4500;
const PAGE_TIMEOUT_MS = 5000;
const MAX_PAGE_BYTES = 800_000;
const MAX_TEXT_CHARS = 30000;
const MAX_TRANSCRIPT_CHARS = 30000;
const READER_TIMEOUT_MS = 7000;
const MAX_READER_FALLBACKS = 4;
const YOUTUBE_TIMEOUT_MS = 7000;
const PDF_DECOMPRESS_TIMEOUT_MS = 3500;
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
const NORMAL_VERIFY_CAP = 8;
const DEEP_VERIFY_CAP = 12;

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
  const candidates = [
    html.match(/<article\b[^>]*>([\s\S]*?)<\/article>/i)?.[1],
    html.match(/<main\b[^>]*>([\s\S]*?)<\/main>/i)?.[1],
    html.match(/<body\b[^>]*>([\s\S]*?)<\/body>/i)?.[1],
    html,
  ].filter(Boolean);
  const source = candidates.find(x => stripTags(x).length >= 400) || candidates[0] || '';
  const text = stripTags(String(source))
    .replace(/\b(function|var|const|let)\s+[^;]{0,180};?/g, ' ')
    .replace(/(?:skip to content|accept cookies|cookie settings|privacy settings|sign in|log in|subscribe)\b/gi, ' ')
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
  const anchors = [...html.matchAll(/<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)];
  for (const m of anchors) {
    let raw = decodeHtml(m[1]);
    try {
      if (raw.startsWith('/url?')) {
        const u = new URL(raw, 'https://www.google.com/');
        raw = u.searchParams.get('q') || u.searchParams.get('url') || '';
      }
    } catch {}
    if (!/^https?:\/\//i.test(raw)) continue;
    const url = cleanUrl(raw, 'https://www.google.com/');
    if (!url) continue;
    const host = hostname(url);
    if (/google\.(com|co\.in)$/i.test(host) && /\/search|\/url\b/i.test(new URL(url).pathname + new URL(url).search)) continue;
    const title = stripTags(m[2]).replace(/\s+/g, ' ').trim();
    if (!title || title.length < 3) continue;
    const key = normalizedKey(url);
    if (seen.has(key)) continue;
    seen.add(key);
    if (forcedType === 'video' && !/youtube\.com|youtu\.be|vimeo\.com/i.test(url)) continue;
    let type = forcedType;
    if (forcedType === 'web') {
      if (isGovUrl(url)) type = 'gov';
      else if (isDocUrl(url)) type = 'doc';
      else if (isLikelyVideoUrl(url)) type = 'video';
    }
    out.push({ title, url, snippet: '', source, type });
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

function extractYouTubeTitle(windowText) {
  const run = windowText.match(/"title":\{"runs":\[\{"text":"((?:\\.|[^"\\])*)"/i);
  if (run?.[1]) return decodeHtml(run[1].replace(/\\"/g, '"').replace(/\\u0026/g, '&'));
  const simple = windowText.match(/"title":\{"simpleText":"((?:\\.|[^"\\])*)"/i);
  if (simple?.[1]) return decodeHtml(simple[1].replace(/\\"/g, '"').replace(/\\u0026/g, '&'));
  return '';
}

function findJsonObjectAfterMarker(text, marker, maxScan = 250000) {
  const idx = String(text || '').indexOf(marker);
  if (idx < 0) return null;
  const start = String(text).indexOf('{', idx + marker.length);
  if (start < 0 || start - idx > maxScan) return null;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < Math.min(String(text).length, start + 900000); i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') { inString = true; continue; }
    if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) {
        try { return JSON.parse(text.slice(start, i + 1)); } catch { return null; }
      }
    }
  }
  return null;
}

function collectYouTubeVideoRenderers(value, out = []) {
  if (!value || out.length >= 20) return out;
  if (Array.isArray(value)) {
    for (const item of value) collectYouTubeVideoRenderers(item, out);
    return out;
  }
  if (typeof value !== 'object') return out;
  if (value.videoRenderer?.videoId) out.push(value.videoRenderer);
  for (const key of Object.keys(value)) {
    if (key === 'videoRenderer') continue;
    collectYouTubeVideoRenderers(value[key], out);
    if (out.length >= 20) break;
  }
  return out;
}

function youtubeRendererTitle(renderer) {
  return decodeHtml(renderer?.title?.runs?.map(x => x?.text || '').join('') || renderer?.title?.simpleText || '').trim();
}

function parseYoutube(html) {
  const out = [];
  const seen = new Set();
  const initialData = findJsonObjectAfterMarker(html, 'ytInitialData');
  const renderers = collectYouTubeVideoRenderers(initialData, []);
  for (const renderer of renderers) {
    const id = renderer?.videoId;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const title = youtubeRendererTitle(renderer) || `YouTube video ${id}`;
    const snippet = decodeHtml(renderer?.detailedMetadataSnippets?.[0]?.snippetText?.runs?.map(x => x?.text || '').join('') || renderer?.descriptionSnippet?.runs?.map(x => x?.text || '').join('') || 'YouTube video result');
    out.push({ title, url: `https://www.youtube.com/watch?v=${id}`, snippet, source: 'youtube', type: 'video' });
    if (out.length >= 20) break;
  }
  if (out.length) return out;

  const rendererMatches = [...html.matchAll(/"videoRenderer":\{[\s\S]*?"videoId":"([\w-]{6,20})"[\s\S]*?\}/g)];
  for (const m of rendererMatches) {
    const id = m[1];
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const idx = m.index || 0;
    const windowText = html.slice(Math.max(0, idx - 200), Math.min(html.length, idx + 7000));
    out.push({ title: extractYouTubeTitle(windowText) || `YouTube video ${id}`, url: `https://www.youtube.com/watch?v=${id}`, snippet: 'YouTube video result', source: 'youtube', type: 'video' });
    if (out.length >= 20) break;
  }
  if (!out.length) {
    const matches = [...html.matchAll(/"videoId":"([\w-]{6,20})"/g)];
    for (const m of matches) {
      const id = m[1];
      if (seen.has(id)) continue;
      seen.add(id);
      out.push({ title: `YouTube video ${id}`, url: `https://www.youtube.com/watch?v=${id}`, snippet: 'YouTube video result', source: 'youtube', type: 'video' });
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
  const qs = [];
  const add = q => { if (q && !qs.includes(q)) qs.push(q); };
  const gov = intent.wantsGov || mode === 'gov';
  const docs = intent.wantsDocs || mode === 'doc';
  const video = intent.wantsVideo || mode === 'video';
  const news = intent.wantsNews || mode === 'news';

  // Put the exact intent-relevant query first so MAX_ENGINE_REQUESTS cannot starve it.
  if (gov) {
    add(`${query} site:gov.in`);
    add(`${query} site:nic.in`);
    add(`${query} site:india.gov.in`);
    add(`${query} site:mygov.in`);
  }
  if (docs) {
    add(`${query} filetype:pdf`);
    add(`${query} official PDF`);
    add(`${query} site:gov.in filetype:pdf`);
  }
  if (video) {
    add(`site:youtube.com ${query}`);
    add(`${query} YouTube`);
  }
  if (news) add(`${query} latest news`);

  add(query);
  if (dateIntent.kind === 'explicit-month' || dateIntent.kind === 'explicit-year') {
    add(`${query} after:${dateIntent.start.slice(0, 10)} before:${dateIntent.end.slice(0, 10)}`);
  }
  if (mode === 'deep') {
    add(`${query} latest update`);
    add(`${query} official source`);
  }
  if (news && !qs.includes(`${query} latest news`)) add(`${query} latest news`);
  if (docs && !qs.includes(`${query} filetype:pdf`)) add(`${query} filetype:pdf`);
  if (gov && !qs.includes(`${query} site:gov.in`)) add(`${query} site:gov.in`);
  return qs.slice(0, 6);
}

function buildEngineUrls(q, intent) {
  const encoded = encodeURIComponent(q);
  const isPdfQuery = /filetype:\s*pdf|\bpdf\b|official pdf/i.test(q);
  const isGovQuery = /site:(?:gov\.in|nic\.in|india\.gov\.in|mygov\.in)/i.test(q);
  const isVideoQuery = /site:youtube\.com|\byoutube\b/i.test(q);
  const typeForQuery = isGovQuery ? 'gov' : isPdfQuery ? 'doc' : isVideoQuery ? 'video' : 'web';
  const urls = [
    { provider: 'bing', type: typeForQuery, url: `https://www.bing.com/search?q=${encoded}&count=20&setlang=en-IN&cc=in` },
    { provider: 'duckduckgo', type: typeForQuery, url: `https://html.duckduckgo.com/html/?q=${encoded}&kl=in-en` },
    { provider: 'mojeek', type: typeForQuery, url: `https://www.mojeek.com/search?q=${encoded}` },
    { provider: 'yahoo', type: typeForQuery, url: `https://search.yahoo.com/search?p=${encoded}` },
    { provider: 'google', type: typeForQuery, url: `https://www.google.com/search?q=${encoded}&num=20&hl=en&gl=in` },
  ];
  if (intent.wantsNews) {
    urls.push({ provider: 'google-news', type: 'news', url: `https://news.google.com/rss/search?q=${encoded}&hl=en-IN&gl=IN&ceid=IN:en` });
  }
  if (intent.wantsVideo) {
    urls.push({ provider: 'youtube', type: 'video', url: `https://www.youtube.com/results?search_query=${encoded}&hl=en-IN` });
    urls.push({ provider: 'google-video', type: 'video', url: `https://www.google.com/search?q=${encodeURIComponent(`site:youtube.com ${q}`)}&num=20&hl=en&gl=in` });
  }
  // A direct official-domain probe is especially useful when public SERPs return weak/no gov results.
  if (isGovQuery) {
    urls.push({ provider: 'google-gov', type: 'gov', url: `https://www.google.com/search?q=${encodeURIComponent(`${q} site:gov.in`)}&num=20&hl=en&gl=in` });
  }
  // Explicit PDF search receives a document-only Google surface.
  if (isPdfQuery) {
    urls.push({ provider: 'google-doc', type: 'doc', url: `https://www.google.com/search?q=${encodeURIComponent(`${q} filetype:pdf`)}&num=20&hl=en&gl=in` });
  }
  return urls;
}

function prioritizeEngineRequests(requests, maxRequests) {
  // Query-first round robin: the first few intent-specific queries (gov/pdf/youtube/news)
  // must reach multiple independent providers before generic variants consume the cap.
  const byQuery = new Map();
  const queryOrder = [];
  for (const req of requests) {
    const qi = Number.isInteger(req.__queryIndex) ? req.__queryIndex : 0;
    if (!byQuery.has(qi)) { byQuery.set(qi, []); queryOrder.push(qi); }
    byQuery.get(qi).push(req);
  }
  const out = [];
  let cursor = 0;
  while (out.length < maxRequests && cursor < queryOrder.length) {
    const qi = queryOrder[cursor];
    for (const req of byQuery.get(qi) || []) {
      if (out.length >= maxRequests) break;
      out.push(req);
    }
    cursor += 1;
  }
  return out;
}

async function discoverOne(engine, deadline) {
  try {
    const text = await fetchText(engine.url, {
      timeout: SEARCH_TIMEOUT_MS,
      maxBytes: engine.provider === 'youtube' ? 1_600_000 : 600_000,
      deadline,
    });
    let results = [];
    if (engine.provider === 'bing') results = parseBing(text);
    else if (engine.provider === 'duckduckgo') results = parseDuckDuckGo(text);
    else if (engine.provider === 'mojeek') results = parseMojeek(text);
    else if (engine.provider === 'yahoo') results = parseYahoo(text);
    else if (engine.provider === 'google') results = parseGoogleWeb(text);
    else if (engine.provider === 'google-video') results = parseGoogleWeb(text, 'google-video', 'video');
    else if (engine.provider === 'google-gov') results = parseGoogleWeb(text, 'google-gov', 'gov');
    else if (engine.provider === 'google-doc') results = parseGoogleWeb(text, 'google-doc', 'doc');
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


async function readBodyBytes(res, maxBytes, deadline) {
  const reader = res.body?.getReader?.();
  if (!reader) {
    const ab = await res.arrayBuffer();
    return new Uint8Array(ab).slice(0, maxBytes);
  }
  const chunks = [];
  let total = 0;
  while (true) {
    if (remainingMs(deadline) <= 100) break;
    const { done, value } = await reader.read();
    if (done) break;
    if (!value) continue;
    const room = maxBytes - total;
    if (room <= 0) break;
    const piece = value.byteLength > room ? value.slice(0, room) : value;
    chunks.push(piece);
    total += piece.byteLength;
    if (total >= maxBytes) break;
  }
  const bytes = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
  return bytes;
}

function bytesToLatin1(bytes) {
  let out = '';
  const step = 8192;
  for (let i = 0; i < bytes.length; i += step) {
    out += String.fromCharCode(...bytes.subarray(i, Math.min(bytes.length, i + step)));
  }
  return out;
}

function decodePdfLiteral(raw) {
  let s = String(raw || '');
  s = s.replace(/\\([nrtbf\\()])/g, (_, c) => ({n:'\n',r:'\r',t:'\t',b:'\b',f:'\f','\\':'\\','(':'(',')':')'})[c] || c);
  s = s.replace(/\\([0-7]{1,3})/g, (_, oct) => String.fromCharCode(parseInt(oct, 8)));
  return s;
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

function extractPdfStringsFromStream(stream) {
  const out = [];
  let i = 0;
  while (i < stream.length) {
    if (stream[i] === '(') {
      let depth = 1;
      let j = i + 1;
      let escaped = false;
      for (; j < stream.length; j++) {
        const ch = stream[j];
        if (escaped) { escaped = false; continue; }
        if (ch === '\\') { escaped = true; continue; }
        if (ch === '(') depth += 1;
        else if (ch === ')') { depth -= 1; if (depth === 0) break; }
      }
      if (j < stream.length) {
        const raw = stream.slice(i + 1, j);
        const text = decodePdfLiteral(raw);
        if (text.trim()) out.push(text);
        i = j + 1;
        continue;
      }
    }
    if (stream[i] === '<' && stream[i + 1] !== '<') {
      const j = stream.indexOf('>', i + 1);
      if (j > i) {
        const text = decodePdfHex(stream.slice(i + 1, j));
        if (text.trim()) out.push(text);
        i = j + 1;
        continue;
      }
    }
    i += 1;
  }
  return out.join(' ');
}

async function inflateDeflate(bytes, deadline) {
  if (typeof DecompressionStream === 'undefined') return null;
  const timeout = boundedTimeout(PDF_DECOMPRESS_TIMEOUT_MS, deadline, 500);
  if (!timeout) return null;
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
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function extractPdfText(bytes, deadline) {
  const raw = bytesToLatin1(bytes);
  const streams = [];
  const re = /<<(?:[\s\S]{0,5000}?)>>\s*stream\r?\n([\s\S]*?)\r?\nendstream/g;
  let match;
  while ((match = re.exec(raw)) && streams.length < 80) {
    const dict = match[0].slice(0, Math.max(0, match[0].indexOf('stream')));
    const payloadStart = match.index + match[0].indexOf(match[1]);
    const payloadEnd = payloadStart + match[1].length;
    const rawBytes = bytes.slice(payloadStart, payloadEnd);
    if (/\/FlateDecode/i.test(dict)) {
      const inflated = await inflateDeflate(rawBytes, deadline);
      if (inflated) streams.push(bytesToLatin1(inflated));
    } else {
      streams.push(match[1]);
    }
  }
  const extracted = streams.map(extractPdfStringsFromStream).filter(Boolean).join(' ')
    .replace(/\s+/g, ' ')
    .trim();
  return truncate(extracted, MAX_TEXT_CHARS);
}

function readerUrl(url) {
  return `https://r.jina.ai/${url}`;
}

async function readerFallback(url, deadline) {
  if (!safeHttpUrl(url) || remainingMs(deadline) < 1100) return null;
  try {
    const timeout = boundedTimeout(READER_TIMEOUT_MS, deadline, 700);
    if (!timeout) return null;
    const res = await fetchResponse(readerUrl(url), {
      timeout,
      deadline,
      headers: {
        accept: 'text/plain,text/markdown,application/json;q=0.9,*/*;q=0.2',
        'x-no-cache': 'true',
      },
    });
    if (!res.ok) {
      try { await res.body?.cancel?.(); } catch {}
      return null;
    }
    const text = await readBodyText(res, MAX_PAGE_BYTES, deadline);
    if (!text || text.length < 120) return null;
    return {
      content: truncate(text, MAX_TEXT_CHARS),
      finalUrl: url,
      title: parseTitleFromHtml(text) || null,
    };
  } catch {
    return null;
  }
}

function extractPlayerResponse(html) {
  return findJsonObjectAfterMarker(html, 'ytInitialPlayerResponse') || findJsonObjectAfterMarker(html, 'PLAYER_RESPONSE');
}

function chooseCaptionTrack(tracks) {
  if (!Array.isArray(tracks) || !tracks.length) return null;
  return tracks.find(t => /^en(?:-|$)/i.test(t?.languageCode || '')) ||
    tracks.find(t => /^en/i.test(t?.languageCode || '')) ||
    tracks[0] || null;
}

function captionXmlToText(xml) {
  const rows = [];
  for (const m of String(xml || '').matchAll(/<text\b[^>]*>([\s\S]*?)<\/text>/gi)) {
    const text = stripTags(m[1]).replace(/\s+/g, ' ').trim();
    if (text) rows.push(text);
  }
  return truncate(rows.join(' '), MAX_TRANSCRIPT_CHARS);
}

async function fetchYoutubePlayer(videoId, deadline) {
  const watchUrl = `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}&hl=en`;
  const html = await fetchText(watchUrl, {
    timeout: YOUTUBE_TIMEOUT_MS,
    maxBytes: 1_600_000,
    deadline,
    headers: { accept: 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.2' },
  });
  let player = extractPlayerResponse(html);
  if (player) return { player, html };

  const apiKey = (html.match(/"INNERTUBE_API_KEY":"([^"]+)"/) || [,''])[1] ||
    (html.match(/INNERTUBE_API_KEY['"]?\s*[:=]\s*['"]([^'"]+)/i) || [,''])[1];
  if (!apiKey) return { player: null, html };
  const timeout = boundedTimeout(YOUTUBE_TIMEOUT_MS, deadline, 700);
  if (!timeout) return { player: null, html };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(`https://www.youtube.com/youtubei/v1/player?key=${encodeURIComponent(apiKey)}`, {
      method: 'POST',
      signal: controller.signal,
      headers: { 'content-type': 'application/json', 'user-agent': USER_AGENT },
      body: JSON.stringify({
        context: {
          client: {
            clientName: 'WEB',
            clientVersion: (html.match(/"INNERTUBE_CLIENT_VERSION":"([^"]+)"/) || [,'2.20260915.01.00'])[1],
            hl: 'en',
            gl: 'IN',
          },
        },
        videoId,
      }),
    });
    if (!response.ok) return { player: null, html };
    const data = await response.json();
    return { player: data, html };
  } catch {
    return { player: null, html };
  } finally {
    clearTimeout(timer);
  }
}

async function enrichYouTubeResult(result, deadline) {
  const match = String(result.url || '').match(/(?:v=|youtu\.be\/|shorts\/)([A-Za-z0-9_-]{6,20})/i);
  const videoId = match?.[1];
  if (!videoId) return { ...result, verified: false, verificationError: 'YOUTUBE_VIDEO_ID_MISSING' };
  try {
    const { player } = await fetchYoutubePlayer(videoId, deadline);
    const videoDetails = player?.videoDetails || {};
    const captions = player?.captions?.playerCaptionsTracklistRenderer?.captionTracks || [];
    const track = chooseCaptionTrack(captions);
    let transcript = null;
    if (track?.baseUrl && remainingMs(deadline) > 700) {
      const timeout = boundedTimeout(YOUTUBE_TIMEOUT_MS, deadline, 500);
      const res = await fetchResponse(track.baseUrl, {
        timeout,
        deadline,
        headers: { accept: 'text/xml,application/xml,text/plain;q=0.9,*/*;q=0.2' },
      });
      if (res.ok) transcript = captionXmlToText(await readBodyText(res, 900_000, deadline));
      else { try { await res.body?.cancel?.(); } catch {} }
    }
    const title = decodeHtml(videoDetails.title || result.title || '').trim() || result.title;
    const description = decodeHtml(videoDetails.shortDescription || result.snippet || '').trim();
    const transcriptText = transcript || null;
    const body = transcriptText ? `YouTube transcript:\n${transcriptText}` : description;
    return {
      ...result,
      title: truncate(title, 300),
      snippet: truncate(description || (transcriptText ? transcriptText.slice(0, 1000) : result.snippet), 1200),
      extractedText: body ? truncate(body, MAX_TEXT_CHARS) : null,
      pageContent: body ? truncate(body, MAX_TEXT_CHARS) : null,
      transcript: transcriptText,
      transcriptAvailable: Boolean(transcriptText),
      transcriptLanguage: track?.languageCode || null,
      transcriptKind: track?.kind || null,
      verified: Boolean(player),
      httpStatus: player ? 200 : null,
      contentType: player ? 'application/json' : null,
      domain: 'youtube.com',
      trust: domainTrust(result.url),
    };
  } catch (error) {
    return { ...result, verified: false, verificationError: error?.message || 'YOUTUBE_ENRICH_FAILED' };
  }
}

async function resolvePublisherUrl(result, deadline) {
  if (!/^news\.google\.com$/i.test(hostname(result.url))) return result;
  let parsed;
  try { parsed = new URL(result.url); } catch { return result; }
  if (!/^\/rss\/articles\//i.test(parsed.pathname)) return result;
  try {
    const timeout = boundedTimeout(NEWS_RESOLVE_TIMEOUT_MS, deadline, 350);
    if (!timeout) return { ...result, verificationError: 'BUDGET_EXHAUSTED' };
    const res = await fetchResponse(result.url, {
      timeout,
      deadline,
      headers: { accept: 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.1' },
    });
    const finalUrl = cleanUrl(res.url || result.url, result.url);
    const body = await readBodyText(res, 220_000, deadline).catch(() => '');
    if (finalUrl && hostname(finalUrl) && !/^news\.google\.com$/i.test(hostname(finalUrl))) {
      return { ...result, url: finalUrl, domain: hostname(finalUrl), publisherResolved: true };
    }
    // Google News can expose the publisher link inside the redirect/landing HTML even when
    // the platform does not surface it as the final response URL.
    const externalLinks = [...body.matchAll(/href=["'](https?:\/\/[^"']+)["']/gi)]
      .map(m => cleanUrl(m[1], result.url))
      .filter(Boolean)
      .filter(u => !/^news\.google\.com$/i.test(hostname(u)) && !/^www\.google\./i.test(hostname(u)) && !/^accounts\.google\./i.test(hostname(u)));
    const candidate = externalLinks.find(u => !/googleusercontent|gstatic|doubleclick|googletagmanager/i.test(u));
    if (candidate) return { ...result, url: candidate, domain: hostname(candidate), publisherResolved: true };
    return { ...result, publisherResolved: false };
  } catch (error) {
    return { ...result, publisherResolved: false, resolutionError: error?.message || 'RESOLVE_FAILED' };
  }
}

async function enrichResult(result, deadline) {
  if (result.type === 'video' && /youtube\.com|youtu\.be/i.test(result.url || '')) {
    return enrichYouTubeResult(result, deadline);
  }
  if (!safeHttpUrl(result.url)) return { ...result, verified: false, verificationError: 'UNSAFE_URL' };
  try {
    const remaining = remainingMs(deadline);
    if (remaining < 500) return { ...result, verified: false, verificationError: 'BUDGET_EXHAUSTED' };
    const res = await fetchResponse(result.url, {
      timeout: Math.min(PAGE_TIMEOUT_MS, Math.max(900, remaining - 150)),
      deadline,
      headers: {
        accept: 'text/html,application/xhtml+xml;q=0.9,application/pdf;q=0.8,text/plain;q=0.8,*/*;q=0.2',
        'accept-language': 'en-IN,en;q=0.9',
        'cache-control': 'no-cache',
        pragma: 'no-cache',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'upgrade-insecure-requests': '1',
      },
    });
    const contentType = res.headers.get('content-type') || '';
    const finalUrl = cleanUrl(res.url || result.url, result.url) || result.url;
    if (!res.ok) {
      try { await res.body?.cancel?.(); } catch {}
      if (remainingMs(deadline) > 1600) {
        const reader = await readerFallback(finalUrl, deadline);
        if (reader?.content) {
          return {
            ...result,
            url: finalUrl,
            title: truncate(reader.title || result.title, 300),
            extractedText: reader.content,
            pageContent: reader.content,
            verified: true,
            verificationMethod: 'jina-reader-fallback',
            httpStatus: res.status,
            contentType: 'text/markdown',
            domain: hostname(finalUrl),
            trust: domainTrust(finalUrl),
          };
        }
      }
      return { ...result, verified: false, httpStatus: res.status, contentType, domain: hostname(finalUrl), trust: domainTrust(finalUrl) };
    }

    if (/application\/pdf/i.test(contentType) || /\.pdf(?:\?|$)/i.test(finalUrl)) {
      const bytes = await readBodyBytes(res, MAX_PAGE_BYTES, deadline);
      let pdfText = await extractPdfText(bytes, deadline);
      if (!pdfText && remainingMs(deadline) > 1600) {
        const reader = await readerFallback(finalUrl, deadline);
        pdfText = reader?.content || '';
      }
      const title = result.title || finalUrl.split('/').pop() || 'PDF document';
      return {
        ...result,
        url: finalUrl,
        title: truncate(title, 300),
        verified: true,
        httpStatus: res.status,
        contentType: contentType || 'application/pdf',
        trust: domainTrust(finalUrl),
        domain: hostname(finalUrl),
        extractedText: pdfText || null,
        pageContent: pdfText || null,
        contentTruncated: Boolean(pdfText && pdfText.length >= MAX_TEXT_CHARS),
        verificationMethod: pdfText ? 'direct-pdf-text' : 'direct-pdf-no-text',
      };
    }

    if (!/text\/html|application\/xhtml|text\/plain|application\/json/i.test(contentType)) {
      try { await res.body?.cancel?.(); } catch {}
      return { ...result, verified: true, httpStatus: res.status, contentType, domain: hostname(finalUrl), trust: domainTrust(finalUrl), url: finalUrl };
    }

    const text = await readBodyText(res, MAX_PAGE_BYTES, deadline);
    // Never call a Google News wrapper a verified publisher page. A wrapper may be
    // reachable while the real article is still unavailable.
    if (result.type === 'news' && /^news\.google\.com$/i.test(hostname(finalUrl))) {
      return {
        ...result,
        url: finalUrl,
        verified: false,
        httpStatus: res.status,
        contentType,
        domain: hostname(finalUrl),
        trust: domainTrust(finalUrl),
        verificationError: 'PUBLISHER_URL_NOT_RESOLVED',
      };
    }
    if (/text\/html|application\/xhtml/i.test(contentType) || /<html\b/i.test(text)) {
      const canonical = parseCanonical(text, finalUrl);
      const canonicalUrl = canonical && safeHttpUrl(canonical) ? canonical : finalUrl;
      const title = parseTitleFromHtml(text) || result.title;
      const description = parseMeta(text, 'description') || parseMeta(text, 'og:description') || result.snippet;
      const publishedAt = parseDateCandidate(text) || result.publishedAt || null;
      let bodyText = extractVisibleText(text);
      let method = 'direct-html';
      if (bodyText.length < 350 && remainingMs(deadline) > 1600) {
        const reader = await readerFallback(canonicalUrl, deadline);
        if (reader?.content && reader.content.length > bodyText.length) {
          bodyText = reader.content;
          method = 'jina-reader-fallback';
        }
      }
      return {
        ...result,
        url: canonicalUrl,
        title: truncate(title, 300),
        snippet: truncate(description || result.snippet, 1000),
        publishedAt,
        extractedText: bodyText || null,
        pageContent: bodyText || null,
        contentTruncated: Boolean(bodyText && bodyText.length >= MAX_TEXT_CHARS),
        verified: true,
        httpStatus: res.status,
        contentType,
        domain: hostname(canonicalUrl),
        trust: domainTrust(canonicalUrl),
        verificationMethod: method,
      };
    }

    const plain = truncate(text, MAX_TEXT_CHARS);
    return {
      ...result,
      url: finalUrl,
      verified: true,
      httpStatus: res.status,
      contentType,
      domain: hostname(finalUrl),
      trust: domainTrust(finalUrl),
      extractedText: plain,
      pageContent: plain,
      contentTruncated: Boolean(plain && plain.length >= MAX_TEXT_CHARS),
      verificationMethod: 'direct-text',
    };
  } catch (error) {
    if (remainingMs(deadline) > 1500 && result.type !== 'video') {
      const reader = await readerFallback(result.url, deadline);
      if (reader?.content) {
        return {
          ...result,
          extractedText: reader.content,
          pageContent: reader.content,
          verified: true,
          verificationMethod: 'jina-reader-fallback',
          domain: hostname(result.url),
          trust: domainTrust(result.url),
        };
      }
    }
    return { ...result, verified: false, verificationError: error?.message || 'ENRICH_FAILED', domain: hostname(result.url), trust: domainTrust(result.url) };
  }
}

async function resolveTopNews(results, deadline) {
  const candidates = results.filter(r => r.type === 'news' && /^news\.google\.com$/i.test(hostname(r.url))).slice(0, Math.min(8, MAX_NEWS_RESOLVES));
  if (!candidates.length || remainingMs(deadline) < 1000) return results;
  const resolved = await Promise.all(candidates.map(r => resolvePublisherUrl(r, deadline)));
  const byKey = new Map(candidates.map((r, i) => [normalizedKey(r.url), resolved[i]]));
  return results.map(r => byKey.get(normalizedKey(r.url)) || r);
}

function selectVerificationCandidates(results, count) {
  const out = [];
  const usedDomains = new Set();
  const usedUrls = new Set();
  const ranked = [...results].sort((a, b) => {
    const aw = (/^news\.google\.com$/i.test(hostname(a.url)) ? -8 : 0) + (a.extractedText ? 4 : 0);
    const bw = (/^news\.google\.com$/i.test(hostname(b.url)) ? -8 : 0) + (b.extractedText ? 4 : 0);
    return ((b._score || 0) + bw) - ((a._score || 0) + aw);
  });
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


function enforceRequestedType(results, requestedType, mode, warnings) {
  const wanted = String(requestedType || mode || '').toLowerCase();
  let type = null;
  if (wanted === 'gov') type = 'gov';
  else if (wanted === 'doc' || wanted === 'docs' || wanted === 'document') type = 'doc';
  else if (wanted === 'video') type = 'video';
  else if (wanted === 'news') type = 'news';
  if (!type) return results;
  const matching = results.filter(r => {
    if (type === 'gov') return isGovUrl(r.url);
    if (type === 'doc') return r.type === 'doc' || isDocUrl(r.url);
    return r.type === type;
  });
  if (matching.length) {
    if (matching.length < results.length) warnings.push(`Restricted selected results to requested ${type} sources.`);
    return matching;
  }
  warnings.push(`No verified/discovered ${type} source matched the requested type; broader live results were retained.`);
  return results;
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
    pageContentPreview: truncate(r.pageContent || r.extractedText || '', 2200),
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
  if (discovered.some(r => r.type === 'news' && /^news\.google\.com$/i.test(hostname(r.url))) && remainingMs(deadline) > 3000) {
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
  discovered = enforceRequestedType(discovered, requestedType, mode, warnings);
  for (const r of discovered) {
    r.domain = hostname(r.url);
    r._score = scoreResult(r, query, baseIntent, dateIntent) + (r.verified ? 2 : 0) + domainTrust(r.url);
  }
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
      pageContent: r.pageContent || r.extractedText || null,
      contentTruncated: Boolean(r.contentTruncated),
      verificationMethod: r.verificationMethod || null,
      transcript: r.transcript || null,
      transcriptAvailable: Boolean(r.transcriptAvailable),
      transcriptLanguage: r.transcriptLanguage || null,
      transcriptKind: r.transcriptKind || null,
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
      'AI-readable cleaned source content is exposed in pageContent/extractedText; YouTube captions are exposed in transcript when available.',
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
