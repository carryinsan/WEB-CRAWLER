/*
 * ArixAI Live Web Search / Crawler
 * Vercel Edge Function — single-file, dependency-free, ESM.
 *
 * File: api/crawler.js
 *
 * What it does
 * - Zero required search-provider API keys for core web discovery.
 * - Uses multiple public web surfaces: DuckDuckGo HTML, Bing HTML, Google News RSS,
 *   YouTube search pages, plus direct page fetching for verification/enrichment.
 * - Optional Common Crawl URL-index lookup for historical/cached evidence.
 * - Optional Groq GPT-OSS 20B for query planning + result reranking/verification.
 * - Client can request 1–40 results.
 * - Supports web, news, video, gov, docs, mixed modes.
 * - Returns structured JSON designed for another AI app to consume.
 *
 * Important:
 * This is an independent crawler/search layer, not a full internet-scale search engine.
 * Public search pages and target sites can rate-limit or block automated requests.
 * Core discovery does not require a paid search API, but there is no guarantee of
 * unlimited traffic or universal coverage.
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

const USER_AGENT =
  'Mozilla/5.0 (compatible; ArixAI-LiveSearch/1.0; +https://lexis-ai-chatini.vercel.app/)';

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
        return Number.isFinite(n) ? String.fromCodePoint(n) : _;
      }
      if (c.startsWith('#')) {
        const n = parseInt(c.slice(1), 10);
        return Number.isFinite(n) ? String.fromCodePoint(n) : _;
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
    // Drop common tracking parameters while retaining meaningful query parameters.
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
  const h = hostname(url);
  return GOV_DOMAINS.some(d => h === d || h.endsWith(`.${d}`));
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchText(url, { timeout = SEARCH_TIMEOUT_MS, headers = {}, maxBytes = MAX_PAGE_BYTES } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
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

async function fetchResponse(url, { timeout = SEARCH_TIMEOUT_MS, headers = {} } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
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
  const re = new RegExp(`<meta[^>]+(?:name|property)=["']${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}["'][^>]*content=["']([\\s\\S]*?)["'][^>]*>`, 'i');
  const re2 = new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+(?:name|property)=["']${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}["'][^>]*>`, 'i');
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
    parseMeta(html, 'datePublished'),
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
  const anchors = [...html.matchAll(/<a[^>]+class=[\"'](?:ob|title|result)[^\"']*[\"'][^>]+href=[\"']([^\"']+)[\"'][^>]*>([\s\S]*?)<\/a>/gi)];
  for (const m of anchors) {
    const url = cleanUrl(m[1], 'https://www.mojeek.com/');
    if (!url || /mojeek\.com\/search/i.test(url)) continue;
    out.push({ title: stripTags(m[2]), url, snippet: '', source: 'mojeek', type: 'web' });
  }
  return out.slice(0, 20);
}

function parseYahoo(html) {
  const out = [];
  const blocks = html.match(/<div[^>]+class=[\"'][^\"']*comp[^\"']*[\"'][\s\S]*?<\/div>/gi) || [];
  for (const block of blocks) {
    const m = block.match(/<h3[^>]*>\s*<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>([\s\S]*?)<\/a>/i);
    if (!m) continue;
    const url = cleanUrl(m[1], 'https://search.yahoo.com/');
    if (!url || /search\.yahoo\.com\/search/i.test(url)) continue;
    const snippet = stripTags((block.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [,''])[1]);
    out.push({ title: stripTags(m[2]), url, snippet, source: 'yahoo', type: 'web' });
  }
  return out.slice(0, 20);
}

function parseGoogleWeb(html) {
  const out = [];
  const seen = new Set();
  const anchors = [...html.matchAll(/<a[^>]+href=[\"'](?:\/url\?q=|)([^\"']+)[\"'][^>]*>([\s\S]*?)<\/a>/gi)];
  for (const m of anchors) {
    let raw = m[1];
    if (raw.startsWith('http://') || raw.startsWith('https://')) {
      // direct URL
    } else if (raw.startsWith('\/url?q=')) {
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
    out.push({ title, url, snippet: '', source: 'google', type: 'web' });
    if (out.length >= 20) break;
  }
  return out;
}

function parseDuckDuckGo(html) {
  const out = [];
  const blocks = html.match(/<div[^>]+class=["'][^"']*result[^"']*["'][\s\S]*?<\/div>\s*<\/div>/gi) || [];
  // DDG markup changes. The anchor/snippet pair fallback is intentionally permissive.
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
    const sourceName = stripTags((item.match(/<source[^>]*>([\s\S]*?)<\/source>/i) || [,''])[1]);
    const url = cleanUrl(link, 'https://news.google.com/');
    if (!title || !url) continue;
    out.push({
      title,
      url,
      snippet: desc,
      publishedAt: Number.isNaN(Date.parse(pubDate)) ? null : new Date(pubDate).toISOString(),
      source: sourceName ? `google-news:${sourceName}` : 'google-news',
      type: 'news',
    });
  }
  return out;
}

function parseYoutube(html) {
  const out = [];
  const seen = new Set();
  const matches = [...html.matchAll(/"videoId":"([\w-]{6,20})"/g)];
  for (const m of matches) {
    const id = m[1];
    if (seen.has(id)) continue;
    seen.add(id);
    const idx = m.index || 0;
    const window = html.slice(Math.max(0, idx - 500), Math.min(html.length, idx + 2500));
    const title = decodeHtml(
      (window.match(/"title":\{"runs":\[\{"text":"([^"\\]*(?:\\.[^"\\]*)*)"/i) || [,''])[1]
        .replace(/\\"/g, '"')
        .replace(/\\u0026/g, '&')
    );
    out.push({
      title: title || `YouTube video ${id}`,
      url: `https://www.youtube.com/watch?v=${id}`,
      snippet: 'YouTube video result',
      source: 'youtube',
      type: 'video',
    });
    if (out.length >= 20) break;
  }
  return out;
}

function queryIntent(query, requestedType) {
  const q = query.toLowerCase();
  const hasAny = xs => xs.some(x => q.includes(x));
  const mixed = requestedType === 'mixed' || requestedType === 'all';
  return {
    type: requestedType || (hasAny(VIDEO_QUERY_HINTS) ? 'video' : hasAny(NEWS_QUERY_HINTS) ? 'news' : hasAny(DOC_QUERY_HINTS) ? 'doc' : 'web'),
    wantsNews: mixed || requestedType === 'news' || hasAny(NEWS_QUERY_HINTS),
    wantsVideo: mixed || requestedType === 'video' || hasAny(VIDEO_QUERY_HINTS),
    wantsGov: mixed || requestedType === 'gov' || /\b(india|indian|government|govt|ministry|scheme|gst|income tax|mca|rbi|sebi|law|act|notification|circular|policy)\b/i.test(q),
    wantsDocs: mixed || requestedType === 'doc' || hasAny(DOC_QUERY_HINTS),
  };
}

function buildSearchQueries(query, intent, mode) {
  const qs = new Set([query]);
  if (intent.wantsNews) qs.add(`${query} latest news`);
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
    {
      provider: 'bing',
      type: 'web',
      url: `https://www.bing.com/search?q=${encoded}&count=20&setlang=en-IN&cc=in`,
    },
    {
      provider: 'duckduckgo',
      type: 'web',
      url: `https://html.duckduckgo.com/html/?q=${encoded}&kl=in-en`,
    },
    {
      provider: 'mojeek',
      type: 'web',
      url: `https://www.mojeek.com/search?q=${encoded}`,
    },
    {
      provider: 'yahoo',
      type: 'web',
      url: `https://search.yahoo.com/search?p=${encoded}`,
    },
    {
      provider: 'google',
      type: 'web',
      url: `https://www.google.com/search?q=${encoded}&num=20&hl=en&gl=in`,
    },
  ];
  if (intent.wantsNews) {
    urls.push({
      provider: 'google-news',
      type: 'news',
      url: `https://news.google.com/rss/search?q=${encoded}&hl=en-IN&gl=IN&ceid=IN:en`,
    });
  }
  if (intent.wantsVideo) {
    urls.push({
      provider: 'youtube',
      type: 'video',
      url: `https://www.youtube.com/results?search_query=${encoded}&hl=en-IN`,
    });
  }
  return urls;
}

async function discoverOne(engine) {
  try {
    const text = await fetchText(engine.url, { timeout: SEARCH_TIMEOUT_MS, maxBytes: 600_000 });
    let results = [];
    if (engine.provider === 'bing') results = parseBing(text);
    else if (engine.provider === 'duckduckgo') results = parseDuckDuckGo(text);
    else if (engine.provider === 'mojeek') results = parseMojeek(text);
    else if (engine.provider === 'yahoo') results = parseYahoo(text);
    else if (engine.provider === 'google') results = parseGoogleWeb(text);
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
    return `${u.hostname.toLowerCase()}${u.pathname.replace(/\/+$/, '')}${u.search}`
      .replace(/^www\./, '');
  } catch {
    return url.toLowerCase();
  }
}

function scoreResult(r, query, intent) {
  const q = query.toLowerCase().split(/\s+/).filter(x => x.length > 2);
  const title = (r.title || '').toLowerCase();
  const snippet = (r.snippet || '').toLowerCase();
  const url = (r.url || '').toLowerCase();
  let score = 0;
  for (const token of q) {
    if (title.includes(token)) score += 4;
    if (snippet.includes(token)) score += 1.5;
    if (url.includes(token)) score += 0.5;
  }
  if (intent.wantsGov && isGovUrl(r.url)) score += 8;
  if (intent.wantsNews && r.type === 'news') score += 7;
  if (intent.wantsVideo && r.type === 'video') score += 7;
  if (intent.wantsDocs && isDocUrl(r.url)) score += 5;
  if (r.publishedAt) {
    const ageHours = Math.max(0, (Date.now() - Date.parse(r.publishedAt)) / 36e5);
    score += Math.max(0, 5 - Math.log10(ageHours + 1) * 2);
  }
  if (/login|signin|subscribe|advertis|cookie/i.test(`${title} ${snippet}`)) score -= 1.5;
  if (url.length > 180) score -= 0.25;
  return score;
}

function dedupeResults(results) {
  const map = new Map();
  for (const r of results) {
    const key = normalizedKey(r.url);
    const existing = map.get(key);
    if (!existing || (r.snippet || '').length > (existing.snippet || '').length) {
      map.set(key, r);
    }
  }
  return [...map.values()];
}

function domainTrust(url) {
  const h = hostname(url);
  if (!h) return 0.2;
  if (isGovUrl(url)) return 1.0;
  if (/\.edu(\.|$)/i.test(h)) return 0.95;
  if (/\.ac\.(in|uk|jp|nz)$/i.test(h)) return 0.95;
  if (/wikipedia\.org$/i.test(h)) return 0.8;
  if (/youtube\.com$|youtu\.be$/i.test(h)) return 0.65;
  return 0.6;
}

function safeHttpUrl(url) {
  try {
    const u = new URL(url);
    if (!['http:', 'https:'].includes(u.protocol)) return false;
    const h = u.hostname.toLowerCase();
    if (h === 'localhost' || h.endsWith('.localhost') || h === '0.0.0.0') return false;
    if (/^(127\.|10\.|192\.168\.|169\.254\.)/.test(h)) return false;
    if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(h)) return false;
    if (h === '::1' || h.startsWith('fc') || h.startsWith('fd')) return false;
    return true;
  } catch {
    return false;
  }
}

async function enrichResult(result) {
  if (!safeHttpUrl(result.url)) return { ...result, verified: false, verificationError: 'UNSAFE_URL' };
  try {
    const res = await fetchResponse(result.url, {
      timeout: PAGE_TIMEOUT_MS,
      headers: { accept: 'text/html,application/xhtml+xml;q=0.9,text/plain;q=0.8,*/*;q=0.2' },
    });
    const contentType = res.headers.get('content-type') || '';
    const text = await (async () => {
      const reader = res.body?.getReader?.();
      if (!reader) return await res.text();
      const chunks = [];
      let total = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        total += value.byteLength;
        if (total > MAX_PAGE_BYTES) break;
        chunks.push(value);
      }
      const bytes = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
      let off = 0;
      for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
      return new TextDecoder().decode(bytes);
    })();

    if (/text\/html|application\/xhtml/i.test(contentType) || /<html\b/i.test(text)) {
      const canonical = parseCanonical(text, result.url);
      const title = parseTitleFromHtml(text) || result.title;
      const description = parseMeta(text, 'description') || parseMeta(text, 'og:description') || result.snippet;
      const publishedAt = parseDateCandidate(text) || result.publishedAt || null;
      const bodyText = extractVisibleText(text);
      const finalUrl = canonical || result.url;
      return {
        ...result,
        url: finalUrl,
        title: truncate(title, 300),
        snippet: truncate(description || result.snippet, 1000),
        publishedAt,
        extractedText: bodyText,
        verified: true,
        httpStatus: res.status,
        contentType,
        domain: hostname(finalUrl),
        trust: domainTrust(finalUrl),
      };
    }

    return {
      ...result,
      verified: res.ok,
      httpStatus: res.status,
      contentType,
      domain: hostname(result.url),
      trust: domainTrust(result.url),
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

async function commonCrawlLookup(url) {
  // Best-effort historical verification. No API key required.
  if (!safeHttpUrl(url)) return null;
  const encoded = encodeURIComponent(url);
  for (const index of COMMON_CRAWL_INDEXES) {
    try {
      const ccUrl = `https://index.commoncrawl.org/${index}-index?url=${encoded}&output=json&filter=status:200&limit=3`;
      const body = await fetchText(ccUrl, {
        timeout: 3500,
        maxBytes: 80_000,
        headers: { accept: 'application/json,text/plain;q=0.8,*/*;q=0.1' },
      });
      const lines = body.trim().split('\n').filter(Boolean);
      if (!lines.length) continue;
      const rows = lines.map(x => {
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


async function groqBrowserFallback(query, count) {
  const key = typeof process !== 'undefined' ? process.env?.GROQ_API_KEY : undefined;
  if (!key) return [];
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 9000);
  try {
    const res = await fetch('https://api.groq.com/openai/v1/responses', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: 'openai/gpt-oss-20b',
        input: `Search the live web for: ${query}. Return ${Math.min(20, count)} distinct high-quality results as JSON in this exact shape: {"results":[{"title":"...","url":"https://...","snippet":"...","type":"web|news|video|gov|doc","publishedAt":"ISO-or-null"}]}. Only include URLs you actually found through browser search. Do not invent URLs. Prefer primary/official sources when appropriate.`,
        tool_choice: 'required',
        tools: [{ type: 'browser_search' }],
      }),
    });
    if (!res.ok) return [];
    const data = await res.json();
    const outputText = data?.output_text || data?.output?.map(x => x?.content?.map(c => c?.text || '').join(' ')).join('\n') || '';
    const parsed = extractJson(outputText);
    if (!parsed?.results || !Array.isArray(parsed.results)) return [];
    return parsed.results.slice(0, 20).map(r => ({
      title: truncate(r.title || '', 300),
      url: cleanUrl(r.url, 'https://www.google.com/'),
      snippet: truncate(r.snippet || '', 1200),
      source: 'groq-browser',
      type: ['web', 'news', 'video', 'gov', 'doc'].includes(r.type) ? r.type : 'web',
      publishedAt: r.publishedAt ? new Date(r.publishedAt).toISOString() : null,
    })).filter(r => r.url && safeHttpUrl(r.url));
  } catch {
    return [];
  } finally {
    clearTimeout(timer);
  }
}

async function callGroq(messages, maxTokens = 1600) {
  const key = typeof process !== 'undefined' ? process.env?.GROQ_API_KEY : undefined;
  if (!key) return null;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 7000);
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

function extractJson(text) {
  if (!text) return null;
  try { return JSON.parse(text); } catch {}
  const m = text.match(/\{[\s\S]*\}/);
  if (!m) return null;
  try { return JSON.parse(m[0]); } catch { return null; }
}

async function aiPlan(query, mode, count) {
  const text = await callGroq([
    {
      role: 'system',
      content: [
        'You are a search-routing planner for a web search engine.',
        'Return only JSON.',
        'Do not fabricate websites or URLs.',
        'Create concise query variants for public web search engines.',
        'Prefer official/primary sources when appropriate.',
        'Avoid overfitting to a single domain unless the query is clearly domain-specific.',
      ].join(' '),
    },
    {
      role: 'user',
      content: JSON.stringify({
        query,
        mode,
        requestedResults: count,
        schema: {
          intent: 'web|news|video|gov|doc|mixed',
          queries: ['query 1', 'query 2', 'query 3'],
          mustPreferOfficial: true,
          freshness: 'live|recent|any',
        },
      }),
    },
  ], 900);
  const data = extractJson(text);
  if (!data) return null;
  return {
    intent: data.intent || 'web',
    queries: Array.isArray(data.queries) ? data.queries.filter(Boolean).slice(0, 6) : [],
    mustPreferOfficial: Boolean(data.mustPreferOfficial),
    freshness: ['live', 'recent', 'any'].includes(data.freshness) ? data.freshness : 'any',
  };
}

async function aiRerank(query, results, mode) {
  if (!results.length) return null;
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
        'You are a web-search reranker and verifier.',
        'Never invent evidence.',
        'Rank only the provided result IDs.',
        'Prefer direct/primary sources, exact query match, freshness when requested, and verified pages.',
        'Return JSON only.',
      ].join(' '),
    },
    {
      role: 'user',
      content: JSON.stringify({
        query,
        mode,
        results: payload,
        output: { order: [0, 1], notes: 'short optional note', confidence: 0.0 },
      }),
    },
  ], 1400);
  const data = extractJson(text);
  if (!data || !Array.isArray(data.order)) return null;
  return data.order.map(x => Number(x)).filter(Number.isInteger).filter(x => x >= 0 && x < results.length);
}

function sortResults(results) {
  return results.sort((a, b) => (b._score || 0) - (a._score || 0));
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

async function readInput(req) {
  const url = new URL(req.url);
  if (req.method === 'GET') {
    return Object.fromEntries(url.searchParams.entries());
  }
  const raw = await req.text();
  if (!raw) return {};
  try { return JSON.parse(raw); } catch { return Object.fromEntries(new URLSearchParams(raw).entries()); }
}

function parseCount(value) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n)) return DEFAULT_RESULTS;
  return Math.min(MAX_RESULTS, Math.max(1, n));
}

export default async function handler(req) {
  const started = Date.now();
  if (req.method === 'OPTIONS') return withCors({ ok: true, version: VERSION });

  try {
    const input = await readInput(req);
    const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
    const count = parseCount(input.count ?? input.limit ?? DEFAULT_RESULTS);
    const mode = String(input.mode || 'auto').toLowerCase();
    const requestedType = String(input.type || '').toLowerCase();
    const deep = String(input.deep ?? 'false').toLowerCase() === 'true';
    const useAi = String(input.ai ?? input.useAi ?? 'auto').toLowerCase();
    const verifyRequested = input.verify == null ? true : String(input.verify).toLowerCase() !== 'false';
    const useCc = input.commonCrawl == null ? false : String(input.commonCrawl).toLowerCase() === 'true';

    if (!query) {
      return withCors({
        ok: false,
        error: 'MISSING_QUERY',
        message: 'Provide query in ?query=... or a JSON body with {"query":"..."}.',
      }, 400);
    }

    if (query.length < 2) {
      return withCors({ ok: false, error: 'QUERY_TOO_SHORT' }, 400);
    }

    const baseIntent = queryIntent(query, requestedType || null);
    const autoNeedsAi = deep || count > 10 || query.length > 120 ||
      baseIntent.wantsNews || baseIntent.wantsVideo || baseIntent.wantsGov ||
      baseIntent.wantsDocs || mode === 'deep' || mode === 'gov' || mode === 'doc';
    const shouldAiPlan = useAi === 'true' || (useAi === 'auto' && autoNeedsAi);
    let ai = null;
    if (shouldAiPlan) ai = await aiPlan(query, mode, count);

    const intent = {
      ...baseIntent,
      type: requestedType || ai?.intent || baseIntent.type,
      wantsNews: baseIntent.wantsNews || ai?.intent === 'news',
      wantsVideo: baseIntent.wantsVideo || ai?.intent === 'video',
      wantsGov: baseIntent.wantsGov || ai?.intent === 'gov' || mode === 'gov',
      wantsDocs: baseIntent.wantsDocs || ai?.intent === 'doc' || mode === 'doc',
    };

    const plannedQueries = ai?.queries?.length
      ? [...new Set([query, ...ai.queries])].slice(0, 6)
      : buildSearchQueries(query, intent, mode);

    let engineRequests = [];
    for (const q of plannedQueries) {
      engineRequests.push(...buildEngineUrls(q, intent));
    }
    // Preserve a hard request cap so an adversarial query cannot explode fetch count.
    engineRequests = engineRequests.slice(0, MAX_ENGINE_REQUESTS);

    const discoveryResponses = await Promise.allSettled(engineRequests.map(discoverOne));
    let discovered = [];
    const providerStats = {};
    for (const entry of discoveryResponses) {
      if (entry.status !== 'fulfilled') continue;
      providerStats[entry.value.provider] = providerStats[entry.value.provider] || { ok: 0, failed: 0, results: 0 };
      if (entry.value.ok) providerStats[entry.value.provider].ok += 1;
      else providerStats[entry.value.provider].failed += 1;
      providerStats[entry.value.provider].results += entry.value.results.length;
      discovered.push(...entry.value.results);
    }

    discovered = discovered
      .filter(r => r?.url && safeHttpUrl(r.url))
      .map(r => ({ ...r, type: inferType(r), domain: hostname(r.url) }));

    discovered = dedupeResults(discovered);

    // Optional resilience fallback: Groq's built-in browser search is only used when
    // the keyless public surfaces returned nothing and GROQ_API_KEY exists.
    if (!discovered.length && (useAi === 'true' || useAi === 'auto')) {
      const fallback = await groqBrowserFallback(query, count);
      discovered = dedupeResults(fallback.map(r => ({ ...r, domain: hostname(r.url) })));
    }

    // Deterministic local ranking first. AI reranking is an enhancement, never a dependency.
    for (const r of discovered) {
      r._score = scoreResult(r, query, intent) + (r.verified ? 2 : 0) + domainTrust(r.url);
    }
    sortResults(discovered);

    const verifyCount = verifyRequested ? Math.min(discovered.length, deep ? DEEP_VERIFY : DEFAULT_VERIFY) : 0;
    if (verifyCount > 0) {
      const top = discovered.slice(0, verifyCount);
      const verified = await Promise.all(top.map(enrichResult));
      for (let i = 0; i < verifyCount; i++) discovered[i] = verified[i];
      for (const r of discovered) {
        r._score = scoreResult(r, query, intent) + (r.verified ? 3 : 0) + (r.trust || domainTrust(r.url));
      }
      sortResults(discovered);
    }

    if (useCc && discovered.length) {
      const ccCount = Math.min(4, discovered.length);
      const ccRows = await Promise.all(discovered.slice(0, ccCount).map(async r => ({
        url: r.url,
        cc: await commonCrawlLookup(r.url),
      })));
      const ccMap = new Map(ccRows.map(x => [x.url, x.cc]));
      for (const r of discovered) r.commonCrawl = ccMap.get(r.url) || null;
    }

    let aiOrder = null;
    const shouldAiRerank = useAi === 'true' || (useAi === 'auto' && (deep || count > 10 || discovered.length > 15 || ai));
    if (shouldAiRerank && discovered.length > 1) {
      aiOrder = await aiRerank(query, discovered, mode);
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

    const finalResults = discovered.slice(0, count).map((r, i) => ({
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
      extractedText: r.extractedText || null,
      commonCrawl: r.commonCrawl || null,
    }));

    return withCors({
      ok: true,
      version: VERSION,
      query,
      requestedResults: count,
      returnedResults: finalResults.length,
      mode,
      intent,
      generatedAt: nowIso(),
      latencyMs: Date.now() - started,
      keylessCoreSearch: true,
      groqUsed: Boolean(ai || aiOrder),
      providers: providerStats,
      searchPlan: {
        queryVariants: plannedQueries,
        engineRequests: engineRequests.length,
        verificationRequested: verifyRequested,
        verificationPerformed: verifyCount,
        commonCrawlEnabled: useCc,
      },
      results: finalResults,
      warnings: [
        'Core discovery is keyless but depends on public web surfaces that may rate-limit or block automated requests.',
        'This endpoint is not a substitute for an internet-scale index; use a provider or your own persistent index for very high volume.',
      ],
    });
  } catch (error) {
    return withCors({
      ok: false,
      version: VERSION,
      error: 'SEARCH_FAILED',
      message: error?.message || 'Unexpected crawler error',
    }, 500);
  }
}
