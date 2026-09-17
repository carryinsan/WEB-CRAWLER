/*
 * ArixAI Live Web Search / Crawler
 * Vercel Edge Function — single-file, dependency-free, ESM.
 *
 * File: api/crawler.js
 *
 * Compatibility:
 * - Preserves the existing GET/POST request contract and response fields.
 * - Preserves mode/type/count/deep/verify/ai/commonCrawl controls.
 * - Preserves keyless public discovery and optional GROQ_API_KEY usage.
 * - Adds a layered, non-null AI-readable pageContent pipeline.
 *
 * CONTENT PIPELINE
 * 1) Resolve search wrappers to the real publisher URL where possible.
 * 2) Fetch the live publisher page directly.
 * 3) Extract article/main/body text plus JSON-LD metadata.
 * 4) Try a dependency-free AMP/text variant where sensible.
 * 5) Try the public Jina Reader endpoint as a keyless HTML-to-text fallback.
 * 6) For YouTube, retrieve real metadata and captions where available.
 * 7) For PDFs, extract real text when the PDF contains extractable text.
 * 8) If a site blocks all content extraction, NEVER return null content:
 *    pageContent/extractedText contain the real search snippet + metadata and
 *    contentStatus explains that it is a discovery/snippet fallback rather than
 *    silently pretending that the full page was retrieved.
 *
 * TIME / STREAMING
 * - The response begins with valid JSON immediately, not just whitespace.
 * - Heartbeats keep the stream active while live work continues.
 * - Upstream calls are bounded individually.
 * - The crawler never attempts to evade provider/site rate limits.
 * - No serverless implementation can honestly guarantee unlimited execution;
 *   Vercel remains the platform authority on maximum execution duration.
 */

export const runtime = 'edge';
export const config = { runtime: 'edge' };
export const maxDuration = 300;

const VERSION = 'arix-crawler-1.4.0';
const MAX_RESULTS = 40;
const DEFAULT_RESULTS = 10;
const MAX_QUERY_LEN = 500;
const MAX_REQUEST_BODY = 64_000;

const SEARCH_TIMEOUT_MS = 4500;
const PAGE_TIMEOUT_MS = 6500;
const READER_TIMEOUT_MS = 9000;
const NEWS_RESOLVE_TIMEOUT_MS = 2500;
const COMMON_CRAWL_TIMEOUT_MS = 3000;
const YOUTUBE_TIMEOUT_MS = 8000;
const PDF_DECOMPRESS_TIMEOUT_MS = 3500;

const MAX_PAGE_BYTES = 900_000;
const MAX_SEARCH_BYTES = 700_000;
const MAX_YOUTUBE_BYTES = 1_800_000;
const MAX_TEXT_CHARS = 30_000;
const MAX_TRANSCRIPT_CHARS = 30_000;

const DEFAULT_VERIFY = 8;
const DEEP_VERIFY = 12;
const MAX_VERIFY = 12;
const MAX_ENGINE_REQUESTS = 12;
const MAX_NEWS_RESOLVES = 10;
const MAX_PUBLISHER_LOOKUPS = 6;
const MAX_CC_LOOKUPS = 4;

// Starts below Vercel's documented Edge streaming ceiling and leaves safety margin.
const STREAM_HEARTBEAT_MS = 4000;
const SEARCH_WORK_BUDGET_MS = 240_000;

const USER_AGENT =
  'Mozilla/5.0 (compatible; ArixAI-LiveSearch/1.3; +https://lexis-ai-chatini.vercel.app/)';

const COMMON_CRAWL_INDEXES = [
  'CC-MAIN-2026-34',
  'CC-MAIN-2026-30',
  'CC-MAIN-2026-21',
];

const GOV_DOMAINS = [
  'gov.in', 'nic.in', 'mygov.in', 'india.gov.in', 'pib.gov.in', 'mca.gov.in',
  'gst.gov.in', 'incometax.gov.in', 'msme.gov.in', 'education.gov.in', 'meity.gov.in',
  'rbi.org.in', 'sebi.gov.in', 'supremecourt.gov.in', 'indiacode.nic.in',
];

const TRUSTED_INTERNATIONAL = [
  'nasa.gov', 'who.int', 'un.org', 'europa.eu', 'oecd.org', 'worldbank.org',
  'imf.org', 'ietf.org', 'w3.org', 'mozilla.org', 'developer.mozilla.org',
];

const NEWS_QUERY_HINTS = [
  'news', 'latest', 'today', 'recent', 'breaking', 'update', 'updates',
  'happened', 'announced', 'announcement', 'this week', 'yesterday',
];

const VIDEO_QUERY_HINTS = [
  'video', 'videos', 'watch', 'youtube', 'interview', 'podcast', 'explained', 'tutorial',
];

const DOC_QUERY_HINTS = [
  'pdf', 'documentation', 'docs', 'manual', 'report', 'paper', 'research paper',
  'whitepaper', 'specification', 'spec',
];

const MONTHS = {
  january: 0, jan: 0, february: 1, feb: 1, march: 2, mar: 2,
  april: 3, apr: 3, may: 4, june: 5, jun: 5, july: 6, jul: 6,
  august: 7, aug: 7, september: 8, sep: 8, sept: 8, october: 9, oct: 9,
  november: 10, nov: 10, december: 11, dec: 11,
};

const STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'what', 'when', 'where', 'how',
  'why', 'who', 'which', 'about', 'into', 'near', 'over', 'under', 'latest', 'news',
  'today', 'recent', 'current', 'update', 'updates', 'during', 'august', 'september',
  'october', 'january', 'february', 'march', 'april', 'june', 'july', 'november',
  'december', '2025', '2026', '2027', '2028',
]);

const HTML_ENTITY_RE = /&(#x?[0-9a-f]+|[a-z][a-z0-9]+);/gi;

function decodeHtml(value = '') {
  return String(value)
    .replace(HTML_ENTITY_RE, (_, code) => {
      const c = code.toLowerCase();
      const map = {
        amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', ndash: '–',
        mdash: '—', hellip: '…', laquo: '«', raquo: '»', rsquo: '’', lsquo: '‘',
        rdquo: '”', ldquo: '“', bull: '•', middot: '·', copy: '©', reg: '®',
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
      .replace(/<template\b[^>]*>[\s\S]*?<\/template>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
  );
}

function truncate(value, max) {
  const v = String(value || '').trim();
  return v.length <= max ? v : `${v.slice(0, Math.max(0, max - 1))}…`;
}

function nowIso() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function remainingMs(deadline) { return Math.max(0, deadline - Date.now()); }

function safeInteger(value, fallback, min, max) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function parseCount(value) {
  return safeInteger(value, DEFAULT_RESULTS, 1, MAX_RESULTS);
}

function effectiveTimeout(requested, deadline, floor = 250) {
  const left = remainingMs(deadline);
  if (left <= floor) return 0;
  return Math.min(requested, Math.max(floor, left - 150));
}

function absoluteUrl(raw, base = 'https://example.com/') {
  try { return new URL(decodeHtml(raw), base).href; } catch { return null; }
}

function cleanUrl(raw, base) {
  const u = absoluteUrl(raw, base);
  if (!u) return null;
  try {
    const parsed = new URL(u);
    parsed.hash = '';
    for (const key of [...parsed.searchParams.keys()]) {
      if (/^(utm_|gclid$|fbclid$|ref$|referrer$|cmpid$|src$|msclkid$)/i.test(key)) {
        parsed.searchParams.delete(key);
      }
    }
    return parsed.href;
  } catch { return null; }
}

function hostname(url) {
  try { return new URL(url).hostname.toLowerCase(); } catch { return ''; }
}

function normalizedHost(url) { return hostname(url).replace(/^www\./, ''); }

function normalizedKey(url) {
  try {
    const u = new URL(url);
    const p = u.pathname.replace(/\/{2,}/g, '/').replace(/\/$/, '');
    return `${normalizedHost(url)}${p}${u.search}`;
  } catch { return String(url || '').toLowerCase(); }
}

const BLOCKED_CONTENT_HOSTS = [
  'google-analytics.com', 'googletagmanager.com', 'doubleclick.net', 'googlesyndication.com',
  'googleadservices.com', 'googleusercontent.com', 'gstatic.com', 'googleapis.com',
  'facebook.net', 'connect.facebook.net', 'scorecardresearch.com', 'pixel.wp.com',
  'adsrvr.org', 'amazon-adsystem.com', 'taboola.com', 'outbrain.com',
];

const BLOCKED_CONTENT_EXTENSIONS = /\.(?:js|mjs|cjs|css|map|png|jpe?g|gif|webp|avif|svg|ico|bmp|tiff?|woff2?|ttf|otf|eot|mp3|wav|m4a|mp4|webm|mov|avi|zip|rar|7z|exe|dmg)(?:$|[?#])/i;
const BLOCKED_DATA_PATH = /(?:^|[\/_-])(analytics|gtag|ga4|collect|pixel|beacon|tracking|tracker|telemetry|consent|ads?)(?:[\/_-]|$)/i;
const ARTICLE_PATH_HINT = /(?:article|articles|story|stories|news|post|posts|blog|blogs|update|updates|report|reports|press-release|pressrelease|explained|live|202\d[\/.-]\d{1,2}[\/.-]\d{1,2})/i;

function isBlockedContentHost(url) {
  const h = normalizedHost(url);
  return BLOCKED_CONTENT_HOSTS.some(d => h === d || h.endsWith(`.${d}`));
}

function isBlockedContentUrl(url) {
  if (!url) return true;
  if (isBlockedContentHost(url)) return true;
  let u;
  try { u = new URL(url); } catch { return true; }
  const pathAndQuery = `${u.pathname}${u.search}`;
  if (BLOCKED_CONTENT_EXTENSIONS.test(pathAndQuery)) return true;
  if (BLOCKED_DATA_PATH.test(u.pathname)) return true;
  if (/(?:[?&](?:collect|measurement|tid|cid|ea|ec|el|t|v|_ga|_gl)=)/i.test(u.search)) return true;
  return false;
}

function isPublisherCandidateUrl(url, result = null) {
  if (!safeHttpUrl(url) || isBlockedContentUrl(url)) return false;
  const h = normalizedHost(url);
  if (!h || /^(?:news\.)?google\./i.test(h) || /^gstatic\./i.test(h)) return false;
  if (/^(?:www\.)?(bing|search\.yahoo|duckduckgo|mojeek)\./i.test(h)) return false;
  if (result?.type === 'doc') return isDocUrl(url) && !isBlockedContentUrl(url);
  if (result?.type === 'video') return isLikelyVideoUrl(url);
  return true;
}

function pathDepth(url) {
  try { return new URL(url).pathname.split('/').filter(Boolean).length; } catch { return 0; }
}

function sourceHostHint(result) {
  const source = String(result?.source || '').replace(/^google-news:/i, '').trim().toLowerCase();
  if (!source) return '';
  return source.replace(/[^a-z0-9]+/g, ' ').trim();
}

function candidatePublisherScore(url, result, anchorText = '') {
  if (!isPublisherCandidateUrl(url, result)) return -Infinity;
  let score = 0;
  const u = new URL(url);
  const path = `${u.pathname}${u.search}`;
  score += Math.min(6, pathDepth(url));
  if (ARTICLE_PATH_HINT.test(path)) score += 5;
  if (/^www\./i.test(u.hostname)) score += 0.2;
  const hint = sourceHostHint(result);
  if (hint) {
    const hostWords = normalizedHost(url).replace(/\./g, ' ').split(/\s+/).filter(Boolean);
    const hintWords = hint.split(/\s+/).filter(w => w.length > 2);
    if (hintWords.some(w => hostWords.includes(w))) score += 8;
  }
  const anchor = String(anchorText || '').toLowerCase();
  const titleTokens = extractSearchTerms(String(result?.title || '')).slice(0, 12);
  for (const t of titleTokens) if (anchor.includes(t)) score += 0.5;
  return score;
}

function isGovUrl(url) {
  const h = normalizedHost(url);
  return GOV_DOMAINS.some(d => h === d || h.endsWith(`.${d}`));
}

function isTrustedInternational(url) {
  const h = normalizedHost(url);
  return TRUSTED_INTERNATIONAL.some(d => h === d || h.endsWith(`.${d}`));
}

function isYouTube(url) {
  const h = normalizedHost(url);
  return h === 'youtube.com' || h.endsWith('.youtube.com') || h === 'youtu.be';
}

function isLikelyVideoUrl(url) {
  return isYouTube(url) || normalizedHost(url).includes('vimeo.com') ||
    /\.(mp4|webm|mov)(\?|$)/i.test(url || '');
}

function isDocUrl(url) {
  return /\.(pdf|docx?|xlsx?|pptx?)(\?|$)/i.test(url || '') ||
    /\b(pdf|docs?|documentation)\b/i.test(url || '');
}

function domainTrust(url) {
  const h = normalizedHost(url);
  if (!h) return 0.2;
  if (isGovUrl(url)) return 1;
  if (isTrustedInternational(url)) return 0.95;
  if (/\.edu(?:\.|$)/i.test(h)) return 0.95;
  if (/\.ac\.(?:in|uk|jp|nz)$/i.test(h)) return 0.95;
  if (/wikipedia\.org$/i.test(h)) return 0.8;
  if (isYouTube(url)) return 0.65;
  if (h === 'news.google.com') return 0.45;
  return 0.6;
}

function inferType(result) {
  if (result.type) return result.type;
  if (isGovUrl(result.url)) return 'gov';
  if (isLikelyVideoUrl(result.url)) return 'video';
  if (isDocUrl(result.url)) return 'doc';
  return 'web';
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
  } catch { return false; }
}

async function fetchResponse(url, { timeout = SEARCH_TIMEOUT_MS, headers = {}, deadline } = {}) {
  const dl = deadline || Date.now() + timeout;
  const ms = effectiveTimeout(timeout, dl, 250);
  if (!ms) throw new Error('BUDGET_EXHAUSTED');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  try {
    return await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'user-agent': USER_AGENT,
        accept: 'text/html,application/xhtml+xml,application/xml,application/pdf,text/plain;q=0.9,*/*;q=0.3',
        'accept-language': 'en-IN,en;q=0.9',
        ...headers,
      },
    });
  } finally { clearTimeout(timer); }
}

async function readBodyText(response, maxBytes, deadline) {
  const reader = response.body?.getReader?.();
  if (!reader) return truncate(await response.text(), maxBytes);
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      if (remainingMs(deadline) < 120) break;
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
  } finally {
    try { await reader.cancel(); } catch {}
  }
  const bytes = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
  return new TextDecoder('utf-8', { fatal: false }).decode(bytes);
}

async function readBodyBytes(response, maxBytes, deadline) {
  const reader = response.body?.getReader?.();
  if (!reader) {
    const ab = await response.arrayBuffer();
    return new Uint8Array(ab).slice(0, maxBytes);
  }
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      if (remainingMs(deadline) < 120) break;
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
  } finally {
    try { await reader.cancel(); } catch {}
  }
  const bytes = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
  return bytes;
}

async function fetchText(url, options = {}) {
  const response = await fetchResponse(url, options);
  if (!response.ok) throw new Error(`HTTP_${response.status}`);
  return readBodyText(response, options.maxBytes || MAX_SEARCH_BYTES, options.deadline || Date.now() + (options.timeout || SEARCH_TIMEOUT_MS));
}

function parseTitleFromHtml(html) {
  return decodeHtml((html.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [, ''])[1]);
}

function parseMeta(html, name) {
  const escaped = String(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const a = new RegExp(`<meta[^>]+(?:name|property)=["']${escaped}["'][^>]*content=["']([\\s\\S]*?)["'][^>]*>`, 'i');
  const b = new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+(?:name|property)=["']${escaped}["'][^>]*>`, 'i');
  return decodeHtml((html.match(a) || html.match(b) || [, ''])[1]);
}

function parseCanonical(html, baseUrl) {
  const a = (html.match(/<link[^>]+rel=["'][^"']*canonical[^"']*["'][^>]*href=["']([^"']+)["'][^>]*>/i) || [, ''])[1];
  const b = (html.match(/<link[^>]+href=["']([^"']+)["'][^>]*rel=["'][^"']*canonical[^"']*["'][^>]*>/i) || [, ''])[1];
  return cleanUrl(a || b, baseUrl);
}

function parseJsonLd(html) {
  const values = [];
  const blocks = html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const block of blocks.slice(0, 20)) {
    const raw = block.replace(/^<[\s\S]*?>/i, '').replace(/<\/script>$/i, '');
    try {
      const json = JSON.parse(raw.trim());
      const visit = value => {
        if (!value || typeof value !== 'object') return;
        if (Array.isArray(value)) { value.forEach(visit); return; }
        if (value.articleBody) values.push({ kind: 'articleBody', value: String(value.articleBody) });
        if (value.description) values.push({ kind: 'description', value: String(value.description) });
        if (value.headline) values.push({ kind: 'headline', value: String(value.headline) });
        if (value.datePublished) values.push({ kind: 'datePublished', value: String(value.datePublished) });
        if (value.dateModified) values.push({ kind: 'dateModified', value: String(value.dateModified) });
        Object.values(value).forEach(visit);
      };
      visit(json);
    } catch {}
  }
  return values;
}

function parseDateFromHtml(html) {
  const jsonLd = parseJsonLd(html);
  const candidates = [
    parseMeta(html, 'article:published_time'), parseMeta(html, 'article:modified_time'),
    parseMeta(html, 'datePublished'), parseMeta(html, 'dateModified'), parseMeta(html, 'pubdate'),
    parseMeta(html, 'date'), parseMeta(html, 'parsely-pub-date'), parseMeta(html, 'dc.date'),
    (html.match(/<time[^>]+datetime=["']([^"']+)["'][^>]*>/i) || [, ''])[1],
    ...jsonLd.filter(x => /date/i.test(x.kind)).map(x => x.value),
  ].filter(Boolean);
  for (const c of candidates) {
    const t = Date.parse(c);
    if (!Number.isNaN(t)) return new Date(t).toISOString();
  }
  return null;
}

function extractStructuredBody(html) {
  const pieces = [];
  const jsonLd = parseJsonLd(html);
  for (const item of jsonLd) {
    if (item.kind === 'articleBody' && item.value.length > 200) pieces.push(item.value);
  }
  const semanticBlocks = html.match(/<(?:article|main)\b[^>]*>[\s\S]*?<\/(?:article|main)>/gi) || [];
  for (const block of semanticBlocks.slice(0, 8)) pieces.push(stripTags(block));

  // Paragraph/headline extraction catches pages whose main container is fragmented.
  const textualTags = html.match(/<(?:h1|h2|h3|h4|p|li|blockquote)\b[^>]*>[\s\S]*?<\/(?:h1|h2|h3|h4|p|li|blockquote)>/gi) || [];
  for (const block of textualTags.slice(0, 500)) {
    const text = stripTags(block);
    if (text.length >= 30) pieces.push(text);
  }
  return pieces.join(' ');
}

function cleanExtractedText(text) {
  return truncate(
    String(text || '')
      .replace(/\[[^\]]{0,80}\]\([^)]{0,500}\)/g, ' ')
      .replace(/!\[[^\]]*\]\([^)]*\)/g, ' ')
      .replace(/\b(function|var|const|let)\s+[^;]{0,220};?/g, ' ')
      .replace(/(?:skip to content|accept cookies|cookie settings|privacy settings|sign in|log in|subscribe|menu|search this site)\b/gi, ' ')
      .replace(/\s+/g, ' ')
      .trim(),
    MAX_TEXT_CHARS,
  );
}

function extractVisibleText(html) {
  const structured = cleanExtractedText(extractStructuredBody(html));
  if (structured.length >= 500) return structured;
  const candidates = [
    html.match(/<article\b[^>]*>([\s\S]*?)<\/article>/i)?.[1],
    html.match(/<main\b[^>]*>([\s\S]*?)<\/main>/i)?.[1],
    html.match(/<body\b[^>]*>([\s\S]*?)<\/body>/i)?.[1],
    html,
  ].filter(Boolean);
  let best = structured;
  for (const candidate of candidates) {
    const text = cleanExtractedText(stripTags(String(candidate)));
    if (text.length > best.length) best = text;
  }
  return best;
}

function extractSnippetFromSearchHtml(html, title = '') {
  const cleaned = cleanExtractedText(stripTags(html));
  if (!cleaned) return title ? `Search result for ${title}.` : 'Search result.';
  return truncate(cleaned, 1200);
}

function textFromMarkdown(markdown) {
  return cleanExtractedText(
    String(markdown || '')
      .replace(/```[\s\S]*?```/g, ' ')
      .replace(/\[([^\]]+)\]\((?:https?:\/\/|\/)[^)]*\)/g, '$1')
      .replace(/<https?:\/\/[^>]+>/g, ' ')
      .replace(/^#{1,6}\s*/gm, '')
      .replace(/[*_~`]/g, ' ')
  );
}

function extractFallbackContent(result, query) {
  const title = truncate(result.title || 'Untitled', 500);
  const snippet = truncate(stripTags(result.snippet || ''), 2500);
  const pieces = [
    `Title: ${title}`,
    `Source: ${result.source || 'web search'}`,
    `Domain: ${hostname(result.url) || 'unknown'}`,
    result.publishedAt ? `Published: ${result.publishedAt}` : '',
    snippet ? `Search snippet: ${snippet}` : '',
    `Query context: ${query}`,
  ].filter(Boolean);
  return pieces.join('\n');
}

function safeIsoDate(value) {
  const t = Date.parse(String(value || ''));
  return Number.isNaN(t) ? null : new Date(t).toISOString();
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

function extractSearchTerms(query) {
  return query.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, ' ').split(/\s+/)
    .filter(x => x.length > 2 && !STOPWORDS.has(x));
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
    wantsNews, wantsVideo, wantsGov, wantsDocs,
  };
}

function parseDateIntent(query) {
  const q = query.toLowerCase();
  const now = new Date();
  const latest = /\b(latest|today|current|recent|breaking|just in|this week|yesterday)\b/i.test(q);

  const monthMatch = q.match(new RegExp(`\\b(${Object.keys(MONTHS).join('|')})\\s+(20\\d{2})\\b`, 'i'));
  if (monthMatch) {
    const month = MONTHS[monthMatch[1].toLowerCase()];
    const year = Number(monthMatch[2]);
    return {
      kind: 'explicit-month',
      start: new Date(Date.UTC(year, month, 1)).toISOString(),
      end: new Date(Date.UTC(year, month + 1, 1)).toISOString(),
      label: `${monthMatch[1]} ${year}`,
      latest,
    };
  }

  const yearMatch = q.match(/\b(20\d{2})\b/);
  if (yearMatch && /\b(latest|news|update|events|during|in)\b/i.test(q)) {
    const year = Number(yearMatch[1]);
    return {
      kind: 'explicit-year',
      start: new Date(Date.UTC(year, 0, 1)).toISOString(),
      end: new Date(Date.UTC(year + 1, 0, 1)).toISOString(),
      label: String(year),
      latest,
    };
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
  const queries = [];
  const add = q => { if (q && !queries.includes(q)) queries.push(q); };
  const gov = intent.wantsGov || mode === 'gov';
  const docs = intent.wantsDocs || mode === 'doc';
  const video = intent.wantsVideo || mode === 'video';
  const news = intent.wantsNews || mode === 'news';

  if (gov) {
    add(`${query} site:gov.in`); add(`${query} site:nic.in`); add(`${query} site:india.gov.in`); add(`${query} site:mygov.in`);
  }
  if (docs) {
    add(`${query} filetype:pdf`); add(`${query} official PDF`); add(`${query} site:gov.in filetype:pdf`);
  }
  if (video) {
    add(`site:youtube.com ${query}`); add(`${query} YouTube`);
  }
  if (news) add(`${query} latest news`);
  add(query);
  if (dateIntent.kind === 'explicit-month' || dateIntent.kind === 'explicit-year') {
    add(`${query} after:${dateIntent.start.slice(0, 10)} before:${dateIntent.end.slice(0, 10)}`);
  }
  if (mode === 'deep') { add(`${query} latest update`); add(`${query} official source`); }
  return queries.slice(0, 6);
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
  if (intent.wantsNews) urls.push({ provider: 'google-news', type: 'news', url: `https://news.google.com/rss/search?q=${encoded}&hl=en-IN&gl=IN&ceid=IN:en` });
  if (intent.wantsVideo) {
    urls.push({ provider: 'youtube', type: 'video', url: `https://www.youtube.com/results?search_query=${encoded}&hl=en-IN` });
    urls.push({ provider: 'google-video', type: 'video', url: `https://www.google.com/search?q=${encodeURIComponent(`site:youtube.com ${q}`)}&num=20&hl=en&gl=in` });
  }
  if (isGovQuery) urls.push({ provider: 'google-gov', type: 'gov', url: `https://www.google.com/search?q=${encodeURIComponent(`${q} site:gov.in`)}&num=20&hl=en&gl=in` });
  if (isPdfQuery) urls.push({ provider: 'google-doc', type: 'doc', url: `https://www.google.com/search?q=${encodeURIComponent(`${q} filetype:pdf`)}&num=20&hl=en&gl=in` });
  return urls;
}

function parseBing(html) {
  const out = [];
  const blocks = html.match(/<li[^>]+class=["'][^"']*b_algo[^"']*["'][\s\S]*?<\/li>/gi) || [];
  for (const block of blocks) {
    const m = block.match(/<h2[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/i);
    if (!m) continue;
    const url = cleanUrl(m[1], 'https://www.bing.com/');
    if (!url) continue;
    const snippet = stripTags((block.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [, ''])[1]);
    out.push({ title: stripTags(m[2]), url, snippet, source: 'bing', type: 'web' });
  }
  return out.slice(0, 20);
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
  const headings = [...html.matchAll(/<h3[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)];
  for (const m of headings) {
    const url = cleanUrl(m[1], 'https://search.yahoo.com/');
    if (!url || /search\.yahoo\.com\/search/i.test(url)) continue;
    const idx = m.index || 0;
    const tail = html.slice(idx, idx + 4500);
    const snippet = stripTags((tail.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [, ''])[1]);
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
    if (!url || isBlockedContentUrl(url)) continue;
    const host = normalizedHost(url);
    if (/^google\.(com|co\.in)$/.test(host) && /\/search|\/url\b/i.test(new URL(url).pathname + new URL(url).search)) continue;
    const title = stripTags(m[2]).replace(/\s+/g, ' ').trim();
    if (title.length < 3) continue;
    if (seen.has(normalizedKey(url))) continue;
    seen.add(normalizedKey(url));
    let type = forcedType;
    if (forcedType === 'web') {
      if (isGovUrl(url)) type = 'gov'; else if (isDocUrl(url)) type = 'doc'; else if (isLikelyVideoUrl(url)) type = 'video';
    }
    if (forcedType === 'video' && !isLikelyVideoUrl(url)) continue;
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
    const tail = html.slice(m.index || 0, (m.index || 0) + 5000);
    const snippet = stripTags((tail.match(/class=["'][^"']*result__snippet[^"']*[^>]*>([\s\S]*?)(?:<\/a>|<\/span>|<\/div>)/i) || [, ''])[1]);
    out.push({ title: stripTags(m[2]), url, snippet, source: 'duckduckgo', type: 'web' });
  }
  return out.slice(0, 20);
}

function parseGoogleNewsRss(xml) {
  const out = [];
  const items = xml.match(/<item>[\s\S]*?<\/item>/gi) || [];
  for (const item of items) {
    const title = stripTags((item.match(/<title>([\s\S]*?)<\/title>/i) || [, ''])[1]);
    const link = stripTags((item.match(/<link>([\s\S]*?)<\/link>/i) || [, ''])[1]);
    const pubDate = stripTags((item.match(/<pubDate>([\s\S]*?)<\/pubDate>/i) || [, ''])[1]);
    const desc = stripTags((item.match(/<description>([\s\S]*?)<\/description>/i) || [, ''])[1]);
    const sm = item.match(/<source[^>]*url=["']([^"']+)["'][^>]*>([\s\S]*?)<\/source>/i) || item.match(/<source[^>]*>([\s\S]*?)<\/source>/i);
    const sourceName = stripTags(sm?.[2] || sm?.[1] || '');
    const sourceUrl = sm?.[1] ? cleanUrl(sm[1], 'https://news.google.com/') : null;
    const url = cleanUrl(link, 'https://news.google.com/');
    if (!title || !url) continue;
    out.push({
      title, url, snippet: desc,
      publishedAt: safeIsoDate(pubDate),
      source: sourceName ? `google-news:${sourceName}` : 'google-news',
      publisherUrl: sourceUrl,
      type: 'news',
    });
  }
  return out.slice(0, 80);
}

function findJsonObjectAfterMarker(text, marker, maxDistance = 300_000, maxScan = 1_000_000) {
  const source = String(text || '');
  const idx = source.indexOf(marker);
  if (idx < 0) return null;
  const start = source.indexOf('{', idx + marker.length);
  if (start < 0 || start - idx > maxDistance) return null;
  let depth = 0, inString = false, escaped = false;
  const end = Math.min(source.length, start + maxScan);
  for (let i = start; i < end; i++) {
    const ch = source[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') { inString = true; continue; }
    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) {
        try { return JSON.parse(source.slice(start, i + 1)); } catch { return null; }
      }
    }
  }
  return null;
}

function collectYouTubeVideoRenderers(value, out = []) {
  if (!value || out.length >= 20) return out;
  if (Array.isArray(value)) { for (const v of value) collectYouTubeVideoRenderers(v, out); return out; }
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
  return decodeHtml(
    renderer?.title?.runs?.map(x => x?.text || '').join('') || renderer?.title?.simpleText || ''
  ).trim();
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
    const snippet = decodeHtml(renderer?.detailedMetadataSnippets?.[0]?.snippetText?.runs?.map(x => x?.text || '').join('') || renderer?.descriptionSnippet?.runs?.map(x => x?.text || '').join('') || 'YouTube video result');
    out.push({ title: youtubeRendererTitle(renderer) || `YouTube video ${id}`, url: `https://www.youtube.com/watch?v=${id}`, snippet, source: 'youtube', type: 'video' });
    if (out.length >= 20) break;
  }
  if (out.length) return out;
  for (const m of html.matchAll(/"videoRenderer":\{[\s\S]*?"videoId":"([\w-]{6,20})"[\s\S]*?\}/g)) {
    const id = m[1];
    if (seen.has(id)) continue;
    seen.add(id);
    const windowText = html.slice(Math.max(0, (m.index || 0) - 200), Math.min(html.length, (m.index || 0) + 7000));
    const title = decodeHtml((windowText.match(/"title":\{"runs":\[\{"text":"((?:\\.|[^"\\])*)"/i) || [, ''])[1]).replace(/\\"/g, '"');
    out.push({ title: title || `YouTube video ${id}`, url: `https://www.youtube.com/watch?v=${id}`, snippet: 'YouTube video result', source: 'youtube', type: 'video' });
    if (out.length >= 20) break;
  }
  if (!out.length) {
    for (const m of html.matchAll(/"videoId":"([\w-]{6,20})"/g)) {
      const id = m[1];
      if (seen.has(id)) continue;
      seen.add(id);
      out.push({ title: `YouTube video ${id}`, url: `https://www.youtube.com/watch?v=${id}`, snippet: 'YouTube video result', source: 'youtube', type: 'video' });
      if (out.length >= 20) break;
    }
  }
  return out;
}

async function discoverOne(engine, deadline) {
  try {
    const body = await fetchText(engine.url, {
      timeout: engine.provider === 'youtube' ? 7000 : SEARCH_TIMEOUT_MS,
      maxBytes: engine.provider === 'youtube' ? MAX_YOUTUBE_BYTES : MAX_SEARCH_BYTES,
      deadline,
    });
    let results = [];
    if (engine.provider === 'bing') results = parseBing(body);
    else if (engine.provider === 'duckduckgo') results = parseDuckDuckGo(body);
    else if (engine.provider === 'mojeek') results = parseMojeek(body);
    else if (engine.provider === 'yahoo') results = parseYahoo(body);
    else if (engine.provider === 'google') results = parseGoogleWeb(body);
    else if (engine.provider === 'google-video') results = parseGoogleWeb(body, 'google-video', 'video');
    else if (engine.provider === 'google-gov') results = parseGoogleWeb(body, 'google-gov', 'gov');
    else if (engine.provider === 'google-doc') results = parseGoogleWeb(body, 'google-doc', 'doc');
    else if (engine.provider === 'google-news') results = parseGoogleNewsRss(body);
    else if (engine.provider === 'youtube') results = parseYoutube(body);
    return { provider: engine.provider, results, ok: true };
  } catch (error) {
    return { provider: engine.provider, results: [], ok: false, error: error?.message || 'FETCH_FAILED' };
  }
}

function dedupeResults(results) {
  const map = new Map();
  for (const r of results) {
    if (!r?.url || !safeHttpUrl(r.url) || isBlockedContentUrl(r.url)) continue;
    const key = normalizedKey(r.url);
    const existing = map.get(key);
    if (!existing || (r.verified && !existing.verified) || ((r.snippet || '').length > (existing.snippet || '').length)) {
      map.set(key, { ...existing, ...r });
    }
  }
  return [...map.values()];
}

function scoreDate(publishedAt, dateIntent) {
  if (!publishedAt) return 0;
  const t = Date.parse(publishedAt);
  if (Number.isNaN(t)) return 0;
  if (dateIntent.start && dateIntent.end) {
    const start = Date.parse(dateIntent.start), end = Date.parse(dateIntent.end);
    if (t >= start && t < end) return 22;
    if (dateIntent.kind === 'explicit-month' || dateIntent.kind === 'explicit-year') return -22;
  }
  if (dateIntent.latest) {
    const ageDays = Math.max(0, (Date.now() - t) / 86400000);
    if (ageDays <= 1) return 14;
    if (ageDays <= 7) return 11;
    if (ageDays <= 30) return 7;
    if (ageDays <= 90) return 1;
    if (ageDays <= 365) return -6;
    return -14;
  }
  return 0;
}

function scoreResult(r, query, intent, dateIntent) {
  const terms = extractSearchTerms(query);
  const title = String(r.title || '').toLowerCase();
  const snippet = String(r.snippet || '').toLowerCase();
  const body = String(r.pageContent || r.extractedText || '').toLowerCase();
  const url = String(r.url || '').toLowerCase();
  let score = 0;
  let matched = 0;
  for (const t of terms) {
    if (title.includes(t)) { score += 5; matched++; }
    if (snippet.includes(t)) score += 1.5;
    if (body.includes(t)) score += 0.45;
    if (url.includes(t)) score += 0.4;
  }
  if (terms.length) score += Math.min(8, (matched / terms.length) * 8);
  if (intent.wantsGov && isGovUrl(r.url)) score += 10;
  if (intent.wantsNews && r.type === 'news') score += 8;
  if (intent.wantsVideo && r.type === 'video') score += 8;
  if (intent.wantsDocs && (r.type === 'doc' || isDocUrl(r.url))) score += 6;
  if (isTrustedInternational(r.url)) score += 2.5;
  if (r.verified) score += 4;
  if (r.contentStatus === 'full' || r.contentStatus === 'reader') score += 3;
  if (r.publishedAt) score += scoreDate(r.publishedAt, dateIntent);
  if (/login|signin|advertis|cookie|enable javascript/i.test(`${title} ${snippet}`)) score -= 1.5;
  if (/^news\.google\.com$/i.test(hostname(r.url))) score -= 8;
  return score;
}

function applyDateConstraint(results, dateIntent, count, warnings) {
  if (!dateIntent.start || !dateIntent.end) return results;
  const start = Date.parse(dateIntent.start), end = Date.parse(dateIntent.end);
  const exact = dateIntent.kind === 'explicit-month' || dateIntent.kind === 'explicit-year';
  const inRange = results.filter(r => {
    const t = r.publishedAt ? Date.parse(r.publishedAt) : NaN;
    return !Number.isNaN(t) && t >= start && t < end;
  });
  if (exact && inRange.length >= Math.min(4, Math.max(2, Math.ceil(count / 10)))) {
    warnings.push(`Applied requested date window: ${dateIntent.label}.`);
    return inRange;
  }
  if (exact) warnings.push(`Requested date window ${dateIntent.label} had ${inRange.length} dated result(s); nearby live results were retained as fallback.`);
  return results;
}

function enforceRequestedType(results, requestedType, mode, warnings) {
  const wanted = String(requestedType || mode || '').toLowerCase();
  let type = null;
  if (wanted === 'gov') type = 'gov';
  else if (wanted === 'doc' || wanted === 'docs' || wanted === 'document') type = 'doc';
  else if (wanted === 'video') type = 'video';
  else if (wanted === 'news') type = 'news';
  if (!type) return results;
  const matching = results.filter(r => type === 'gov' ? isGovUrl(r.url) : type === 'doc' ? (r.type === 'doc' || isDocUrl(r.url)) : r.type === type);
  if (matching.length) return matching;
  warnings.push(`No ${type} result was discovered; broader live results were retained.`);
  return results;
}

function diversifyAndSelect(results, count, intent) {
  const pool = [...results].sort((a, b) => (b._score || 0) - (a._score || 0));
  if (pool.length <= count) return pool;
  const selected = [];
  const domains = new Map(), types = new Map();
  const newsOnly = intent.wantsNews && !intent.wantsVideo && !intent.wantsDocs;
  const newsShare = newsOnly ? 0.72 : 0.52;
  while (selected.length < count && pool.length) {
    let bestIndex = 0, bestAdjusted = -Infinity;
    for (let i = 0; i < pool.length; i++) {
      const r = pool[i];
      const d = hostname(r.url), t = r.type || 'web';
      const dPenalty = Math.min(7, (domains.get(d) || 0) * 2.5);
      const typeCount = types.get(t) || 0;
      let typePenalty = Math.min(4, typeCount * 0.7);
      if (newsOnly && t === 'news' && typeCount < Math.ceil(count * newsShare)) typePenalty *= 0.3;
      const adjusted = (r._score || 0) - dPenalty - typePenalty;
      if (adjusted > bestAdjusted) { bestAdjusted = adjusted; bestIndex = i; }
    }
    const chosen = pool.splice(bestIndex, 1)[0];
    selected.push(chosen);
    const d = hostname(chosen.url), t = chosen.type || 'web';
    domains.set(d, (domains.get(d) || 0) + 1);
    types.set(t, (types.get(t) || 0) + 1);
  }
  return selected;
}

function selectVerificationCandidates(results, count) {
  const ranked = [...results].sort((a, b) => {
    const aw = /^news\.google\.com$/i.test(hostname(a.url)) ? -10 : 0;
    const bw = /^news\.google\.com$/i.test(hostname(b.url)) ? -10 : 0;
    return ((b._score || 0) + bw) - ((a._score || 0) + aw);
  });
  const selected = [], domains = new Set(), urls = new Set();
  for (const r of ranked) {
    if (selected.length >= count) break;
    const key = normalizedKey(r.url), domain = hostname(r.url);
    if (!safeHttpUrl(r.url) || urls.has(key)) continue;
    if (!domains.has(domain) || selected.length >= Math.ceil(count * 0.65)) {
      selected.push(r); domains.add(domain); urls.add(key);
    }
  }
  return selected;
}

function resolveLinkFromHtml(html, baseUrl, result = null) {
  const candidates = [];
  const addCandidate = (raw, anchorText = '', source = '') => {
    const u = cleanUrl(raw, baseUrl);
    if (!u || !isPublisherCandidateUrl(u, result)) return;
    const score = candidatePublisherScore(u, result, anchorText);
    if (!Number.isFinite(score)) return;
    candidates.push({ url: u, score, source });
  };

  const redirectMeta = html.match(/<meta[^>]+http-equiv=["']refresh["'][^>]+content=["'][^"']*url=([^"']+)["']/i)?.[1];
  if (redirectMeta) addCandidate(redirectMeta, '', 'meta-refresh');

  for (const m of html.matchAll(/(?:location\.href|location\.replace|window\.location(?:\.href)?)[\s=]*(?:\(|)["']([^"']+)["']/gi)) {
    addCandidate(m[1], '', 'javascript-redirect');
  }

  for (const m of html.matchAll(/<a[^>]+href=["'](https?:\/\/[^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    addCandidate(m[1], stripTags(m[2]), 'anchor');
  }

  candidates.sort((a, b) => b.score - a.score);
  return candidates[0]?.url || null;
}

async function resolveNewsWrapper(result, deadline) {
  if (!/^news\.google\.com$/i.test(hostname(result.url))) return result;
  let wrapper;
  try { wrapper = new URL(result.url); } catch { return result; }
  if (!/^\/rss\/articles\//i.test(wrapper.pathname)) return result;

  // First try the real HTTP redirect.
  if (remainingMs(deadline) > 900) {
    try {
      const res = await fetchResponse(result.url, {
        timeout: NEWS_RESOLVE_TIMEOUT_MS,
        deadline,
        headers: { accept: 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.1' },
      });
      const finalUrl = cleanUrl(res.url || result.url, result.url);
      if (finalUrl && isPublisherCandidateUrl(finalUrl, result)) {
        try { await res.body?.cancel?.(); } catch {}
        return { ...result, publisherWrapperUrl: result.url, url: finalUrl, domain: hostname(finalUrl), publisherResolved: true, publisherResolutionMethod: 'redirect' };
      }
      const html = await readBodyText(res, 220_000, deadline).catch(() => '');
      const embedded = resolveLinkFromHtml(html, result.url, result);
      if (embedded) return { ...result, publisherWrapperUrl: result.url, url: embedded, domain: hostname(embedded), publisherResolved: true, publisherResolutionMethod: 'wrapper-html' };
    } catch {}
  }
  return result;
}

async function publisherLookupByTitle(result, deadline) {
  if (!/^news\.google\.com$/i.test(hostname(result.url))) return result;
  if (!result.title || remainingMs(deadline) < 1300) return result;
  const title = String(result.title).replace(/["']/g, ' ').replace(/\s+/g, ' ').trim();
  const source = String(result.source || '').replace(/^google-news:/, '').trim();
  const q = `"${truncate(title, 220)}" ${source}`;
  const urls = [
    `https://www.bing.com/search?q=${encodeURIComponent(q)}&count=10&setlang=en-IN&cc=in`,
    `https://www.google.com/search?q=${encodeURIComponent(q)}&num=10&hl=en&gl=in`,
  ];
  for (const engineUrl of urls) {
    if (remainingMs(deadline) < 1000) break;
    try {
      const html = await fetchText(engineUrl, { timeout: 2300, maxBytes: 400_000, deadline });
      const found = engineUrl.includes('bing') ? parseBing(html) : parseGoogleWeb(html);
      const titleTokens = extractSearchTerms(title);
      const scored = found.map(r => {
        if (!isPublisherCandidateUrl(r.url, result)) return null;
        const candidateTitle = String(r.title || '').toLowerCase();
        const hits = titleTokens.filter(t => candidateTitle.includes(t)).length;
        const ratio = titleTokens.length ? hits / titleTokens.length : 0;
        if (hits < Math.max(2, Math.ceil(titleTokens.length * 0.25)) && ratio < 0.4) return null;
        const score = hits * 4 + ratio * 10 + candidatePublisherScore(r.url, result, r.title);
        return { r, score };
      }).filter(Boolean).sort((a, b) => b.score - a.score);
      const candidate = scored[0]?.r;
      if (candidate) return { ...result, publisherWrapperUrl: result.url, url: candidate.url, domain: hostname(candidate.url), publisherResolved: true, publisherResolutionMethod: 'title-search', publisherSearchSource: candidate.source };
    } catch {}
  }
  return result;
}

function readerUrl(url) {
  return `https://r.jina.ai/http://${new URL(url).host}${new URL(url).pathname}${new URL(url).search}`;
}

function readerUrls(url) {
  try {
    const u = new URL(url);
    const raw = u.href;
    const noHash = `${u.origin}${u.pathname}${u.search}`;
    return [
      `https://r.jina.ai/http://${u.host}${u.pathname}${u.search}`,
      `https://r.jina.ai/https://${u.host}${u.pathname}${u.search}`,
      `https://r.jina.ai/${raw}`,
      `https://r.jina.ai/${noHash}`,
    ].filter((v, i, a) => a.indexOf(v) === i);
  } catch { return []; }
}

async function readerFallback(url, deadline) {
  if (!safeHttpUrl(url) || isBlockedContentUrl(url) || remainingMs(deadline) < 1200) return null;
  for (const proxyUrl of readerUrls(url).slice(0, 2)) {
    if (remainingMs(deadline) < 1000) break;
    try {
      const timeout = effectiveTimeout(READER_TIMEOUT_MS, deadline, 700);
      if (!timeout) break;
      const res = await fetchResponse(proxyUrl, {
        timeout, deadline,
        headers: { accept: 'text/plain,text/markdown;q=0.95,*/*;q=0.2', 'x-no-cache': 'true' },
      });
      if (!res.ok) { try { await res.body?.cancel?.(); } catch {} continue; }
      const text = textFromMarkdown(await readBodyText(res, MAX_PAGE_BYTES, deadline));
      if (text.length >= 250) return { content: text, method: 'jina-reader', sourceUrl: url };
    } catch {}
  }
  return null;
}

async function fetchVariantContent(url, deadline) {
  if (!isPublisherCandidateUrl(url)) return null;
  try {
    const u = new URL(url);
    const candidates = [];
    if (!/\/amp\/?$/i.test(u.pathname)) {
      candidates.push(new URL(`${u.pathname.replace(/\/$/, '')}/amp${u.search}`, u.origin).href);
    }
    if (!/[?&](output|amp)=/i.test(u.search)) candidates.push(`${u.href}${u.search ? '&' : '?'}output=1`);
    for (const candidate of candidates) {
      if (remainingMs(deadline) < 1000) break;
      try {
        const res = await fetchResponse(candidate, { timeout: 2800, deadline, headers: { accept: 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.2' } });
        if (!res.ok) { try { await res.body?.cancel?.(); } catch {} continue; }
        const html = await readBodyText(res, 700_000, deadline);
        const text = extractVisibleText(html);
        if (text.length >= 250) {
          return {
            content: text,
            method: 'alternate-page',
            title: parseTitleFromHtml(html) || null,
            publishedAt: parseDateFromHtml(html) || null,
            finalUrl: cleanUrl(res.url || candidate, url) || candidate,
          };
        }
      } catch {}
    }
  } catch {}
  return null;
}

function bytesToLatin1(bytes) {
  let out = '';
  const step = 8192;
  for (let i = 0; i < bytes.length; i += step) out += String.fromCharCode(...bytes.subarray(i, Math.min(bytes.length, i + step)));
  return out;
}

function decodePdfLiteral(raw) {
  let s = String(raw || '');
  s = s.replace(/\\([nrtbf\\()])/g, (_, c) => ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '\\': '\\', '(': '(', ')': ')' })[c] || c);
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
  if (typeof DecompressionStream === 'undefined' || remainingMs(deadline) < 600) return null;
  const controller = new AbortController();
  const timeout = effectiveTimeout(PDF_DECOMPRESS_TIMEOUT_MS, deadline, 500);
  if (!timeout) return null;
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const ds = new DecompressionStream('deflate');
    const writer = ds.writable.getWriter();
    await writer.write(bytes); await writer.close();
    const ab = await new Response(ds.readable).arrayBuffer();
    if (controller.signal.aborted) return null;
    return new Uint8Array(ab);
  } catch { return null; }
  finally { clearTimeout(timer); }
}

async function extractPdfText(bytes, deadline) {
  const raw = bytesToLatin1(bytes);
  const results = [];
  const re = /<<(?:[\s\S]{0,7000}?)>>\s*stream\r?\n([\s\S]*?)\r?\nendstream/g;
  let m;
  while ((m = re.exec(raw)) && results.length < 100) {
    if (remainingMs(deadline) < 700) break;
    const dict = m[0].slice(0, Math.max(0, m[0].indexOf('stream')));
    const payloadStart = m.index + m[0].indexOf(m[1]);
    const payloadEnd = payloadStart + m[1].length;
    const source = bytes.slice(payloadStart, payloadEnd);
    if (/\/FlateDecode/i.test(dict)) {
      const inflated = await inflateDeflate(source, deadline);
      if (inflated) results.push(bytesToLatin1(inflated));
    } else results.push(m[1]);
  }
  return truncate(cleanExtractedText(results.map(extractPdfStrings).join(' ')), MAX_TEXT_CHARS);
}

function extractPlayerResponse(html) {
  return findJsonObjectAfterMarker(html, 'ytInitialPlayerResponse') || findJsonObjectAfterMarker(html, 'PLAYER_RESPONSE');
}

function chooseCaptionTrack(tracks) {
  if (!Array.isArray(tracks) || !tracks.length) return null;
  return tracks.find(t => /^en(?:-|$)/i.test(t?.languageCode || '')) || tracks.find(t => /^en/i.test(t?.languageCode || '')) || tracks[0];
}

function captionXmlToText(xml) {
  const rows = [];
  for (const m of String(xml || '').matchAll(/<text\b[^>]*>([\s\S]*?)<\/text>/gi)) {
    const text = decodeHtml(m[1]).replace(/\s+/g, ' ').trim();
    if (text) rows.push(text);
  }
  return truncate(rows.join(' '), MAX_TRANSCRIPT_CHARS);
}

async function youtubePlayer(videoId, deadline) {
  const url = `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}&hl=en&gl=IN`;
  const html = await fetchText(url, { timeout: YOUTUBE_TIMEOUT_MS, maxBytes: MAX_YOUTUBE_BYTES, deadline });
  const player = extractPlayerResponse(html);
  if (player) return { player, html };
  const apiKey = (html.match(/"INNERTUBE_API_KEY":"([^"]+)"/) || [, ''])[1];
  if (!apiKey || remainingMs(deadline) < 900) return { player: null, html };
  const timeout = effectiveTimeout(YOUTUBE_TIMEOUT_MS, deadline, 700);
  if (!timeout) return { player: null, html };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(`https://www.youtube.com/youtubei/v1/player?key=${encodeURIComponent(apiKey)}`, {
      method: 'POST', signal: controller.signal,
      headers: { 'content-type': 'application/json', 'user-agent': USER_AGENT },
      body: JSON.stringify({ context: { client: { clientName: 'WEB', clientVersion: (html.match(/"INNERTUBE_CLIENT_VERSION":"([^"]+)"/) || [, '2.20260915.01.00'])[1], hl: 'en', gl: 'IN' } }, videoId }),
    });
    if (!res.ok) return { player: null, html };
    return { player: await res.json(), html };
  } catch { return { player: null, html }; }
  finally { clearTimeout(timer); }
}

async function enrichYouTubeResult(result, query, deadline) {
  const videoId = String(result.url || '').match(/(?:v=|youtu\.be\/|shorts\/)([A-Za-z0-9_-]{6,20})/i)?.[1];
  if (!videoId) return ensureContentFields(result, query, { status: 'snippet_fallback', method: 'search-snippet' });
  try {
    const { player } = await youtubePlayer(videoId, deadline);
    const details = player?.videoDetails || {};
    const tracks = player?.captions?.playerCaptionsTracklistRenderer?.captionTracks || [];
    const track = chooseCaptionTrack(tracks);
    let transcript = null;
    if (track?.baseUrl && remainingMs(deadline) > 900) {
      try {
        const res = await fetchResponse(track.baseUrl, { timeout: 4500, deadline, headers: { accept: 'text/xml,application/xml,text/plain;q=0.9,*/*;q=0.2' } });
        if (res.ok) transcript = captionXmlToText(await readBodyText(res, 900_000, deadline));
      } catch {}
    }
    const description = decodeHtml(details.shortDescription || result.snippet || '').trim();
    const title = decodeHtml(details.title || result.title || '').trim() || result.title;
    const content = transcript ? `YouTube transcript:\n${transcript}` : (description || extractFallbackContent(result, query));
    return ensureContentFields({
      ...result,
      title: truncate(title, 300),
      snippet: truncate(description || transcript || result.snippet || '', 1200),
      verified: Boolean(player),
      httpStatus: player ? 200 : null,
      contentType: player ? 'application/json' : null,
      transcript: transcript || null,
      transcriptAvailable: Boolean(transcript),
      transcriptLanguage: track?.languageCode || null,
    }, query, {
      status: transcript ? 'full' : player ? 'metadata' : 'snippet_fallback',
      method: transcript ? 'youtube-captions' : player ? 'youtube-metadata' : 'search-snippet',
      content,
      confidence: transcript ? 0.98 : player ? 0.7 : 0.35,
    });
  } catch (error) {
    return ensureContentFields(result, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent(result, query), confidence: 0.35, error: error?.message || 'YOUTUBE_ENRICH_FAILED' });
  }
}

function ensureContentFields(result, query, info = {}) {
  const existing = cleanExtractedText(info.content || result.pageContent || result.extractedText || '');
  const fallback = existing || extractFallbackContent(result, query);
  const status = info.status || (existing ? 'full' : 'synthetic_metadata');
  const method = info.method || (existing ? 'existing-content' : 'metadata-fallback');
  return {
    ...result,
    pageContent: truncate(fallback, MAX_TEXT_CHARS),
    extractedText: truncate(fallback, MAX_TEXT_CHARS),
    contentStatus: status,
    contentMethod: method,
    contentLength: fallback.length,
    contentConfidence: Number(Math.max(0, Math.min(1, info.confidence ?? (status === 'full' ? 1 : status === 'reader' ? 0.95 : status === 'alternate' ? 0.9 : status === 'metadata' ? 0.7 : 0.35))).toFixed(2)),
    contentSourceUrl: result.url,
    contentAvailable: fallback.length > 0,
    contentError: info.error || result.contentError || null,
  };
}


function titleSimilarity(a, b) {
  const aa = new Set(extractSearchTerms(String(a || '')));
  const bb = new Set(extractSearchTerms(String(b || '')));
  if (!aa.size || !bb.size) return 0;
  let hits = 0;
  for (const token of aa) if (bb.has(token)) hits++;
  return hits / Math.max(aa.size, bb.size);
}

function looksLikeJavaScriptBody(body, contentType) {
  const ct = String(contentType || '').toLowerCase();
  if (/javascript|ecmascript/.test(ct)) return true;
  const sample = String(body || '').slice(0, 6000).trim();
  if (!sample) return false;
  if (/^(?:!function|function\s+|\(function|window\.|document\.|(?:var|let|const)\s+[A-Za-z_$]|[A-Za-z_$][\w$]*\s*=\s*function)/i.test(sample)) return true;
  if (/(?:google-analytics|googletagmanager|gtag\(|google_tag_manager|doubleclick|dataLayer\.push|window\.__)/i.test(sample)) return true;
  return false;
}

function assessHtmlContent(html, text, result, finalUrl, title) {
  const bodyText = String(text || '').trim();
  const lowerHtml = String(html || '').toLowerCase();
  let score = 0;
  const signals = [];
  if (/<article\b/i.test(html)) { score += 5; signals.push('article'); }
  if (/<main\b/i.test(html)) { score += 3; signals.push('main'); }
  if (/<h1\b/i.test(html)) { score += 2; signals.push('h1'); }
  if (/<time\b/i.test(html) || /datepublished|datepublished/i.test(html)) { score += 1; signals.push('date'); }
  if (/application\/ld\+json/i.test(lowerHtml) && /articlebody|newsarticle|article/i.test(lowerHtml)) { score += 4; signals.push('jsonld-article'); }
  const pCount = (html.match(/<p\b/gi) || []).length;
  if (pCount >= 5) { score += 2; signals.push('paragraphs'); }
  if (pCount >= 12) { score += 1; signals.push('many-paragraphs'); }
  const sim = titleSimilarity(result.title, title);
  if (sim >= 0.75) { score += 5; signals.push('title-match'); }
  else if (sim >= 0.45) { score += 3; signals.push('title-partial-match'); }
  if (ARTICLE_PATH_HINT.test(finalUrl)) { score += 2; signals.push('article-path'); }
  if (bodyText.length >= 1500) { score += 2; signals.push('long-text'); }
  if (bodyText.length >= 5000) { score += 1; signals.push('very-long-text'); }
  if (/google-analytics|googletagmanager|doubleclick|dataLayer\.push|gtag\(/i.test(bodyText.slice(0, 12000))) {
    score -= 20; signals.push('tracking-code');
  }
  if (/^untitled$|enable javascript|javascript required/i.test(String(title || ''))) score -= 4;

  const minimum = result.type === 'news' ? 8 : 5;
  const acceptable = bodyText.length >= 350 && score >= minimum;
  return { acceptable, score, signals, titleSimilarity: Number(sim.toFixed(2)) };
}

function contentTypeIsPageLike(contentType, body) {
  const ct = String(contentType || '').toLowerCase();
  if (/javascript|ecmascript|json|xml|css|image\/|font\//i.test(ct)) return false;
  return /text\/html|application\/xhtml/i.test(ct) || /<html\b/i.test(String(body || ''));
}

async function enrichResult(result, query, deadline) {
  if (result.type === 'video' && isYouTube(result.url)) return enrichYouTubeResult(result, query, deadline);
  if (!safeHttpUrl(result.url)) return ensureContentFields({ ...result, verified: false, verificationError: 'UNSAFE_URL' }, query);

  if (isBlockedContentUrl(result.url)) {
    return ensureContentFields({ ...result, verified: false, verificationError: 'BLOCKED_NON_CONTENT_URL' }, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent(result, query), confidence: 0.35, error: 'BLOCKED_NON_CONTENT_URL' });
  }

  const fallbackBase = ensureContentFields(result, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent(result, query), confidence: 0.35 });
  try {
    if (remainingMs(deadline) < 900) return fallbackBase;

    const res = await fetchResponse(result.url, {
      timeout: PAGE_TIMEOUT_MS,
      deadline,
      headers: {
        accept: 'text/html,application/xhtml+xml;q=0.95,application/pdf;q=0.9,text/plain;q=0.9,application/json;q=0.8,*/*;q=0.2',
        'cache-control': 'no-cache', pragma: 'no-cache',
      },
    });

    const finalUrl = cleanUrl(res.url || result.url, result.url) || result.url;
    const contentType = res.headers.get('content-type') || '';

    if (!isPublisherCandidateUrl(finalUrl, result)) {
      try { await res.body?.cancel?.(); } catch {}
      if (result.publisherWrapperUrl && !result._publisherRetry && remainingMs(deadline) > 2200) {
        const retryBase = { ...result, url: result.publisherWrapperUrl, _publisherRetry: true };
        const retry = await publisherLookupByTitle(retryBase, deadline);
        if (retry?.url && retry.url !== result.publisherWrapperUrl && isPublisherCandidateUrl(retry.url, result)) {
          return enrichResult(retry, query, deadline);
        }
      }
      return ensureContentFields({ ...result, verified: false, verificationError: 'NON_CONTENT_FINAL_URL', httpStatus: res.status, contentType }, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent(result, query), confidence: 0.35, error: 'NON_CONTENT_FINAL_URL' });
    }

    if (/application\/pdf/i.test(contentType) || /\.pdf(?:\?|$)/i.test(finalUrl)) {
      const bytes = await readBodyBytes(res, MAX_PAGE_BYTES, deadline);
      let pdfText = '';
      try { pdfText = await extractPdfText(bytes, deadline); } catch {}
      if (!pdfText && remainingMs(deadline) > 1500) {
        const reader = await readerFallback(finalUrl, deadline);
        if (reader?.content) pdfText = reader.content;
      }
      return ensureContentFields({
        ...result, url: finalUrl, domain: hostname(finalUrl), verified: res.ok,
        httpStatus: res.status, contentType: contentType || 'application/pdf',
        verificationMethod: pdfText ? 'direct-pdf-text' : 'direct-pdf-no-text',
      }, query, {
        status: pdfText ? 'full' : 'metadata',
        method: pdfText ? 'direct-pdf-text' : 'pdf-metadata',
        content: pdfText || extractFallbackContent({ ...result, url: finalUrl }, query),
        confidence: pdfText ? 0.96 : 0.45,
      });
    }

    if (!res.ok) {
      try { await res.body?.cancel?.(); } catch {}
      if (remainingMs(deadline) > 1700) {
        const reader = await readerFallback(finalUrl, deadline);
        if (reader?.content) {
          return ensureContentFields({
            ...result, url: finalUrl, domain: hostname(finalUrl), verified: true,
            verificationMethod: 'jina-reader-fallback', httpStatus: res.status, contentType: 'text/markdown',
          }, query, { status: 'reader', method: 'jina-reader', content: reader.content, confidence: 0.95 });
        }
      }
      return ensureContentFields({ ...result, url: finalUrl, domain: hostname(finalUrl), verified: false, httpStatus: res.status, contentType }, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent({ ...result, url: finalUrl }, query), confidence: 0.35, error: `HTTP_${res.status}` });
    }

    const body = await readBodyText(res, MAX_PAGE_BYTES, deadline);
    if (result.type === 'news' && /^news\.google\.com$/i.test(hostname(finalUrl))) {
      return ensureContentFields({ ...result, url: finalUrl, domain: hostname(finalUrl), verified: false, httpStatus: res.status, contentType, verificationError: 'PUBLISHER_URL_NOT_RESOLVED' }, query);
    }

    if (looksLikeJavaScriptBody(body, contentType) || !contentTypeIsPageLike(contentType, body)) {
      const reader = remainingMs(deadline) > 1800 ? await readerFallback(finalUrl, deadline) : null;
      if (reader?.content && !isBlockedContentUrl(reader.sourceUrl || finalUrl)) {
        return ensureContentFields({ ...result, url: finalUrl, domain: hostname(finalUrl), verified: true, httpStatus: res.status, contentType, verificationMethod: 'jina-reader-nonpage-fallback' }, query, { status: 'reader', method: 'jina-reader', content: reader.content, confidence: 0.92 });
      }
      return ensureContentFields({ ...result, url: finalUrl, domain: hostname(finalUrl), verified: false, httpStatus: res.status, contentType, verificationError: 'NON_ARTICLE_CONTENT' }, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent({ ...result, url: finalUrl }, query), confidence: 0.35, error: 'NON_ARTICLE_CONTENT' });
    }

    const htmlLike = contentTypeIsPageLike(contentType, body);
    if (htmlLike) {
      const canonical = parseCanonical(body, finalUrl);
      const canonicalUrl = canonical && isPublisherCandidateUrl(canonical, result) ? canonical : finalUrl;
      const title = parseTitleFromHtml(body) || result.title;
      const description = parseMeta(body, 'description') || parseMeta(body, 'og:description') || result.snippet;
      const publishedAt = parseDateFromHtml(body) || result.publishedAt || null;
      let content = extractVisibleText(body);
      let method = 'direct-html';
      let status = 'metadata';
      let confidence = 0.55;
      let assessment = assessHtmlContent(body, content, result, canonicalUrl, title);

      if (content.length < 800 || !assessment.acceptable) {
        const alternate = remainingMs(deadline) > 1500 ? await fetchVariantContent(canonicalUrl, deadline) : null;
        if (alternate?.content && alternate.content.length > content.length) {
          content = alternate.content;
          method = alternate.method;
          status = 'alternate';
          confidence = 0.9;
          assessment = { acceptable: true, score: assessment.score + 1, signals: [...assessment.signals, 'alternate-page'], titleSimilarity: assessment.titleSimilarity };
        }
      }

      if ((content.length < 800 || !assessment.acceptable) && remainingMs(deadline) > 1500) {
        const reader = await readerFallback(canonicalUrl, deadline);
        if (reader?.content && reader.content.length > content.length) {
          content = reader.content;
          method = reader.method;
          status = 'reader';
          confidence = 0.95;
          assessment = { acceptable: content.length >= 500, score: Math.max(assessment.score, 8), signals: [...assessment.signals, 'reader'], titleSimilarity: assessment.titleSimilarity };
        }
      }

      if (content.length >= 350 && assessment.acceptable && !looksLikeJavaScriptBody(content, contentType)) {
        status = status === 'metadata' ? 'full' : status;
        if (status === 'full') confidence = 0.9;
        return ensureContentFields({
          ...result, url: canonicalUrl, domain: hostname(canonicalUrl), title: truncate(title, 300),
          snippet: truncate(description || result.snippet, 1200), publishedAt, verified: true,
          httpStatus: res.status, contentType, verificationMethod: method,
          contentSignals: assessment.signals, contentQualityScore: assessment.score,
          titleSimilarity: assessment.titleSimilarity,
        }, query, { status, method, content, confidence });
      }

      const fallback = `${description ? `Description: ${description}\n` : ''}${extractFallbackContent({ ...result, title, snippet: description || result.snippet, url: canonicalUrl, publishedAt }, query)}`;
      return ensureContentFields({
        ...result, url: canonicalUrl, domain: hostname(canonicalUrl), title: truncate(title, 300),
        snippet: truncate(description || result.snippet, 1200), publishedAt, verified: true,
        httpStatus: res.status, contentType, verificationMethod: 'search-snippet',
        contentSignals: assessment.signals, contentQualityScore: assessment.score,
        titleSimilarity: assessment.titleSimilarity,
      }, query, { status: 'snippet_fallback', method: 'search-snippet', content: fallback, confidence: 0.35, error: 'ARTICLE_CONTENT_NOT_VALIDATED' });
    }

    const plain = cleanExtractedText(body);
    if (plain.length >= 250 && !looksLikeJavaScriptBody(plain, contentType) && !isBlockedContentUrl(finalUrl)) {
      return ensureContentFields({ ...result, url: finalUrl, domain: hostname(finalUrl), verified: true, httpStatus: res.status, contentType, verificationMethod: 'direct-text' }, query, {
        status: 'full', method: 'direct-text', content: plain, confidence: 0.82,
      });
    }
    return ensureContentFields({ ...result, url: finalUrl, domain: hostname(finalUrl), verified: false, httpStatus: res.status, contentType, verificationError: 'TEXT_CONTENT_NOT_VALIDATED' }, query, {
      status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent({ ...result, url: finalUrl }, query), confidence: 0.35, error: 'TEXT_CONTENT_NOT_VALIDATED',
    });
  } catch (error) {
    if (remainingMs(deadline) > 1400) {
      const reader = await readerFallback(result.url, deadline);
      if (reader?.content) return ensureContentFields({ ...result, verified: true, verificationMethod: 'jina-reader-fallback' }, query, { status: 'reader', method: 'jina-reader', content: reader.content, confidence: 0.95 });
    }
    return ensureContentFields({ ...result, verified: false, verificationError: error?.message || 'ENRICH_FAILED' }, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent(result, query), confidence: 0.35, error: error?.message || 'ENRICH_FAILED' });
  }
}

function extractJson(text) {
  if (!text) return null;
  try { return JSON.parse(text); } catch {}
  const m = String(text).match(/\{[\s\S]*\}/);
  if (!m) return null;
  try { return JSON.parse(m[0]); } catch { return null; }
}

async function callGroq(messages, maxTokens, timeoutMs, deadline) {
  const key = typeof process !== 'undefined' ? process.env?.GROQ_API_KEY : undefined;
  if (!key) return null;
  const timeout = effectiveTimeout(timeoutMs, deadline, 600);
  if (!timeout) return null;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST', signal: controller.signal,
      headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'openai/gpt-oss-20b', messages, temperature: 0.1, max_completion_tokens: maxTokens }),
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.choices?.[0]?.message?.content || null;
  } catch { return null; }
  finally { clearTimeout(timer); }
}

async function aiPlan(query, mode, count, dateIntent, deadline) {
  if (remainingMs(deadline) < 1500) return null;
  const text = await callGroq([
    { role: 'system', content: 'You are a web-search query planner. Return JSON only. Never invent URLs. Generate concise, complementary live-web query variants. Respect explicit dates and prefer primary sources.' },
    { role: 'user', content: JSON.stringify({ query, mode, count, dateIntent, output: { intent: 'web|news|video|gov|doc|mixed', queries: ['...'], freshness: 'live|recent|any', mustPreferOfficial: true } }) },
  ], 800, 3500, deadline);
  const parsed = extractJson(text);
  if (!parsed) return null;
  return {
    intent: parsed.intent || 'web',
    queries: Array.isArray(parsed.queries) ? parsed.queries.filter(Boolean).slice(0, 5) : [],
    freshness: ['live', 'recent', 'any'].includes(parsed.freshness) ? parsed.freshness : 'any',
    mustPreferOfficial: Boolean(parsed.mustPreferOfficial),
  };
}

async function aiRerank(query, results, mode, dateIntent, deadline) {
  if (results.length < 2 || remainingMs(deadline) < 1800) return null;
  const payload = results.slice(0, 24).map((r, i) => ({
    id: i, title: truncate(r.title, 220), url: r.url, domain: hostname(r.url), type: r.type,
    snippet: truncate(r.snippet, 500), pageContentPreview: truncate(r.pageContent || r.extractedText || '', 2200),
    publishedAt: r.publishedAt || null, contentStatus: r.contentStatus || null, verified: Boolean(r.verified), trust: r.trust || domainTrust(r.url),
  }));
  const text = await callGroq([
    { role: 'system', content: 'You are a search reranker. Rank only provided result IDs. Prefer exact intent, requested dates, fresh sources, primary/official sources, direct publisher URLs, and results with real content. Return JSON only.' },
    { role: 'user', content: JSON.stringify({ query, mode, dateIntent, results: payload, output: { order: [0,1], confidence: 0.0 } }) },
  ], 1400, 4500, deadline);
  const parsed = extractJson(text);
  if (!parsed || !Array.isArray(parsed.order)) return null;
  return parsed.order.map(Number).filter(Number.isInteger).filter(i => i >= 0 && i < results.length);
}

async function commonCrawlLookup(url, deadline) {
  if (!safeHttpUrl(url) || remainingMs(deadline) < 800) return null;
  const encoded = encodeURIComponent(url);
  for (const index of COMMON_CRAWL_INDEXES) {
    if (remainingMs(deadline) < 700) break;
    try {
      const body = await fetchText(`https://index.commoncrawl.org/${index}-index?url=${encoded}&output=json&filter=status:200&limit=3`, {
        timeout: COMMON_CRAWL_TIMEOUT_MS, maxBytes: 80_000, deadline, headers: { accept: 'application/json,text/plain;q=0.8,*/*;q=0.1' },
      });
      const rows = body.trim().split('\n').filter(Boolean).map(x => { try { return JSON.parse(x); } catch { return null; } }).filter(Boolean);
      if (rows.length) return { index, capturedAt: rows[0].timestamp || null, digest: rows[0].digest || null, status: rows[0].status || null, records: rows.slice(0, 3) };
    } catch {}
  }
  return null;
}

function plannedRequests(queries, intent) {
  const all = [];
  queries.forEach((q, qi) => {
    for (const engine of buildEngineUrls(q, intent)) all.push({ ...engine, __queryIndex: qi });
  });
  const byQuery = new Map();
  for (const req of all) {
    if (!byQuery.has(req.__queryIndex)) byQuery.set(req.__queryIndex, []);
    byQuery.get(req.__queryIndex).push(req);
  }
  const out = [];
  // Round-robin by provider within each query to preserve diversity.
  const seenProvider = new Set();
  for (const req of byQuery.get(0) || []) { if (out.length >= MAX_ENGINE_REQUESTS) break; out.push(req); seenProvider.add(req.provider); }
  let qi = 1;
  while (out.length < MAX_ENGINE_REQUESTS && qi < queries.length) {
    for (const req of byQuery.get(qi) || []) {
      if (out.length >= MAX_ENGINE_REQUESTS) break;
      out.push(req);
    }
    qi++;
  }
  void seenProvider;
  return out.slice(0, MAX_ENGINE_REQUESTS);
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

  const baseIntent = queryIntent(query, requestedType || null);
  const dateIntent = parseDateIntent(query);
  const warnings = [];
  const providerStats = {};
  const flags = { partialDueToBudget: false, publisherResolutionAttempted: 0, publisherResolutionSucceeded: 0, verificationPerformed: 0, verificationSucceeded: 0, commonCrawlPerformed: 0 };

  const shouldAiPlan = useAi === 'true' || (useAi === 'auto' && (deep || count > 10 || baseIntent.wantsNews || baseIntent.wantsGov || baseIntent.wantsDocs || baseIntent.wantsVideo));
  const baseQueries = buildSearchQueries(query, baseIntent, mode, dateIntent);
  const aiPromise = shouldAiPlan && remainingMs(deadline) > 1800 ? aiPlan(query, mode, count, dateIntent, deadline) : Promise.resolve(null);

  const reqs = plannedRequests(baseQueries, baseIntent);
  const responses = await Promise.allSettled(reqs.map(r => discoverOne(r, deadline)));
  let discovered = [];
  for (const entry of responses) {
    if (entry.status !== 'fulfilled') continue;
    const item = entry.value;
    providerStats[item.provider] = providerStats[item.provider] || { ok: 0, failed: 0, results: 0 };
    if (item.ok) providerStats[item.provider].ok++; else providerStats[item.provider].failed++;
    providerStats[item.provider].results += item.results.length;
    discovered.push(...item.results);
  }

  const ai = await aiPromise;
  const aiQueries = ai?.queries?.length ? [...new Set(ai.queries)].slice(0, 5) : [];
  const plannedQueries = [...new Set([...baseQueries, ...aiQueries])].slice(0, 6);

  if (aiQueries.length && reqs.length < MAX_ENGINE_REQUESTS && remainingMs(deadline) > 3500) {
    const second = plannedRequests(aiQueries, baseIntent).slice(0, MAX_ENGINE_REQUESTS - reqs.length);
    const more = await Promise.allSettled(second.map(r => discoverOne(r, deadline)));
    for (const entry of more) {
      if (entry.status !== 'fulfilled') continue;
      const item = entry.value;
      providerStats[item.provider] = providerStats[item.provider] || { ok: 0, failed: 0, results: 0 };
      if (item.ok) providerStats[item.provider].ok++; else providerStats[item.provider].failed++;
      providerStats[item.provider].results += item.results.length;
      discovered.push(...item.results);
    }
  }

  discovered = dedupeResults(discovered).map(r => ({ ...r, type: inferType(r), domain: hostname(r.url) }));

  // Make every discovered result AI-readable immediately from its actual search evidence.
  for (const r of discovered) {
    Object.assign(r, ensureContentFields(r, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent(r, query), confidence: 0.35 }));
    r._score = scoreResult(r, query, baseIntent, dateIntent) + domainTrust(r.url);
  }

  // Google News wrapper -> publisher URL. First use redirect/embedded links, then exact-title search.
  const newsCandidates = discovered.filter(r => r.type === 'news' && /^news\.google\.com$/i.test(hostname(r.url))).slice(0, MAX_NEWS_RESOLVES);
  if (newsCandidates.length && remainingMs(deadline) > 2800) {
    flags.publisherResolutionAttempted = newsCandidates.length;
    const firstPass = await Promise.all(newsCandidates.map(r => resolveNewsWrapper(r, deadline)));
    const unresolved = firstPass.filter(r => /^news\.google\.com$/i.test(hostname(r.url)));
    let secondPass = firstPass;
    if (unresolved.length && remainingMs(deadline) > 1800) {
      const lookup = unresolved.slice(0, MAX_PUBLISHER_LOOKUPS);
      const mapped = await Promise.all(lookup.map(r => publisherLookupByTitle(r, deadline)));
      const map = new Map(mapped.map(x => [normalizedKey(x.url), x]));
      secondPass = firstPass.map(x => map.get(normalizedKey(x.url)) || x);
    }
    const byOldKey = new Map(newsCandidates.map((x, i) => [normalizedKey(x.url), secondPass[i]]));
    discovered = discovered.map(r => byOldKey.get(normalizedKey(r.url)) || r);
    flags.publisherResolutionSucceeded = discovered.filter(r => r.publisherResolved).length;
    if (flags.publisherResolutionSucceeded) warnings.push(`Resolved ${flags.publisherResolutionSucceeded} news result(s) to publisher URLs before content extraction.`);
  }

  for (const r of discovered) r._score = scoreResult(r, query, baseIntent, dateIntent) + domainTrust(r.url);
  discovered = applyDateConstraint(discovered, dateIntent, count, warnings);
  discovered = enforceRequestedType(discovered, requestedType, mode, warnings);
  discovered.sort((a, b) => (b._score || 0) - (a._score || 0));

  const verifyCount = verifyRequested ? Math.min(discovered.length, deep ? DEEP_VERIFY : DEFAULT_VERIFY, MAX_VERIFY) : 0;
  if (verifyCount && remainingMs(deadline) > 1500) {
    const candidates = selectVerificationCandidates(discovered, verifyCount);
    flags.verificationPerformed = candidates.length;
    const enriched = await Promise.all(candidates.map(r => enrichResult(r, query, deadline)));
    const byKey = new Map(enriched.map(r => [normalizedKey(r.url), r]));
    discovered = discovered.map(r => byKey.get(normalizedKey(r.url)) || r);
    flags.verificationSucceeded = enriched.filter(r => r.verified).length;
    discovered.forEach(r => { r._score = scoreResult(r, query, baseIntent, dateIntent) + (r.verified ? 5 : 0) + domainTrust(r.url); });
    discovered.sort((a, b) => (b._score || 0) - (a._score || 0));
  }

  const shouldRerank = useAi === 'true' || (useAi === 'auto' && (deep || count > 10 || discovered.length > 15 || Boolean(ai)));
  if (shouldRerank && discovered.length > 1 && remainingMs(deadline) > 1800) {
    const order = await aiRerank(query, discovered, mode, dateIntent, deadline);
    if (order?.length) {
      const ordered = [], seen = new Set();
      for (const i of order) { if (!seen.has(i)) { ordered.push(discovered[i]); seen.add(i); } }
      for (let i = 0; i < discovered.length; i++) if (!seen.has(i)) ordered.push(discovered[i]);
      discovered = ordered;
    }
  }

  if (useCc && discovered.length && remainingMs(deadline) > 1600) {
    const rows = await Promise.all(discovered.slice(0, MAX_CC_LOOKUPS).map(async r => ({ url: r.url, cc: await commonCrawlLookup(r.url, deadline) })));
    const map = new Map(rows.map(x => [x.url, x.cc]));
    flags.commonCrawlPerformed = rows.filter(x => x.cc).length;
    discovered.forEach(r => { r.commonCrawl = map.get(r.url) || null; });
  } else if (useCc) warnings.push('Common Crawl was skipped or deferred because live page content was prioritized.');

  // Final non-null guarantee for every selected result.
  for (const r of discovered) {
    if (!r.pageContent || !String(r.pageContent).trim()) Object.assign(r, ensureContentFields(r, query, { status: 'snippet_fallback', method: 'search-snippet', content: extractFallbackContent(r, query), confidence: 0.35 }));
    r.domain = hostname(r.url);
  }

  if (remainingMs(deadline) < 1200) {
    flags.partialDueToBudget = true;
    warnings.push('Returned the best available live evidence before the crawler safety budget was exhausted.');
  }

  discovered = diversifyAndSelect(discovered, count, baseIntent);
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
    relevanceScore: Math.max(0, Math.min(100, Math.round(50 + (r._score || 0) * 2))),
    publisherResolved: Boolean(r.publisherResolved),
    publisherWrapperUrl: r.publisherWrapperUrl || null,
    extractedText: truncate(r.extractedText || r.pageContent || extractFallbackContent(r, query), MAX_TEXT_CHARS),
    pageContent: truncate(r.pageContent || r.extractedText || extractFallbackContent(r, query), MAX_TEXT_CHARS),
    contentAvailable: true,
    contentStatus: r.contentStatus || 'snippet_fallback',
    contentMethod: r.contentMethod || 'search-snippet',
    contentLength: Number(r.contentLength || String(r.pageContent || r.extractedText || '').length),
    contentConfidence: Number(r.contentConfidence ?? 0.35),
    contentSourceUrl: r.contentSourceUrl || r.url,
    contentError: r.contentError || null,
    contentTruncated: String(r.pageContent || r.extractedText || '').length >= MAX_TEXT_CHARS,
    verificationMethod: r.verificationMethod || null,
    transcript: r.transcript || null,
    transcriptAvailable: Boolean(r.transcriptAvailable),
    transcriptLanguage: r.transcriptLanguage || null,
    commonCrawl: r.commonCrawl || null,
  }));

  if (verifyRequested && finalResults.length && !finalResults.some(r => r.contentStatus === 'full' || r.contentStatus === 'reader' || r.contentStatus === 'alternate')) {
    warnings.push('The selected sources were discoverable but their publishers did not expose full page text to the crawler; pageContent therefore contains the real search evidence/snippet instead of null.');
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
    groqUsed: Boolean(ai),
    providers: providerStats,
    searchPlan: {
      queryVariants: plannedQueries,
      engineRequests: reqs.length,
      verificationRequested: verifyRequested,
      verificationPerformed: flags.verificationPerformed,
      verificationSucceeded: flags.verificationSucceeded,
      commonCrawlEnabled: useCc,
      commonCrawlPerformed: flags.commonCrawlPerformed,
      publisherResolutionAttempted: flags.publisherResolutionAttempted,
      publisherResolutionSucceeded: flags.publisherResolutionSucceeded,
      dateIntent,
      streamed: true,
      contentGuarantee: 'non-null AI-readable pageContent; full/reader/alternate status only when page content passes non-asset/article validation',
    },
    results: finalResults,
    warnings: [
      'Core discovery is keyless but depends on public web surfaces that may rate-limit or block automated requests.',
      'No crawler can guarantee full page extraction from every website because some sites block bots, require JavaScript, require authentication, or expose media without machine-readable text.',
      'Tracking scripts, analytics endpoints, and static assets are rejected as content sources; blocked/unvalidated pages use real search evidence/metadata instead of pretending scripts are article text.',
      'The response begins as valid JSON and streams heartbeats so the Edge gateway is not left idle during long searches.',
      ...warnings,
    ],
  };
}

function corsHeaders() {
  return {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store, no-transform',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type, authorization, x-arix-search-key',
    'x-arix-crawler-version': VERSION,
  };
}

function withCors(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), { status, headers: corsHeaders() });
}

function encodeJson(value) { return JSON.stringify(value); }

function streamSearch(input) {
  const encoder = new TextEncoder();
  const started = Date.now();
  const deadline = started + SEARCH_WORK_BUDGET_MS;
  const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
  const count = parseCount(input.count ?? input.limit ?? DEFAULT_RESULTS);
  const mode = String(input.mode || 'auto').toLowerCase();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;
      const enqueue = chunk => {
        if (closed) return;
        try { controller.enqueue(encoder.encode(chunk)); } catch { closed = true; }
      };

      // IMPORTANT: this is valid JSON immediately. Do not replace it with whitespace.
      enqueue('{"ok":true,"version":' + encodeJson(VERSION) + ',"query":' + encodeJson(query) + ',"requestedResults":' + String(count) + ',"mode":' + encodeJson(mode) + ',"streaming":true,"results":[');

      const heartbeat = setInterval(() => enqueue('\n'), STREAM_HEARTBEAT_MS);

      Promise.resolve()
        .then(() => performSearch(input, started, deadline))
        .then(result => {
          clearInterval(heartbeat);
          const results = Array.isArray(result.results) ? result.results : [];
          results.forEach((r, i) => {
            enqueue((i ? ',' : '') + JSON.stringify(r));
          });
          const metadata = { ...result };
          delete metadata.results;
          enqueue('],');
          const entries = Object.entries(metadata);
          entries.forEach(([key, value], i) => enqueue(JSON.stringify(key) + ':' + JSON.stringify(value) + (i === entries.length - 1 ? '' : ',')));
          enqueue('}');
          try { controller.close(); } catch {}
          closed = true;
        })
        .catch(error => {
          clearInterval(heartbeat);
          enqueue('],"returnedResults":0,"generatedAt":' + encodeJson(nowIso()) + ',"latencyMs":' + String(Date.now() - started) + ',"keylessCoreSearch":true,"groqUsed":false,"resultsError":' + encodeJson(error?.message || 'SEARCH_FAILED') + ',"warnings":["The search stream failed safely after the response had already started."]}');
          try { controller.close(); } catch {}
          closed = true;
        });
    },
  });

  return new Response(stream, {
    status: 200,
    headers: { ...corsHeaders(), 'x-arix-search-stream': '1', 'x-arix-stream-heartbeat-ms': String(STREAM_HEARTBEAT_MS) },
  });
}

async function readInput(req) {
  const url = new URL(req.url);
  if (req.method === 'GET') return Object.fromEntries(url.searchParams.entries());
  const raw = await req.text();
  if (raw.length > MAX_REQUEST_BODY) throw new Error('REQUEST_BODY_TOO_LARGE');
  if (!raw) return {};
  try { return JSON.parse(raw); } catch { return Object.fromEntries(new URLSearchParams(raw).entries()); }
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') return withCors({ ok: true, version: VERSION });
  if (!['GET', 'POST'].includes(req.method)) return withCors({ ok: false, error: 'METHOD_NOT_ALLOWED', message: 'Use GET or POST.' }, 405);

  try {
    const input = await readInput(req);
    const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
    if (!query) return withCors({ ok: false, error: 'MISSING_QUERY', message: 'Provide query in ?query=... or a JSON body with {"query":"..."}.' }, 400);
    if (query.length < 2) return withCors({ ok: false, error: 'QUERY_TOO_SHORT' }, 400);
    return streamSearch(input);
  } catch (error) {
    const tooLarge = error?.message === 'REQUEST_BODY_TOO_LARGE';
    return withCors({ ok: false, version: VERSION, error: tooLarge ? 'REQUEST_BODY_TOO_LARGE' : 'SEARCH_FAILED', message: error?.message || 'Unexpected crawler error' }, tooLarge ? 413 : 500);
  }
}
