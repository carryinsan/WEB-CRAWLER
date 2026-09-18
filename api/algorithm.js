/*
 * ArixAI Precision Page Algorithm
 * v1.5.0 (Full Page Content Extractor Edition)
 * 
 * Purpose: Advanced query planning, REAL page acquisition, highly robust HTML/SPA extraction, 
 * analytics/ad rejection, and post-fetch query comparison.
 */

export const runtime = 'edge';
export const config = { runtime: 'edge' };
export const maxDuration = 300;

const VERSION = 'arix-content-algorithm-1.5.0';
const MAX_RESULTS = 40;
const MAX_QUERY_LEN = 700;
const MAX_CANDIDATES = 500;
const MAX_PAGE_BYTES = 1_000_000;
const MAX_TEXT_CHARS = 24_000;
const MIN_REAL_CONTENT = 150;

// Increased timeouts to ensure we have enough time to fetch and extract complex pages
const DEFAULT_BUDGET_MS = 14_500;
const PAGE_TIMEOUT_MS = 4_500;
const READER_TIMEOUT_MS = 5_200;
const CONTENT_CONCURRENCY = 48;
const RECOVERY_CONCURRENCY = 36;
const CACHE_TTL_MS = 120_000;
const CACHE_MAX = 160;
const CONTENT_CACHE = new Map();

const BLOCKED_HOSTS = new Set([
  'google-analytics.com', 'googletagmanager.com', 'googlesyndication.com',
  'googleadservices.com', 'doubleclick.net', 'gstatic.com', 'googleapis.com',
  'facebook.net', 'connect.facebook.net', 'scorecardresearch.com', 'pixel.wp.com',
  'adsrvr.org', 'amazon-adsystem.com', 'taboola.com', 'outbrain.com',
  'segment.io', 'hotjar.com', 'clarity.ms', 'datadome.co'
]);

const BLOCKED_EXT = /\.(?:js|mjs|cjs|css|map|png|jpeg|gif|webp|avif|svg|ico|bmp|tiff?|woff2?|woff|ttf|otf|eot|mp3|wav|m4a|mp4|webm|mov|avi|zip|rar|7z|exe|dmg)(?:\?|#|$)/i;
const DATA_PATH = /(?:^|[\/_-])(?:analytics|gtag|ga4|collect|pixel|beacon|tracking|tracker|telemetry|consent|ads?)(?:[\/_-]|$)/i;
const ARTICLE_PATH = /(?:article|articles|story|stories|news|post|posts|blog|blogs|report|reports|press[-_]?release|explained|timeline|history|wiki|paper|research)/i;

const GOV_DOMAINS = [
  'gov.in', 'nic.in', 'mygov.in', 'india.gov.in', 'pib.gov.in', 'mca.gov.in',
  'gst.gov.in', 'incometax.gov.in', 'msme.gov.in', 'education.gov.in', 'meity.gov.in',
  'rbi.org.in', 'sebi.gov.in', 'supremecourt.gov.in', 'indiacode.nic.in'
];

const TRUSTED_DOMAINS = [
  'who.int', 'un.org', 'nasa.gov', 'oecd.org', 'worldbank.org', 'imf.org',
  'w3.org', 'ietf.org', 'mozilla.org', 'developer.mozilla.org'
];

const STOPWORDS = new Set([
  'a', 'an', 'the', 'and', 'or', 'of', 'to', 'in', 'on', 'for', 'with', 'from', 'by', 'as', 'at', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
  'this', 'that', 'these', 'those', 'what', 'when', 'where', 'how', 'why', 'who', 'which', 'about', 'into', 'near', 'over', 'under', 'than',
  'then', 'during', 'through', 'latest', 'current', 'recent', 'today', 'news', 'update', 'updates', 'please', 'show', 'find', 'give', 'tell',
  'me', 'can', 'you', 'i', 'we', 'it', 'its', 'their', 'our', 'your', 'my', 'more', 'information', 'info', 'details', 'best', 'all', 'some'
]);

const SYNONYM_GROUPS = [
  ['car', 'cars', 'automobile', 'automobiles', 'motorcar', 'motorcars', 'vehicle', 'vehicles'],
  ['history', 'historical', 'timeline', 'timelines', 'origins', 'origin', 'evolution', 'development', 'heritage'],
  ['india', 'indian'],
  ['price', 'prices', 'cost', 'costs', 'rate', 'rates', 'pricing', 'priced'],
  ['law', 'laws', 'legal', 'legislation', 'act', 'acts', 'regulation', 'regulations', 'rule', 'rules'],
  ['policy', 'policies', 'framework', 'initiative', 'initiatives', 'programme', 'program', 'programs'],
  ['company', 'companies', 'firm', 'firms', 'business', 'businesses', 'corporation', 'corporations'],
  ['founder', 'founders', 'created', 'creator', 'cofounder', 'co-founder', 'originator'],
  ['population', 'people', 'residents', 'inhabitants', 'demographics'],
  ['economy', 'economic', 'economics', 'gdp', 'market', 'markets'],
  ['education', 'school', 'schools', 'student', 'students', 'curriculum', 'syllabus'],
  ['research', 'study', 'studies', 'paper', 'papers', 'report', 'reports', 'analysis'],
  ['technology', 'technologies', 'tech', 'technical'],
  ['electric', 'ev', 'electricity', 'battery', 'battery-powered'],
  ['manufacturing', 'manufacture', 'production', 'factory', 'factories'],
  ['video', 'videos', 'watch', 'youtube', 'interview', 'podcast'],
  ['guide', 'guides', 'tutorial', 'tutorials', 'manual', 'documentation', 'docs']
];

const SYNONYM_INDEX = new Map();
for (const group of SYNONYM_GROUPS) {
  const canonical = group[0];
  for (const x of group) SYNONYM_INDEX.set(x, canonical);
}

function clamp(n, a, b) { return Math.min(b, Math.max(a, Number(n) || 0)); }
function truncate(v, max) { const s = String(v ?? '').trim(); return s.length <= max ? s : `${s.slice(0, max - 1)}…`; }
function left(deadline) { return Math.max(0, deadline - Date.now()); }
function nowIso() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function safeInt(v, fallback, min, max) {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? Math.min(max, Math.max(min, n)) : fallback;
}

function normalizeText(v) {
  return String(v || '').normalize('NFKC').toLowerCase().replace(/[^\p{L}\p{N}]+/gu, ' ').replace(/\s+/g, ' ').trim();
}
function tokens(v) {
  return normalizeText(v).split(' ').filter(x => x.length > 1 && !STOPWORDS.has(x));
}
function unique(arr) { return [...new Set(arr.filter(Boolean))]; }

function safeUrl(url) {
  try {
    const u = new URL(url);
    if (!/^https?:$/i.test(u.protocol)) return false;
    const h = u.hostname.toLowerCase();
    if (!h || h === 'localhost' || h.endsWith('.localhost')) return false;
    if (/^(127\.|10\.|192\.168\.|169\.254\.)/.test(h)) return false;
    if (/^172\.(?:1[6-9]|2[0-9]|3[0-1])\./.test(h)) return false;
    return true;
  } catch { return false; }
}

function host(url) { try { return new URL(url).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; } }

function blocked(url) {
  if (!safeUrl(url)) return true;
  const h = host(url);
  if ([...BLOCKED_HOSTS].some(x => h === x || h.endsWith(`.${x}`))) return true;
  try {
    const u = new URL(url);
    if (BLOCKED_EXT.test(`${u.pathname}${u.search}`)) return true;
    if (DATA_PATH.test(u.pathname)) return true;
  } catch { return true; }
  return false;
}

function isGov(url) { const h = host(url); return GOV_DOMAINS.some(x => h === x || h.endsWith(`.${x}`)); }
function isTrusted(url) {
  const h = host(url);
  return TRUSTED_DOMAINS.some(x => h === x || h.endsWith(`.${x}`)) || /\.edu(?:\.|$)/i.test(h) || /\.ac\.(?:in|uk|jp|nz)$/i.test(h);
}
function isYouTube(url) { const h = host(url); return h === 'youtube.com' || h.endsWith('.youtube.com') || h === 'youtu.be'; }
function isVideo(url) { return isYouTube(url) || /(?:vimeo|dailymotion)\.com$/i.test(host(url)); }
function isDoc(url) { return /\.(?:pdf|docx?|xlsx?|pptx?)(?:\?|$)/i.test(url || '') || /(?:\/|^|[\W_])(?:pdf|documentation|docs?|manual|report|paper)(?:[\/\W_]|$)/i.test(url || ''); }

function normalizedUrl(url) {
  try {
    const u = new URL(url); u.hash = '';
    for (const k of [...u.searchParams.keys()]) if (/^(utm_gclid$|fbclid$|msclkid$|ref$|referrer$|cmpid$|src$)/i.test(k)) u.searchParams.delete(k);
    return u.href;
  } catch { return ''; }
}

function normalizedKey(url) { const u = normalizedUrl(url); return u ? `${host(u)}${new URL(u).pathname.replace(/\/{2,}/g, '/').replace(/\/$/, '')}${new URL(u).search}` : ''; }

function decodeHtml(v = '') {
  return String(v).replace(/&nbsp;/gi, ' ').replace(/&quot;/gi, '"').replace(/&#39;/gi, "'").replace(/&apos;/gi, "'")
    .replace(/&amp;/gi, '&').replace(/&lt;/gi, '<').replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);?/gi, (_, x) => { const n = parseInt(x, 16); return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : _; })
    .replace(/&#(\d+);?/g, (_, x) => { const n = parseInt(x, 10); return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : _; });
}

function strip(html = '') {
  // 1. Completely obliterate dangerous and noisy structural tags.
  let s = String(html)
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<(script|style|noscript|iframe|nav|footer|aside|header|form|menu|dialog|canvas|svg|button|map|object|embed|picture|video|audio)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ');

  // 2. Eradicate common UI wrappers, ads, sidebars, cookie popups, and analytics via class/id patterns.
  s = s.replace(/<div\b[^>]*\b(?:class|id)=["']?(?:[^"']*(?:cookie|banner|nav-|footer|sidebar|advert|promo|menu|widget|social|modal|popup|consent|related|share|comments|ad-|sponsor|search|auth|login|signup))["']?[^>]*>[\s\S]*?<\/div>/gi, ' ');

  // 3. Remove all remaining HTML tags
  s = s.replace(/<[^>]+>/g, ' ');

  return decodeHtml(s).replace(/\s+/g, ' ').trim();
}

function cleanContent(v) {
  let s = String(v || '').replace(/\s+/g, ' ').trim();
  // Remove markdown artifacts
  s = s.replace(/\[[^\]]{0,80}\]\([^)]{0,500}\)/g, '');
  s = s.replace(/!\[[^\]]*\]\([^)]*\)/g, '');
  // Remove obvious inline JS or CSS blocks that slipped through the HTML stripper
  s = s.replace(/(?:\bfunction\b|\bconst\b|\blet\b|\bvar\b|\bwindow\.|document\.|console\.)[^;{}]+?[;{}]/gi, ' ');
  // Remove boilerplate navigation links
  s = s.replace(/(?:skip to content|accept cookies|cookie settings|privacy settings|sign in|log in|search this site)\b/gi, ' ');
  return truncate(s.replace(/\s+/g, ' ').trim(), MAX_TEXT_CHARS);
}

function isBotChallenge(text) {
  const t = String(text || '').toLowerCase();
  // Genuine articles are usually long. Bot challenges are brief stubs.
  if (t.length > 4500) return false; 
  const triggers = [
    'just a moment...', 'checking your browser', 'enable javascript and cookies',
    'please enable js', 'cloudflare', 'cf-browser-verification', 'verify you are human',
    'security check to access', 'why do i have to complete a captcha', 'attention required!',
    'robot or human', 'datadome', 'perimeterx', 'access denied', '403 forbidden',
    'checking if the site connection is secure', 'needs to review the security of your connection'
  ];
  return triggers.some(trigger => t.includes(trigger));
}

function textFromMarkdown(v) {
  return cleanContent(String(v || '')
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/\[([^\]]+)\]\((?:https?:\/\/|\/)[^)]*\)/g, '$1')
    .replace(/^#{1,6}\s*/gm, '')
    .replace(/[*_`]/g, ''));
}

function extractTitle(html) { return strip((String(html).match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [, ''])[1]); }
function extractMeta(html, name) {
  const e = String(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const a = String(html).match(new RegExp(`<meta[^>]+(?:name|property)=["']${e}["'][^>]*content=["']([\\s\\S]*?)["']`, 'i'));
  const b = String(html).match(new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+(?:name|property)=["']${e}["']`, 'i'));
  return decodeHtml((a || b || [, ''])[1] || '').trim();
}

function extractCanonical(html, base) {
  const a = (String(html).match(/<link[^>]+rel=["'](?:[^"']*\s)?canonical(?:\s[^"']*)?["'][^>]*href=["']([^"']+)["']/i) || [])[1];
  const b = (String(html).match(/<link[^>]+href=["']([^"']+)["'][^>]*rel=["'](?:[^"']*\s)?canonical(?:\s[^"']*)?["']/i) || [])[1];
  try { return new URL(a || b, base).href; } catch { return null; }
}

function extractStringsFromObj(obj, outArray, maxDepth = 6) {
  if (maxDepth <= 0 || !obj) return;
  if (typeof obj === 'string') {
    const t = obj.trim();
    // Keep substantial sentences/paragraphs. Ignore code, URLs, and tiny UI labels.
    if (t.length > 80 && !t.startsWith('http') && !t.startsWith('/') && t.includes(' ')) {
      outArray.push(t);
    }
    return;
  }
  if (typeof obj === 'object') {
    if (Array.isArray(obj)) {
      for (const item of obj) extractStringsFromObj(item, outArray, maxDepth - 1);
    } else {
      for (const key of Object.keys(obj)) {
        // Skip purely technical keys
        if (key.startsWith('__') || /css|style|class|config/i.test(key)) continue;
        extractStringsFromObj(obj[key], outArray, maxDepth - 1);
      }
    }
  }
}

function extractSpaState(html) {
  const out = [];
  // Target Next.js, Nuxt, or general JSON payload islands
  const scripts = String(html).match(/<script[^>]+type=["']application\/json["'][^>]*>[\s\S]*?<\/script>/gi) || [];
  for (const block of scripts) {
    const raw = block.replace(/^<[\s\S]*?>/i, '').replace(/<\/script>$/i, '');
    try {
      const data = JSON.parse(raw.trim());
      extractStringsFromObj(data, out);
    } catch {}
  }
  return out.join('\n\n');
}

function extractJsonLdBody(html) {
  const out = [];
  for (const block of String(html).match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>[\s\S]*?<\/script>/gi) || []) {
    const raw = block.replace(/^<[\s\S]*?>/i, '').replace(/<\/script>$/i, '');
    try {
      const value = JSON.parse(raw.trim());
      extractStringsFromObj(value, out);
    } catch {}
  }
  return out;
}

function extractArticleText(html) {
  // Pre-clean removes the vast majority of junk (navs, footers, scripts, ads, sidebars)
  const cleanHtml = String(html || '')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<(script|style|noscript|iframe|nav|footer|aside|header|form|menu|dialog|canvas|svg|button|map|object|embed|picture|video|audio)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<div\b[^>]*\b(?:class|id)=["']?(?:[^"']*(?:cookie|banner|nav-|footer|sidebar|advert|promo|menu|widget|social|modal|popup|consent|related|share|comments|ad-|sponsor|search|auth|login|signup))["']?[^>]*>[\s\S]*?<\/div>/gi, ' ');

  // 1. Check for JSON-LD schema (NewsArticle, Article, etc) which is often pristine
  const jsonLd = cleanContent(extractJsonLdBody(html).join('\n\n'));
  if (jsonLd.length >= 300 && !isBotChallenge(jsonLd)) return jsonLd;

  // 2. Check SPA Data (Next.js __NEXT_DATA__, Nuxt, Apollo state)
  const spaText = cleanContent(extractSpaState(html));
  if (spaText.length >= 300 && !isBotChallenge(spaText)) return spaText;

  // 3. Extract core Semantic blocks (<article>, <main>)
  let mainBlocks = [];
  for (const m of cleanHtml.matchAll(/<(article|main)\b[^>]*>([\s\S]*?)<\/\1>/gi)) {
     const t = strip(m[2]);
     if (t.length >= 50) mainBlocks.push(t);
  }
  if (mainBlocks.length > 0) {
     const mText = cleanContent(mainBlocks.join('\n\n'));
     if (mText.length >= 300 && !isBotChallenge(mText)) return mText;
  }

  // 4. Extract common Content Divs
  let divBlocks = [];
  for (const m of cleanHtml.matchAll(/<div\b[^>]*\b(?:class|id)=["']?(?:[^"']*(?:content|article|body|post|story|text))["']?[^>]*>([\s\S]*?)<\/div>/gi)) {
     const t = strip(m[1]);
     if (t.length >= 50) divBlocks.push(t);
  }
  if (divBlocks.length > 0) {
     const dText = cleanContent(divBlocks.join('\n\n'));
     if (dText.length >= 300 && !isBotChallenge(dText)) return dText;
  }

  // 5. General Paragraphs fallback
  const pBlocks = [];
  for (const m of cleanHtml.matchAll(/<(?:p|h[1-6]|li|blockquote)\b[^>]*>([\s\S]*?)<\/(?:p|h[1-6]|li|blockquote)>/gi)) {
    const t = strip(m[1]);
    // Only accept paragraphs with enough words to filter out UI links/buttons
    if (t.length >= 40 && t.split(/\s+/).length >= 6) pBlocks.push(t);
  }
  const pText = cleanContent(pBlocks.join('\n\n'));
  if (pText.length >= 300 && !isBotChallenge(pText)) return pText;

  // 6. Ultimate fallback: just strip everything from the remaining body
  const body = cleanHtml.match(/<body\b[^>]*>([\s\S]*?)<\/body>/i);
  const fallbackText = cleanContent(strip(body ? body[1] : cleanHtml));
  if (!isBotChallenge(fallbackText) && fallbackText.length >= MIN_REAL_CONTENT) return fallbackText;
  
  return '';
}

function contentCacheGet(url) {
  const hit = CONTENT_CACHE.get(normalizedKey(url));
  if (!hit) return null;
  if (Date.now() - hit.at > CACHE_TTL_MS) { CONTENT_CACHE.delete(normalizedKey(url)); return null; }
  return hit.value;
}

function contentCacheSet(url, value) {
  CONTENT_CACHE.set(normalizedKey(url), { at: Date.now(), value });
  while (CONTENT_CACHE.size > CACHE_MAX) CONTENT_CACHE.delete(CONTENT_CACHE.keys().next().value);
}

async function fetchResponse(url, timeout, deadline, headers = {}) {
  const budget = Math.min(timeout, Math.max(350, left(deadline) - 80));
  if (budget <= 0) throw new Error('BUDGET_EXHAUSTED');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), budget);
  try {
    return await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'user-agent': 'Mozilla/5.0 (compatible; ArixAI-PageAlgorithm/1.5.0; +https://lexis-ai-chatini.vercel.app/)',
        'accept': 'text/html,application/xhtml+xml,application/pdf,text/plain;q=0.9,*/*;q=0.2',
        'accept-language': 'en-IN,en;q=0.9',
        ...headers,
      }
    });
  } finally { clearTimeout(timer); }
}

async function readText(res, maxBytes, deadline) {
  const reader = res.body?.getReader?.();
  if (!reader) return truncate(await res.text(), maxBytes);
  const chunks = []; let total = 0;
  try {
    while (total < maxBytes && left(deadline) > 80) {
      const { done, value } = await reader.read(); if (done) break; if (!value) continue;
      const room = maxBytes - total; const c = value.byteLength > room ? value.slice(0, room) : value;
      chunks.push(c); total += c.byteLength;
    }
  } finally { try { await reader.cancel(); } catch {} }
  const bytes = new Uint8Array(total); let off = 0; for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
  return new TextDecoder('utf-8', { fatal: false }).decode(bytes);
}

async function readBytes(res, maxBytes, deadline) {
  const reader = res.body?.getReader?.();
  if (!reader) return new Uint8Array((await res.arrayBuffer()).slice(0, maxBytes));
  const chunks = []; let total = 0;
  try {
    while (total < maxBytes && left(deadline) > 80) {
      const { done, value } = await reader.read(); if (done) break; if (!value) continue;
      const room = maxBytes - total; const c = value.byteLength > room ? value.slice(0, room) : value;
      chunks.push(c); total += c.byteLength;
    }
  } finally { try { await reader.cancel(); } catch {} }
  const bytes = new Uint8Array(total); let off = 0; for (const c of chunks) { bytes.set(c, off); off += c.byteLength; }
  return bytes;
}

function bytesLatin1(bytes) { let out = ''; for (let i = 0; i < bytes.length; i += 8192) out += String.fromCharCode(...bytes.subarray(i, Math.min(i + 8192, bytes.length))); return out; }
function pdfLiteral(s) { return String(s || '').replace(/\\([nrtbf\\()])/g, (_, c) => ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '\\': '\\', '(': '(', ')': ')' })[c] || c).replace(/\\([0-7]{1,3})/g, (_, o) => String.fromCharCode(parseInt(o, 8))); }
function pdfHex(s) { const h = String(s || '').replace(/[^0-9a-f]/gi, ''); if (!h) return ''; const e = h.length % 2 ? h + '0' : h; const b = new Uint8Array(e.length / 2); for (let i = 0; i < b.length; i++) b[i] = parseInt(e.slice(i * 2, i * 2 + 2), 16); try { if (b[0] === 0xfe && b[1] === 0xff) return new TextDecoder('utf-16be').decode(b.slice(2)); } catch {} return new TextDecoder().decode(b); }

async function inflate(bytes, deadline) {
  if (typeof DecompressionStream === 'undefined' || left(deadline) < 400) return null;
  try { const ds = new DecompressionStream('deflate'); const w = ds.writable.getWriter(); await w.write(bytes); await w.close(); return new Uint8Array(await new Response(ds.readable).arrayBuffer()); } catch { return null; }
}

async function extractPdfText(bytes, deadline) {
  const raw = bytesLatin1(bytes); const parts = []; const re = /<<(?:[\s\S]{0,7000}?)>>\s*stream\r\n([\s\S]*?)\r?\nendstream/g; let m;
  while ((m = re.exec(raw)) && parts.length < 90 && left(deadline) > 300) {
    const header = m[0].slice(0, m[0].indexOf('stream')); const start = m.index + m[0].indexOf(m[1]); const source = bytes.slice(start, start + m[1].length);
    let data = m[1]; if (/\/FlateDecode/i.test(header)) { const inf = await inflate(source, deadline); if (inf) data = bytesLatin1(inf); }
    const found = []; for (let i = 0; i < data.length; i++) { if (data[i] === '(') { let d = 1, j = i + 1, e = false; for (; j < data.length; j++) { const c = data[j]; if (e) { e = false; continue; } if (c === '\\') { e = true; continue; } if (c === '(') d++; else if (c === ')') { d--; if (!d) break; } } if (j < data.length) found.push(pdfLiteral(data.slice(i + 1, j))); i = j; } else if (data[i] === '<' && data[i + 1] !== '<') { const j = data.indexOf('>', i + 1); if (j > i) { found.push(pdfHex(data.slice(i + 1, j))); i = j; } } }
    parts.push(found.join(''));
  }
  if (!cleanContent(parts.join(''))) {
    const loose = []; for (const m2 of raw.matchAll(/\(([^\\]{3,500})\)/g)) loose.push(pdfLiteral(m2[1]));
    parts.push(loose.join(''));
  }
  return cleanContent(parts.join(''));
}

function queryPlanTokens(query) {
  const raw = tokens(query); const canonical = raw.map(x => SYNONYM_INDEX.get(x) || x); return { raw, canonical };
}
function hasToken(text, token) { return new Set(tokens(text)).has(token); }
function phraseHit(text, query) { const a = normalizeText(text); const b = normalizeText(query); return Boolean(b && a.includes(b)); }
function canonicalHits(text, planTokens) {
  const set = new Set(tokens(text)); let hits = 0; const matched = [];
  for (const t of planTokens) { const c = SYNONYM_INDEX.get(t) || t; if (set.has(c) || set.has(t)) { hits++; matched.push(c); } }
  return { hits, matched: unique(matched), ratio: planTokens.length ? hits / planTokens.length : 1 };
}

export function analyzeQuery(query, options = {}) {
  const q = truncate(String(query || '').trim(), MAX_QUERY_LEN);
  const requested = String(options.type || '').toLowerCase();
  const mode = String(options.mode || 'auto').toLowerCase();
  let type = requested;
  if (!type || type === 'mixed' || type === 'all') {
    if (mode === 'news') type = 'news'; else if (mode === 'video') type = 'video'; else if (mode === 'gov') type = 'gov'; else if (mode === 'doc' || mode === 'docs' || mode === 'document') type = 'doc'; else type = 'web';
  }
  const p = queryPlanTokens(q);
  const concepts = unique(p.canonical.map(x => SYNONYM_INDEX.get(x) || x)).map(x => ({ id: x, core: x, terms: SYNONYM_GROUPS.find(g => g[0] === x) || [x] }));
  const history = /\b(history|historical|timeline|origins?|evolution)\b/i.test(q);
  const official = /\b(official|government|govt|ministry|scheme|policy|law|act|rule|regulation|tax|gst|rbi|sebi|mca)\b/i.test(q);
  const academic = /\b(research|study|paper|academic|journal|thesis|evidence)\b/i.test(q);
  const live = /\b(latest|today|current|recent|breaking|this week|yesterday)\b/i.test(q);
  const anchors = (q.match(/\b(?:India|Indian|[A-Z][a-z]{2,}|20\d{2})\b/g) || []);
  
  return { query: q, type, requestedType: requested, mode, tokens: p.raw, canonicalTokens: p.canonical, concepts, anchors: unique(anchors), dateIntent: { live, kind: live ? 'live' : 'none' }, flags: { wantsHistory: history, wantsOfficial: official, wantsAcademic: academic, explicitNews: type === 'news', explicitVideo: type === 'video', explicitDoc: type === 'doc', explicitGov: type === 'gov' } };
}

export function buildPreciseQueries(query, options = {}) {
  const plan = options.plan || analyzeQuery(query, options); const set = new Set(); const add = x => { x = String(x || '').replace(/"/g, '').trim(); if (x && x.length >= 3 && x.length < 500) set.add(x); };
  add(plan.query);
  const important = plan.canonicalTokens.slice(0, 8); if (important.length >= 2) add(`"${important.join(' ')}"`);
  if (plan.flags.wantsHistory) { add(`${plan.query} history timeline origins evolution`); add(`${plan.query} historical overview milestones`); }
  if (plan.flags.wantsOfficial) { add(`${plan.query} official source`); if (plan.flags.explicitGov) add(`${plan.query} site:gov.in`); }
  if (plan.flags.wantsAcademic) { add(`${plan.query} research paper evidence`); }
  if (plan.flags.explicitNews) { add(`${plan.query} latest news`); add(`${plan.query} recent developments`); }
  if (plan.flags.explicitVideo) { add(`${plan.query} video YouTube`); }
  if (plan.flags.explicitDoc) { add(`${plan.query} filetype:pdf`); add(`${plan.query} official PDF`); }
  return [...set].slice(0, 8);
}

function typeFits(c, plan) {
  if (plan.type === 'gov') return isGov(c.url) || c.type === 'gov';
  if (plan.type === 'doc') return isDoc(c.url) || c.type === 'doc';
  if (plan.type === 'video') return isVideo(c.url) || c.type === 'video';
  if (plan.type === 'news') return c.type === 'news' || ARTICLE_PATH.test(String(c.url || ''));
  return true;
}

export function comparePageToQuery(query, page = {}) {
  const plan = analyzeQuery(query, { type: page.type || '' });
  const title = String(page.title || ''); 
  // Safety check: if snippet is passed in lieu of full content, ensure it's evaluated properly
  const body = String(page.pageContent || page.extractedText || page.content || page.rawContent || page.snippet || ''); 
  const all = `${title} ${body}`;
  const canonSet = value => { const out = new Set(); for (const x of tokens(value)) out.add(SYNONYM_INDEX.get(x) || x); return out; };
  const tset = canonSet(title); const bset = canonSet(body);
  let titleHits = 0, bodyHits = 0; const matched = []; const missing = [];
  for (const raw of plan.canonicalTokens) { const c = SYNONYM_INDEX.get(raw) || raw; if (tset.has(c)) titleHits++; if (bset.has(c)) { bodyHits++; matched.push(c); } else missing.push(c); }
  const n = Math.max(1, plan.canonicalTokens.length);
  const titleCoverage = titleHits / n; const bodyCoverage = bodyHits / n; const phrase = phraseHit(all, plan.query); const concept = canonicalHits(all, plan.canonicalTokens);
  
  let score = 0; score += titleCoverage * 50; score += bodyCoverage * 38; score += phrase ? 10 : 0; score += concept.ratio * 18;
  if (isGov(page.url) && plan.flags.wantsOfficial) score += 7;
  if (isTrusted(page.url) && (plan.flags.wantsOfficial || plan.flags.wantsAcademic)) score += 5;
  if (ARTICLE_PATH.test(String(page.url || '')) && (plan.flags.wantsHistory || plan.flags.wantsAcademic)) score += 3;
  if (plan.flags.wantsHistory && !concept.matched.includes('history') && /\b(new cars?|upcoming cars?|car prices?|buy a car|best cars?)\b/i.test(all)) score -= 18;
  if (plan.type === 'gov' && !isGov(page.url) && page.type !== 'gov') score -= 10;
  if (plan.type === 'doc' && !isDoc(page.url) && page.type !== 'doc') score -= 10;
  if (plan.type === 'video' && !isVideo(page.url) && page.type !== 'video') score -= 12;
  if (plan.type === 'news' && page.type !== 'news' && !ARTICLE_PATH.test(String(page.url || ''))) score -= 12;
  
  const band = score >= 78 ? 'excellent' : score >= 58 ? 'strong' : score >= 36 ? 'usable' : score > 2 ? 'related' : 'weak';
  
  return { score: Number(clamp(score, 0, 100).toFixed(2)), titleCoverage: Number(titleCoverage.toFixed(3)), bodyCoverage: Number(bodyCoverage.toFixed(3)), conceptCoverage: Number(concept.ratio.toFixed(3)), matchedConcepts: unique(matched), missingConcepts: unique(missing), exactPhrase: phrase, acceptable: score > 0, band };
}

export function rankCandidates(candidates, queryOrPlan, options = {}) {
  const plan = typeof queryOrPlan === 'string' ? analyzeQuery(queryOrPlan, options) : (queryOrPlan || analyzeQuery('', options));
  const list = (Array.isArray(candidates) ? candidates : []).slice(0, MAX_CANDIDATES); const ranked = [];
  for (const raw of list) {
    const url = normalizedUrl(raw?.url || raw?.link || raw?.sourceUrl || '');
    if (!url || blocked(url) || !typeFits({ ...raw, url }, plan)) continue;
    const c = { ...raw, url, title: truncate(raw?.title || raw?.name || '', 500), snippet: truncate(raw?.snippet || raw?.description, 2500), type: raw?.type || 'web' };
    const preview = { ...c, pageContent: raw?.pageContent || raw?.extractedText || raw?.rawContent || '' };
    const rel = comparePageToQuery(plan.query, preview);
    ranked.push({ ...c, _relevance: rel });
  }
  ranked.sort((a, b) => {
    const d = (b._relevance?.score || 0) - (a._relevance?.score || 0); if (Math.abs(d) > 0.01) return d;
    const bt = (b.title || '').length - (a.title || '').length; if (bt) return bt;
    return Number(b.semanticSearchScore || 0) - Number(a.semanticSearchScore || 0);
  });
  return ranked;
}

async function readerContent(candidate, plan, deadline) {
  if (!safeUrl(candidate.url) || blocked(candidate.url) || left(deadline) < 350) return null;
  let u;
  try {
    // Jina Reader natively reads standard URLs.
    u = `https://r.jina.ai/${candidate.url}`;
  } catch { return null; }
  try {
    const res = await fetchResponse(u, READER_TIMEOUT_MS, deadline, { accept: 'text/plain,text/markdown;q=0.9,*/*;q=0.2' });
    if (!res.ok) return null;
    const raw = await readText(res, MAX_PAGE_BYTES, deadline);
    
    // Parse Markdown response from Jina Reader
    const title = raw.match(/^Title:\s*(.+)$/im)?.[1]?.trim() || candidate.title;
    const sourceUrl = raw.match(/^URL Source:\s*(\S+)$/im)?.[1]?.trim() || candidate.url;
    const text = textFromMarkdown(raw);
    
    // Check against title-only and bot challenges
    if (text.length < MIN_REAL_CONTENT || isBotChallenge(text) || text === title) return null;
    
    const rel = comparePageToQuery(plan.query, { ...candidate, title, pageContent: text });
    return { ...candidate, title, pageContent: text, extractedText: text, contentStatus: 'reader', contentMethod: 'jina-reader', contentSourceUrl: sourceUrl, contentLength: text.length, contentConfidence: 0.88, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, relevanceAccepted: false, verified: true, verificationMethod: 'jina-reader', contentType: 'text/markdown' };
  } catch { return null; }
}

async function directContent(candidate, plan, deadline) {
  const cache = contentCacheGet(candidate.url); if (cache) return { ...candidate, ...cache };
  if (!safeUrl(candidate.url) || blocked(candidate.url) || left(deadline) < 350) return null;
  
  const directJob = (async () => {
    try {
      const res = await fetchResponse(candidate.url, PAGE_TIMEOUT_MS, deadline);
      const finalUrl = normalizedUrl(res.url || candidate.url) || candidate.url;
      const ct = String(res.headers.get('content-type') || '').toLowerCase();
      if (!safeUrl(finalUrl) || blocked(finalUrl)) return null;
      
      if (/application\/pdf/i.test(ct) || /\.pdf(?:\?|$)/i.test(finalUrl)) {
        const bytes = await readBytes(res, MAX_PAGE_BYTES, deadline);
        const text = await extractPdfText(bytes, deadline).catch(() => '');
        if (text.length < MIN_REAL_CONTENT) return null;
        const rel = comparePageToQuery(plan.query, { ...candidate, url: finalUrl, pageContent: text });
        return { ...candidate, url: finalUrl, domain: host(finalUrl), pageContent: text, extractedText: text, contentStatus: 'full', contentMethod: 'direct-pdf-text', contentSourceUrl: finalUrl, contentLength: text.length, contentConfidence: 0.94, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, verified: Boolean(res.ok), httpStatus: res.status, contentType: ct || 'application/pdf', verificationMethod: 'direct-pdf-text' };
      }
      
      if (!res.ok) return null;
      const body = await readText(res, MAX_PAGE_BYTES, deadline);
      if (/javascript|ecmascript|json|xml|css/i.test(ct) || /^\s*(?:\{|\[|function\s)/.test(body)) return null;
      
      if (/text\/plain/i.test(ct) && body.trim().length >= MIN_REAL_CONTENT) {
        const text = cleanContent(body);
        if (isBotChallenge(text)) return null; // Reject plain text Cloudflare challenges
        const rel = comparePageToQuery(plan.query, { ...candidate, url: finalUrl, pageContent: text });
        const out = { ...candidate, url: finalUrl, title: truncate(candidate.title || 'Text page', 300), domain: host(finalUrl), pageContent: text, extractedText: text, contentStatus: 'full', contentMethod: 'direct-text', contentSourceUrl: finalUrl, contentLength: text.length, contentConfidence: 0.9, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, verified: true, httpStatus: res.status, contentType: ct || 'text/plain', verificationMethod: 'direct-text' };
        contentCacheSet(candidate.url, stripCached(out)); return out;
      }
      
      const title = extractTitle(body) || extractMeta(body, 'og:title') || candidate.title;
      const canonical = extractCanonical(body, finalUrl);
      const usableCanonical = canonical && safeUrl(canonical) && !blocked(canonical) ? canonical : finalUrl;
      
      // Highly robust text extraction using new multi-layered SPA and HTML extraction
      const text = extractArticleText(body);
      
      // Reject if it's a bot challenge, barely any text, or just returned the title
      if (text.length < MIN_REAL_CONTENT || isBotChallenge(text) || text === title) return null;
      
      const rel = comparePageToQuery(plan.query, { ...candidate, url: usableCanonical, title, pageContent: text });
      const publishedAt = extractMeta(body, 'article:published_time') || extractMeta(body, 'datePublished') || ((body.match(/<time[^>]+datetime=["']([^"']+)["']/i) || [])[1] || null);
      const out = { ...candidate, url: usableCanonical, title: truncate(title, 300), snippet: truncate(extractMeta(body, 'description') || candidate.snippet, 1200), publishedAt, domain: host(usableCanonical), pageContent: text, extractedText: text, contentStatus: 'full', contentMethod: ARTICLE_PATH.test(usableCanonical) ? 'direct-html-article' : 'direct-html', contentSourceUrl: usableCanonical, contentLength: text.length, contentConfidence: 0.92, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, verified: true, httpStatus: res.status, contentType: ct || 'text/html', verificationMethod: 'direct-html' };
      
      contentCacheSet(candidate.url, stripCached(out)); 
      return out;
    } catch { return null; }
  })();

  const readerJob = readerContent(candidate, plan, deadline);
  const rows = await Promise.allSettled([directJob, readerJob]);
  const valid = rows.map(x => x.status === 'fulfilled' ? x.value : null).filter(isRealSourceContent);
  if (!valid.length) return null;
  
  valid.sort((a, b) => {
    const qa = (a.relevanceScore || 0) + (a.contentMethod === 'direct-html-article' ? 4 : 0) + (a.contentMethod === 'direct-pdf-text' ? 4 : 0);
    const qb = (b.relevanceScore || 0) + (b.contentMethod === 'direct-html-article' ? 4 : 0) + (b.contentMethod === 'direct-pdf-text' ? 4 : 0);
    return qb - qa || String(b.pageContent || '').length - String(a.pageContent || '').length;
  });
  
  const best = valid[0];
  if (isRealSourceContent(best)) contentCacheSet(candidate.url, stripCached(best));
  return best;
}

function stripCached(v) {
  const keep = {}; for (const k of ['url', 'title', 'snippet', 'publishedAt', 'domain', 'pageContent', 'extractedText', 'contentStatus', 'contentMethod', 'contentSourceUrl', 'contentLength', 'contentConfidence', 'contentTargetMatched', 'contentTitleSimilarity', 'contentConceptCoverage', 'relevanceScore', 'relevanceBand', 'relevance', 'verified', 'httpStatus', 'contentType', 'verificationMethod', 'type', 'source', 'publisherResolved', 'publisherWrapperUrl', 'publisherResolutionMethod']) if (v[k] !== undefined) keep[k] = v[k]; return keep;
}

export function isRealSourceContent(result) {
  const c = String(result?.pageContent || result?.extractedText || '').trim();
  const s = String(result?.contentStatus || '').toLowerCase();
  const m = String(result?.contentMethod || '').toLowerCase();
  if (c.length < MIN_REAL_CONTENT || isBotChallenge(c)) return false;
  if (['snippet', 'search-snippet', 'metadata', 'metadata-fallback'].includes(s) || m === 'search-snippet') return false;
  if (blocked(result?.contentSourceUrl || result?.url)) return false;
  return ['full', 'reader'].includes(s) || /direct-html|direct-pdf|jina-reader/i.test(m);
}

async function mapConcurrent(list, limit, worker) {
  const arr = Array.isArray(list) ? list : []; const out = new Array(arr.length); let cursor = 0; const n = Math.max(1, Math.min(limit, arr.length || 1));
  const runner = async () => { while (true) { const i = cursor++; if (i >= arr.length) return; try { out[i] = await worker(arr[i], i); } catch { out[i] = null; } } };
  await Promise.all(Array.from({ length: n }, runner)); return out;
}

export async function enrichCandidates(candidates, queryOrPlan, options = {}) {
  const started = Date.now();
  const budgetMs = safeInt(options.budgetMs, DEFAULT_BUDGET_MS, 1_800, 16_000);
  const deadline = started + budgetMs;
  const count = safeInt(options.count, 10, 1, MAX_RESULTS);
  const plan = typeof queryOrPlan === 'string' ? analyzeQuery(queryOrPlan, options) : (queryOrPlan || analyzeQuery('', options));
  
  const dedupe = new Map();
  for (const raw of Array.isArray(candidates) ? candidates : []) {
    const url = normalizedUrl(raw?.url || raw?.link || '');
    if (!url || blocked(url) || !typeFits({ ...raw, url }, plan)) continue;
    const key = normalizedKey(url);
    if (!dedupe.has(key)) dedupe.set(key, { ...raw, url, title: truncate(raw?.title || '', 500), snippet: truncate(raw?.snippet || '', 2500), type: raw?.type || 'web' });
  }
  
  const all = [...dedupe.values()].slice(0, MAX_CANDIDATES);
  
  let enriched = (await mapConcurrent(all, CONTENT_CONCURRENCY, c => directContent(c, plan, deadline))).filter(isRealSourceContent);
  
  const enrichedKeys = new Set(enriched.map(x => normalizedKey(x.url)));
  const failed = all.filter(c => !enrichedKeys.has(normalizedKey(c.url)));
  
  if (failed.length && left(deadline) > 650) {
    const recovery = await mapConcurrent(failed.slice(0, Math.min(RECOVERY_CONCURRENCY, failed.length)), RECOVERY_CONCURRENCY, c => readerContent(c, plan, deadline));
    enriched.push(...recovery.filter(isRealSourceContent));
  }
  
  enriched = enriched.map(x => {
    const rel = x.relevance || comparePageToQuery(plan.query, x);
    return { ...x, relevance: rel, relevanceScore: Number(rel?.score || x.relevanceScore || 0), relevanceBand: rel?.band || x.relevanceBand || 'related', contentTargetMatched: true, contentAvailable: true, relevanceAccepted: false };
  });
  
  const uniqueMap = new Map();
  for (const x of enriched) {
    const key = normalizedKey(x.url);
    if (!key) continue;
    const old = uniqueMap.get(key);
    if (!old || Number(x.relevanceScore || 0) > Number(old.relevanceScore || 0)) uniqueMap.set(key, x);
  }
  
  enriched = [...uniqueMap.values()].sort((a, b) => Number(b.relevanceScore || 0) - Number(a.relevanceScore || 0));
  
  return { ok: true, version: VERSION, query: plan.query, requestedResults: count, returnedResults: enriched.length, fetchedSources: enriched.length, latencyMs: Date.now() - started, results: enriched, plan, contentPolicy: 'real-content-only', sourceChecker: 'fetch-all-candidates-then-soft-query-rank', warnings: enriched.length < count ? [`${enriched.length} real-content pages were successfully fetched. Unreachable/unreadable pages were filtered.`] : [] };
}

export async function runAlgorithm(input = {}) {
  const query = truncate(String(input.query || input.q || '').trim(), MAX_QUERY_LEN); if (!query) throw new Error('MISSING_QUERY');
  const count = safeInt(input.count ?? input.limit, 10, 1, MAX_RESULTS); const plan = analyzeQuery(query, input); const candidates = Array.isArray(input.candidates) ? input.candidates : Array.isArray(input.sources) ? input.sources : [];
  return enrichCandidates(candidates, plan, { ...input, count, budgetMs: safeInt(input.budgetMs, DEFAULT_BUDGET_MS, 1_200, 16_000) });
}

function response(body, status = 200) { return new Response(JSON.stringify(body, null, 2), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, POST, OPTIONS', 'access-control-allow-headers': 'content-type, authorization, x-arix-search-key', 'x-arix-algorithm-version': VERSION } }); }

async function readInput(req) { const url = new URL(req.url); if (req.method === 'GET') return Object.fromEntries(url.searchParams.entries()); const raw = await req.text(); if (raw.length > 100000) throw new Error('REQUEST_BODY_TOO_LARGE'); if (!raw) return {}; try { return JSON.parse(raw); } catch { return Object.fromEntries(new URLSearchParams(raw).entries()); } }

export default async function handler(req) { if (req.method === 'OPTIONS') return response({ ok: true, version: VERSION }); if (!['GET', 'POST'].includes(req.method)) return response({ ok: false, version: VERSION, error: 'METHOD_NOT_ALLOWED' }, 405); try { return response(await runAlgorithm(await readInput(req))); } catch (error) { return response({ ok: false, version: VERSION, error: error?.message || 'ALGORITHM_FAILED' }, error?.message === 'MISSING_QUERY' ? 400 : 500); } }

export const ALGORITHM_CONTRACT = Object.freeze({ version: VERSION, maxResults: MAX_RESULTS, realContentMinimumChars: MIN_REAL_CONTENT, defaultBudgetMs: DEFAULT_BUDGET_MS, contentConcurrency: CONTENT_CONCURRENCY, sourceChecker: 'fetch direct publisher and reader content in parallel, extract SPA/Next.js and semantic content, then soft-rank actual page content' });
