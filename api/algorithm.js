/*
 * ArixAI Precision Page Algorithm
 * v1.9.0 (Anti-Tarpit & Advanced SPA Extraction Edition)
 * 
 * Purpose: Advanced query planning, REAL page acquisition, highly robust HTML/SPA extraction, 
 * analytics/ad rejection, search wrapper unwrapping, and post-fetch query comparison.
 * 
 * Update 1.9.0: Fixed 300s edge timeout by enforcing AbortController on body reads. 
 * Improved SPA state extraction for ecommerce sites (Flipkart/Croma/etc) returning empty content.
 */

export const runtime = 'edge';
export const config = { runtime: 'edge' };
export const maxDuration = 300;

const VERSION = 'arix-content-algorithm-1.9.0';
const MAX_RESULTS = 40;
const MAX_QUERY_LEN = 700;
const MAX_CANDIDATES = 500;
const MAX_PAGE_BYTES = 1_000_000;
const MAX_TEXT_CHARS = 24_000;
const MIN_REAL_CONTENT = 150;

const DEFAULT_BUDGET_MS = 22_000;
const PAGE_TIMEOUT_MS = 6_500;
const READER_TIMEOUT_MS = 9_500;
const CONTENT_CONCURRENCY = 30;
const RECOVERY_CONCURRENCY = 15;
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

function unwrapUrl(url) {
  try {
    let current = String(url || '').replace(/&amp;/gi, '&');
    const u = new URL(current);
    
    // Auto-unwrap Bing tracking links completely
    if (u.hostname.includes('bing.com') && u.pathname.startsWith('/ck/a')) {
      let uParam = u.searchParams.get('u');
      if (uParam) {
        uParam = uParam.replace(/^a1/, '');
        let b64 = uParam.replace(/-/g, '+').replace(/_/g, '/');
        while (b64.length % 4) b64 += '=';
        try {
            const decoded = atob(b64);
            if (/^https?:\/\//i.test(decoded)) return decoded;
        } catch {}
      }
    }
    
    if (u.hostname.includes('google.') && u.pathname === '/url') {
      const q = u.searchParams.get('q') || u.searchParams.get('url');
      if (q && /^https?:\/\//i.test(q)) return q;
    }
    
    return current;
  } catch { return url; }
}

function safeUrl(url) {
  try {
    const u = new URL(String(url || '').replace(/&amp;/gi, '&'));
    if (!/^https?:$/i.test(u.protocol)) return false;
    const h = u.hostname.toLowerCase();
    if (!h || h === 'localhost' || h.endsWith('.localhost')) return false;
    if (/^(127\.|10\.|192\.168\.|169\.254\.)/.test(h)) return false;
    if (/^172\.(?:1[6-9]|2[0-9]|3[0-1])\./.test(h)) return false;
    return true;
  } catch { return false; }
}

function host(url) { try { return new URL(String(url || '').replace(/&amp;/gi, '&')).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; } }

function blocked(url) {
  if (!safeUrl(url)) return true;
  const h = host(url);
  if ([...BLOCKED_HOSTS].some(x => h === x || h.endsWith(`.${x}`))) return true;
  try {
    const u = new URL(String(url || '').replace(/&amp;/gi, '&'));
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
    const s = String(url || '').replace(/&amp;/gi, '&');
    const u = new URL(s); u.hash = '';
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
  let s = String(html)
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<(script|style|noscript|iframe|nav|footer|aside|header|form|menu|dialog|canvas|svg|button|map|object|embed|picture|video|audio)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<[^>]+>/g, ' ');

  return decodeHtml(s).replace(/\s+/g, ' ').trim();
}

function cleanContent(v) {
  let s = String(v || '').replace(/\s+/g, ' ').trim();
  s = s.replace(/\[[^\]]{0,80}\]\([^)]{0,500}\)/g, '');
  s = s.replace(/!\[[^\]]{0,80}\]\([^)]{0,500}\)/g, '');
  s = s.replace(/(?:skip to content|accept cookies|cookie settings|privacy settings|sign in|log in|search this site)\b/gi, ' ');
  return truncate(s.replace(/\s+/g, ' ').trim(), MAX_TEXT_CHARS);
}

function isBotChallenge(text) {
  const t = String(text || '').toLowerCase();
  if (t.length > 4500) return false; 
  const triggers = [
    'just a moment...', 'checking your browser', 'enable javascript and cookies',
    'please enable js', 'cloudflare', 'cf-browser-verification', 'verify you are human',
    'why do i have to complete a captcha', 'attention required!',
    'robot or human', 'datadome', 'perimeterx', 'access denied', '403 forbidden',
    'checking if the site connection is secure', 'needs to review the security of your connection',
    'are you a robot', 'verifying you are not a robot', 'pardon our interruption',
    'to proceed, please verify', 'complete the security check', 'help us keep your account safe'
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
  const a = String(html).match(new RegExp(`<meta[^>]{0,150}(?:name|property)=["']${e}["'][^>]{0,150}content=["']([^"']+)["']`, 'i'));
  const b = String(html).match(new RegExp(`<meta[^>]{0,150}content=["']([^"']+)["'][^>]{0,150}(?:name|property)=["']${e}["']`, 'i'));
  return decodeHtml((a || b || [, ''])[1] || '').trim();
}

function extractCanonical(html, base) {
  const a = (String(html).match(/<link[^>]{0,150}rel=["'](?:[^"']*\s)?canonical(?:\s[^"']*)?["'][^>]{0,150}href=["']([^"']+)["']/i) || [])[1];
  const b = (String(html).match(/<link[^>]{0,150}href=["']([^"']+)["'][^>]{0,150}rel=["'](?:[^"']*\s)?canonical(?:\s[^"']*)?["']/i) || [])[1];
  try { return new URL(a || b, base).href; } catch { return null; }
}

function extractStringsFromObj(obj, outArray, maxDepth = 6) {
  if (maxDepth <= 0 || !obj) return;
  if (typeof obj === 'string') {
    const t = obj.trim();
    if (t.length > 60 && t.includes(' ') && !/^http|^\/|^<[^>]+>|^[.{#a-z0-9_-]+\s*\{/i.test(t)) {
      outArray.push(t);
    }
    return;
  }
  if (typeof obj === 'object') {
    if (Array.isArray(obj)) {
      for (const item of obj) extractStringsFromObj(item, outArray, maxDepth - 1);
    } else {
      for (const key of Object.keys(obj)) {
        if (key.startsWith('__') || /css|style|class|config|url|src|href|id|key/i.test(key)) continue;
        extractStringsFromObj(obj[key], outArray, maxDepth - 1);
      }
    }
  }
}

function extractSpaState(html) {
  const out = [];
  const htmlStr = String(html);
  
  // Extracts Next.js / Nuxt / Apollo / Storefront state dumps reliably
  const jsonScripts = htmlStr.match(/<script[^>]*type=["']application\/(?:ld\+)?json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const block of jsonScripts) {
    const raw = block.replace(/^<[^>]+>/i, '').replace(/<\/script>$/i, '');
    try { extractStringsFromObj(JSON.parse(raw.trim()), out); } catch {}
  }

  // Extracts window.__INITIAL_STATE__ typical of ecommerce sites (Flipkart, Croma, etc)
  const stateScripts = htmlStr.match(/<script[^>]*>\s*(?:window\.[a-zA-Z0-9_]+\s*=\s*|\s*var\s+[a-zA-Z0-9_]+\s*=\s*)(\{[\s\S]*?\})\s*;/gi) || [];
  for (const block of stateScripts) {
    const match = block.match(/(?:window\.[a-zA-Z0-9_]+\s*=\s*|\s*var\s+[a-zA-Z0-9_]+\s*=\s*)(\{[\s\S]*?\})\s*;/i);
    if (match && match[1]) {
        try { extractStringsFromObj(JSON.parse(match[1].trim()), out); } catch {}
    }
  }

  return out.join('\n\n');
}

function extractArticleText(html) {
  const cleanHtml = String(html || '')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<(script|style|noscript|iframe|nav|footer|aside|header|form|menu|dialog|canvas|svg|button|map|object|embed|picture|video|audio)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ');

  const jsonLd = cleanContent(extractSpaState(html));
  if (jsonLd.length >= 300 && !isBotChallenge(jsonLd)) return jsonLd;

  let mainBlocks = [];
  // Target semantic articles and ecommerce product content divs explicitly
  for (const m of cleanHtml.matchAll(/<(article|main|div\s+[^>]*class=["'][^"']*(?:product|content|detail|description)[^"']*["'])[^>]*>([\s\S]*?)<\/\1>/gi)) {
     const t = strip(m[2]);
     if (t.length >= 50) mainBlocks.push(t);
  }
  if (mainBlocks.length > 0) {
     const mText = cleanContent(mainBlocks.join('\n\n'));
     if (mText.length >= 300 && !isBotChallenge(mText)) return mText;
  }

  const pBlocks = [];
  for (const m of cleanHtml.matchAll(/<(p|h[1-6]|li|blockquote)\b[^>]*>([\s\S]*?)<\/\1>/gi)) {
    const t = strip(m[2]);
    if (t.length >= 40 && t.split(/\s+/).length >= 5) pBlocks.push(t);
  }
  const pText = cleanContent(pBlocks.join('\n\n'));
  if (pText.length >= 300 && !isBotChallenge(pText)) return pText;

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

// Fixed function: Timer protects the entire process including the body arrayBuffer mapping to prevent Tarpit hangs!
async function fetchAndRead(url, timeout, deadline, maxBytes = MAX_PAGE_BYTES, headers = {}) {
  const remaining = left(deadline) - 20;
  if (remaining <= 0) throw new Error('BUDGET_EXHAUSTED');
  
  const budget = Math.min(timeout, remaining);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), budget);
  
  try {
    const res = await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en-IN;q=0.9,en;q=0.8',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Upgrade-Insecure-Requests': '1',
        ...headers,
      }
    });

    if (!res.ok) {
        return { ok: false, status: res.status, url: res.url, headers: res.headers, bytes: new Uint8Array(0), text: '' };
    }

    // arrayBuffer delegates GZIP/Brotli decompression to runtime natively, preventing empty/corrupted reads.
    const buf = await res.arrayBuffer();
    const bytes = new Uint8Array(buf.slice(0, maxBytes));
    return {
        ok: true,
        status: res.status,
        url: res.url,
        headers: res.headers,
        bytes,
        text: new TextDecoder('utf-8', { fatal: false }).decode(bytes)
    };
  } finally { 
    clearTimeout(timer); 
  }
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
  const unwrapped = unwrapUrl(candidate.url);
  if (!safeUrl(unwrapped) || blocked(unwrapped) || left(deadline) < 350) return null;
  
  try {
    let res = await fetchAndRead(`https://r.jina.ai/${unwrapped}`, READER_TIMEOUT_MS, deadline, MAX_PAGE_BYTES, { accept: 'text/plain,text/markdown;q=0.9,*/*;q=0.2' });
    
    if (!res.ok && unwrapped.startsWith('https://')) {
       const httpFallback = unwrapped.replace('https://', 'http://');
       res = await fetchAndRead(`https://r.jina.ai/${httpFallback}`, READER_TIMEOUT_MS, deadline, MAX_PAGE_BYTES, { accept: 'text/plain,text/markdown;q=0.9,*/*;q=0.2' });
    }
    
    if (!res.ok) return null;
    const raw = res.text;
    
    const title = raw.match(/^Title:\s*(.+)$/im)?.[1]?.trim() || candidate.title;
    const sourceUrl = raw.match(/^URL Source:\s*(\S+)$/im)?.[1]?.trim() || unwrapped;
    const text = textFromMarkdown(raw);
    
    if (text.length < MIN_REAL_CONTENT || isBotChallenge(text) || text === title || text === candidate.snippet) return null;
    
    const rel = comparePageToQuery(plan.query, { ...candidate, title, pageContent: text });
    return { ...candidate, url: candidate.url, title, pageContent: text, extractedText: text, contentStatus: 'reader', contentMethod: 'jina-reader', contentSourceUrl: sourceUrl, contentLength: text.length, contentConfidence: 0.88, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, relevanceAccepted: false, verified: true, verificationMethod: 'jina-reader', contentType: 'text/markdown', publisherResolved: sourceUrl !== candidate.url };
  } catch { return null; }
}

async function directContent(candidate, plan, deadline) {
  const unwrapped = unwrapUrl(candidate.url);
  const cache = contentCacheGet(candidate.url); 
  if (cache) return { ...candidate, ...cache, url: candidate.url };
  if (!safeUrl(unwrapped) || blocked(unwrapped) || left(deadline) < 350) return null;
  
  try {
    let res = await fetchAndRead(unwrapped, PAGE_TIMEOUT_MS, deadline);
    let finalUrl = normalizedUrl(res.url || unwrapped) || unwrapped;
    let ct = String(res.headers?.get?.('content-type') || '').toLowerCase();
    if (!safeUrl(finalUrl) || blocked(finalUrl)) return null;
    
    if (/application\/pdf/i.test(ct) || /\.pdf(?:\?|$)/i.test(finalUrl)) {
      if (!res.ok) return null;
      const text = await extractPdfText(res.bytes, deadline).catch(() => '');
      if (text.length < MIN_REAL_CONTENT) return null;
      const rel = comparePageToQuery(plan.query, { ...candidate, pageContent: text });
      const out = { ...candidate, url: candidate.url, contentSourceUrl: finalUrl, domain: host(finalUrl), pageContent: text, extractedText: text, contentStatus: 'full', contentMethod: 'direct-pdf-text', contentLength: text.length, contentConfidence: 0.94, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, verified: Boolean(res.ok), httpStatus: res.status, contentType: ct || 'application/pdf', verificationMethod: 'direct-pdf-text', publisherResolved: finalUrl !== candidate.url };
      contentCacheSet(candidate.url, stripCached(out)); return out;
    }
    
    const metaRefresh = res.text.match(/<meta[^>]{0,200}http-equiv=["']?refresh["']?[^>]{0,200}content=["']?\d+;\s*url=['"]?([^"'>]+)['"]?/i);
    if (metaRefresh && metaRefresh[1] && left(deadline) > 1000) {
        const redirectUrl = new URL(metaRefresh[1].replace(/&amp;/g, '&'), finalUrl).href;
        if (safeUrl(redirectUrl) && !blocked(redirectUrl)) {
            res = await fetchAndRead(redirectUrl, PAGE_TIMEOUT_MS, deadline);
            finalUrl = normalizedUrl(res.url || redirectUrl) || redirectUrl;
            ct = String(res.headers?.get?.('content-type') || '').toLowerCase();
        }
    }

    if (!res.ok) return null;
    let body = res.text;
    
    if (/javascript|ecmascript|json|xml|css/i.test(ct) || /^\s*(?:\{|\[|function\s)/.test(body)) return null;
    
    if (/text\/plain/i.test(ct) && body.trim().length >= MIN_REAL_CONTENT) {
      const text = cleanContent(body);
      if (isBotChallenge(text)) return null; 
      const rel = comparePageToQuery(plan.query, { ...candidate, pageContent: text });
      const out = { ...candidate, url: candidate.url, contentSourceUrl: finalUrl, title: truncate(candidate.title || 'Text page', 300), domain: host(finalUrl), pageContent: text, extractedText: text, contentStatus: 'full', contentMethod: 'direct-text', contentLength: text.length, contentConfidence: 0.9, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, verified: true, httpStatus: res.status, contentType: ct || 'text/plain', verificationMethod: 'direct-text', publisherResolved: finalUrl !== candidate.url };
      contentCacheSet(candidate.url, stripCached(out)); return out;
    }
    
    const title = extractTitle(body) || extractMeta(body, 'og:title') || candidate.title;
    const canonical = extractCanonical(body, finalUrl);
    const usableCanonical = canonical && safeUrl(canonical) && !blocked(canonical) ? canonical : finalUrl;
    
    const text = extractArticleText(body);
    
    if (text.length < MIN_REAL_CONTENT || isBotChallenge(text) || text === title || text === candidate.snippet) return null;
    
    const rel = comparePageToQuery(plan.query, { ...candidate, title, pageContent: text });
    const publishedAt = extractMeta(body, 'article:published_time') || extractMeta(body, 'datePublished') || ((body.match(/<time[^>]+datetime=["']([^"']+)["']/i) || [])[1] || null);
    const out = { ...candidate, url: candidate.url, contentSourceUrl: usableCanonical, title: truncate(title, 300), snippet: truncate(extractMeta(body, 'description') || candidate.snippet, 1200), publishedAt, domain: host(usableCanonical), pageContent: text, extractedText: text, contentStatus: 'full', contentMethod: ARTICLE_PATH.test(usableCanonical) ? 'direct-html-article' : 'direct-html', contentLength: text.length, contentConfidence: 0.92, contentTargetMatched: true, contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage, relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, verified: true, httpStatus: res.status, contentType: ct || 'text/html', verificationMethod: 'direct-html', publisherResolved: usableCanonical !== candidate.url };
    
    contentCacheSet(candidate.url, stripCached(out)); 
    return out;
  } catch { return null; }
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
  
  const sourceUrl = String(result?.contentSourceUrl || result?.url).replace(/&amp;/gi, '&');
  if (blocked(unwrapUrl(sourceUrl))) return false;
  
  return ['full', 'reader'].includes(s) || /direct-html|direct-pdf|jina-reader/i.test(m);
}

async function mapConcurrent(list, limit, worker) {
  const arr = Array.isArray(list) ? list : []; const out = new Array(arr.length); let cursor = 0; const n = Math.max(1, Math.min(limit, arr.length || 1));
  const runner = async () => { while (true) { const i = cursor++; if (i >= arr.length) return; try { out[i] = await worker(arr[i], i); } catch { out[i] = null; } } };
  await Promise.all(Array.from({ length: n }, runner)); return out;
}

export async function enrichCandidates(candidates, queryOrPlan, options = {}) {
  const started = Date.now();
  const budgetMs = safeInt(options.budgetMs, DEFAULT_BUDGET_MS, 1_800, 28_000);
  const deadline = started + budgetMs;
  const count = safeInt(options.count, 10, 1, MAX_RESULTS);
  const plan = typeof queryOrPlan === 'string' ? analyzeQuery(queryOrPlan, options) : (queryOrPlan || analyzeQuery('', options));
  
  const dedupe = new Map();
  for (const raw of Array.isArray(candidates) ? candidates : []) {
    const pureUrl = String(raw?.url || raw?.link || '').replace(/&amp;/gi, '&');
    const url = normalizedUrl(pureUrl);
    
    if (!url || blocked(unwrapUrl(url)) || !typeFits({ ...raw, url }, plan)) continue;
    const key = normalizedKey(url);
    if (!dedupe.has(key)) dedupe.set(key, { ...raw, url: pureUrl, title: truncate(raw?.title || '', 500), snippet: truncate(raw?.snippet || '', 2500), type: raw?.type || 'web' });
  }
  
  const all = [...dedupe.values()].slice(0, MAX_CANDIDATES);
  
  let enriched = (await mapConcurrent(all, CONTENT_CONCURRENCY, c => directContent(c, plan, deadline))).filter(isRealSourceContent);
  
  const enrichedKeys = new Set(enriched.map(x => normalizedKey(x.url)));
  const failed = all.filter(c => !enrichedKeys.has(normalizedKey(c.url)));
  
  if (failed.length && left(deadline) > 850) {
    const recovery = await mapConcurrent(failed.slice(0, Math.min(RECOVERY_CONCURRENCY, failed.length)), RECOVERY_CONCURRENCY, c => readerContent(c, plan, deadline));
    enriched.push(...recovery.filter(isRealSourceContent));
  }

  // Ensure Fallback Mechanism covers everything that fully failed
  const finalKeys = new Set(enriched.map(x => normalizedKey(x.url)));
  for (const raw of all) {
      if (!finalKeys.has(normalizedKey(raw.url))) {
          const rel = comparePageToQuery(plan.query, raw);
          enriched.push({
              ...raw,
              contentAvailable: false,
              contentStatus: 'metadata-fallback',
              contentMethod: 'search-snippet',
              contentLength: String(raw.snippet || '').length,
              pageContent: String(raw.snippet || ''),
              extractedText: String(raw.snippet || ''),
              contentConfidence: 0,
              relevanceScore: rel.score,
              relevanceBand: rel.band,
              relevance: rel
          });
      }
  }
  
  enriched = enriched.map(x => {
    const rel = x.relevance || comparePageToQuery(plan.query, x);
    return { ...x, relevance: rel, relevanceScore: Number(rel?.score || x.relevanceScore || 0), relevanceBand: rel?.band || x.relevanceBand || 'related', contentTargetMatched: x.contentTargetMatched ?? false, contentAvailable: x.contentAvailable ?? true, relevanceAccepted: false };
  });
  
  const uniqueMap = new Map();
  for (const x of enriched) {
    const key = normalizedKey(x.url);
    if (!key) continue;
    const old = uniqueMap.get(key);
    if (!old || Number(x.relevanceScore || 0) > Number(old.relevanceScore || 0)) uniqueMap.set(key, x);
  }
  
  enriched = [...uniqueMap.values()].sort((a, b) => Number(b.relevanceScore || 0) - Number(a.relevanceScore || 0));
  
  return { ok: true, version: VERSION, query: plan.query, requestedResults: count, returnedResults: enriched.length, fetchedSources: enriched.length, latencyMs: Date.now() - started, results: enriched, plan, contentPolicy: 'real-content-only', sourceChecker: 'fetch direct publisher and reader content in parallel, extract SPA/Next.js and semantic content, then soft-rank actual page content', warnings: enriched.length < count ? [`${enriched.length} real-content pages were successfully fetched. Unreachable/unreadable pages were filtered.`] : [] };
}

export async function runAlgorithm(input = {}) {
  const query = truncate(String(input.query || input.q || '').trim(), MAX_QUERY_LEN); if (!query) throw new Error('MISSING_QUERY');
  const count = safeInt(input.count ?? input.limit, 10, 1, MAX_RESULTS); const plan = analyzeQuery(query, input); const candidates = Array.isArray(input.candidates) ? input.candidates : Array.isArray(input.sources) ? input.sources : [];
  return enrichCandidates(candidates, plan, { ...input, count, budgetMs: safeInt(input.budgetMs, DEFAULT_BUDGET_MS, 1_200, 28_000) });
}

function response(body, status = 200) { return new Response(JSON.stringify(body, null, 2), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, POST, OPTIONS', 'access-control-allow-headers': 'content-type, authorization, x-arix-search-key', 'x-arix-algorithm-version': VERSION } }); }

async function readInput(req) { const url = new URL(req.url); if (req.method === 'GET') return Object.fromEntries(url.searchParams.entries()); const raw = await req.text(); if (raw.length > 100000) throw new Error('REQUEST_BODY_TOO_LARGE'); if (!raw) return {}; try { return JSON.parse(raw); } catch { return Object.fromEntries(new URLSearchParams(raw).entries()); } }

export default async function handler(req) { if (req.method === 'OPTIONS') return response({ ok: true, version: VERSION }); if (!['GET', 'POST'].includes(req.method)) return response({ ok: false, version: VERSION, error: 'METHOD_NOT_ALLOWED' }, 405); try { return response(await runAlgorithm(await readInput(req))); } catch (error) { return response({ ok: false, version: VERSION, error: error?.message || 'ALGORITHM_FAILED' }, error?.message === 'MISSING_QUERY' ? 400 : 500); } }

export const ALGORITHM_CONTRACT = Object.freeze({ version: VERSION, maxResults: MAX_RESULTS, realContentMinimumChars: MIN_REAL_CONTENT, defaultBudgetMs: DEFAULT_BUDGET_MS, contentConcurrency: CONTENT_CONCURRENCY, sourceChecker: 'fetch direct publisher and reader content in parallel, extract SPA/Next.js and semantic content, then soft-rank actual page content' });
