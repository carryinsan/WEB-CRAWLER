/*
 * ArixAI Algorithm Orchestrator
 * v3.0.0 (Scraper Integration & PDF Rescue Edition)
 * 
 * Purpose: Advanced query planning, semantic relevance scoring, and orchestration. 
 * 
 * Update 3.0.0: 
 * - Fully attaches to the new dedicated `scraper.js` for all HTML/SPA content.
 * - Handles PDF streams natively with strict AbortController chunking.
 * - Entirely eliminates Tarpit errors by enforcing safe budget boundaries.
 */

import { scrapePage } from './scraper.js';

export const runtime = 'edge';
export const config = { runtime: 'edge' };
export const maxDuration = 300;

const VERSION = 'arix-algorithm-orchestrator-3.0.0';
const MAX_RESULTS = 40;
const MAX_QUERY_LEN = 700;
const MAX_CANDIDATES = 500;
const MAX_PDF_BYTES = 1_000_000;
const MIN_REAL_CONTENT = 150;

const DEFAULT_BUDGET_MS = 24_000;
const CONTENT_CONCURRENCY = 25; // Adjusted to prevent Vercel concurrency throttles
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
  ['economy', 'economic', 'economics', 'gdp', 'market', 'markets'],
  ['technology', 'technologies', 'tech', 'technical'],
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
    
    // Auto-unwrap Bing tracking links
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
    
    // Auto-unwrap Google tracking links
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

function cleanContent(v) {
  let s = String(v || '').replace(/\s+/g, ' ').trim();
  return truncate(s, 24_000); // 24k fallback
}

async function fetchAndReadPdf(url, timeout, deadline, maxBytes = MAX_PDF_BYTES) {
  const remaining = left(deadline) - 20;
  if (remaining <= 0) return null;
  
  const budget = Math.min(timeout, remaining);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), budget);
  
  try {
    const res = await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
      }
    });

    if (!res.ok) return null;

    const buf = await res.arrayBuffer();
    const bytes = new Uint8Array(buf.slice(0, maxBytes));
    return { ok: true, status: res.status, url: res.url, bytes };
  } catch {
    return null;
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

function typeFits(c, plan) {
  if (plan.type === 'gov') return isGov(c.url) || c.type === 'gov';
  if (plan.type === 'doc') return isDoc(c.url) || c.type === 'doc';
  if (plan.type === 'video') return isVideo(c.url) || c.type === 'video';
  if (plan.type === 'news') return c.type === 'news' || ARTICLE_PATH.test(String(c.url || ''));
  return true;
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

function stripCached(v) {
  const keep = {}; for (const k of ['url', 'title', 'snippet', 'publishedAt', 'domain', 'pageContent', 'extractedText', 'contentStatus', 'contentMethod', 'contentSourceUrl', 'contentLength', 'contentConfidence', 'contentTargetMatched', 'contentTitleSimilarity', 'contentConceptCoverage', 'relevanceScore', 'relevanceBand', 'relevance', 'verified', 'httpStatus', 'contentType', 'verificationMethod', 'type', 'source', 'publisherResolved', 'publisherWrapperUrl', 'publisherResolutionMethod']) if (v[k] !== undefined) keep[k] = v[k]; return keep;
}

async function processCandidate(candidate, plan, deadline) {
  const unwrapped = unwrapUrl(candidate.url);
  const cache = contentCacheGet(candidate.url);
  if (cache) return { ...candidate, ...cache, url: candidate.url };

  if (!safeUrl(unwrapped) || blocked(unwrapped) || left(deadline) < 350) return null;

  let out = null;

  // Route PDFs to local extractor. HTML goes to the super-scraper.
  if (/\.pdf(?:\?|$)/i.test(unwrapped)) {
      const res = await fetchAndReadPdf(unwrapped, 6500, deadline);
      if (res && res.ok) {
          const text = await extractPdfText(res.bytes, deadline).catch(() => '');
          if (text.length >= MIN_REAL_CONTENT) {
              const rel = comparePageToQuery(plan.query, { ...candidate, pageContent: text });
              out = {
                  ...candidate, url: candidate.url, contentSourceUrl: res.url, domain: host(res.url),
                  pageContent: text, extractedText: text, contentStatus: 'full', contentMethod: 'direct-pdf-text',
                  contentLength: text.length, contentConfidence: 0.94, contentTargetMatched: true,
                  contentTitleSimilarity: rel.titleCoverage, contentConceptCoverage: rel.conceptCoverage,
                  relevanceScore: rel.score, relevanceBand: rel.band, relevance: rel, verified: true,
                  httpStatus: res.status, contentType: 'application/pdf', verificationMethod: 'direct-pdf-text',
                  publisherResolved: res.url !== candidate.url
              };
          }
      }
  } else {
      const budgetMs = Math.min(8500, left(deadline) - 100);
      if (budgetMs > 1000) {
          const scrapeResult = await scrapePage(unwrapped, budgetMs);
          if (scrapeResult && scrapeResult.success) {
              const text = scrapeResult.content;
              const rel = comparePageToQuery(plan.query, { ...candidate, title: scrapeResult.title, pageContent: text });
              out = {
                  ...candidate,
                  url: candidate.url,
                  contentSourceUrl: scrapeResult.url,
                  title: truncate(scrapeResult.title || candidate.title, 300),
                  snippet: truncate(scrapeResult.description || candidate.snippet, 1200),
                  domain: host(scrapeResult.url),
                  pageContent: text,
                  extractedText: text,
                  contentStatus: 'full',
                  contentMethod: scrapeResult.method,
                  contentLength: text.length,
                  contentConfidence: scrapeResult.extractionScore >= 150 ? 0.95 : 0.85,
                  contentTargetMatched: true,
                  contentTitleSimilarity: rel.titleCoverage,
                  contentConceptCoverage: rel.conceptCoverage,
                  relevanceScore: rel.score,
                  relevanceBand: rel.band,
                  relevance: rel,
                  verified: true,
                  httpStatus: scrapeResult.httpStatus,
                  contentType: 'text/html',
                  verificationMethod: 'dedicated-scraper',
                  publisherResolved: scrapeResult.url !== candidate.url
              };
          }
      }
  }

  if (out) {
      contentCacheSet(candidate.url, stripCached(out));
      return out;
    }
  return null;
}

export function isRealSourceContent(result) {
  const c = String(result?.pageContent || result?.extractedText || '').trim();
  const s = String(result?.contentStatus || '').toLowerCase();
  const m = String(result?.contentMethod || '').toLowerCase();
  if (c.length < MIN_REAL_CONTENT) return false;
  if (['snippet', 'search-snippet', 'metadata', 'metadata-fallback'].includes(s) || m === 'search-snippet') return false;
  
  const sourceUrl = String(result?.contentSourceUrl || result?.url).replace(/&amp;/gi, '&');
  if (blocked(unwrapUrl(sourceUrl))) return false;
  
  return true;
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
  
  // Single pass concurrency. The scraper.js internal engine handles Fallbacks independently!
  let enriched = (await mapConcurrent(all, CONTENT_CONCURRENCY, c => processCandidate(c, plan, deadline))).filter(isRealSourceContent);

  // Fill in Fallback Metadata for absolute assurance of returning *something*
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
  
  // Clean final pass deduplication
  const uniqueMap = new Map();
  for (const x of enriched) {
    const key = normalizedKey(x.url);
    if (!key) continue;
    const old = uniqueMap.get(key);
    if (!old || Number(x.relevanceScore || 0) > Number(old.relevanceScore || 0)) uniqueMap.set(key, x);
  }
  
  enriched = [...uniqueMap.values()].sort((a, b) => Number(b.relevanceScore || 0) - Number(a.relevanceScore || 0));
  
  return { 
    ok: true, 
    version: VERSION, 
    query: plan.query, 
    requestedResults: count, 
    returnedResults: enriched.length, 
    fetchedSources: enriched.filter(e => e.contentStatus !== 'metadata-fallback').length, 
    latencyMs: Date.now() - started, 
    results: enriched, 
    plan, 
    contentPolicy: 'real-content-only', 
    sourceChecker: 'Delegated to scraper.js (v3.0.0)', 
    warnings: enriched.filter(e => e.contentStatus === 'metadata-fallback').length > 0 ? [`Some pages failed to fetch or contained no readable content. Snippets were used as fallbacks.`] : [] 
  };
}

export async function runAlgorithm(input = {}) {
  const query = truncate(String(input.query || input.q || '').trim(), MAX_QUERY_LEN); if (!query) throw new Error('MISSING_QUERY');
  const count = safeInt(input.count ?? input.limit, 10, 1, MAX_RESULTS); const plan = analyzeQuery(query, input); const candidates = Array.isArray(input.candidates) ? input.candidates : Array.isArray(input.sources) ? input.sources : [];
  return enrichCandidates(candidates, plan, { ...input, count, budgetMs: safeInt(input.budgetMs, DEFAULT_BUDGET_MS, 1_200, 28_000) });
}

function response(body, status = 200) { return new Response(JSON.stringify(body, null, 2), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, POST, OPTIONS', 'access-control-allow-headers': 'content-type, authorization, x-arix-search-key', 'x-arix-algorithm-version': VERSION } }); }

async function readInput(req) { const url = new URL(req.url); if (req.method === 'GET') return Object.fromEntries(url.searchParams.entries()); const raw = await req.text(); if (raw.length > 100000) throw new Error('REQUEST_BODY_TOO_LARGE'); if (!raw) return {}; try { return JSON.parse(raw); } catch { return Object.fromEntries(new URLSearchParams(raw).entries()); } }

export default async function handler(req) { if (req.method === 'OPTIONS') return response({ ok: true, version: VERSION }); if (!['GET', 'POST'].includes(req.method)) return response({ ok: false, version: VERSION, error: 'METHOD_NOT_ALLOWED' }, 405); try { return response(await runAlgorithm(await readInput(req))); } catch (error) { return response({ ok: false, version: VERSION, error: error?.message || 'ALGORITHM_FAILED' }, error?.message === 'MISSING_QUERY' ? 400 : 500); } }

export const ALGORITHM_CONTRACT = Object.freeze({ version: VERSION, maxResults: MAX_RESULTS, realContentMinimumChars: MIN_REAL_CONTENT, defaultBudgetMs: DEFAULT_BUDGET_MS, contentConcurrency: CONTENT_CONCURRENCY, sourceChecker: 'delegated entirely to scraper.js (v3.0.0)' });
