/*
 * ArixAI Live Web Search / Crawler
 * v2.0.0 — Standalone Search Engine Fusion
 *
 * Vercel Edge / dependency-free.
 * No local helper dependency or page-content extractor.
 *
 * Design goals:
 * - High-recall discovery from multiple public search surfaces.
 * - Precision-first fusion/ranking without throwing away valid candidates too early.
 * - Fast parallel requests with bounded concurrency and per-request timeouts.
 * - Optional lightweight verification of top URLs; no page-content extractor.
 * - Search snippets remain snippets; they are never mislabeled as page content.
 * - Conservative SSRF protection without over-blocking ordinary public sources.
 */

export const runtime = 'edge';
export const config = { runtime: 'edge' };
export const maxDuration = 300;

const VERSION = 'arix-crawler-2.0.0';
const MAX_RESULTS = 40;
const DEFAULT_RESULTS = 10;
const MAX_QUERY_LEN = 700;
const MAX_REQUEST_BODY = 100_000;

const SEARCH_BUDGET_MS = 9_500;
const SEARCH_TIMEOUT_MS = 1_250;
const VERIFY_TIMEOUT_MS = 900;
const VERIFY_HEADROOM_MS = 140;
const DISCOVERY_CUTOFF_MS = 3_400;
const SEARCH_CONCURRENCY = 24;
const VERIFY_CONCURRENCY = 18;
const MAX_ENGINE_REQUESTS = 24;
const MAX_DISCOVERY_RESULTS = 500;
const MAX_LIVE_LOG = 80;
const OUTPUT_MAX_SOURCES = 500;
const CACHE_MAX = 120;
const CACHE_TTL_MS = 7_000;
const LIVE_CACHE_TTL_MS = 2_500;
const CACHE = new Map();

const COMMON_CRAWL_TIMEOUT_MS = 650;
const MAX_COMMON_CRAWL = 4;
const COMMON_CRAWL_INDEXES = ['CC-MAIN-2026-34', 'CC-MAIN-2026-30'];

const AI_RERANK_TIMEOUT_MS = 850;

const USER_AGENT = 'Mozilla/5.0 (compatible; ArixAI-LiveSearch/2.0.0; +https://lexis-ai-chatini.vercel.app/)';

/* Only obvious tracking / measurement surfaces are blocked. Ordinary public sites,
 * PDFs, JS documentation, forums, media pages, etc. are not blanket-blocked. */
const BLOCKED_HOSTS = new Set([
  'google-analytics.com', 'googletagmanager.com', 'googlesyndication.com',
  'googleadservices.com', 'doubleclick.net', 'scorecardresearch.com',
  'pixel.wp.com', 'adsrvr.org', 'amazon-adsystem.com', 'taboola.com',
  'outbrain.com', 'segment.io', 'hotjar.com', 'clarity.ms',
]);

const TRACKING_PATH = /(?:^|[\/_-])(?:analytics|gtag|ga4|collect|pixel|beacon|tracking|tracker|telemetry)(?:[\/_-]|$)/i;
const SEARCH_HOSTS = new Set([
  'bing.com', 'www.bing.com', 'google.com', 'www.google.com', 'google.co.in',
  'duckduckgo.com', 'html.duckduckgo.com', 'search.yahoo.com', 'yahoo.com',
  'mojeek.com', 'www.mojeek.com', 'news.google.com', 'youtube.com', 'www.youtube.com'
]);

const GOV_DOMAINS = [
  'gov.in', 'nic.in', 'mygov.in', 'india.gov.in', 'pib.gov.in', 'mca.gov.in',
  'gst.gov.in', 'incometax.gov.in', 'msme.gov.in', 'education.gov.in', 'meity.gov.in',
  'rbi.org.in', 'sebi.gov.in', 'supremecourt.gov.in', 'indiacode.nic.in'
];

const TRUSTED_DOMAINS = [
  'who.int', 'un.org', 'europa.eu', 'nasa.gov', 'oecd.org', 'worldbank.org',
  'imf.org', 'ietf.org', 'w3.org', 'mozilla.org', 'developer.mozilla.org',
  'nih.gov', 'cdc.gov', 'mit.edu', 'stanford.edu', 'harvard.edu'
];

const STOPWORDS = new Set([
  'a','an','the','and','or','of','to','in','on','for','with','from','by','as','at',
  'is','are','was','were','be','been','being','this','that','these','those','what',
  'when','where','how','why','who','which','about','into','near','over','under','than',
  'then','during','through','latest','current','recent','today','news','update','updates',
  'please','show','find','give','tell','me','can','you','i','we','it','its','their','our',
  'your','my','more','information','info','details','best','all','some','does','do','did',
  'explain','explained','need','want','using','use','from','vs','versus','compare','comparison'
]);

const SYNONYMS = [
  ['car','cars','automobile','automobiles','motorcar','motorcars','vehicle','vehicles'],
  ['history','historical','timeline','timelines','origins','origin','evolution','development','heritage'],
  ['india','indian'],
  ['price','prices','cost','costs','rate','rates','pricing','priced'],
  ['law','laws','legal','legislation','act','acts','regulation','regulations','rule','rules'],
  ['policy','policies','framework','initiative','initiatives','programme','program','programs'],
  ['company','companies','firm','firms','business','businesses','corporation','corporations'],
  ['founder','founders','created','creator','cofounder','co-founder','originator'],
  ['population','people','residents','inhabitants','demographics'],
  ['economy','economic','economics','gdp','market','markets'],
  ['education','school','schools','student','students','curriculum','syllabus'],
  ['research','study','studies','paper','papers','report','reports','analysis'],
  ['technology','technologies','tech','technical'],
  ['electric','ev','electricity','battery','battery-powered'],
  ['manufacturing','manufacture','production','factory','factories'],
  ['guide','guides','tutorial','tutorials','manual','documentation','docs'],
  ['ai','artificial-intelligence','artificial intelligence','machine-learning','machine learning'],
];
const SYNONYM_MAP = new Map();
for (const group of SYNONYMS) for (const term of group) SYNONYM_MAP.set(term, group[0]);

function nowIso() { return new Date().toISOString(); }
function left(deadline) { return Math.max(0, deadline - Date.now()); }
function clamp(v, min, max) { return Math.min(max, Math.max(min, Number(v) || 0)); }
function truncate(v, max) { const s = String(v ?? '').trim(); return s.length <= max ? s : `${s.slice(0, max - 1)}…`; }
function encode(v) { return JSON.stringify(v); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function env(name) { try { return typeof process !== 'undefined' ? String(process.env?.[name] || '').trim() : ''; } catch { return ''; } }
function safeInt(v, fallback, min, max) { const n = Number.parseInt(v, 10); return Number.isFinite(n) ? clamp(n, min, max) : fallback; }
function unique(arr) { return [...new Set((arr || []).filter(Boolean))]; }

function normalizeText(v) {
  return String(v || '').normalize('NFKC').toLowerCase()
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ').trim();
}
function rawTokens(v) { return normalizeText(v).split(' ').filter(Boolean); }
function canonicalToken(v) {
  const x = String(v || '').toLowerCase();
  return SYNONYM_MAP.get(x) || x;
}
function stem(x) {
  let t = String(x || '').toLowerCase();
  if (t.length <= 4) return t;
  t = t.replace(/(ings|ies|ied)$/i, m => m[0].toLowerCase() === 'i' ? 'y' : '');
  t = t.replace(/(ing|ed|es)$/i, '');
  if (t.endsWith('s') && !t.endsWith('ss')) t = t.slice(0, -1);
  return t;
}
function contentTokens(v) {
  return rawTokens(v).filter(x => x.length > 1 && !STOPWORDS.has(x)).map(x => canonicalToken(x));
}
function tokenSet(v) { return new Set(contentTokens(v)); }

function normalizeHost(url) {
  try { return new URL(url).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; }
}
function safeUrl(url) {
  try {
    const u = new URL(String(url));
    if (!/^https?:$/i.test(u.protocol)) return false;
    const h = u.hostname.toLowerCase();
    if (!h || h === 'localhost' || h.endsWith('.localhost')) return false;
    if (/^(127\.|10\.|192\.168\.|169\.254\.)/.test(h)) return false;
    if (/^172\.(?:1[6-9]|2\d|3[0-1])\./.test(h)) return false;
    if (h === '::1' || h.startsWith('fc') || h.startsWith('fd')) return false;
    return true;
  } catch { return false; }
}
function blocked(url) {
  if (!safeUrl(url)) return true;
  const h = normalizeHost(url);
  if ([...BLOCKED_HOSTS].some(x => h === x || h.endsWith(`.${x}`))) return true;
  try { return TRACKING_PATH.test(new URL(url).pathname); } catch { return true; }
}
function isGov(url) { const h = normalizeHost(url); return GOV_DOMAINS.some(d => h === d || h.endsWith(`.${d}`)); }
function isTrusted(url) {
  const h = normalizeHost(url);
  return TRUSTED_DOMAINS.some(d => h === d || h.endsWith(`.${d}`)) || /\.edu(?:\.|$)/i.test(h) || /\.ac\.(?:in|uk|jp|nz)$/i.test(h);
}
function isYouTube(url) { const h = normalizeHost(url); return h === 'youtube.com' || h.endsWith('.youtube.com') || h === 'youtu.be'; }
function isVideo(url) { return isYouTube(url) || /(?:vimeo\.com|dailymotion\.com)$/i.test(normalizeHost(url)); }
function isDoc(url) { return /\.(?:pdf|docx?|xlsx?|pptx?|csv|txt)(?:[?#]|$)/i.test(String(url || '')); }
function isLikelySearchUrl(url) {
  const h = normalizeHost(url);
  try {
    const u = new URL(url);
    return SEARCH_HOSTS.has(h) && (/\/search|\/url|\/ck\/a|\/l\/?|\/results|\/watch/i.test(u.pathname + '?' + u.search) || h === 'news.google.com');
  } catch { return false; }
}

function normalizedUrl(url) {
  try {
    const u = new URL(String(url));
    u.hash = '';
    for (const k of [...u.searchParams.keys()]) if (/^(utm_|fbclid$|gclid$|dclid$|msclkid$|ref$|referrer$|cmpid$|src$|trk$|tracking)/i.test(k)) u.searchParams.delete(k);
    return u.href;
  } catch { return ''; }
}
function normalizedKey(url) {
  const u = normalizedUrl(url);
  if (!u) return '';
  try {
    const x = new URL(u);
    return `${x.hostname.toLowerCase().replace(/^www\./,'')}${x.pathname.replace(/\/+/g,'/').replace(/\/$/,'')}${x.search}`;
  } catch { return u.toLowerCase(); }
}
function absoluteUrl(raw, base) { try { return new URL(String(raw || ''), base).href; } catch { return null; } }
function decodeUri(v) { try { return decodeURIComponent(String(v || '')); } catch { return String(v || ''); } }

function unwrap(raw, base) {
  let current = absoluteUrl(raw, base);
  if (!current) return null;
  for (let depth = 0; depth < 5; depth++) {
    let u; try { u = new URL(current); } catch { return null; }
    const h = normalizeHost(u.href);
    let next = null;

    if (h === 'bing.com' && /^\/ck\/a/i.test(u.pathname)) {
      const val = u.searchParams.get('u') || u.searchParams.get('url') || u.searchParams.get('target');
      if (val) {
        const decoded = decodeUri(val);
        if (/^https?:\/\//i.test(decoded)) next = decoded;
        if (!next) {
          try {
            let s = val.replace(/-/g, '+').replace(/_/g, '/'); while (s.length % 4) s += '=';
            const bin = atob(s); let decoded2 = '';
            for (let i = 0; i < bin.length; i++) decoded2 += String.fromCharCode(bin.charCodeAt(i));
            const t = new TextDecoder().decode(new Uint8Array([...decoded2].map(c => c.charCodeAt(0))));
            if (/^https?:\/\//i.test(t)) next = t;
          } catch {}
        }
      }
    }
    if (!next && /^google\.[^./]+(?:\.[^./]+)?$/i.test(h) && /^\/url$/i.test(u.pathname)) next = u.searchParams.get('url') || u.searchParams.get('q');
    if (!next && (h === 'duckduckgo.com' || h === 'html.duckduckgo.com') && /^\/l\/?$/i.test(u.pathname)) next = u.searchParams.get('uddg') || u.searchParams.get('u');
    if (!next && h === 'search.yahoo.com') {
      const ru = u.pathname.match(/\/RU=([^/]+)/i)?.[1];
      next = ru || u.searchParams.get('RU') || u.searchParams.get('url');
    }
    if (!next) break;
    const clean = absoluteUrl(decodeUri(next), current);
    if (!clean || clean === current) break;
    current = clean;
  }
  return normalizedUrl(current);
}

function stripTags(html) {
  return String(html || '')
    .replace(/<!--(?:[\s\S]*?)-->/g, ' ')
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'").replace(/&lt;/gi, '<').replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);?/gi, (_, x) => { const n = parseInt(x,16); return Number.isFinite(n) ? String.fromCodePoint(Math.min(n,0x10ffff)) : ' '; })
    .replace(/&#(\d+);?/g, (_, x) => { const n = parseInt(x,10); return Number.isFinite(n) ? String.fromCodePoint(Math.min(n,0x10ffff)) : ' '; })
    .replace(/\s+/g, ' ').trim();
}
function cleanSnippet(v) { return truncate(stripTags(v).replace(/\s+/g,' ').trim(), 3000); }
function titleFromHtml(html) { return cleanSnippet((String(html).match(/<title[^>]*>([\s\S]*?)<\/title>/i)||[, ''])[1]); }
function metaFromHtml(html, name) {
  const e = String(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const a = String(html).match(new RegExp(`<meta[^>]+(?:name|property|itemprop)=["']${e}["'][^>]*content=["']([\\s\\S]*?)["']`, 'i'));
  const b = String(html).match(new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+(?:name|property|itemprop)=["']${e}["']`, 'i'));
  return cleanSnippet((a||b||[, ''])[1] || '');
}
function dateFromHtml(html) {
  const vals = [metaFromHtml(html,'article:published_time'),metaFromHtml(html,'datePublished'),metaFromHtml(html,'dateModified'),metaFromHtml(html,'date'),(String(html).match(/<time[^>]+datetime=["']([^"']+)["']/i)||[])[1]];
  for (const v of vals) { const t = Date.parse(v || ''); if (!Number.isNaN(t)) return new Date(t).toISOString(); }
  return null;
}
function parseMaybeDateFromText(v) {
  const s = String(v || '');
  const candidates = s.match(/\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:,\s*|\s+)\d{4}\b|\b20\d{2}-\d{1,2}-\d{1,2}\b/ig) || [];
  for (const c of candidates) { const t = Date.parse(c); if (!Number.isNaN(t)) return new Date(t).toISOString(); }
  return null;
}

function queryPlan(query, options = {}) {
  const q = truncate(String(query || '').trim(), MAX_QUERY_LEN);
  const requested = String(options.type || '').toLowerCase();
  const mode = String(options.mode || 'auto').toLowerCase();
  let type = requested;
  if (!type || type === 'mixed' || type === 'all') {
    if (mode === 'news') type = 'news'; else if (mode === 'video') type = 'video'; else if (mode === 'gov') type = 'gov'; else if (mode === 'doc' || mode === 'docs' || mode === 'document') type = 'doc'; else type = 'web';
  }
  const content = contentTokens(q);
  const uniqueTerms = unique(content);
  const live = /\b(latest|today|current|recent|breaking|this week|this month|yesterday|newly)\b/i.test(q);
  const history = /\b(history|historical|timeline|origins?|evolution|development|milestones)\b/i.test(q);
  const official = /\b(official|government|govt|ministry|scheme|policy|law|act|rule|regulation|tax|gst|rbi|sebi|mca|notification|circular|guideline)\b/i.test(q);
  const academic = /\b(research|study|paper|academic|journal|thesis|evidence|peer[- ]reviewed)\b/i.test(q);
  const exactish = uniqueTerms.length <= 7 ? uniqueTerms.join(' ') : uniqueTerms.slice(0, 7).join(' ');
  const anchors = unique((q.match(/\b(?:India|Indian|[A-Z][A-Za-z]{2,}|20\d{2})\b/g) || []).map(x => x.toLowerCase()));
  return {
    query:q, type, requestedType:requested, mode, terms:uniqueTerms, exactish, anchors,
    flags:{live,history,official,academic,explicitNews:type==='news',explicitVideo:type==='video',explicitDoc:type==='doc',explicitGov:type==='gov'}
  };
}

function buildQueries(query, plan, deep = false) {
  const set = new Set();
  const add = s => { const x = truncate(String(s || '').replace(/\s+/g,' ').trim(), 460); if (x.length >= 3) set.add(x); };
  add(plan.query);
  if (plan.terms.length >= 2 && plan.terms.length <= 8) add(`"${plan.terms.join(' ')}"`);
  if (plan.terms.length >= 3) add(`${plan.terms.slice(0, Math.min(plan.terms.length, 6)).join(' ')} explained`);
  if (plan.flags.live) add(`${plan.query} latest`);
  if (plan.flags.history) add(`${plan.query} timeline history`);
  if (plan.flags.official) add(`${plan.query} official source`);
  if (plan.flags.academic) add(`${plan.query} research paper evidence`);
  if (plan.flags.explicitNews) add(`${plan.query} latest news`);
  if (plan.flags.explicitVideo) add(`${plan.query} video`);
  if (plan.flags.explicitDoc) add(`${plan.query} filetype:pdf`);
  if (plan.flags.explicitGov) add(`${plan.query} site:gov.in`);
  if (plan.mode === 'deep' || deep) {
    if (plan.terms.length >= 2) add(`intitle:${plan.terms.slice(0,3).join(' ')} ${plan.query}`);
    if (plan.anchors.length) add(`${plan.query} ${plan.anchors.slice(0,2).join(' ')}`);
  }
  return [...set].slice(0, deep ? 8 : 6);
}

function typeFromUrl(url, fallback = 'web') {
  if (isGov(url)) return 'gov';
  if (isDoc(url)) return 'doc';
  if (isVideo(url)) return 'video';
  return fallback || 'web';
}
function typeFits(item, plan) {
  if (plan.type === 'gov') return isGov(item.url) || item.type === 'gov';
  if (plan.type === 'doc') return isDoc(item.url) || item.type === 'doc';
  if (plan.type === 'video') return isVideo(item.url) || item.type === 'video';
  if (plan.type === 'news') return item.type === 'news' || /\/(?:news|article|story|stories|post|press[-_]?release)\b/i.test(item.url || '');
  return true;
}

function makeCandidate(url, title, snippet, provider, type, extra = {}) {
  const clean = unwrap(url, extra.base || 'https://example.com/');
  if (!clean || blocked(clean) || isLikelySearchUrl(clean)) return null;
  const t = typeFromUrl(clean, type || 'web');
  const result = {
    title:truncate(cleanSnippet(title),500) || truncate(cleanSnippet(new URL(clean).hostname),300),
    url:clean,
    snippet:cleanSnippet(snippet || ''),
    source:provider,
    type:t,
    providerRank:Number(extra.providerRank || 0),
    queryVariant:Number(extra.queryVariant || 0),
    publishedAt:extra.publishedAt || parseMaybeDateFromText(`${title} ${snippet}`),
    searchWrapperResolved:Boolean(extra.searchWrapperResolved),
  };
  delete extra.base; delete extra.providerRank; delete extra.queryVariant; delete extra.publishedAt; delete extra.searchWrapperResolved;
  Object.assign(result, extra);
  if (!result.title || result.title.length < 2) return null;
  return result;
}

function parseBing(html, req) {
  const out = []; const s = String(html || '');
  const blocks = s.match(/<li[^>]+class=["'][^"']*b_algo[^"']*["'][\s\S]*?<\/li>/gi) || [];
  let rank = 0;
  for (const block of blocks) {
    const m = block.match(/<h2[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/i);
    if (!m) continue;
    const r = makeCandidate(m[1],m[2],(block.match(/<p[^>]*>([\s\S]*?)<\/p>/i)||[, ''])[1],'bing',req.type,{base:'https://www.bing.com/',providerRank:++rank,queryVariant:req.queryVariant,searchWrapperResolved:true});
    if (r) out.push(r);
    if (out.length >= 30) break;
  }
  if (!out.length) {
    rank = 0;
    for (const m of s.matchAll(/<h2[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
      const r = makeCandidate(m[1],m[2],'','bing',req.type,{base:'https://www.bing.com/',providerRank:++rank,queryVariant:req.queryVariant,searchWrapperResolved:true});
      if (r) out.push(r);
      if (out.length >= 30) break;
    }
  }
  return out;
}

function parseGoogle(html, req, source = 'google') {
  const out=[]; const seen=new Set(); const s=String(html || ''); let rank=0;
  for (const m of s.matchAll(/<h3[^>]*>([\s\S]*?)<\/h3>/gi)) {
    const idx=m.index || 0; const window=s.slice(Math.max(0,idx-1800),Math.min(s.length,idx+700));
    const links=[...window.matchAll(/<a[^>]+href=["']([^"']+)["'][^>]*>/gi)].map(x=>x[1]);
    let raw=links.reverse().find(x=>/^https?:\/\//i.test(decodeUri(x)) || /^\//.test(x));
    if (!raw) continue;
    try { if (raw.startsWith('/url?')) { const u=new URL(raw,'https://www.google.com/'); raw=u.searchParams.get('q')||u.searchParams.get('url')||raw; } } catch {}
    const url=unwrap(raw,'https://www.google.com/'); if(!url || blocked(url) || isLikelySearchUrl(url)) continue;
    const key=normalizedKey(url); if(seen.has(key)) continue; seen.add(key);
    const r=makeCandidate(url,m[1],stripTags(window),source,req.type,{providerRank:++rank,queryVariant:req.queryVariant,searchWrapperResolved:true});
    if(r) out.push(r); if(out.length>=30) break;
  }
  return out;
}

function parseDuck(html, req) {
  const out=[]; let rank=0; const s=String(html || '');
  for (const m of s.matchAll(/<a[^>]+class=["'][^"']*result__a[^"']*["'][^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const idx=m.index||0; const window=s.slice(idx,idx+2800);
    const r=makeCandidate(m[1],m[2],(window.match(/class=["'][^"']*result__snippet[^"']*["'][^>]*>([\s\S]*?)<\//i)||[,stripTags(window)])[1],'duckduckgo',req.type,{base:'https://html.duckduckgo.com/',providerRank:++rank,queryVariant:req.queryVariant,searchWrapperResolved:true});
    if(r) out.push(r); if(out.length>=30) break;
  }
  return out;
}

function parseYahoo(html, req) {
  const out=[]; let rank=0; const s=String(html || '');
  for (const m of s.matchAll(/<h3[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const idx=m.index||0; const window=s.slice(Math.max(0,idx-300),Math.min(s.length,idx+2200));
    const r=makeCandidate(m[1],m[2],stripTags(window),'yahoo',req.type,{base:'https://search.yahoo.com/',providerRank:++rank,queryVariant:req.queryVariant,searchWrapperResolved:true});
    if(r) out.push(r); if(out.length>=30) break;
  }
  return out;
}

function parseMojeek(html, req) {
  const out=[]; let rank=0; const s=String(html || '');
  const patterns=[
    /<a[^>]+href=["']([^"']+)["'][^>]+class=["'][^"']*(?:title|ob|result)[^"']*["'][^>]*>([\s\S]*?)<\/a>/gi,
    /<h2[^>]*>\s*<a[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi
  ];
  for(const re of patterns){
    for(const m of s.matchAll(re)){
      const idx=m.index||0; const window=s.slice(idx,idx+2500);
      const r=makeCandidate(m[1],m[2],stripTags(window),'mojeek',req.type,{base:'https://www.mojeek.com/',providerRank:++rank,queryVariant:req.queryVariant,searchWrapperResolved:true});
      if(r) out.push(r); if(out.length>=30) break;
    }
    if(out.length>=30) break;
  }
  return out;
}

function parseGoogleNews(xml, req) {
  const out=[]; let rank=0;
  for(const item of String(xml||'').match(/<item>[\s\S]*?<\/item>/gi)||[]){
    const title=stripTags((item.match(/<title>([\s\S]*?)<\/title>/i)||[, ''])[1]);
    const link=stripTags((item.match(/<link>([\s\S]*?)<\/link>/i)||[, ''])[1]);
    const pub=stripTags((item.match(/<pubDate>([\s\S]*?)<\/pubDate>/i)||[, ''])[1]);
    const desc=stripTags((item.match(/<description>([\s\S]*?)<\/description>/i)||[, ''])[1]);
    const sm=item.match(/<source[^>]+url=["']([^"']+)["'][^>]*>([\s\S]*?)<\/source>/i);
    const sourceName=stripTags(sm?.[2]||'');
    const url=unwrap(link,'https://news.google.com/');
    if(!title||!url||normalizeHost(url)==='news.google.com') continue;
    const r=makeCandidate(url,title,desc,sourceName?`google-news:${sourceName}`:'google-news','news',{publishedAt:safeDate(pub),providerRank:++rank,queryVariant:req.queryVariant});
    if(r) out.push(r); if(out.length>=80) break;
  }
  return out;
}

function parseYoutube(html, req) {
  const out=[]; const seen=new Set(); let rank=0; const s=String(html||'');
  for(const m of s.matchAll(/"videoRenderer":\{[\s\S]*?"videoId":"([A-Za-z0-9_-]{6,20})"[\s\S]*?"title":\{"runs":\[\{"text":"((?:\\.|[^"\\])*)"/g)){
    const id=m[1]; if(seen.has(id)) continue; seen.add(id);
    const title=m[2].replace(/\\"/g,'"').replace(/\\\\/g,'\\');
    const r=makeCandidate(`https://www.youtube.com/watch?v=${id}`,title,'YouTube result','youtube','video',{providerRank:++rank,queryVariant:req.queryVariant});
    if(r) out.push(r); if(out.length>=30) break;
  }
  if(!out.length){
    for(const m of s.matchAll(/"videoId":"([A-Za-z0-9_-]{6,20})"/g)){
      const id=m[1]; if(seen.has(id)) continue; seen.add(id);
      const r=makeCandidate(`https://www.youtube.com/watch?v=${id}`,`YouTube video ${id}`,'YouTube result','youtube','video',{providerRank:++rank,queryVariant:req.queryVariant});
      if(r) out.push(r); if(out.length>=30) break;
    }
  }
  return out;
}

function safeDate(value) { const t=Date.parse(String(value||'')); return Number.isNaN(t)?null:new Date(t).toISOString(); }
function freshness(publishedAt) {
  if(!publishedAt) return 'unknown'; const t=Date.parse(publishedAt); if(Number.isNaN(t)) return 'unknown';
  const age=Math.max(0,(Date.now()-t)/86400000);
  if(age<=1)return 'last_24h'; if(age<=7)return 'last_7d'; if(age<=30)return 'last_30d'; if(age<=90)return 'last_90d'; if(age<=365)return 'last_year'; return 'older';
}

function providerRequests(queries, plan, deep) {
  const out=[]; const seen=new Set();
  const add=(provider,q,type,url,queryVariant)=>{ if(out.length>=MAX_ENGINE_REQUESTS || !url || seen.has(url)) return; seen.add(url); out.push({provider,query:q,type,url,queryVariant}); };
  const base=queries[0]||plan.query;
  const q0=encodeURIComponent(base);
  const pages=deep?4:2;
  for(let first=0;first<pages;first++) add('bing',base,plan.type==='news'?'news':'web',`https://www.bing.com/search?q=${q0}&count=10&first=${first*10}&setlang=en-IN&cc=in`,0);
  for(let i=0;i<queries.length;i++){
    const q=queries[i], e=encodeURIComponent(q);
    if(i<3) add('google',q,plan.type==='news'?'news':'web',`https://www.google.com/search?q=${e}&num=20&hl=en&gl=in`,i);
    if(i<3) add('duckduckgo',q,plan.type==='news'?'news':'web',`https://html.duckduckgo.com/html/?q=${e}&kl=in-en`,i);
    if(i<3) add('yahoo',q,plan.type==='news'?'news':'web',`https://search.yahoo.com/search?p=${e}`,i);
    if(i<3) add('mojeek',q,plan.type==='news'?'news':'web',`https://www.mojeek.com/search?q=${e}&lb=EN&lbb=100&rb=IN&rbb=10&s=${1+(i*10)}&t=10&fmt=html`,i);
  }
  if(plan.flags.live || plan.type==='news') for(let i=0;i<Math.min(2,queries.length);i++){ const q=queries[i]; add('google-news',q,'news',`https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en-IN&gl=IN&ceid=IN:en`,i); }
  if(plan.type==='video' || plan.flags.explicitVideo) for(let i=0;i<Math.min(2,queries.length);i++){ const q=queries[i]; add('youtube',q,'video',`https://www.youtube.com/results?search_query=${encodeURIComponent(q)}&hl=en`,i); }
  if(plan.type==='doc' || plan.flags.explicitDoc) for(let i=0;i<Math.min(2,queries.length);i++){ const q=queries[i]; add('google-doc',q,'doc',`https://www.google.com/search?q=${encodeURIComponent(`${q} filetype:pdf`)}&num=20&hl=en&gl=in`,i); }
  if(plan.type==='gov' || plan.flags.explicitGov) for(let i=0;i<Math.min(2,queries.length);i++){ const q=queries[i]; add('google-gov',q,'gov',`https://www.google.com/search?q=${encodeURIComponent(`${q} site:gov.in`)}&num=20&hl=en&gl=in`,i); }
  return out;
}

async function fetchWithTimeout(url, timeout, deadline, headers = {}) {
  const budget=Math.min(timeout,Math.max(250,left(deadline)-VERIFY_HEADROOM_MS)); if(budget<=0) throw new Error('BUDGET_EXHAUSTED');
  const controller=new AbortController(); const timer=setTimeout(()=>controller.abort(),budget);
  try{
    return await fetch(url,{method:'GET',redirect:'follow',signal:controller.signal,headers:{'user-agent':USER_AGENT,'accept':'text/html,application/xhtml+xml,application/xml,application/rss+xml,text/plain;q=0.9,application/pdf;q=0.8,*/*;q=0.1','accept-language':'en-IN,en;q=0.9',...headers}});
  } finally { clearTimeout(timer); }
}
async function readLimited(res,maxBytes,deadline){
  const reader=res.body?.getReader?.();
  if(!reader) return truncate(await res.text(),maxBytes);
  const chunks=[]; let total=0;
  try{
    while(left(deadline)>50 && total<maxBytes){
      const {done,value}=await reader.read(); if(done)break; if(!value)continue;
      const room=maxBytes-total; const c=value.byteLength>room?value.slice(0,room):value; chunks.push(c); total+=c.byteLength;
    }
  } finally { try{await reader.cancel();}catch{} }
  const bytes=new Uint8Array(total); let off=0; for(const c of chunks){bytes.set(c,off);off+=c.byteLength;}
  return new TextDecoder('utf-8',{fatal:false}).decode(bytes);
}
async function fetchSearch(req,deadline){
  const res=await fetchWithTimeout(req.url,SEARCH_TIMEOUT_MS,deadline,{accept:req.provider==='google-news'?'application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.1':'text/html,application/xhtml+xml;q=0.9,*/*;q=0.1'});
  if(!res.ok) throw new Error(`HTTP_${res.status}`);
  return readLimited(res,750_000,deadline);
}

async function discoverOne(req,deadline){
  try{
    const body=await fetchSearch(req,deadline); let results=[];
    if(req.provider==='bing')results=parseBing(body,req);
    else if(req.provider==='google' || req.provider==='google-video' || req.provider==='google-doc' || req.provider==='google-gov')results=parseGoogle(body,req,req.provider);
    else if(req.provider==='duckduckgo')results=parseDuck(body,req);
    else if(req.provider==='yahoo')results=parseYahoo(body,req);
    else if(req.provider==='mojeek')results=parseMojeek(body,req);
    else if(req.provider==='google-news')results=parseGoogleNews(body,req);
    else if(req.provider==='youtube')results=parseYoutube(body,req);
    return {provider:req.provider,ok:true,results};
  }catch(error){ return {provider:req.provider,ok:false,results:[],error:error?.message||'DISCOVERY_FAILED'}; }
}

function dedupe(list){
  const map=new Map();
  for(const raw of list||[]){
    if(!raw?.url)continue; const url=normalizedUrl(raw.url); if(!url||blocked(url)||isLikelySearchUrl(url))continue;
    const key=normalizedKey(url); if(!key)continue;
    const old=map.get(key);
    if(!old)map.set(key,{...raw,url});
    else{
      const providers=new Set([...(old.providers||[old.source]),...(raw.providers||[raw.source])].filter(Boolean));
      const better=(raw.providerRank||99)<(old.providerRank||99);
      const merged={...old,...raw,url,providers:[...providers],source:providers.size>1?'multi-source':(better?raw.source:old.source)};
      if(!raw.snippet && old.snippet)merged.snippet=old.snippet;
      if(!raw.publishedAt && old.publishedAt)merged.publishedAt=old.publishedAt;
      map.set(key,merged);
    }
  }
  return [...map.values()].slice(0,MAX_DISCOVERY_RESULTS);
}

function titleTerms(text, terms){
  const norm=normalizeText(text); const set=tokenSet(text); let hits=0; const matched=[];
  for(const term of terms){ const c=canonicalToken(term); if(set.has(c) || set.has(stem(c)) || norm.includes(` ${c} `)){ hits++; matched.push(c); } }
  return {hits,matched:unique(matched)};
}
function phraseScore(query,text){
  const q=normalizeText(query); const t=normalizeText(text); if(!q||!t)return 0;
  if(t.includes(q))return 1;
  const qs=contentTokens(q).slice(0,8); if(qs.length<2)return 0;
  const joined=qs.join(' '); if(t.includes(joined))return .82;
  let run=0,best=0; const tt=contentTokens(t); let pos=0;
  for(const qx of qs){ const i=tt.indexOf(qx,pos); if(i>=0){run++;pos=i+1;best=Math.max(best,run);}else{run=0;}}
  return best/qs.length>=.7 ? .58 : best/qs.length>=.5 ? .35 : 0;
}

function domainQuality(url){
  const h=normalizeHost(url); let score=.55;
  if(isGov(url))score=.98; else if(isTrusted(url))score=.93; else if(/\.edu(?:\.|$)/i.test(h)||/\.ac\.(?:in|uk|jp|nz)$/i.test(h))score=.90;
  else if(/(?:reuters|apnews|bbc|theguardian|nytimes|economist|nature|science|arxiv|nasa|who|wikipedia|britannica|investopedia|microsoft|google|apple|ibm|intel|mitre|github)\./i.test(h))score=.86;
  else if(/(?:medium|quora|reddit|facebook|pinterest|instagram|tiktok)\./i.test(h))score=.48;
  const labels=h.split('.'); if(labels.length<=1)score-=.08;
  return clamp(score,0,1);
}
function urlIntentScore(url,plan){
  const s=String(url||'').toLowerCase(); let x=0;
  if(plan.flags.history && /history|timeline|evolution|origin|milestone/.test(s))x+=5;
  if(plan.flags.academic && /paper|research|study|journal|arxiv|doi/.test(s))x+=5;
  if(plan.flags.official && (isGov(url)||/official|policy|notification|circular/.test(s)))x+=6;
  if(plan.type==='doc' && isDoc(url))x+=9;
  if(plan.type==='video' && isVideo(url))x+=8;
  if(plan.type==='news' && /news|article|story|press|reuters|apnews|bbc/.test(s))x+=5;
  if(/(?:\/tag\/|\/tags\/|\/category\/|\/categories\/|\/search[/?]|\/author\/|\/authors\/|\/topic\/|\/topics\/|\/(?:home|homepage)\/?$)/i.test(s))x-=6;
  return x;
}
function rankOne(item,plan){
  const title=String(item.title||''); const snippet=String(item.snippet||''); const url=String(item.url||'');
  const terms=plan.terms; if(!terms.length)return 35;
  const t=titleTerms(title,terms), s=titleTerms(snippet,terms), u=titleTerms(url.replace(/[-_/?.=&]+/g,' '),terms);
  const n=terms.length;
  const titleCoverage=t.hits/n, snippetCoverage=s.hits/n, urlCoverage=u.hits/n;
  const phrase=phraseScore(plan.query,`${title} ${snippet}`);
  const consensus=Math.min(1,((item.providers?.length||1)-1)/3);
  const qRank=item.providerRank>0 ? Math.max(0,1-(item.providerRank-1)/30) : .35;
  const dq=domainQuality(url);
  const freshnessBonus=plan.flags.live ? ({last_24h:10,last_7d:7,last_30d:3}[freshness(item.publishedAt)]||0) : 0;
  let score=0;
  score += titleCoverage*45;
  score += snippetCoverage*17;
  score += urlCoverage*6;
  score += phrase*18;
  score += consensus*7;
  score += qRank*7;
  score += dq*7;
  score += urlIntentScore(url,plan);
  score += freshnessBonus;
  if(item.source==='multi-source')score+=4;
  if(plan.flags.live && !item.publishedAt && /\b20\d{2}\b/.test(`${title} ${snippet}`))score+=1;
  if(plan.type==='gov' && !isGov(url))score-=10;
  if(plan.type==='doc' && !isDoc(url))score-=12;
  if(plan.type==='video' && !isVideo(url))score-=15;
  if(plan.type==='news' && item.type!=='news' && !/\/(news|article|story|press)/i.test(url))score-=10;
  return clamp(Math.round(score*100)/100,0,100);
}
function relevanceObject(item,plan){
  const title=titleTerms(item.title,plan.terms), body=titleTerms(item.snippet,plan.terms), phrase=phraseScore(plan.query,`${item.title} ${item.snippet}`);
  const n=Math.max(1,plan.terms.length);
  return {
    score:rankOne(item,plan), titleCoverage:Number((title.hits/n).toFixed(3)), bodyCoverage:Number((body.hits/n).toFixed(3)),
    conceptCoverage:Number(((title.hits+body.hits)/(n*2)).toFixed(3)), matchedConcepts:unique([...title.matched,...body.matched]),
    exactPhrase:phrase>=.8, phraseScore:Number(phrase.toFixed(3)), providers:item.providers||[item.source]
  };
}

function fusionRank(results,plan){
  const ranked=results.map((r,index)=>{ const rel=relevanceObject(r,plan); return {...r,relevance:rel,relevanceScore:rel.score,relevanceBand:rel.score>=82?'excellent':rel.score>=65?'strong':rel.score>=48?'usable':rel.score>=25?'related':'weak',_inputOrder:index}; });
  ranked.sort((a,b)=>{
    const d=Number(b.relevanceScore)-Number(a.relevanceScore); if(Math.abs(d)>.01)return d;
    const dp=Number(domainQuality(b.url))-Number(domainQuality(a.url)); if(Math.abs(dp)>.001)return dp;
    const bp=(b.providers?.length||1)-(a.providers?.length||1); if(bp)return bp;
    return (a._inputOrder||0)-(b._inputOrder||0);
  });
  return ranked;
}

async function mapConcurrent(list,limit,worker){
  const arr=Array.isArray(list)?list:[]; const out=new Array(arr.length); let cursor=0; const n=Math.max(1,Math.min(limit,arr.length||1));
  const runner=async()=>{while(true){const i=cursor++;if(i>=arr.length)return;try{out[i]=await worker(arr[i],i);}catch{out[i]=null;}}};
  await Promise.all(Array.from({length:n},runner)); return out;
}

async function verifyOne(candidate,deadline){
  if(!safeUrl(candidate.url) || left(deadline)<220) return {...candidate,verified:false,verificationMethod:'budget'};
  const localDeadline=Date.now()+Math.min(VERIFY_TIMEOUT_MS,left(deadline)-100);
  try{
    const res=await fetchWithTimeout(candidate.url,VERIFY_TIMEOUT_MS,localDeadline,{accept:'text/html,application/xhtml+xml,application/xml,text/plain,application/pdf;q=0.8,*/*;q=0.1',range:'bytes=0-16383'});
    const finalUrl=normalizedUrl(res.url||candidate.url)||candidate.url;
    if(!safeUrl(finalUrl) || blocked(finalUrl)) return {...candidate,verified:false,verificationMethod:'redirect-rejected',httpStatus:res.status};
    const ct=String(res.headers.get('content-type')||'').toLowerCase();
    const body=await readLimited(res,20_000,localDeadline).catch(()=> '');
    const title=titleFromHtml(body) || metaFromHtml(body,'og:title') || candidate.title;
    const canonical=metaFromHtml(body,'og:url');
    const published=dateFromHtml(body) || candidate.publishedAt || null;
    const reachable=res.ok || (res.status>=300 && res.status<500);
    return {...candidate,url:finalUrl,title:truncate(title,500),publishedAt:published,httpStatus:res.status,contentType:ct||null,verified:Boolean(reachable),verificationMethod:'light-http-check',contentAvailable:false,contentStatus:'metadata-only',contentMethod:'search-metadata'};
  }catch(error){ return {...candidate,verified:false,verificationMethod:'light-http-check-failed',verificationError:error?.message||'VERIFY_FAILED'}; }
}

async function optionalGroqRerank(query,results,deadline){
  const key=env('GROQ_API_KEY'); if(!key || results.length<4 || left(deadline)<500) return null;
  const controller=new AbortController(); const timeout=Math.min(AI_RERANK_TIMEOUT_MS,Math.max(350,left(deadline)-100)); const timer=setTimeout(()=>controller.abort(),timeout);
  try{
    const payload=results.slice(0,30).map((r,i)=>({id:i,title:truncate(r.title,180),domain:normalizeHost(r.url),snippet:truncate(r.snippet,280),score:r.relevanceScore,providers:r.providers?.length||1,type:r.type}));
    const res=await fetch('https://api.groq.com/openai/v1/chat/completions',{method:'POST',signal:controller.signal,headers:{authorization:`Bearer ${key}`,'content-type':'application/json'},body:JSON.stringify({model:'llama-3.1-8b-instant',messages:[{role:'system',content:'Return JSON only. Reorder the supplied ids by relevance to the query. Never invent ids. Do not omit any id. Prefer exact topical match and direct source evidence.'},{role:'user',content:JSON.stringify({query,results:payload,order:payload.map(x=>x.id)})}],temperature:0,max_tokens:500})});
    if(!res.ok)return null; const data=await res.json(); const txt=String(data?.choices?.[0]?.message?.content||''); const obj=JSON.parse(txt.match(/\{[\s\S]*\}/)?.[0]||txt);
    if(!Array.isArray(obj.order))return null; return obj.order.map(Number).filter(i=>Number.isInteger(i)&&i>=0&&i<Math.min(30,results.length));
  }catch{return null}finally{clearTimeout(timer)}
}

async function commonCrawlMeta(url,deadline){
  if(!safeUrl(url)||left(deadline)<250)return null; const encoded=encodeURIComponent(url);
  for(const index of COMMON_CRAWL_INDEXES){
    if(left(deadline)<200)break;
    try{
      const res=await fetchWithTimeout(`https://index.commoncrawl.org/${index}-index?url=${encoded}&output=json&filter=status:200&limit=1`,COMMON_CRAWL_TIMEOUT_MS,deadline,{accept:'application/json,text/plain;q=0.8'});
      if(!res.ok)continue; const body=await readLimited(res,20_000,deadline); const row=body.split('\n').map(x=>{try{return JSON.parse(x)}catch{return null}}).find(Boolean);
      if(row)return {index,timestamp:row.timestamp||null,digest:row.digest||null};
    }catch{}
  }
  return null;
}

function createLogger(){
  const logs=[]; const add=(event,message,extra={})=>{const x={at:nowIso(),event,message,...extra};logs.push(x);if(logs.length>MAX_LIVE_LOG)logs.shift();try{console.log(`[ArixAI ${event}] ${message}`)}catch{}};
  return {logs,add};
}
function cacheKey(input,query,plan){
  return JSON.stringify({q:query,mode:String(input.mode||'auto').toLowerCase(),type:plan.type,deep:String(input.deep||'false').toLowerCase(),verify:String(input.verify??'true').toLowerCase(),ai:String(input.ai??'auto').toLowerCase(),cc:String(input.commonCrawl||'false').toLowerCase()});
}
function cacheGet(key){const x=CACHE.get(key);if(!x)return null;if(Date.now()-x.at>x.ttl){CACHE.delete(key);return null}return x.value;}
function cacheSet(key,value,ttl){CACHE.set(key,{at:Date.now(),ttl,value});while(CACHE.size>CACHE_MAX)CACHE.delete(CACHE.keys().next().value);}

function decorateResult(r,i,plan){
  const type=typeFromUrl(r.url,r.type);
  const score=Math.round(Number(r.relevanceScore||0));
  return {
    rank:i+1,title:truncate(r.title||'Untitled',300),url:r.url,domain:normalizeHost(r.url),type,
    source:r.source||'search',providers:r.providers||[r.source||'search'],snippet:truncate(r.snippet||'',1200),publishedAt:r.publishedAt||null,
    freshness:freshness(r.publishedAt),verified:Boolean(r.verified),httpStatus:r.httpStatus||null,contentType:r.contentType||null,
    trust:Number(domainQuality(r.url).toFixed(2)),relevanceScore:score,relevanceBand:r.relevanceBand||'related',relevance:r.relevance||null,
    publisherResolved:Boolean(r.publisherResolved),publisherWrapperUrl:r.publisherWrapperUrl||null,searchWrapperResolved:Boolean(r.searchWrapperResolved),
    extractedText:'',pageContent:'',contentAvailable:false,contentStatus:'metadata-only',contentMethod:'search-metadata',contentLength:0,
    contentConfidence:0,contentSourceUrl:r.url,contentTargetMatched:false,contentTitleSimilarity:Number(r.relevance?.titleCoverage||0).toFixed(3),
    contentConceptCoverage:Number(r.relevance?.conceptCoverage||0).toFixed(3),contentFormat:'none',contentRole:'search_metadata_only',contentForAI:null,
    verificationMethod:r.verificationMethod||null,verificationError:r.verificationError||null,commonCrawl:r.commonCrawl||null,
    queryMatch:{planType:plan.type,liveIntent:plan.flags.live,historyIntent:plan.flags.history,officialIntent:plan.flags.official,terms:plan.terms}
  };
}

function buildResponse(ctx){
  const {query,count,mode,requestedType,plan,preciseQueries,providerStats,results,logger,started,verifyRequested,deep,aiRequested,useCc,cacheHit=false}=ctx;
  const verifiedCount=results.filter(r=>r.verified).length;
  return {
    ok:true,version:VERSION,query,requestedResults:count,returnedResults:results.length,sourceCountMode:'all-discovered-ranked',
    resultSelectionPolicy:'return-all-discovered-ranked-by-query-match',mode,
    intent:{type:requestedType||plan.type,wantsNews:plan.flags.explicitNews||plan.type==='news'||plan.flags.live,wantsVideo:plan.flags.explicitVideo||plan.type==='video',wantsGov:plan.flags.explicitGov||plan.type==='gov',wantsDocs:plan.flags.explicitDoc||plan.type==='doc',wantsHistory:plan.flags.history,wantsAcademic:plan.flags.academic},
    generatedAt:nowIso(),started,latencyMs:Date.now()-started,keylessCoreSearch:true,groqUsed:Boolean(ctx.groqUsed),cached:cacheHit,
    providers:providerStats,
    quality:{discoveredResults:results.length,validatedResults:verifiedCount,requestedResults:count,realContentOnly:false,contentGuarantee:'Search results are source metadata/snippets. No snippet is labeled as full page content.'},
    searchPlan:{queryVariants:preciseQueries,engineRequests:Object.values(providerStats).reduce((s,p)=>s+Number(p.requests||0),0),verificationRequested:verifyRequested,verificationPerformed:Number(ctx.verificationPerformed || 0),verificationSucceeded:verifiedCount,commonCrawlEnabled:useCc,commonCrawlPerformed:results.filter(r=>r.commonCrawl).length,streamed:true,logStreamSupported:true,deep,aiRequested},
    liveLog:logger.logs,allFetchedSources:results.length,results,warnings:results.length<count?[`${results.length} ranked sources were discovered. Public engines may return fewer results than requested.`]:[]
  };
}

async function performSearch(input,started,logger){
  const deadline=started+SEARCH_BUDGET_MS;
  const query=truncate(String(input.query??input.q??'').trim(),MAX_QUERY_LEN);
  const count=safeInt(input.count??input.limit,DEFAULT_RESULTS,1,MAX_RESULTS);
  const mode=String(input.mode||'auto').toLowerCase();
  const requestedType=String(input.type||'').toLowerCase();
  const deep=String(input.deep??'false').toLowerCase()==='true' || mode==='deep';
  const verifyRequested=input.verify==null ? true : String(input.verify).toLowerCase()!=='false';
  const aiRequested=String(input.ai??'auto').toLowerCase();
  const useCc=String(input.commonCrawl??'false').toLowerCase()==='true';

  const plan=queryPlan(query,{mode,type:requestedType});
  const key=cacheKey(input,query,plan); const cached=cacheGet(key);
  if(cached){logger.add('cache-hit','Returned a warm cached search result.');return {...cached,generatedAt:nowIso(),latencyMs:Date.now()-started,cached:true,liveLog:logger.logs};}

  logger.add('query-analyzed','Built multi-surface precision query plan.',{type:plan.type,terms:plan.terms.slice(0,12),live:plan.flags.live});
  const preciseQueries=buildQueries(query,plan,deep);
  const requests=providerRequests(preciseQueries,plan,deep);
  logger.add('discovery-start',`Launching ${requests.length} public search requests in parallel.`,{queries:preciseQueries});

  const discoveryDeadline=Math.min(deadline,started+DISCOVERY_CUTOFF_MS);
  const entries=await mapConcurrent(requests,SEARCH_CONCURRENCY,r=>discoverOne(r,discoveryDeadline));
  const providerStats={}; let discovered=[];
  for(let i=0;i<entries.length;i++){
    const entry=entries[i]||{provider:requests[i]?.provider,ok:false,results:[]}; const p=entry.provider||requests[i]?.provider||'unknown';
    providerStats[p] ||= {requests:0,ok:0,failed:0,results:0}; providerStats[p].requests++;
    if(entry.ok){providerStats[p].ok++;providerStats[p].results+=entry.results.length;discovered.push(...entry.results);} else providerStats[p].failed++;
  }
  discovered=dedupe(discovered);
  logger.add('discovery-complete',`Discovery produced ${discovered.length} unique public sources.`,{providers:Object.fromEntries(Object.entries(providerStats).map(([k,v])=>[k,{ok:v.ok,failed:v.failed,results:v.results}]))});

  if(!discovered.length){
    const empty=buildResponse({query,count,mode,requestedType,plan,preciseQueries,providerStats,results:[],logger,started,verifyRequested,deep,aiRequested,useCc,verifyLimit:0});
    cacheSet(key,empty,plan.flags.live?LIVE_CACHE_TTL_MS:CACHE_TTL_MS); return empty;
  }

  let ranked=fusionRank(discovered,plan);
  logger.add('ranking-complete',`Fused provider rank, query match, source quality, consensus and intent.`);

  /* Verify more than the displayed count when possible, but never make verification the
   * bottleneck. Unverified sources are still retained and ranked. */
  const verifyLimit=Math.min(ranked.length,deep?40:Math.max(12,count*2));
  let verificationPerformed = 0;
  if(verifyRequested && left(deadline)>550 && verifyLimit){
    logger.add('verification-start',`Running lightweight reachability checks on ${verifyLimit} top sources.`);
    verificationPerformed = verifyLimit;
    const verifyDeadline=Math.min(deadline-60,Date.now()+Math.max(450,left(deadline)-60));
    const checked=await mapConcurrent(ranked.slice(0,verifyLimit),VERIFY_CONCURRENCY,(r)=>verifyOne(r,verifyDeadline));
    const checkedMap=new Map(checked.filter(Boolean).map(r=>[normalizedKey(r.url),r]));
    ranked=ranked.map(r=>checkedMap.get(normalizedKey(r.url))||r);
    ranked=fusionRank(ranked,plan);
    logger.add('verification-complete',`Verification finished; ${ranked.slice(0,verifyLimit).filter(r=>r.verified).length} of ${verifyLimit} top sources responded usefully.`);
  } else {
    logger.add('verification-skipped','Verification skipped because the remaining wall-clock budget was too small.');
  }

  let groqUsed=false;
  if((aiRequested==='true'||aiRequested==='auto') && env('GROQ_API_KEY') && left(deadline)>850){
    const order=await optionalGroqRerank(query,ranked.slice(0,30),deadline);
    if(Array.isArray(order)&&order.length){
      const top=ranked.slice(0,30); const reordered=order.map(i=>top[i]).filter(Boolean); const seen=new Set(reordered.map(x=>normalizedKey(x.url)));
      ranked=[...reordered,...ranked.slice(30).filter(x=>!seen.has(normalizedKey(x.url)))]; groqUsed=true; logger.add('ai-rerank-complete','Optional Groq reranking reordered the top candidate set.');
    }
  }

  let final=ranked.slice(0,OUTPUT_MAX_SOURCES);
  if(useCc && left(deadline)>500){
    const ccDeadline=Date.now()+Math.min(500,left(deadline)-50);
    const rows=await Promise.all(final.slice(0,MAX_COMMON_CRAWL).map(async r=>({url:r.url,cc:await commonCrawlMeta(r.url,ccDeadline)})));
    const ccMap=new Map(rows.filter(x=>x.cc).map(x=>[normalizedKey(x.url),x.cc]));
    final=final.map(r=>({...r,commonCrawl:ccMap.get(normalizedKey(r.url))||null}));
  }

  const decorated=final.map((r,i)=>decorateResult(r,i,plan));
  const result=buildResponse({query,count,mode,requestedType,plan,preciseQueries,providerStats,results:decorated,logger,started,verifyRequested,deep,aiRequested,useCc,groqUsed,verifyLimit,verificationPerformed});
  cacheSet(key,result,plan.flags.live?LIVE_CACHE_TTL_MS:CACHE_TTL_MS);
  return result;
}

async function readInput(req){
  const url=new URL(req.url);
  if(req.method==='GET')return Object.fromEntries(url.searchParams.entries());
  const raw=await req.text(); if(raw.length>MAX_REQUEST_BODY)throw new Error('REQUEST_BODY_TOO_LARGE'); if(!raw)return {};
  try{return JSON.parse(raw)}catch{return Object.fromEntries(new URLSearchParams(raw).entries())}
}
function corsHeaders(contentType='application/json; charset=utf-8'){
  return {'content-type':contentType,'cache-control':'no-store, no-transform','access-control-allow-origin':'*','access-control-allow-methods':'GET, POST, OPTIONS','access-control-allow-headers':'content-type, authorization, x-arix-search-key','x-arix-crawler-version':VERSION};
}
function jsonResponse(body,status=200){return new Response(JSON.stringify(body,null,2),{status,headers:corsHeaders()});}

function streamJsonSearch(input){
  const encoder=new TextEncoder(); const started=Date.now(); const logger=createLogger(); const query=truncate(String(input.query??input.q??'').trim(),MAX_QUERY_LEN); const count=safeInt(input.count??input.limit,DEFAULT_RESULTS,1,MAX_RESULTS); const mode=String(input.mode||'auto').toLowerCase();
  const stream=new ReadableStream({start(controller){let closed=false; const send=x=>{if(closed)return;try{controller.enqueue(encoder.encode(x))}catch{closed=true}}; logger.add('request-start',`Starting live search for ${query}.`,{count,mode}); send(`{"ok":true,"version":${encode(VERSION)},"query":${encode(query)},"requestedResults":${count},"mode":${encode(mode)},"streaming":true,"results":[\n`); const heartbeat=setInterval(()=>send(' \n'),1000);
    Promise.resolve().then(()=>performSearch(input,started,logger)).then(result=>{clearInterval(heartbeat); const rows=Array.isArray(result.results)?result.results:[]; rows.forEach((r,i)=>send(`${i?',\n':''}${JSON.stringify(r)}\n`)); const meta={...result}; delete meta.results; send('],\n'); const entries=Object.entries(meta); entries.forEach(([k,v],i)=>send(`${JSON.stringify(k)}:${JSON.stringify(v)}${i===entries.length-1?'':',\n'}`)); send('}'); try{controller.close()}catch{} closed=true;}).catch(error=>{clearInterval(heartbeat); send(`],"returnedResults":0,"generatedAt":${encode(nowIso())},"latencyMs":${Date.now()-started},"keylessCoreSearch":true,"groqUsed":false,"resultsError":${encode(error?.message||'SEARCH_FAILED')},"liveLog":${JSON.stringify(logger.logs)},"warnings":[${encode('The crawler failed safely after the stream had already started.').slice(1,-1)}]}`);try{controller.close()}catch{}closed=true;});
  }});
  return new Response(stream,{status:200,headers:{...corsHeaders(),'x-arix-search-stream':'1','x-arix-stream-heartbeat-ms':'1000'}});
}

function streamSseSearch(input){
  const encoder=new TextEncoder(); const started=Date.now(); const logger=createLogger();
  const stream=new ReadableStream({start(controller){let closed=false; const send=(event,data)=>{if(closed)return;try{controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`))}catch{closed=true}}; const originalAdd=logger.add; logger.add=(event,message,extra={})=>{originalAdd(event,message,extra);send('log',logger.logs[logger.logs.length-1])}; const heartbeat=setInterval(()=>send('heartbeat',{at:nowIso(),version:VERSION}),1000);
    Promise.resolve().then(()=>performSearch(input,started,logger)).then(result=>{clearInterval(heartbeat);send('result',result);send('done',{ok:true,latencyMs:Date.now()-started});try{controller.close()}catch{}closed=true}).catch(error=>{clearInterval(heartbeat);send('error',{ok:false,error:error?.message||'SEARCH_FAILED',latencyMs:Date.now()-started});try{controller.close()}catch{}closed=true});
  }});
  return new Response(stream,{status:200,headers:{...corsHeaders('text/event-stream; charset=utf-8'),'x-arix-log-stream':'sse'}});
}

export async function runSearch(input={}){
  const query=truncate(String(input.query??input.q??'').trim(),MAX_QUERY_LEN); if(!query)throw new Error('MISSING_QUERY'); return performSearch({...input,query},Date.now(),createLogger());
}
export const SEARCH_CONTRACT=Object.freeze({version:VERSION,maxResults:MAX_RESULTS,standalone:true,dependencies:[],searchSurfaces:['bing','google','duckduckgo','yahoo','mojeek','google-news','youtube'],ranking:'multi-engine-query-fusion',contentMode:'metadata-and-snippets-only',returnsAllDiscovered:true,optionalVerification:true,noTavily:true});

export default async function handler(req){
  if(req.method==='OPTIONS')return jsonResponse({ok:true,version:VERSION});
  if(!['GET','POST'].includes(req.method))return jsonResponse({ok:false,version:VERSION,error:'METHOD_NOT_ALLOWED',message:'Use GET or POST.'},405);
  try{
    const input=await readInput(req); const query=truncate(String(input.query??input.q??'').trim(),MAX_QUERY_LEN);
    if(!query)return jsonResponse({ok:false,version:VERSION,error:'MISSING_QUERY'},400); if(query.length<2)return jsonResponse({ok:false,version:VERSION,error:'QUERY_TOO_SHORT'},400);
    const wantsSse=String(input.logStream||'').toLowerCase()==='true'||req.headers.get('accept')?.includes('text/event-stream');
    return wantsSse?streamSseSearch(input):streamJsonSearch(input);
  }catch(error){const status=error?.message==='REQUEST_BODY_TOO_LARGE'?413:500;return jsonResponse({ok:false,version:VERSION,error:error?.message||'SEARCH_FAILED'},status)}
}
