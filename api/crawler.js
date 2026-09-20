/**
 * ArixAI Live Web Search / Crawler
 * v2.3.0 — Standalone Search Engine Fusion
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

const VERSION = 'arix-crawler-2.3.0';
const MAX_RESULTS = 40;
const DEFAULT_RESULTS = 10;
const MAX_QUERY_LEN = 1600;
const MAX_REQUEST_BODY = 100_000;
const SEARCH_BUDGET_MS = 8_800;
const SEARCH_TIMEOUT_MS = 1_050;
const VERIFY_TIMEOUT_MS = 850;
const VERIFY_HEADROOM_MS = 90;
const DISCOVERY_CUTOFF_MS = 3_600;
const SEARCH_CONCURRENCY = 24;
const VERIFY_CONCURRENCY = 18;
const MAX_ENGINE_REQUESTS = 24;
const MAX_DISCOVERY_RESULTS = 1000;
const MAX_LIVE_LOG = 80;
const OUTPUT_MAX_SOURCES = 1000;
const CACHE_MAX = 120;
const CACHE_TTL_MS = 7_000;
const LIVE_CACHE_TTL_MS = 2_500;
const CACHE = new Map();
const COMMON_CRAWL_TIMEOUT_MS = 650;
const MAX_COMMON_CRAWL = 4;
const COMMON_CRAWL_INDEXES = ['CC-MAIN-2026-34', 'CC-MAIN-2026-30'];
const AI_RERANK_TIMEOUT_MS = 700;
const USER_AGENT = 'Mozilla/5.0 (compatible; ArixAI-LiveSearch/2.1.0; +https://lexis-ai-chatini.vercel.app/)';

/* Only obvious tracking / measurement surfaces are blocked. Ordinary public sites,
 * PDFs, JS documentation, forums, media pages, etc. are not blanket-blocked. */
const BLOCKED_HOSTS = new Set([
  'google-analytics.com',
  'googletagmanager.com',
  'googlesyndication.com',
  'googleadservices.com',
  'doubleclick.net',
  'scorecardresearch.com',
  'pixel.wp.com',
  'adsrvr.org',
  'amazon-adsystem.com',
  'taboola.com',
  'outbrain.com',
  'segment.io',
  'hotjar.com',
  'clarity.ms',
]);

/* Strict search-result/source blacklist. These hosts are never allowed to become
 * candidate source URLs, even after redirect/unwrapping/canonicalization. This is
 * intentionally separate from SEARCH_HOSTS so the blacklist is enforced at every
 * candidate -> verification -> final-result boundary. */
const SEARCH_SOURCE_BLACKLIST = new Set([
  'google.com',
  'google.co.in',
  'google.co.uk',
  'google.de',
  'google.fr',
  'google.ca',
  'bing.com',
  'search.brave.com',
  'yahoo.com',
  'search.yahoo.com',
  'mojeek.com',
  'duckduckgo.com',
  'html.duckduckgo.com',
  'news.google.com',
  'ecosia.org',
  'yandex.com',
  'yandex.ru',
  'qwant.com',
  'startpage.com',
  'search.aol.com',
  'ask.com',
  'baidu.com',
  'sogou.com',
  'search.naver.com',
]);

const TRACKING_PATH = /(?:^|[\/_-])(?:analytics|gtag|ga4|collect|pixel|beacon|tracking|tracker|telemetry)(?:[\/_-]|$)/i;
const SEARCH_HOSTS = new Set([...SEARCH_SOURCE_BLACKLIST, 'youtube.com', 'www.youtube.com']);

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
  'indiacode.nic.in'
];

const TRUSTED_DOMAINS = [
  'who.int',
  'un.org',
  'europa.eu',
  'nasa.gov',
  'oecd.org',
  'worldbank.org',
  'imf.org',
  'ietf.org',
  'w3.org',
  'mozilla.org',
  'developer.mozilla.org',
  'nih.gov',
  'cdc.gov',
  'mit.edu',
  'stanford.edu',
  'harvard.edu'
];

const STOPWORDS = new Set([
  'a', 'an', 'the', 'and', 'or', 'of', 'to', 'in', 'on', 'for', 'with', 'from', 'by', 'as', 'at',
  'is', 'are', 'was', 'were', 'be', 'been', 'being', 'this', 'that', 'these', 'those', 'what',
  'when', 'where', 'how', 'why', 'who', 'which', 'about', 'into', 'near', 'over', 'under', 'than',
  'then', 'during', 'through', 'latest', 'current', 'recent', 'today', 'news', 'update', 'updates',
  'please', 'show', 'find', 'give', 'tell', 'me', 'can', 'you', 'i', 'we', 'it', 'its', 'their', 'our',
  'your', 'my', 'more', 'information', 'info', 'details', 'best', 'all', 'some', 'does', 'do', 'did',
  'explain', 'explained', 'need', 'want', 'using', 'use', 'from', 'vs', 'versus', 'compare', 'comparison'
]);

const SYNONYMS = [
  ['car', 'cars', 'automobile', 'automobiles', 'motorcar', 'motorcars'],
  ['history', 'historical'],
  ['india', 'indian'],
  ['price', 'prices', 'cost', 'costs', 'pricing', 'priced'],
  ['law', 'laws', 'legal', 'legislation'],
  ['policy', 'policies'],
  ['programme', 'program', 'programs'],
  ['company', 'companies', 'corporation', 'corporations', 'firm', 'firms'],
  ['population', 'populations'],
  ['people', 'residents', 'inhabitants'],
  ['economy', 'economic', 'economics'],
  ['market', 'markets'],
  ['education', 'educational'],
  ['school', 'schools'],
  ['student', 'students'],
  ['research', 'researches'],
  ['study', 'studies'],
  ['paper', 'papers'],
  ['report', 'reports'],
  ['technology', 'technologies', 'tech'],
  ['electric', 'electrical'],
  ['manufacturing', 'manufacture'],
  ['factory', 'factories'],
  ['guide', 'guides', 'tutorial', 'tutorials', 'manual'],
  ['documentation', 'docs'],
  ['ai', 'artificial-intelligence'],
];

const SYNONYM_MAP = new Map();
for (const group of SYNONYMS) {
  for (const term of group) {
    SYNONYM_MAP.set(term, group[0]);
  }
}

function nowIso() {
  return new Date().toISOString();
}

function left(deadline) {
  return Math.max(0, deadline - Date.now());
}

function clamp(v, min, max) {
  return Math.min(max, Math.max(min, Number(v) || 0));
}

function truncate(v, max) {
  const s = String(v ?? '').trim();
  return s.length <= max ? s : `${s.slice(0, max - 1)}…`;
}

function encode(v) {
  return JSON.stringify(v);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function env(name) {
  try {
    return typeof process !== 'undefined' ? String(process.env?.[name] || '').trim() : '';
  } catch {
    return '';
  }
}

function safeInt(v, fallback, min, max) {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? clamp(n, min, max) : fallback;
}

function unique(arr) {
  return [...new Set((arr || []).filter(Boolean))];
}

function normalizeText(v) {
  return String(v || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function rawTokens(v) {
  return normalizeText(v).split(' ').filter(Boolean);
}

function canonicalToken(v) {
  const x = String(v || '').toLowerCase();
  return SYNONYM_MAP.get(x) || x;
}

function stem(x) {
  let t = String(x || '').toLowerCase();
  if (t.length <= 4) return t;
  t = t.replace(/(ings|ies|ied)$/i, (m) => (m[0].toLowerCase() === 'i' ? 'y' : ''));
  t = t.replace(/(ing|ed|es)$/i, '');
  if (t.endsWith('s') && !t.endsWith('ss')) t = t.slice(0, -1);
  return t;
}

function contentTokens(v) {
  return rawTokens(v)
    .filter((x) => x.length > 1 && !STOPWORDS.has(x))
    .map((x) => canonicalToken(x));
}

function tokenSet(v) {
  return new Set(contentTokens(v));
}

function normalizeHost(url) {
  try {
    return new URL(url).hostname.toLowerCase().replace(/^www\./, '');
  } catch {
    return '';
  }
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
  } catch {
    return false;
  }
}

function blocked(url) {
  if (!safeUrl(url)) return true;
  const h = normalizeHost(url);
  if ([...BLOCKED_HOSTS].some((x) => h === x || h.endsWith(`.${x}`))) return true;
  if ([...SEARCH_SOURCE_BLACKLIST].some((x) => h === x || h.endsWith(`.${x}`))) return true;
  try {
    const u = new URL(url);
    const raw = `${u.href} ${decodeUri(u.href)}`;
    if (/(?:^|[/:.?=&_-])(?:www\.)?(?:google\.com|bing\.com|search\.brave\.com|yahoo\.com|mojeek\.com)(?:[/:?#=&_-]|$)/i.test(raw)) {
      return true;
    }
    return TRACKING_PATH.test(u.pathname);
  } catch {
    return true;
  }
}

function isGov(url) {
  const h = normalizeHost(url);
  return GOV_DOMAINS.some((d) => h === d || h.endsWith(`.${d}`));
}

function isTrusted(url) {
  const h = normalizeHost(url);
  return (
    TRUSTED_DOMAINS.some((d) => h === d || h.endsWith(`.${d}`)) ||
    /\.edu(?:\.|$)/i.test(h) ||
    /\.ac\.(?:in|uk|jp|nz)$/i.test(h)
  );
}

function isYouTube(url) {
  const h = normalizeHost(url);
  return h === 'youtube.com' || h.endsWith('.youtube.com') || h === 'youtu.be';
}

function isVideo(url) {
  return isYouTube(url) || /(?:vimeo\.com|dailymotion\.com)$/i.test(normalizeHost(url));
}

function isDoc(url) {
  return /\.(?:pdf|docx?|xlsx?|pptx?|csv|txt)(?:[?#]|$)/i.test(String(url || ''));
}

function isLikelySearchUrl(url) {
  const h = normalizeHost(url);
  try {
    const u = new URL(url);
    if (h === 'youtube.com' || h === 'www.youtube.com') {
      return !/^\/watch(?:$|\?)/i.test(u.pathname + u.search);
    }
    if (h === 'youtu.be') return false;
    return (
      SEARCH_HOSTS.has(h) &&
      (/\/search|\/url|\/ck\/a|\/l\/?|\/results/i.test(u.pathname + '?' + u.search) || h === 'news.google.com')
    );
  } catch {
    return false;
  }
}

function normalizedUrl(url) {
  try {
    const u = new URL(String(url));
    u.hash = '';
    for (const k of [...u.searchParams.keys()]) {
      if (/^(utm_|fbclid$|gclid$|dclid$|msclkid$|ref$|referrer$|cmpid$|src$|trk$|tracking)/i.test(k)) {
        u.searchParams.delete(k);
      }
    }
    return u.href;
  } catch {
    return '';
  }
}

function normalizedKey(url) {
  const u = normalizedUrl(url);
  if (!u) return '';
  try {
    const x = new URL(u);
    return `${x.hostname.toLowerCase().replace(/^www\./, '')}${x.pathname.replace(/\/+/g, '/').replace(/\/$/, '')}${x.search}`;
  } catch {
    return u.toLowerCase();
  }
}

function absoluteUrl(raw, base) {
  try {
    return new URL(String(raw || ''), base).href;
  } catch {
    return null;
  }
}

function decodeUri(v) {
  try {
    return decodeURIComponent(String(v || ''));
  } catch {
    return String(v || '');
  }
}

function unwrap(raw, base) {
  let current = absoluteUrl(raw, base);
  if (!current) return null;
  for (let depth = 0; depth < 5; depth++) {
    let u;
    try {
      u = new URL(current);
    } catch {
      return null;
    }
    const h = normalizeHost(u.href);
    let next = null;
    if (h === 'bing.com' && /^\/ck\/a/i.test(u.pathname)) {
      const val = u.searchParams.get('u') || u.searchParams.get('url') || u.searchParams.get('target');
      if (val) {
        const decoded = decodeUri(val);
        if (/^https?:\/\//i.test(decoded)) next = decoded;
        if (!next) {
          try {
            let s = val.replace(/-/g, '+').replace(/_/g, '/');
            while (s.length % 4) s += '=';
            const bin = atob(s);
            let decoded2 = '';
            for (let i = 0; i < bin.length; i++) decoded2 += String.fromCharCode(bin.charCodeAt(i));
            const t = new TextDecoder().decode(new Uint8Array([...decoded2].map((c) => c.charCodeAt(0))));
            if (/^https?:\/\//i.test(t)) next = t;
          } catch {}
        }
      }
    }
    if (!next && /^google\.[^./]+(?:\.[^./]+)?$/i.test(h) && /^\/url$/i.test(u.pathname)) {
      next = u.searchParams.get('url') || u.searchParams.get('q');
    }
    if (!next && (h === 'duckduckgo.com' || h === 'html.duckduckgo.com') && /^\/l\/?$/i.test(u.pathname)) {
      next = u.searchParams.get('uddg') || u.searchParams.get('u');
    }
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
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);?/gi, (_, x) => {
      const n = parseInt(x, 16);
      return Number.isFinite(n) ? String.fromCodePoint(Math.min(n, 0x10ffff)) : ' ';
    })
    .replace(/&#(\d+);?/g, (_, x) => {
      const n = parseInt(x, 10);
      return Number.isFinite(n) ? String.fromCodePoint(Math.min(n, 0x10ffff)) : ' ';
    })
    .replace(/\s+/g, ' ')
    .trim();
}

function cleanSnippet(v) {
  return truncate(stripTags(v).replace(/\s+/g, ' ').trim(), 3000);
}

function titleFromHtml(html) {
  return cleanSnippet((String(html).match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [, ''])[1]);
}

function metaFromHtml(html, name) {
  const e = String(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const a = String(html).match(new RegExp(`<meta[^>]+(?:name|property|itemprop)=["']${e}["'][^>]*content=["']([\\s\\S]*?)["']`, 'i'));
  const b = String(html).match(new RegExp(`<meta[^>]+content=["']([\\s\\S]*?)["'][^>]+(?:name|property|itemprop)=["']${e}["']`, 'i'));
  return cleanSnippet((a || b || [, ''])[1] || '');
}

function dateFromHtml(html) {
  const vals = [
    metaFromHtml(html, 'article:published_time'),
    metaFromHtml(html, 'datePublished'),
    metaFromHtml(html, 'dateModified'),
    metaFromHtml(html, 'date'),
    (String(html).match(/<time[^>]+datetime=["']([^"']+)["']/i) || [])[1]
  ];
  for (const v of vals) {
    const t = Date.parse(v || '');
    if (!Number.isNaN(t)) return new Date(t).toISOString();
  }
  return null;
}

function parseMaybeDateFromText(v) {
  const s = String(v || '');
  const candidates =
    s.match(
      /\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:,\s*|\s+)\d{4}\b|\b20\d{2}-\d{1,2}-\d{1,2}\b/gi
    ) || [];
  for (const c of candidates) {
    const t = Date.parse(c);
    if (!Number.isNaN(t)) return new Date(t).toISOString();
  }
  return null;
}

const QUERY_FILLERS = new Set([
  'available', 'allow', 'allows', 'allowed', 'also', 'answer', 'answers', 'around', 'based', 'can', 'could',
  'details', 'different', 'does', 'explain', 'explained', 'focus', 'give', 'help', 'important', 'include',
  'including', 'just', 'kind', 'kinds', 'known', 'look', 'looking', 'make', 'need', 'needs', 'provide', 'really',
  'should', 'show', 'specific', 'tell', 'things', 'ways', 'well', 'would', 'which', 'what', 'when', 'where', 'who',
  'why', 'how', 'latest', 'current', 'recent', 'today', 'tomorrow', 'yesterday', 'please', 'find', 'search', 'information',
  'information-on', 'information-about', 'details-on', 'details-about', 'using', 'use', 'used', 'want', 'would-like'
]);

function extractQuotedPhrases(q) {
  const out = [];
  for (const m of String(q || '').matchAll(/["“”]([^"“”]{4,180})["“”]/g)) {
    const x = normalizeText(m[1]);
    if (x) out.push(x);
  }
  return unique(out);
}

function selectCoreTerms(q, allTerms, anchors) {
  const quoted = extractQuotedPhrases(q);
  const filtered = allTerms.filter((t) => !QUERY_FILLERS.has(t));
  if (filtered.length <= 10) return unique([...quoted.flatMap(rawTokens), ...filtered]).slice(0, 12);
  const anchorSet = new Set((anchors || []).map(normalizeText));
  const scored = filtered
    .map((term, idx) => {
      let score = 0;
      if (anchorSet.has(term) || anchorSet.has(stem(term))) score += 5;
      if (/^20\d{2}$|^\d{4,}$/.test(term)) score += 4;
      if (term.length >= 9) score += 2.5;
      else if (term.length >= 6) score += 1.5;
      if (idx < 5) score += 1.0;
      if (SYNONYM_MAP.has(term)) score += 0.5;
      return { term, score, idx };
    })
    .sort((a, b) => b.score - a.score || a.idx - b.idx);
  const chosen = scored.slice(0, 10).sort((a, b) => a.idx - b.idx).map((x) => x.term);
  return unique([...quoted.flatMap(rawTokens), ...chosen]).slice(0, 12);
}

function queryPlan(query, options = {}) {
  const q = truncate(String(query || '').trim(), MAX_QUERY_LEN);
  const requested = String(options.type || '').toLowerCase();
  const mode = String(options.mode || 'auto').toLowerCase();
  let type = requested;
  if (!type || type === 'mixed' || type === 'all') {
    if (mode === 'news') type = 'news';
    else if (mode === 'video') type = 'video';
    else if (mode === 'gov') type = 'gov';
    else if (mode === 'doc' || mode === 'docs' || mode === 'document') type = 'doc';
    else type = 'web';
  }
  const content = contentTokens(q);
  const uniqueTerms = unique(content);
  const anchors = unique((q.match(/\b(?:[A-Z][A-Za-z0-9.-]{2,}|20\d{2}|[A-Z]{2,5})\b/g) || []).map(normalizeText));
  const coreTerms = selectCoreTerms(q, uniqueTerms, anchors);
  const quotedPhrases = extractQuotedPhrases(q);
  const live = /\b(latest|today|current|recent|breaking|this week|this month|yesterday|newly|as of)\b/i.test(q);
  const history = /\b(history|historical|timeline|origins?|evolution|development|milestones)\b/i.test(q);
  const official = /\b(official|government|govt|ministry|scheme|policy|law|act|rule|regulation|tax|gst|rbi|sebi|mca|notification|circular|guideline|statute)\b/i.test(q);
  const academic = /\b(research|study|paper|academic|journal|thesis|evidence|peer[- ]reviewed|literature)\b/i.test(q);
  const exactish = coreTerms.length <= 7 ? coreTerms.join(' ') : coreTerms.slice(0, 7).join(' ');
  const coreQuery = coreTerms.join(' ').trim();

  return {
    query: q,
    type,
    requestedType: requested,
    mode,
    terms: uniqueTerms,
    coreTerms,
    coreQuery,
    quotedPhrases,
    exactish,
    anchors,
    longQuery: q.length > 520 || uniqueTerms.length > 12,
    flags: {
      live,
      history,
      official,
      academic,
      explicitNews: type === 'news',
      explicitVideo: type === 'video',
      explicitDoc: type === 'doc',
      explicitGov: type === 'gov'
    }
  };
}

function buildQueries(query, plan, deep = false) {
  const set = new Set();
  const add = (s) => {
    const x = truncate(String(s || '').replace(/\s+/g, ' ').trim(), 460);
    if (x.length >= 3) set.add(x);
  };

  const original = plan.query;
  const core = plan.coreQuery || plan.exactish || original;
  const subject = plan.coreTerms.slice(0, Math.min(6, plan.coreTerms.length)).join(' ').trim();

  /* For long questions, do NOT blindly send the entire natural-language prompt to
   * every engine. Preserve the original only as one fallback arm; use its extracted
   * subject/constraints for the high-precision arms. */
  if (!plan.longQuery && original.length <= 520) add(original);
  else if (subject) add(subject);

  if (plan.quotedPhrases.length) {
    for (const phrase of plan.quotedPhrases.slice(0, 2)) add(`"${phrase}"`);
  }
  if (core && core !== original) add(core);
  if (subject && subject !== core) add(subject);
  if (plan.flags.live) add(`${core} latest`);
  if (plan.flags.history) add(`${core} history timeline`);
  if (plan.flags.official) add(`${core} official source`);
  if (plan.flags.academic) add(`${core} research evidence`);
  if (plan.flags.explicitNews) add(`${core} latest news`);
  if (plan.flags.explicitVideo) add(`${core} video`);
  if (plan.flags.explicitDoc) add(`${core} filetype:pdf`);
  if (plan.flags.explicitGov) add(`${core} site:gov.in`);

  if ((plan.mode === 'deep' || deep) && plan.coreTerms.length) {
    add(`intitle:${plan.coreTerms.slice(0, Math.min(3, plan.coreTerms.length)).join(' ')} ${subject || core}`);
    if (plan.anchors.length) add(`${subject || core} ${plan.anchors.slice(0, 2).join(' ')}`);
  }

  const preferred = unique([...set]);
  return preferred.slice(0, deep ? 8 : 6);
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
    title: truncate(cleanSnippet(title), 500) || truncate(cleanSnippet(new URL(clean).hostname), 300),
    url: clean,
    snippet: cleanSnippet(snippet || ''),
    source: provider,
    type: t,
    providerRank: Number(extra.providerRank || 0),
    queryVariant: Number(extra.queryVariant || 0),
    publishedAt: extra.publishedAt || parseMaybeDateFromText(`${title} ${snippet}`),
    searchWrapperResolved: Boolean(extra.searchWrapperResolved),
  };
  delete extra.base;
  delete extra.providerRank;
  delete extra.queryVariant;
  delete extra.publishedAt;
  delete extra.searchWrapperResolved;
  Object.assign(result, extra);
  if (!result.title || result.title.length < 2) return null;
  return result;
}

function decodeEntities(v = '') {
  return String(v)
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);?/gi, (_, x) => {
      const n = parseInt(x, 16);
      return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : ' ';
    })
    .replace(/&#(\d+);?/g, (_, x) => {
      const n = parseInt(x, 10);
      return Number.isFinite(n) ? String.fromCodePoint(Math.min(0x10ffff, n)) : ' ';
    });
}

function searchTitle(v) {
  const t = cleanSnippet(decodeEntities(v || '')).replace(/\s+/g, ' ').trim();
  if (!t || t.length < 2 || /^(images?|videos?|maps?|news|shopping|settings|sign in|log in|menu|more|next|previous|feedback|tools)$/i.test(t)) {
    return '';
  }
  return truncate(t, 500);
}

function hasSearchNoiseTitle(v) {
  const t = normalizeText(v);
  return !t || /^(sponsored|ad|advertisement|ads|sign in|log in|privacy|terms|cookie|feedback|more results|related searches|people also ask|images|videos|maps|shopping|news)$/.test(t);
}

function resultContext(html, index, width = 2500) {
  const s = String(html || '');
  return s.slice(Math.max(0, index - Math.floor(width * 0.35)), Math.min(s.length, index + width));
}

function nearbySnippet(html, index) {
  const w = resultContext(html, index, 2600).replace(/<(script|style|noscript|svg|template|form)\b[\s\S]*?<\/\1>/gi, ' ');
  return cleanSnippet(stripTags(w));
}

function scoreSearchAnchor(url, title, context, provider) {
  let score = 0;
  const h = normalizeHost(url);
  const t = normalizeText(title);
  const c = normalizeText(context);

  if (!safeUrl(url) || blocked(url) || isLikelySearchUrl(url)) return -999;
  if (hasSearchNoiseTitle(title)) return -999;
  if (title.length >= 12) score += 8;
  if (title.length >= 30) score += 5;
  if (title.length <= 180) score += 3;
  else score -= 4;

  if (/\b(home|homepage|about us|contact us|careers|login|sign in|privacy policy|terms of service)\b/i.test(title)) score -= 8;
  if (/\b(sponsored|advertisement|ad)\b/i.test(title)) score -= 18;
  if (new URL(url).pathname && new URL(url).pathname !== '/') score += 3;
  if (/\b(?:result|organic|web-result|b_algo|MjjYud|result__a|algo|search-result)\b/i.test(context)) score += 12;
  if (/<(?:h1|h2|h3|h4)\b/i.test(context)) score += 8;
  if (/\b(?:sponsored|advertisement|ads by)\b/i.test(c)) score -= 20;

  if (provider === 'bing' && /b_algo/i.test(context)) score += 12;
  if (provider === 'google' && /MjjYud|g\.h3|<h3/i.test(context)) score += 8;
  if (provider === 'duckduckgo' && /result__a|result__body|results_links/i.test(context)) score += 12;
  if (provider === 'yahoo' && /search-result|algo-sr/i.test(context)) score += 8;
  if (provider === 'mojeek' && /result|title/i.test(context)) score += 6;
  if (provider === 'brave' && /snippet|result|fdb/i.test(context)) score += 7;

  if (isGov(url)) score += 4;
  if (isTrusted(url)) score += 3;
  if (/\.(pdf|docx?|xlsx?|pptx?)$/i.test(new URL(url).pathname)) score += 2;
  if (h === 'youtube.com' || h === 'youtu.be') score += 2;

  return score;
}

function extractAnchors(html, base, provider, req, cap = 80) {
  const out = [];
  const s = String(html || '');
  const seen = new Set();
  const re = /<a\b([^>]*?)\bhref\s*=\s*(["'])([\s\S]*?)\2([^>]*)>([\s\S]*?)<\/a\s*>/gi;
  let m;
  while ((m = re.exec(s)) && out.length < cap * 4) {
    const raw = decodeEntities(m[3]).trim();
    const url = unwrap(raw, base);
    if (!url || blocked(url) || isLikelySearchUrl(url)) continue;
    const title = searchTitle(m[5]);
    if (!title) continue;
    const context = resultContext(s, m.index || 0, 3000);
    const score = scoreSearchAnchor(url, title, context, provider);
    if (score < 2) continue;
    const key = normalizedKey(url);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({
      candidate: makeCandidate(url, title, nearbySnippet(s, m.index || 0), provider, req.type, {
        base,
        providerRank: out.length + 1,
        queryVariant: req.queryVariant,
        searchWrapperResolved: true
      }),
      score
    });
  }
  out.sort((a, b) => b.score - a.score);
  return out.filter((x) => x.candidate).slice(0, cap).map((x) => x.candidate);
}

function extractHeadingAnchors(html, base, provider, req, heading = 'h3', cap = 40) {
  const s = String(html || '');
  const out = [];
  const seen = new Set();
  const re = new RegExp(`<${heading}\\b[^>]*>([\\s\\S]*?)<\\/${heading}\\s*>`, 'gi');
  let m;
  while ((m = re.exec(s)) && out.length < cap) {
    const idx = m.index || 0;
    const before = s.slice(Math.max(0, idx - 1600), idx);
    const after = s.slice(idx, Math.min(s.length, idx + 1600));
    const around = `${before}${after}`;
    const links = [];
    for (const lm of around.matchAll(/<a\b[^>]*\bhref\s*=\s*(["'])([\s\S]*?)\1[^>]*>/gi)) {
      links.push(lm[2]);
    }
    for (const raw of links.reverse()) {
      const url = unwrap(decodeEntities(raw), base);
      if (!url || blocked(url) || isLikelySearchUrl(url)) continue;
      const title = searchTitle(m[1]);
      if (!title) continue;
      const key = normalizedKey(url);
      if (!key || seen.has(key)) continue;
      const score = scoreSearchAnchor(url, title, around, provider) + 12;
      if (score < 8) continue;
      seen.add(key);
      const c = makeCandidate(url, title, nearbySnippet(s, idx), provider, req.type, {
        base,
        providerRank: out.length + 1,
        queryVariant: req.queryVariant,
        searchWrapperResolved: true
      });
      if (c) {
        out.push(c);
        break;
      }
    }
  }
  return out;
}

function extractEmbeddedResults(html, base, provider, req, cap = 40) {
  const s = String(html || '');
  const out = [];
  const seen = new Set();
  const patterns = [
    /"(?:url|link|targetUrl|resultUrl)"\s*:\s*"(https?:\/\/[^"]+)"/gi,
    /(?:url|href)=["'](https?:\/\/[^"']+)["']/gi
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(s)) && out.length < cap) {
      const url = unwrap(m[1].replace(/\\\//g, '/'), base);
      if (!url || blocked(url) || isLikelySearchUrl(url)) continue;
      const key = normalizedKey(url);
      if (!key || seen.has(key)) continue;
      const idx = m.index || 0;
      const window = resultContext(s, idx, 1800);
      const textMatches = [];
      for (const tm of window.matchAll(/"(?:title|name|text|displayName)"\s*:\s*"((?:\\.|[^"\\]){5,280})"/gi)) {
        textMatches.push(tm[1]);
      }
      const title = searchTitle(textMatches[0] || stripTags(window).slice(0, 240) || normalizeHost(url));
      if (!title) continue;
      const score = scoreSearchAnchor(url, title, window, provider);
      if (score < 2) continue;
      const c = makeCandidate(url, title, nearbySnippet(s, idx), provider, req.type, {
        base,
        providerRank: out.length + 1,
        queryVariant: req.queryVariant,
        searchWrapperResolved: true
      });
      if (c) {
        seen.add(key);
        out.push(c);
      }
    }
  }
  return out;
}

function parseRssSearch(text, req, provider = req.provider) {
  const out = [];
  const items = String(text || '').match(/<item\b[\s\S]*?<\/item\s*>/gi) || [];
  for (let i = 0; i < items.length && out.length < 80; i++) {
    const item = items[i];
    const title = searchTitle((item.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [, ''])[1]);
    const link = decodeEntities((item.match(/<link[^>]*>([\s\S]*?)<\/link>/i) || [, ''])[1]);
    const desc = cleanSnippet((item.match(/<(?:description|content:encoded)[^>]*>([\s\S]*?)<\/(?:description|content:encoded)>/i) || [, ''])[1]);
    const pub = decodeEntities((item.match(/<(?:pubDate|published|updated)[^>]*>([\s\S]*?)<\/(?:pubDate|published|updated)>/i) || [, ''])[1]);
    const c = makeCandidate(link, title, desc, provider, req.type, {
      publishedAt: safeDate(pub),
      providerRank: out.length + 1,
      queryVariant: req.queryVariant
    });
    if (c) out.push(c);
  }
  return out;
}

function parseBing(html, req) {
  const s = String(html || '');
  const primary = [];
  for (const block of s.match(/<li[^>]+class=["'][^"']*b_algo[^"']*["'][\s\S]*?<\/li>/gi) || []) {
    const m = block.match(/<a\b[^>]*href\s*=\s*(["'])([\s\S]*?)\1[^>]*>([\s\S]*?)<\/a\s*>/i);
    if (!m) continue;
    const t = searchTitle(m[3]);
    const c = makeCandidate(m[2], t, (block.match(/<p[^>]*>([\s\S]*?)<\/p>/i) || [, ''])[1], 'bing', req.type, {
      base: 'https://www.bing.com/',
      providerRank: primary.length + 1,
      queryVariant: req.queryVariant,
      searchWrapperResolved: true
    });
    if (c) primary.push(c);
  }
  return primary.length >= 3
    ? primary.slice(0, 40)
    : [
        ...primary,
        ...extractHeadingAnchors(s, 'https://www.bing.com/', 'bing', req, 'h2', 40),
        ...extractAnchors(s, 'https://www.bing.com/', 'bing', req, 40),
        ...extractEmbeddedResults(s, 'https://www.bing.com/', 'bing', req, 20)
      ]
        .filter((v, i, a) => a.findIndex((x) => normalizedKey(x.url) === normalizedKey(v.url)) === i)
        .slice(0, 40);
}

function parseGoogle(html, req, source = 'google') {
  const s = String(html || '');
  const primary = extractHeadingAnchors(s, 'https://www.google.com/', source, req, 'h3', 40);
  if (primary.length >= 3) return primary;
  const generic = extractAnchors(s, 'https://www.google.com/', source, req, 40);
  const embedded = extractEmbeddedResults(s, 'https://www.google.com/', source, req, 30);
  return [...primary, ...generic, ...embedded]
    .filter((v, i, a) => a.findIndex((x) => normalizedKey(x.url) === normalizedKey(v.url)) === i)
    .slice(0, 40);
}

function parseDuck(html, req) {
  const s = String(html || '');
  const out = [];
  for (const m of s.matchAll(/<a\b[^>]*class=["'][^"']*result__a[^"']*["'][^>]*href\s*=\s*(["'])([\s\S]*?)\1[^>]*>([\s\S]*?)<\/a\s*>/gi)) {
    const c = makeCandidate(m[2], searchTitle(m[3]), nearbySnippet(s, m.index || 0), 'duckduckgo', req.type, {
      base: 'https://html.duckduckgo.com/',
      providerRank: out.length + 1,
      queryVariant: req.queryVariant,
      searchWrapperResolved: true
    });
    if (c) out.push(c);
  }
  if (out.length >= 3) return out.slice(0, 40);
  return [...out, ...extractAnchors(s, 'https://html.duckduckgo.com/', 'duckduckgo', req, 40)]
    .filter((v, i, a) => a.findIndex((x) => normalizedKey(x.url) === normalizedKey(v.url)) === i)
    .slice(0, 40);
}

function parseYahoo(html, req) {
  const s = String(html || '');
  const primary = [];
  for (const m of s.matchAll(/<h3\b[^>]*>[\s\S]*?<a\b[^>]*href\s*=\s*(["'])([\s\S]*?)\1[^>]*>([\s\S]*?)<\/a\s*>[\s\S]*?<\/h3\s*>/gi)) {
    const c = makeCandidate(m[2], searchTitle(m[3]), nearbySnippet(s, m.index || 0), 'yahoo', req.type, {
      base: 'https://search.yahoo.com/',
      providerRank: primary.length + 1,
      queryVariant: req.queryVariant,
      searchWrapperResolved: true
    });
    if (c) primary.push(c);
  }
  return [...primary, ...extractAnchors(s, 'https://search.yahoo.com/', 'yahoo', req, 40)]
    .filter((v, i, a) => a.findIndex((x) => normalizedKey(x.url) === normalizedKey(v.url)) === i)
    .slice(0, 40);
}

function parseMojeek(html, req) {
  const s = String(html || '');
  const out = [];
  for (const m of s.matchAll(/<a\b[^>]*href\s*=\s*(["'])([\s\S]*?)\1[^>]*class=["'][^"']*(?:title|ob|result)[^"']*["'][^>]*>([\s\S]*?)<\/a\s*>/gi)) {
    const c = makeCandidate(m[2], searchTitle(m[3]), nearbySnippet(s, m.index || 0), 'mojeek', req.type, {
      base: 'https://www.mojeek.com/',
      providerRank: out.length + 1,
      queryVariant: req.queryVariant,
      searchWrapperResolved: true
    });
    if (c) out.push(c);
  }
  return [...out, ...extractAnchors(s, 'https://www.mojeek.com/', 'mojeek', req, 40)]
    .filter((v, i, a) => a.findIndex((x) => normalizedKey(x.url) === normalizedKey(v.url)) === i)
    .slice(0, 40);
}

function parseGoogleNews(xml, req) {
  return parseRssSearch(xml, req, 'google-news');
}

function parseYoutube(html, req) {
  const out = [];
  const seen = new Set();
  const s = String(html || '');
  for (const m of s.matchAll(/"videoRenderer"\s*:\s*\{[\s\S]*?"videoId"\s*:\s*"([A-Za-z0-9_-]{6,20})"[\s\S]*?"title"\s*:\s*\{\s*"runs"\s*:\s*\[\s*\{\s*"text"\s*:\s*"((?:\\.|[^"\\])*)"/g)) {
    const id = m[1];
    if (seen.has(id)) continue;
    seen.add(id);
    const title = searchTitle(m[2].replace(/\\"/g, '"').replace(/\\\\/g, '\\')) || `YouTube video ${id}`;
    const c = makeCandidate(`https://www.youtube.com/watch?v=${id}`, title, 'YouTube result', 'youtube', 'video', {
      providerRank: out.length + 1,
      queryVariant: req.queryVariant
    });
    if (c) out.push(c);
    if (out.length >= 40) break;
  }
  if (out.length < 3) {
    for (const m of s.matchAll(/"videoId"\s*:\s*"([A-Za-z0-9_-]{6,20})"/g)) {
      const id = m[1];
      if (seen.has(id)) continue;
      seen.add(id);
      const c = makeCandidate(`https://www.youtube.com/watch?v=${id}`, `YouTube video ${id}`, 'YouTube result', 'youtube', 'video', {
        providerRank: out.length + 1,
        queryVariant: req.queryVariant
      });
      if (c) out.push(c);
      if (out.length >= 40) break;
    }
  }
  return out;
}

function parseBrave(html, req) {
  const s = String(html || '');
  const a = extractHeadingAnchors(s, 'https://search.brave.com/', 'brave', req, 'h2', 40);
  const b = extractHeadingAnchors(s, 'https://search.brave.com/', 'brave', req, 'h3', 40);
  const c = extractAnchors(s, 'https://search.brave.com/', 'brave', req, 40);
  return [...a, ...b, ...c]
    .filter((v, i, arr) => arr.findIndex((x) => normalizedKey(x.url) === normalizedKey(v.url)) === i)
    .slice(0, 40);
}

function safeDate(value) {
  const t = Date.parse(String(value || ''));
  return Number.isNaN(t) ? null : new Date(t).toISOString();
}

function freshness(publishedAt) {
  if (!publishedAt) return 'unknown';
  const t = Date.parse(publishedAt);
  if (Number.isNaN(t)) return 'unknown';
  const age = Math.max(0, (Date.now() - t) / 86400000);
  if (age <= 1) return 'last_24h';
  if (age <= 7) return 'last_7d';
  if (age <= 30) return 'last_30d';
  if (age <= 90) return 'last_90d';
  if (age <= 365) return 'last_year';
  return 'older';
}

function providerRequests(queries, plan, deep) {
  const out = [];
  const seen = new Set();
  const add = (provider, q, type, url, queryVariant) => {
    if (out.length >= MAX_ENGINE_REQUESTS || !url || seen.has(url)) return;
    seen.add(url);
    out.push({ provider, query: q, type, url, queryVariant });
  };

  const qs = queries.slice(0, deep ? 4 : 3);
  const base = qs[0] || plan.query;
  const e0 = encodeURIComponent(base);
  const bingPages = deep ? 3 : 2;

  for (let first = 0; first < bingPages; first++) {
    add(
      'bing',
      base,
      plan.type === 'news' ? 'news' : 'web',
      `https://www.bing.com/search?q=${e0}&count=10&first=${first * 10}&form=QBLH&setlang=en-IN&cc=in`,
      0
    );
  }
  add(
    'bing-rss',
    base,
    plan.type === 'news' ? 'news' : 'web',
    `https://www.bing.com/search?format=rss&q=${e0}&setlang=en-IN&cc=in`,
    0
  );

  for (let i = 0; i < qs.length; i++) {
    const q = qs[i];
    const e = encodeURIComponent(q);
    add('google', q, plan.type === 'news' ? 'news' : 'web', `https://www.google.com/search?gbv=1&q=${e}&num=20&hl=en&gl=in&filter=0`, i);
    if (i < 2) add('duckduckgo', q, plan.type === 'news' ? 'news' : 'web', `https://html.duckduckgo.com/html/?q=${e}&kl=in-en`, i);
    if (i === 0) add('duck-lite', q, plan.type === 'news' ? 'news' : 'web', `https://lite.duckduckgo.com/lite/?q=${e}&kl=in-en`, i);
    if (i < 2) add('yahoo', q, plan.type === 'news' ? 'news' : 'web', `https://search.yahoo.com/search?p=${e}&fr=yfp-t`, i);
    if (i < 2) add('mojeek', q, plan.type === 'news' ? 'news' : 'web', `https://www.mojeek.com/search?q=${e}&lb=EN&lbb=100&rb=IN&rbb=10&fmt=html`, i);
    if (i === 0) add('brave', q, plan.type === 'news' ? 'news' : 'web', `https://search.brave.com/search?q=${e}&source=web`, i);
  }

  if (plan.flags.live || plan.type === 'news') {
    for (let i = 0; i < Math.min(2, qs.length); i++) {
      const q = qs[i];
      add('google-news', q, 'news', `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en-IN&gl=IN&ceid=IN:en`, i);
    }
  }

  if (plan.type === 'video' || plan.flags.explicitVideo) {
    for (let i = 0; i < Math.min(2, qs.length); i++) {
      const q = qs[i];
      add('youtube', q, 'video', `https://www.youtube.com/results?search_query=${encodeURIComponent(q)}&hl=en`, i);
    }
  }

  if (plan.type === 'doc' || plan.flags.explicitDoc) {
    for (let i = 0; i < Math.min(2, qs.length); i++) {
      const q = qs[i];
      add('google-doc', q, 'doc', `https://www.google.com/search?gbv=1&q=${encodeURIComponent(`${q} filetype:pdf`)}&num=20&hl=en&gl=in&filter=0`, i);
    }
  }

  if (plan.type === 'gov' || plan.flags.explicitGov) {
    for (let i = 0; i < Math.min(2, qs.length); i++) {
      const q = qs[i];
      add('google-gov', q, 'gov', `https://www.google.com/search?gbv=1&q=${encodeURIComponent(`${q} site:gov.in`)}&num=20&hl=en&gl=in&filter=0`, i);
    }
  }

  return out;
}

async function fetchWithTimeout(url, timeout, deadline, headers = {}) {
  const budget = Math.min(timeout, Math.max(250, left(deadline) - VERIFY_HEADROOM_MS));
  if (budget <= 0) throw new Error('BUDGET_EXHAUSTED');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), budget);
  try {
    return await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'user-agent': USER_AGENT,
        accept: 'text/html,application/xhtml+xml,application/xml,application/rss+xml,text/plain;q=0.9,application/pdf;q=0.8,*/*;q=0.1',
        'accept-language': 'en-IN,en;q=0.9',
        ...headers
      }
    });
  } finally {
    clearTimeout(timer);
  }
}

async function readLimited(res, maxBytes, deadline) {
  const reader = res.body?.getReader?.();
  if (!reader) return truncate(await res.text(), maxBytes);
  const chunks = [];
  let total = 0;
  try {
    while (left(deadline) > 50 && total < maxBytes) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      const room = maxBytes - total;
      const c = value.byteLength > room ? value.slice(0, room) : value;
      chunks.push(c);
      total += c.byteLength;
    }
  } finally {
    try {
      await reader.cancel();
    } catch {}
  }
  const bytes = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    bytes.set(c, off);
    off += c.byteLength;
  }
  return new TextDecoder('utf-8', { fatal: false }).decode(bytes);
}

async function fetchSearch(req, deadline) {
  const res = await fetchWithTimeout(req.url, SEARCH_TIMEOUT_MS, deadline, {
    accept:
      req.provider === 'google-news'
        ? 'application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.1'
        : 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.1'
  });
  if (!res.ok) throw new Error(`HTTP_${res.status}`);
  return readLimited(res, 750_000, deadline);
}

async function discoverOne(req, deadline) {
  try {
    const body = await fetchSearch(req, deadline);
    let results = [];
    if (req.provider === 'bing') results = parseBing(body, req);
    else if (req.provider === 'google' || req.provider === 'google-video' || req.provider === 'google-doc' || req.provider === 'google-gov') {
      results = parseGoogle(body, req, req.provider);
    } else if (req.provider === 'duckduckgo' || req.provider === 'duck-lite') results = parseDuck(body, req);
    else if (req.provider === 'yahoo') results = parseYahoo(body, req);
    else if (req.provider === 'mojeek') results = parseMojeek(body, req);
    else if (req.provider === 'brave') results = parseBrave(body, req);
    else if (req.provider === 'bing-rss') results = parseRssSearch(body, req, 'bing-rss');
    else if (req.provider === 'google-news') results = parseGoogleNews(body, req);
    else if (req.provider === 'youtube') results = parseYoutube(body, req);

    if (!results.length && !['google-news', 'bing-rss', 'youtube'].includes(req.provider)) {
      results = extractAnchors(body, req.url, req.provider, req, 40);
    }
    return { provider: req.provider, ok: true, results };
  } catch (error) {
    return { provider: req.provider, ok: false, results: [], error: error?.message || 'DISCOVERY_FAILED' };
  }
}

function dedupe(list) {
  const map = new Map();
  for (const raw of list || []) {
    if (!raw?.url) continue;
    const url = normalizedUrl(raw.url);
    if (!url || blocked(url) || isLikelySearchUrl(url)) continue;
    const key = normalizedKey(url);
    if (!key) continue;
    const old = map.get(key);
    if (!old) {
      map.set(key, { ...raw, url });
    } else {
      const providers = new Set([...(old.providers || [old.source]), ...(raw.providers || [raw.source])].filter(Boolean));
      const better = (raw.providerRank || 99) < (old.providerRank || 99);
      const merged = {
        ...old,
        ...raw,
        url,
        providers: [...providers],
        source: providers.size > 1 ? 'multi-source' : better ? raw.source : old.source
      };
      if (!raw.snippet && old.snippet) merged.snippet = old.snippet;
      if (!raw.publishedAt && old.publishedAt) merged.publishedAt = old.publishedAt;
      map.set(key, merged);
    }
  }
  return [...map.values()].slice(0, MAX_DISCOVERY_RESULTS);
}

function titleTerms(text, terms) {
  const norm = normalizeText(text);
  const set = tokenSet(text);
  let hits = 0;
  const matched = [];
  for (const term of terms) {
    const c = canonicalToken(term);
    if (set.has(c) || set.has(stem(c)) || norm.includes(` ${c} `)) {
      hits++;
      matched.push(c);
    }
  }
  return { hits, matched: unique(matched) };
}

function phraseScore(query, text) {
  const q = normalizeText(query);
  const t = normalizeText(text);
  if (!q || !t) return 0;
  if (t.includes(q)) return 1;
  const qs = contentTokens(q).slice(0, 8);
  if (qs.length < 2) return 0;
  const joined = qs.join(' ');
  if (t.includes(joined)) return 0.82;
  let run = 0,
    best = 0;
  const tt = contentTokens(t);
  let pos = 0;
  for (const qx of qs) {
    const i = tt.indexOf(qx, pos);
    if (i >= 0) {
      run++;
      pos = i + 1;
      best = Math.max(best, run);
    } else {
      run = 0;
    }
  }
  return best / qs.length >= 0.7 ? 0.58 : best / qs.length >= 0.5 ? 0.35 : 0;
}

function domainQuality(url) {
  const h = normalizeHost(url);
  let score = 0.55;
  if (isGov(url)) score = 0.98;
  else if (isTrusted(url)) score = 0.93;
  else if (/\.edu(?:\.|$)/i.test(h) || /\.ac\.(?:in|uk|jp|nz)$/i.test(h)) score = 0.9;
  else if (
    /(?:reuters|apnews|bbc|theguardian|nytimes|economist|nature|science|arxiv|nasa|who|wikipedia|britannica|investopedia|microsoft|google|apple|ibm|intel|mitre|github)\./i.test(
      h
    )
  )
    score = 0.86;
  else if (/(?:medium|quora|reddit|facebook|pinterest|instagram|tiktok)\./i.test(h)) score = 0.48;
  const labels = h.split('.');
  if (labels.length <= 1) score -= 0.08;
  return clamp(score, 0, 1);
}

function urlIntentScore(url, plan) {
  const s = String(url || '').toLowerCase();
  let x = 0;
  if (plan.flags.history && /history|timeline|evolution|origin|milestone/.test(s)) x += 5;
  if (plan.flags.academic && /paper|research|study|journal|arxiv|doi/.test(s)) x += 5;
  if (plan.flags.official && (isGov(url) || /official|policy|notification|circular/.test(s))) x += 6;
  if (plan.type === 'doc' && isDoc(url)) x += 9;
  if (plan.type === 'video' && isVideo(url)) x += 8;
  if (plan.type === 'news' && /news|article|story|press|reuters|apnews|bbc/.test(s)) x += 5;
  if (/(?:\/tag\/|\/tags\/|\/category\/|\/categories\/|\/search[/?]|\/author\/|\/authors\/|\/topic\/|\/topics\/|\/(?:home|homepage)\/?$)/i.test(s)) x -= 6;
  return x;
}

function weightedCoverage(text, terms) {
  const norm = normalizeText(text);
  if (!norm || !terms.length) return { coverage: 0, hits: 0, matched: [] };
  let total = 0,
    matchedWeight = 0,
    hits = 0;
  const matched = [];
  for (const term of terms) {
    const c = canonicalToken(term);
    const weight = 1 + Math.min(1.2, Math.max(0, String(c).length - 4) * 0.09);
    total += weight;
    const variants = unique([c, stem(c)]);
    const hit = variants.some((v) => v && (tokenSet(norm).has(v) || norm.includes(` ${v} `)));
    if (hit) {
      matchedWeight += weight;
      hits++;
      matched.push(c);
    }
  }
  return { coverage: total ? matchedWeight / total : 0, hits, matched: unique(matched) };
}

function querySpecificity(plan) {
  const n = plan.coreTerms?.length || plan.terms?.length || 0;
  if (n >= 14) return 1.25;
  if (n >= 10) return 1.15;
  if (n >= 7) return 1.05;
  return 0.95;
}

function genericPagePenalty(url, title) {
  const s = String(url || '').toLowerCase();
  const t = normalizeText(title);
  let p = 0;
  if (/(?:^|\/)(?:tag|tags|category|categories|topic|topics|author|authors|search|results|archive|archives)(?:\/|\?|$)/i.test(s)) p += 10;
  if (/(?:^|\/)(?:home|homepage|index)(?:\.[a-z0-9]+)?$/i.test(s) || s.endsWith('/')) p += 3;
  if (/^(?:home|homepage|welcome|search results|results|index|untitled)$/i.test(t)) p += 9;
  if (/\b(?:login|sign in|subscribe|contact us|privacy policy|terms of service)\b/i.test(t)) p += 8;
  return p;
}

function rankOne(item, plan) {
  const title = String(item.title || '');
  const snippet = String(item.snippet || '');
  const url = String(item.url || '');
  const terms = (plan.coreTerms?.length ? plan.coreTerms : plan.terms) || [];
  if (!terms.length) return 35;
  const t = weightedCoverage(title, terms);
  const s = weightedCoverage(snippet, terms);
  const u = weightedCoverage(url.replace(/[-_/?.=&]+/g, ' '), terms);
  const probe = weightedCoverage(item.pageProbeText || '', terms);
  const queryForPhrase = plan.quotedPhrases?.[0] || plan.coreQuery || plan.query;
  const phrase = phraseScore(queryForPhrase, `${title} ${snippet} ${item.pageProbeText || ''}`);
  const consensus = Math.min(1, ((item.providers?.length || 1) - 1) / 3);
  const qRank = item.providerRank > 0 ? Math.max(0, 1 - (item.providerRank - 1) / 35) : 0.3;
  const dq = domainQuality(url);
  const freshnessBonus = plan.flags.live ? { last_24h: 8, last_7d: 5, last_30d: 2 }[freshness(item.publishedAt)] || 0 : 0;
  const specificity = querySpecificity(plan);

  let score = 0;
  score += t.coverage * 40 * specificity;
  score += s.coverage * 20;
  score += Math.min(0.75, u.coverage) * 5;
  score += phrase * 16;
  score += probe.coverage * 8;
  score += consensus * 5;
  score += qRank * 4;
  score += dq * 5;
  score += urlIntentScore(url, plan);
  score += freshnessBonus;
  if (item.source === 'multi-source') score += 3;
  if (item.verified) score += 4;
  if (plan.flags.live && !item.publishedAt && /\b20\d{2}\b/.test(`${title} ${snippet}`)) score += 1;

  /* Soft negative evidence: generic pages are pushed down, but never hard-dropped. */
  score -= genericPagePenalty(url, title);
  if (t.coverage < 0.2 && s.coverage < 0.25) score -= plan.longQuery ? 9 : 6;
  if (t.coverage < 0.12 && phrase < 0.35) score -= 5;
  if (plan.type === 'gov' && !isGov(url)) score -= 8;
  if (plan.type === 'doc' && !isDoc(url)) score -= 10;
  if (plan.type === 'video' && !isVideo(url)) score -= 12;
  if (plan.type === 'news' && item.type !== 'news' && !/\/(?:news|article|story|press)/i.test(url)) score -= 7;

  return clamp(Math.round(score * 100) / 100, 0, 100);
}

function relevanceObject(item, plan) {
  const terms = (plan.coreTerms?.length ? plan.coreTerms : plan.terms) || [];
  const title = weightedCoverage(item.title, terms);
  const body = weightedCoverage(item.snippet, terms);
  const phrase = phraseScore(plan.quotedPhrases?.[0] || plan.coreQuery || plan.query, `${item.title} ${item.snippet} ${item.pageProbeText || ''}`);
  const n = Math.max(1, terms.length);
  return {
    score: rankOne(item, plan),
    titleCoverage: Number(title.coverage.toFixed(3)),
    bodyCoverage: Number(body.coverage.toFixed(3)),
    conceptCoverage: Number(((title.coverage + body.coverage) / 2).toFixed(3)),
    matchedConcepts: unique([...title.matched, ...body.matched]),
    matchedCount: title.hits + body.hits,
    conceptCount: n,
    exactPhrase: phrase >= 0.8,
    phraseScore: Number(phrase.toFixed(3)),
    providers: item.providers || [item.source],
    focusTerms: terms
  };
}

function fusionRank(results, plan) {
  const ranked = results.map((r, index) => {
    const rel = relevanceObject(r, plan);
    return {
      ...r,
      relevance: rel,
      relevanceScore: rel.score,
      relevanceBand: rel.score >= 82 ? 'excellent' : rel.score >= 65 ? 'strong' : rel.score >= 48 ? 'usable' : rel.score >= 25 ? 'related' : 'weak',
      _inputOrder: index,
      _selectionScore: rel.score
    };
  });
  const domainCounts = new Map();
  ranked.sort((a, b) => {
    const ad = domainCounts.get(normalizeHost(a.url)) || 0;
    const bd = domainCounts.get(normalizeHost(b.url)) || 0;
    const aDiv = Math.min(6, ad) * 1.8;
    const bDiv = Math.min(6, bd) * 1.8;
    const aa = a._selectionScore - aDiv;
    const bb = b._selectionScore - bDiv;
    if (Math.abs(bb - aa) > 0.01) return bb - aa;
    const d = Number(b.relevanceScore) - Number(a.relevanceScore);
    if (Math.abs(d) > 0.01) return d;
    const dp = Number(domainQuality(b.url)) - Number(domainQuality(a.url));
    if (Math.abs(dp) > 0.001) return dp;
    const bp = (b.providers?.length || 1) - (a.providers?.length || 1);
    if (bp) return bp;
    return (a._inputOrder || 0) - (b._inputOrder || 0);
  });
  for (const r of ranked) {
    const h = normalizeHost(r.url);
    domainCounts.set(h, (domainCounts.get(h) || 0) + 1);
    r.domainRepetition = Math.min(6, domainCounts.get(h));
  }
  return ranked;
}

async function mapConcurrent(list, limit, worker) {
  const arr = Array.isArray(list) ? list : [];
  const out = new Array(arr.length);
  let cursor = 0;
  const n = Math.max(1, Math.min(limit, arr.length || 1));
  const runner = async () => {
    while (true) {
      const i = cursor++;
      if (i >= arr.length) return;
      try {
        out[i] = await worker(arr[i], i);
      } catch {
        out[i] = null;
      }
    }
  };
  await Promise.all(Array.from({ length: n }, runner));
  return out;
}

async function verifyOne(candidate, deadline) {
  if (!safeUrl(candidate.url) || left(deadline) < 220) {
    return { ...candidate, verified: false, verificationMethod: 'budget' };
  }
  const localDeadline = Date.now() + Math.min(VERIFY_TIMEOUT_MS, left(deadline) - 100);
  try {
    const res = await fetchWithTimeout(candidate.url, VERIFY_TIMEOUT_MS, localDeadline, {
      accept: 'text/html,application/xhtml+xml,application/xml,text/plain,application/pdf;q=0.8,*/*;q=0.1',
      range: 'bytes=0-16383'
    });
    const finalUrl = normalizedUrl(res.url || candidate.url) || candidate.url;
    if (!safeUrl(finalUrl) || blocked(finalUrl)) {
      return { ...candidate, verified: false, verificationMethod: 'redirect-rejected', httpStatus: res.status };
    }
    const ct = String(res.headers.get('content-type') || '').toLowerCase();
    const body = await readLimited(res, 20_000, localDeadline).catch(() => '');
    const title = titleFromHtml(body) || metaFromHtml(body, 'og:title') || candidate.title;
    const canonical = metaFromHtml(body, 'og:url');
    const metaDescription = metaFromHtml(body, 'description') || metaFromHtml(body, 'og:description');
    const headingSignals = [...String(body).matchAll(/<h[1-3]\b[^>]*>([\s\S]*?)<\/h[1-3]\s*>/gi)]
      .slice(0, 10)
      .map((m) => cleanSnippet(m[1]))
      .join(' ');
    const pageProbeText = truncate(
      normalizeText([title, metaDescription, headingSignals, stripTags(body).slice(0, 7000)].filter(Boolean).join(' ')),
      9000
    );
    const published = dateFromHtml(body) || candidate.publishedAt || null;
    const reachable = res.ok || (res.status >= 300 && res.status < 500);
    return {
      ...candidate,
      url: finalUrl,
      title: truncate(title, 500),
      publishedAt: published,
      httpStatus: res.status,
      contentType: ct || null,
      verified: Boolean(reachable),
      verificationMethod: 'light-http-check',
      contentAvailable: false,
      contentStatus: 'metadata-only',
      contentMethod: 'search-metadata',
      pageProbeText,
      metaDescription: truncate(metaDescription, 900),
      canonicalUrl: safeUrl(canonical) && !blocked(canonical) ? normalizedUrl(canonical) : null
    };
  } catch (error) {
    return {
      ...candidate,
      verified: false,
      verificationMethod: 'light-http-check-failed',
      verificationError: error?.message || 'VERIFY_FAILED'
    };
  }
}

async function optionalGroqRerank(query, results, deadline) {
  const key = env('GROQ_API_KEY');
  if (!key || results.length < 4 || left(deadline) < 500) return null;
  const controller = new AbortController();
  const timeout = Math.min(AI_RERANK_TIMEOUT_MS, Math.max(350, left(deadline) - 100));
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const payload = results.slice(0, 30).map((r, i) => ({
      id: i,
      title: truncate(r.title, 180),
      domain: normalizeHost(r.url),
      snippet: truncate(r.snippet, 280),
      score: r.relevanceScore,
      providers: r.providers?.length || 1,
      type: r.type
    }));
    const res = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: 'llama-3.1-8b-instant',
        messages: [
          {
            role: 'system',
            content: 'Return JSON only. Reorder the supplied ids by relevance to the query. Never invent ids. Do not omit any id. Prefer exact topical match and direct source evidence.'
          },
          {
            role: 'user',
            content: JSON.stringify({ query, results: payload, order: payload.map((x) => x.id) })
          }
        ],
        temperature: 0,
        max_tokens: 500
      })
    });
    if (!res.ok) return null;
    const data = await res.json();
    const txt = String(data?.choices?.[0]?.message?.content || '');
    const obj = JSON.parse(txt.match(/\{[\s\S]*\}/)?.[0] || txt);
    if (!Array.isArray(obj.order)) return null;
    return obj.order.map(Number).filter((i) => Number.isInteger(i) && i >= 0 && i < Math.min(30, results.length));
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function commonCrawlMeta(url, deadline) {
  if (!safeUrl(url) || left(deadline) < 250) return null;
  const encoded = encodeURIComponent(url);
  for (const index of COMMON_CRAWL_INDEXES) {
    if (left(deadline) < 200) break;
    try {
      const res = await fetchWithTimeout(
        `https://index.commoncrawl.org/${index}-index?url=${encoded}&output=json&filter=status:200&limit=1`,
        COMMON_CRAWL_TIMEOUT_MS,
        deadline,
        { accept: 'application/json,text/plain;q=0.8' }
      );
      if (!res.ok) continue;
      const body = await readLimited(res, 20_000, deadline);
      const row = body
        .split('\n')
        .map((x) => {
          try {
            return JSON.parse(x);
          } catch {
            return null;
          }
        })
        .find(Boolean);
      if (row) return { index, timestamp: row.timestamp || null, digest: row.digest || null };
    } catch {}
  }
  return null;
}

function createLogger() {
  const logs = [];
  const add = (event, message, extra = {}) => {
    const x = { at: nowIso(), event, message, ...extra };
    logs.push(x);
    if (logs.length > MAX_LIVE_LOG) logs.shift();
    try {
      console.log(`[ArixAI ${event}] ${message}`);
    } catch {}
  };
  return { logs, add };
}

function cacheKey(input, query, plan) {
  return JSON.stringify({
    q: query,
    mode: String(input.mode || 'auto').toLowerCase(),
    type: plan.type,
    deep: String(input.deep || 'false').toLowerCase(),
    verify: String(input.verify ?? 'true').toLowerCase(),
    ai: String(input.ai ?? 'auto').toLowerCase(),
    cc: String(input.commonCrawl || 'false').toLowerCase()
  });
}

function cacheGet(key) {
  const x = CACHE.get(key);
  if (!x) return null;
  if (Date.now() - x.at > x.ttl) {
    CACHE.delete(key);
    return null;
  }
  return x.value;
}

function cacheSet(key, value, ttl) {
  CACHE.set(key, { at: Date.now(), ttl, value });
  while (CACHE.size > CACHE_MAX) CACHE.delete(CACHE.keys().next().value);
}

function decorateResult(r, i, plan) {
  const type = typeFromUrl(r.url, r.type);
  const score = Math.round(Number(r.relevanceScore || 0));
  return {
    rank: i + 1,
    title: truncate(r.title || 'Untitled', 300),
    url: r.url,
    domain: normalizeHost(r.url),
    type,
    source: r.source || 'search',
    providers: r.providers || [r.source || 'search'],
    snippet: truncate(r.snippet || '', 1200),
    publishedAt: r.publishedAt || null,
    freshness: freshness(r.publishedAt),
    verified: Boolean(r.verified),
    httpStatus: r.httpStatus || null,
    contentType: r.contentType || null,
    trust: Number(domainQuality(r.url).toFixed(2)),
    relevanceScore: score,
    relevanceBand: r.relevanceBand || 'related',
    relevance: r.relevance || null,
    publisherResolved: Boolean(r.publisherResolved),
    publisherWrapperUrl: r.publisherWrapperUrl || null,
    searchWrapperResolved: Boolean(r.searchWrapperResolved),
    extractedText: '',
    pageContent: '',
    contentAvailable: false,
    contentStatus: 'metadata-only',
    contentMethod: 'search-metadata',
    contentLength: 0,
    contentConfidence: 0,
    contentSourceUrl: r.url,
    contentTargetMatched: false,
    contentTitleSimilarity: Number(r.relevance?.titleCoverage || 0).toFixed(3),
    contentConceptCoverage: Number(r.relevance?.conceptCoverage || 0).toFixed(3),
    contentFormat: 'none',
    contentRole: 'search_metadata_only',
    contentForAI: null,
    verificationMethod: r.verificationMethod || null,
    verificationError: r.verificationError || null,
    commonCrawl: r.commonCrawl || null,
    queryMatch: {
      planType: plan.type,
      liveIntent: plan.flags.live,
      historyIntent: plan.flags.history,
      officialIntent: plan.flags.official,
      terms: plan.terms
    }
  };
}

function buildResponse(ctx) {
  const {
    query,
    count,
    mode,
    requestedType,
    plan,
    preciseQueries,
    providerStats,
    results,
    logger,
    started,
    verifyRequested,
    deep,
    aiRequested,
    useCc,
    cacheHit = false
  } = ctx;
  const verifiedCount = results.filter((r) => r.verified).length;
  return {
    ok: true,
    version: VERSION,
    query,
    requestedResults: count,
    returnedResults: results.length,
    sourceCountMode: 'all-discovered-ranked',
    resultSelectionPolicy: 'return-all-discovered-ranked-by-query-match',
    mode,
    intent: {
      type: requestedType || plan.type,
      wantsNews: plan.flags.explicitNews || plan.type === 'news' || plan.flags.live,
      wantsVideo: plan.flags.explicitVideo || plan.type === 'video',
      wantsGov: plan.flags.explicitGov || plan.type === 'gov',
      wantsDocs: plan.flags.explicitDoc || plan.type === 'doc',
      wantsHistory: plan.flags.history,
      wantsAcademic: plan.flags.academic
    },
    generatedAt: nowIso(),
    started,
    latencyMs: Date.now() - started,
    keylessCoreSearch: true,
    groqUsed: Boolean(ctx.groqUsed),
    cached: cacheHit,
    providers: providerStats,
    quality: {
      discoveredResults: results.length,
      validatedResults: verifiedCount,
      requestedResults: count,
      realContentOnly: false,
      contentGuarantee: 'Search results are the public search sources actually discovered and ranked; no fabricated source is added.'
    },
    searchPlan: {
      queryVariants: preciseQueries,
      engineRequests: Object.values(providerStats).reduce((s, p) => s + Number(p.requests || 0), 0),
      verificationRequested: verifyRequested,
      verificationPerformed: Number(ctx.verificationPerformed || 0),
      verificationSucceeded: verifiedCount,
      commonCrawlEnabled: useCc,
      commonCrawlPerformed: results.filter((r) => r.commonCrawl).length,
      streamed: true,
      logStreamSupported: true,
      deep,
      aiRequested
    },
    liveLog: logger.logs,
    allFetchedSources: results.length,
    results,
    warnings:
      results.length < count
        ? [`${results.length} ranked sources were discovered. Public engines may return fewer results than requested.`]
        : []
  };
}

async function performSearch(input, started, logger) {
  const deadline = started + SEARCH_BUDGET_MS;
  const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
  const count = safeInt(input.count ?? input.limit, DEFAULT_RESULTS, 1, MAX_RESULTS);
  const mode = String(input.mode || 'auto').toLowerCase();
  const requestedType = String(input.type || '').toLowerCase();
  const deep = String(input.deep ?? 'false').toLowerCase() === 'true' || mode === 'deep';
  const verifyRequested = input.verify == null ? true : String(input.verify).toLowerCase() !== 'false';
  const aiRequested = String(input.ai ?? 'auto').toLowerCase();
  const useCc = String(input.commonCrawl ?? 'false').toLowerCase() === 'true';

  const plan = queryPlan(query, { mode, type: requestedType });
  const key = cacheKey(input, query, plan);
  const cached = cacheGet(key);
  if (cached) {
    logger.add('cache-hit', 'Returned a warm cached search result.');
    return { ...cached, generatedAt: nowIso(), latencyMs: Date.now() - started, cached: true, liveLog: logger.logs };
  }

  logger.add('query-analyzed', 'Built multi-surface precision query plan.', {
    type: plan.type,
    terms: plan.terms.slice(0, 12),
    live: plan.flags.live
  });

  const preciseQueries = buildQueries(query, plan, deep);
  const requests = providerRequests(preciseQueries, plan, deep);
  logger.add('discovery-start', `Launching ${requests.length} public search requests in parallel.`, { queries: preciseQueries });

  const discoveryDeadline = Math.min(deadline, started + DISCOVERY_CUTOFF_MS);
  const entries = await mapConcurrent(requests, SEARCH_CONCURRENCY, (r) => discoverOne(r, discoveryDeadline));

  const providerStats = {};
  let discovered = [];
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i] || { provider: requests[i]?.provider, ok: false, results: [] };
    const p = entry.provider || requests[i]?.provider || 'unknown';
    providerStats[p] ||= { requests: 0, ok: 0, failed: 0, results: 0 };
    providerStats[p].requests++;
    if (entry.ok) {
      providerStats[p].ok++;
      providerStats[p].results += entry.results.length;
      discovered.push(...entry.results);
    } else {
      providerStats[p].failed++;
    }
  }

  discovered = dedupe(discovered);
  logger.add('discovery-complete', `Discovery produced ${discovered.length} unique public sources.`, {
    providers: Object.fromEntries(Object.entries(providerStats).map(([k, v]) => [k, { ok: v.ok, failed: v.failed, results: v.results }]))
  });

  if (!discovered.length) {
    const empty = buildResponse({
      query,
      count,
      mode,
      requestedType,
      plan,
      preciseQueries,
      providerStats,
      results: [],
      logger,
      started,
      verifyRequested,
      deep,
      aiRequested,
      useCc,
      verifyLimit: 0
    });
    cacheSet(key, empty, plan.flags.live ? LIVE_CACHE_TTL_MS : CACHE_TTL_MS);
    return empty;
  }

  let ranked = fusionRank(discovered, plan);
  logger.add('ranking-complete', 'Fused provider rank, query match, source quality, consensus and intent.');

  /* Verify more than the displayed count when possible, but never make verification the
   * bottleneck. Unverified sources are still retained and ranked. */
  const verifyLimit = Math.min(ranked.length, deep ? 32 : Math.max(12, Math.min(24, count * 2)));
  let verificationPerformed = 0;
  if (verifyRequested && left(deadline) > 550 && verifyLimit) {
    logger.add('verification-start', `Running lightweight reachability checks on ${verifyLimit} top sources.`);
    verificationPerformed = verifyLimit;
    const verifyDeadline = Math.min(deadline - 60, Date.now() + Math.max(450, left(deadline) - 60));
    const checked = await mapConcurrent(ranked.slice(0, verifyLimit), VERIFY_CONCURRENCY, (r) => verifyOne(r, verifyDeadline));
    const checkedMap = new Map(checked.filter(Boolean).map((r) => [normalizedKey(r.url), r]));
    ranked = ranked.map((r) => checkedMap.get(normalizedKey(r.url)) || r);
    ranked = fusionRank(ranked, plan);
    logger.add(
      'verification-complete',
      `Verification finished; ${ranked.slice(0, verifyLimit).filter((r) => r.verified).length} of ${verifyLimit} top sources responded usefully.`
    );
  } else {
    logger.add('verification-skipped', 'Verification skipped because the remaining wall-clock budget was too small.');
  }

  let groqUsed = false;
  if ((aiRequested === 'true' || aiRequested === 'auto') && env('GROQ_API_KEY') && left(deadline) > 850) {
    const order = await optionalGroqRerank(query, ranked.slice(0, 30), deadline);
    if (Array.isArray(order) && order.length) {
      const top = ranked.slice(0, 30);
      const reordered = order.map((i) => top[i]).filter(Boolean);
      const seen = new Set(reordered.map((x) => normalizedKey(x.url)));
      ranked = [...reordered, ...ranked.slice(30).filter((x) => !seen.has(normalizedKey(x.url)))];
      groqUsed = true;
      logger.add('ai-rerank-complete', 'Optional Groq reranking reordered the top candidate set.');
    }
  }

  let final = ranked.filter((r) => r?.url && !blocked(r.url) && !isLikelySearchUrl(r.url)).slice(0, MAX_DISCOVERY_RESULTS);
  if (useCc && left(deadline) > 500) {
    const ccDeadline = Date.now() + Math.min(500, left(deadline) - 50);
    const rows = await Promise.all(
      final.slice(0, MAX_COMMON_CRAWL).map(async (r) => ({ url: r.url, cc: await commonCrawlMeta(r.url, ccDeadline) }))
    );
    const ccMap = new Map(rows.filter((x) => x.cc).map((x) => [normalizedKey(x.url), x.cc]));
    final = final.map((r) => ({ ...r, commonCrawl: ccMap.get(normalizedKey(r.url)) || null }));
  }

  const decorated = final.map((r, i) => decorateResult(r, i, plan));
  const result = buildResponse({
    query,
    count,
    mode,
    requestedType,
    plan,
    preciseQueries,
    providerStats,
    results: decorated,
    logger,
    started,
    verifyRequested,
    deep,
    aiRequested,
    useCc,
    groqUsed,
    verifyLimit,
    verificationPerformed
  });
  cacheSet(key, result, plan.flags.live ? LIVE_CACHE_TTL_MS : CACHE_TTL_MS);
  return result;
}

async function readInput(req) {
  const url = new URL(req.url);
  if (req.method === 'GET') return Object.fromEntries(url.searchParams.entries());
  const raw = await req.text();
  if (raw.length > MAX_REQUEST_BODY) throw new Error('REQUEST_BODY_TOO_LARGE');
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return Object.fromEntries(new URLSearchParams(raw).entries());
  }
}

function corsHeaders(contentType = 'application/json; charset=utf-8') {
  return {
    'content-type': contentType,
    'cache-control': 'no-store, no-transform',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, POST, OPTIONS',
    'access-control-allow-headers': 'content-type, authorization, x-arix-search-key',
    'x-arix-crawler-version': VERSION
  };
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), { status, headers: corsHeaders() });
}

function streamJsonSearch(input) {
  const encoder = new TextEncoder();
  const started = Date.now();
  const logger = createLogger();
  const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
  const count = safeInt(input.count ?? input.limit, DEFAULT_RESULTS, 1, MAX_RESULTS);
  const mode = String(input.mode || 'auto').toLowerCase();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;
      const send = (x) => {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(x));
        } catch {
          closed = true;
        }
      };

      logger.add('request-start', `Starting live search for ${query}.`, { count, mode });
      send(`{"ok":true,"version":${encode(VERSION)},"query":${encode(query)},"requestedResults":${count},"mode":${encode(mode)},"streaming":true,"results":[\n`);

      const heartbeat = setInterval(() => send(' \n'), 1000);

      Promise.resolve()
        .then(() => performSearch(input, started, logger))
        .then((result) => {
          clearInterval(heartbeat);
          const rows = Array.isArray(result.results) ? result.results : [];
          rows.forEach((r, i) => send(`${i ? ',\n' : ''}${JSON.stringify(r)}\n`));
          const meta = { ...result };
          delete meta.results;
          send('],\n');
          const entries = Object.entries(meta);
          entries.forEach(([k, v], i) => send(`${JSON.stringify(k)}:${JSON.stringify(v)}${i === entries.length - 1 ? '' : ',\n'}`));
          send('}');
          try {
            controller.close();
          } catch {}
          closed = true;
        })
        .catch((error) => {
          clearInterval(heartbeat);
          send(
            `],"returnedResults":0,"generatedAt":${encode(nowIso())},"latencyMs":${Date.now() - started},"keylessCoreSearch":true,"groqUsed":false,"resultsError":${encode(error?.message || 'SEARCH_FAILED')},"liveLog":${JSON.stringify(logger.logs)},"warnings":[${encode('The crawler failed safely after the stream had already started.').slice(1, -1)}]}`
          );
          try {
            controller.close();
          } catch {}
          closed = true;
        });
    }
  });

  return new Response(stream, {
    status: 200,
    headers: { ...corsHeaders(), 'x-arix-search-stream': '1', 'x-arix-stream-heartbeat-ms': '1000' }
  });
}

function streamSseSearch(input) {
  const encoder = new TextEncoder();
  const started = Date.now();
  const logger = createLogger();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;
      const send = (event, data) => {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
        } catch {
          closed = true;
        }
      };

      const originalAdd = logger.add;
      logger.add = (event, message, extra = {}) => {
        originalAdd(event, message, extra);
        send('log', logger.logs[logger.logs.length - 1]);
      };

      const heartbeat = setInterval(() => send('heartbeat', { at: nowIso(), version: VERSION }), 1000);

      Promise.resolve()
        .then(() => performSearch(input, started, logger))
        .then((result) => {
          clearInterval(heartbeat);
          send('result', result);
          send('done', { ok: true, latencyMs: Date.now() - started });
          try {
            controller.close();
          } catch {}
          closed = true;
        })
        .catch((error) => {
          clearInterval(heartbeat);
          send('error', { ok: false, error: error?.message || 'SEARCH_FAILED', latencyMs: Date.now() - started });
          try {
            controller.close();
          } catch {}
          closed = true;
        });
    }
  });

  return new Response(stream, {
    status: 200,
    headers: { ...corsHeaders('text/event-stream; charset=utf-8'), 'x-arix-log-stream': 'sse' }
  });
}

export async function runSearch(input = {}) {
  const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
  if (!query) throw new Error('MISSING_QUERY');
  return performSearch({ ...input, query }, Date.now(), createLogger());
}

export const SEARCH_CONTRACT = Object.freeze({
  version: VERSION,
  maxResults: MAX_RESULTS,
  standalone: true,
  dependencies: [],
  searchSurfaces: ['bing', 'google', 'duckduckgo', 'yahoo', 'mojeek', 'google-news', 'youtube'],
  ranking: 'multi-engine-query-fusion',
  contentMode: 'search-discovery-only',
  returnsAllDiscovered: true,
  optionalVerification: true,
  noTavily: true
});

export default async function handler(req) {
  if (req.method === 'OPTIONS') return jsonResponse({ ok: true, version: VERSION });
  if (!['GET', 'POST'].includes(req.method)) {
    return jsonResponse({ ok: false, version: VERSION, error: 'METHOD_NOT_ALLOWED', message: 'Use GET or POST.' }, 405);
  }
  try {
    const input = await readInput(req);
    const query = truncate(String(input.query ?? input.q ?? '').trim(), MAX_QUERY_LEN);
    if (!query) return jsonResponse({ ok: false, version: VERSION, error: 'MISSING_QUERY' }, 400);
    if (query.length < 2) return jsonResponse({ ok: false, version: VERSION, error: 'QUERY_TOO_SHORT' }, 400);
    const wantsSse = String(input.logStream || '').toLowerCase() === 'true' || req.headers.get('accept')?.includes('text/event-stream');
    return wantsSse ? streamSseSearch(input) : streamJsonSearch(input);
  } catch (error) {
    const status = error?.message === 'REQUEST_BODY_TOO_LARGE' ? 413 : 500;
    return jsonResponse({ ok: false, version: VERSION, error: error?.message || 'SEARCH_FAILED' }, status);
  }
}
