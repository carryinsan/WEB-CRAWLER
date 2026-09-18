/*
 * ArixAI Dedicated Content Scraper
 * v3.0.0 — Fast Multi-Path / Scored Full-Content Extraction Edition
 *
 * Goals
 * - Very fast first useful result without sacrificing extraction quality.
 * - Prefer the real article/body over navigation, cookie banners, related links,
 *   recommendation cards, and footer text.
 * - Recover content from normal HTML, semantic HTML, JSON-LD, OpenGraph/meta,
 *   Next/Nuxt/Redux-like state, AMP/print/mobile variants, and Jina Reader.
 * - Read response bodies incrementally so a huge/tarpit response cannot force a
 *   full multi-megabyte allocation before the timeout fires.
 * - Run independent recovery paths concurrently and score their output rather
 *   than trusting the first parser that happens to return text.
 * - Return useful diagnostic metadata for the crawler/search layer.
 *
 * Important limitation
 * - This does NOT bypass authentication, paywalls, CAPTCHAs, robots controls,
 *   or other access controls. It detects challenge pages and tries public
 *   alternate representations instead.
 */

export const runtime = 'edge';

const VERSION = '3.0.0';
const MAX_PAGE_BYTES = 3_500_000;
const MAX_TEXT_CHARS = 120_000;
const MIN_REAL_CONTENT = 180;
const MIN_GOOD_CONTENT = 700;
const DIRECT_TIMEOUT_MS = 4_800;
const READER_TIMEOUT_MS = 5_800;
const TOTAL_BUDGET_MS = 9_500;
const MAX_CANDIDATES = 32;
const MAX_STATE_DEPTH = 8;
const MAX_STATE_STRINGS = 1200;
const MAX_STATE_STRING_CHARS = 50_000;

const BLOCKED_HOSTS = new Set([
  'google-analytics.com', 'googletagmanager.com', 'googlesyndication.com',
  'googleadservices.com', 'doubleclick.net', 'gstatic.com', 'googleapis.com',
  'facebook.net', 'connect.facebook.net', 'scorecardresearch.com', 'pixel.wp.com',
  'datadome.co', 'clarity.ms', 'hotjar.com', 'segment.io', 'segment.com',
  'sentry.io', 'newrelic.com', 'nr-data.net', 'adsrvr.org', 'adnxs.com',
  'rubiconproject.com', 'pubmatic.com'
]);

const BOT_TRIGGERS = [
  'just a moment...', 'checking your browser', 'enable javascript and cookies',
  'please enable js', 'cloudflare', 'cf-browser-verification',
  'verify you are human', 'why do i have to complete a captcha',
  'attention required!', 'robot or human', 'datadome', 'perimeterx',
  'access denied', '403 forbidden', 'are you a robot', 'pardon our interruption',
  'complete the security check', 'security verification required',
  'unusual traffic', 'captcha', 'press & hold', 'human verification'
];

const NOISE_WORDS = [
  'subscribe', 'sign in', 'log in', 'newsletter', 'advertisement', 'advertising',
  'cookie', 'privacy', 'terms of use', 'all rights reserved', 'follow us',
  'share this', 'read more', 'related stories', 'recommended for you',
  'most read', 'trending now', 'latest news', 'download app', 'install app',
  'watch live', 'listen live', 'breaking news', 'skip to content', 'menu'
];

const CONTENT_HINTS = [
  'article', 'article-body', 'articlebody', 'post-content', 'postcontent',
  'entry-content', 'entrycontent', 'story-content', 'storycontent',
  'story-body', 'storybody', 'main-content', 'maincontent', 'content-body',
  'contentbody', 'page-content', 'pagecontent', 'news-content', 'newscontent',
  'body-content', 'bodycontent', 'longread', 'opinion-content', 'blog-content',
  'single-post', 'article-wrapper', 'article-container', 'story-wrapper',
  'story-container', 'article-copy', 'article-text', 'article__body',
  'article-body-copy', 'article-body-content'
];

const NOISE_HINTS = [
  'comment', 'comments', 'sidebar', 'related', 'recommend', 'recommended',
  'trending', 'popular', 'newsletter', 'subscribe', 'social', 'share',
  'advert', 'ads', 'banner', 'cookie', 'consent', 'modal', 'popup',
  'breadcrumb', 'navigation', 'navbar', 'footer', 'header', 'menu', 'login',
  'account', 'author-bio', 'promo', 'sponsor', 'paywall', 'outbrain', 'taboola'
];

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, Number(n) || 0));
}

function truncate(value, max = MAX_TEXT_CHARS) {
  const s = String(value ?? '');
  if (s.length <= max) return s;
  return `${s.slice(0, Math.max(0, max - 1)).trim()}…`;
}

function decodeHtml(value = '') {
  let s = String(value);
  s = s
    .replace(/&nbsp;|&#160;/gi, ' ')
    .replace(/&quot;|&#34;|&#x22;/gi, '"')
    .replace(/&#39;|&apos;|&#x27;/gi, "'")
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&hellip;|&#8230;|&#x2026;/gi, '…')
    .replace(/&mdash;|&#8212;|&#x2014;/gi, '—')
    .replace(/&ndash;|&#8211;|&#x2013;/gi, '–')
    .replace(/&copy;|&#169;/gi, '©')
    .replace(/&reg;|&#174;/gi, '®')
    .replace(/&trade;|&#8482;/gi, '™');

  s = s.replace(/&#x([0-9a-f]+);?/gi, (match, hex) => {
    const n = parseInt(hex, 16);
    if (!Number.isFinite(n) || n < 0 || n > 0x10ffff || (n >= 0xd800 && n <= 0xdfff)) return match;
    try { return String.fromCodePoint(n); } catch { return match; }
  });

  s = s.replace(/&#(\d+);?/g, (match, dec) => {
    const n = parseInt(dec, 10);
    if (!Number.isFinite(n) || n < 0 || n > 0x10ffff || (n >= 0xd800 && n <= 0xdfff)) return match;
    try { return String.fromCodePoint(n); } catch { return match; }
  });

  return s;
}

function stripMarkdownNoise(value = '') {
  return String(value)
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/\[([^\]]+)\]\((?:https?:\/\/|\/)[^)]*\)/g, '$1')
    .replace(/\[\^\d+\]:[^\n]*/g, '')
    .replace(/^\s{0,3}#{1,6}\s*/gm, '')
    .replace(/[*_~`]{1,3}/g, '');
}

function cleanText(value, max = MAX_TEXT_CHARS) {
  let s = decodeHtml(String(value || ''));
  s = stripMarkdownNoise(s);
  s = s
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p\s*>/gi, '\n\n')
    .replace(/<\/h[1-6]\s*>/gi, '\n\n')
    .replace(/<li\s*>/gi, '\n• ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\u00a0/g, ' ')
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  s = s.replace(/(?:skip to content|accept cookies|cookie settings|privacy settings|sign in|log in|search this site)\b/gi, ' ')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  return truncate(s, max);
}

function normalizeLine(value = '') {
  return String(value)
    .replace(/\s+/g, ' ')
    .replace(/^[•\-*]+\s*/, '')
    .trim();
}

function splitBlocks(value = '') {
  const text = cleanText(value, MAX_TEXT_CHARS);
  return text
    .split(/\n{2,}|(?<=[.!?])\s{2,}/)
    .map(normalizeLine)
    .filter(Boolean);
}

function dedupeBlocks(blocks) {
  const out = [];
  const seen = new Set();
  for (const raw of blocks) {
    const block = normalizeLine(raw);
    if (block.length < 20) continue;
    const key = block.toLowerCase()
      .replace(/[^a-z0-9\u0900-\u097f]+/gi, ' ')
      .trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(block);
  }
  return out;
}

function finalizeBlocks(blocks, max = MAX_TEXT_CHARS) {
  let text = dedupeBlocks(blocks).join('\n\n');
  return truncate(text, max);
}

function isBotChallenge(text) {
  const t = String(text || '').toLowerCase();
  if (!t) return false;
  if (t.length > 7000) return false;
  let hits = 0;
  for (const trigger of BOT_TRIGGERS) {
    if (t.includes(trigger)) hits++;
    if (hits >= 2) return true;
  }
  return hits >= 1 && t.length < 2200;
}

function safeUrl(input) {
  try {
    const raw = String(input || '').trim().replace(/&amp;/gi, '&');
    const u = new URL(raw);
    if (!/^https?:$/i.test(u.protocol)) return false;
    const host = u.hostname.toLowerCase().replace(/^www\./, '');
    if (!host || host === 'localhost' || host.endsWith('.localhost')) return false;
    if (BLOCKED_HOSTS.has(host)) return false;
    if ([...BLOCKED_HOSTS].some(h => host.endsWith(`.${h}`))) return false;

    // Basic SSRF hardening for literal private/loopback addresses.
    if (/^127\./.test(host) || host === '0.0.0.0' || host === '::1' || host === '[::1]') return false;
    if (/^10\./.test(host) || /^192\.168\./.test(host)) return false;
    const m172 = host.match(/^172\.(\d+)\./);
    if (m172 && Number(m172[1]) >= 16 && Number(m172[1]) <= 31) return false;
    if (/^169\.254\./.test(host)) return false;
    if (host.endsWith('.internal') || host.endsWith('.local')) return false;

    return true;
  } catch {
    return false;
  }
}

function makeUrlVariants(inputUrl) {
  const urls = [];
  const seen = new Set();
  const push = u => {
    try {
      const s = String(u);
      if (!safeUrl(s)) return;
      const key = new URL(s).toString();
      if (seen.has(key)) return;
      seen.add(key);
      urls.push(key);
    } catch {}
  };

  let u;
  try { u = new URL(inputUrl); } catch { return [inputUrl]; }

  push(u.toString());

  // Remove common analytics/tracking parameters while preserving article IDs and filters.
  const cleaned = new URL(u.toString());
  const tracking = /^(utm_|fbclid$|gclid$|dclid$|msclkid$|mc_cid$|mc_eid$|ref$|ref_src$|spm$|src$|cmpid$|campaign$|_ga$)/i;
  for (const [k] of [...cleaned.searchParams.entries()]) {
    if (tracking.test(k)) cleaned.searchParams.delete(k);
  }
  push(cleaned.toString());

  const path = cleaned.pathname.replace(/\/+$/, '');
  const lowerPath = path.toLowerCase();

  // Common public alternate representations. Limit to paths that are not already variants.
  if (!/(?:^|\/)(?:amp|print|mobile)(?:\/|$)/i.test(lowerPath)) {
    const amp = new URL(cleaned.toString());
    amp.pathname = `${path || ''}/amp`;
    push(amp.toString());

    const print = new URL(cleaned.toString());
    print.searchParams.set('output', '1');
    push(print.toString());
  }

  if (!cleaned.searchParams.has('output')) {
    const ampQuery = new URL(cleaned.toString());
    ampQuery.searchParams.set('output', 'amp');
    push(ampQuery.toString());
  }

  return urls.slice(0, 5);
}

function headerObject(headers) {
  const out = {};
  try {
    for (const [k, v] of headers.entries()) out[k.toLowerCase()] = v;
  } catch {}
  return out;
}

async function readTextLimited(response, maxBytes, signal) {
  // This avoids response.arrayBuffer() on giant pages and lets the AbortSignal
  // terminate a body that stalls after headers arrive.
  if (!response.body || typeof response.body.getReader !== 'function') {
    const buf = await response.arrayBuffer();
    const bytes = new Uint8Array(buf.slice(0, maxBytes));
    return new TextDecoder('utf-8', { fatal: false }).decode(bytes);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: false });
  const chunks = [];
  let total = 0;

  try {
    while (total < maxBytes) {
      if (signal && signal.aborted) throw new Error('aborted');
      const { value, done } = await reader.read();
      if (done) break;
      if (!value || !value.byteLength) continue;

      const remaining = maxBytes - total;
      const piece = value.byteLength <= remaining ? value : value.subarray(0, remaining);
      chunks.push(decoder.decode(piece, { stream: true }));
      total += piece.byteLength;
      if (piece.byteLength < value.byteLength) break;
    }
    chunks.push(decoder.decode());
    return chunks.join('');
  } finally {
    try { await reader.cancel(); } catch {}
  }
}

async function fetchSafely(url, budgetMs, extraHeaders = {}) {
  if (!safeUrl(url)) return {
    ok: false, status: 0, url, finalUrl: url, text: '', headers: {}, error: 'unsafe_url'
  };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(250, budgetMs));
  const started = Date.now();

  try {
    const res = await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      cache: 'no-store',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        ...extraHeaders
      }
    });

    const headers = headerObject(res.headers);
    if (!res.ok) {
      return {
        ok: false, status: res.status, url, finalUrl: res.url || url,
        text: '', headers, latencyMs: Date.now() - started
      };
    }

    const contentType = headers['content-type'] || '';
    if (contentType && !/(?:text\/html|application\/xhtml\+xml|application\/xml|text\/plain|application\/json)/i.test(contentType)) {
      return {
        ok: false, status: res.status, url, finalUrl: res.url || url,
        text: '', headers, contentType, latencyMs: Date.now() - started
      };
    }

    const text = await readTextLimited(res, MAX_PAGE_BYTES, controller.signal);
    return {
      ok: true,
      status: res.status,
      url,
      finalUrl: res.url || url,
      text,
      headers,
      contentType,
      latencyMs: Date.now() - started
    };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      url,
      finalUrl: url,
      text: '',
      headers: {},
      error: String(error?.message || error),
      latencyMs: Date.now() - started
    };
  } finally {
    clearTimeout(timer);
  }
}

function extractMetaFields(html) {
  const meta = { title: '', description: '' };
  const source = String(html || '');

  for (const m of source.matchAll(/<meta\b[^>]*>/gi)) {
    const tag = m[0];
    const get = (name) => {
      const re = new RegExp(`\\b${name}\\s*=\\s*(["'])([\\s\\S]*?)\\1`, 'i');
      const hit = tag.match(re);
      return hit ? decodeHtml(hit[2]) : '';
    };
    const property = get('property').toLowerCase();
    const name = get('name').toLowerCase();
    const content = cleanText(get('content'), 4000);
    if (!content) continue;

    if (!meta.title && (property === 'og:title' || name === 'twitter:title')) meta.title = truncate(content, 1000);
    if (!meta.description && (property === 'og:description' || name === 'description' || name === 'twitter:description')) {
      meta.description = truncate(content, 3000);
    }
  }

  return meta;
}

function extractTitle(html) {
  const meta = extractMetaFields(html);
  if (meta.title) return meta.title;

  const patterns = [
    /<title\b[^>]*>([\s\S]*?)<\/title>/i,
    /<h1\b[^>]*>([\s\S]*?)<\/h1>/i
  ];
  for (const re of patterns) {
    const m = String(html || '').match(re);
    if (m && m[1]) {
      const title = cleanText(m[1], 1000);
      if (title.length >= 3) return title;
    }
  }
  return '';
}

function extractMetaDescription(html) {
  return extractMetaFields(html).description;
}

function decodeJsonString(raw) {
  const s = String(raw || '').trim();
  try { return JSON.parse(`"${s.replace(/\\"/g, '\\\"')}"`); } catch {}
  try { return JSON.parse(s); } catch {}
  return decodeHtml(s);
}

function extractAttribute(html, tag, attr) {
  const re = new RegExp(`<${tag}\\b[^>]*\\b${attr}\\s*=\\s*(["'])([\\s\\S]*?)\\1`, 'i');
  const m = String(html).match(re);
  return m ? decodeHtml(m[2]) : '';
}

function extractDeepJsonLd(html) {
  const candidates = [];
  const scriptRe = /<script\b[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  const scripts = String(html).matchAll(scriptRe);

  const visit = (obj, depth = 0) => {
    if (depth > 8 || obj == null) return;
    if (typeof obj === 'string') return;
    if (Array.isArray(obj)) {
      for (const item of obj) visit(item, depth + 1);
      return;
    }
    if (typeof obj !== 'object') return;

    const typeValue = Array.isArray(obj['@type']) ? obj['@type'].join(' ') : String(obj['@type'] || '');
    const type = typeValue.toLowerCase();
    const isContentObject = /article|newsarticle|blogposting|report|analysis|review|recipe|webpage/i.test(type);

    if (isContentObject) {
      const fields = [
        obj.articleBody, obj.text, obj.description, obj.mainEntityOfPage?.articleBody,
        obj.mainEntityOfPage?.text
      ];
      for (const field of fields) {
        if (typeof field !== 'string') continue;
        const text = cleanText(field, MAX_TEXT_CHARS);
        if (text.length >= MIN_REAL_CONTENT) candidates.push({ text, kind: 'json-ld-body' });
      }
    }

    // Some publishers use custom JSON-LD objects whose keys are not Article schemas.
    for (const [key, value] of Object.entries(obj)) {
      if (/articlebody|body|fulltext|full_text|content|description|text/i.test(key) && typeof value === 'string') {
        const text = cleanText(value, MAX_TEXT_CHARS);
        if (text.length >= 350 && text.split(/\s+/).length >= 60) {
          candidates.push({ text, kind: `json-ld-${key}` });
        }
      } else if (value && typeof value === 'object') {
        visit(value, depth + 1);
      }
    }
  };

  for (const match of scripts) {
    const raw = String(match[1] || '').trim();
    if (!raw) continue;
    try {
      const data = JSON.parse(raw);
      visit(data);
    } catch {
      // Handle malformed/truncated JSON-LD with targeted articleBody extraction.
      const bodyRe = /["']articleBody["']\s*:\s*(["'])([\s\S]*?)\1/gi;
      for (const m of raw.matchAll(bodyRe)) {
        const text = cleanText(m[2], MAX_TEXT_CHARS);
        if (text.length >= MIN_REAL_CONTENT) candidates.push({ text, kind: 'json-ld-regex' });
      }
    }
  }

  candidates.sort((a, b) => scoreText(b.text, 'json-ld') - scoreText(a.text, 'json-ld'));
  return candidates[0] || null;
}

function extractKnownJsonText(html) {
  const candidates = [];
  const source = String(html);
  const keyPatterns = [
    /["'](?:articleBody|article_body|articleText|article_text|bodyText|body_text|fullText|full_text)["']\s*:\s*(["'])([\s\S]*?)\1/gi,
    /["'](?:content|contentHtml|content_html|postContent|post_content|storyContent|story_content|description)["']\s*:\s*(["'])([\s\S]{120,}?)\1/gi
  ];
  for (const re of keyPatterns) {
    for (const m of source.matchAll(re)) {
      const decoded = decodeJsonString(m[2]);
      const text = cleanText(decoded, MAX_TEXT_CHARS);
      if (text.length >= MIN_REAL_CONTENT && text.split(/\s+/).length >= 35) {
        candidates.push({ text, kind: 'embedded-json' });
      }
      if (candidates.length >= 20) break;
    }
  }
  candidates.sort((a, b) => scoreText(b.text, a.kind) - scoreText(a.text, b.kind));
  return candidates[0] || null;
}

function extractDeepSpaState(html) {
  const candidates = [];
  const strings = [];
  let visitedStrings = 0;

  const stateScripts = String(html).matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi);

  const deepExtract = (obj, depth = 0, keyHint = '') => {
    if (depth > MAX_STATE_DEPTH || strings.length >= MAX_STATE_STRINGS || visitedStrings >= MAX_STATE_STRINGS * 4) return;
    if (obj == null) return;

    if (typeof obj === 'string') {
      visitedStrings++;
      const t = cleanText(obj, MAX_STATE_STRING_CHARS);
      const key = String(keyHint || '').toLowerCase();
      const hinted = /article|body|content|story|text|description|summary|paragraph|copy|transcript/i.test(key);
      if (t.length >= (hinted ? 80 : 180) && t.includes(' ') && !/^https?:\/\//i.test(t) && !/^\/{1,2}[a-z0-9_-]+\//i.test(t)) {
        strings.push(t);
      }
      return;
    }

    if (Array.isArray(obj)) {
      for (const item of obj) deepExtract(item, depth + 1, keyHint);
      return;
    }

    if (typeof obj === 'object') {
      for (const [key, value] of Object.entries(obj)) {
        if (/^(css|style|class|className|src|href|url|image|thumbnail|icon|logo|tracking|analytics|ad|ads|scripts?|fonts?|config)$/i.test(key)) continue;
        deepExtract(value, depth + 1, key);
      }
    }
  };

  for (const match of stateScripts) {
    const raw = String(match[1] || '').trim();
    if (!raw || raw.length < 40) continue;

    // __NEXT_DATA__, __NUXT__, initial Redux-ish state, and common JSON script blobs.
    const likelyState = /__NEXT_DATA__|__NUXT__|initialState|initial_state|preloadedState|preloaded_state|apollo|redux|__APOLLO_STATE__|pageProps|dehydratedState/i.test(`${match[0]}\n${raw}`);
    if (!likelyState && !/<script[^>]*type=["']application\/json["']/i.test(match[0])) continue;

    const jsonText = raw
      .replace(/^window\.[\w$]+\s*=\s*/i, '')
      .replace(/^self\.[\w$]+\s*=\s*/i, '')
      .replace(/^globalThis\.[\w$]+\s*=\s*/i, '')
      .replace(/;\s*$/, '')
      .trim();

    try {
      const data = JSON.parse(jsonText);
      deepExtract(data);
    } catch {
      // Target large quoted content fields from invalid/truncated state scripts.
      const fallback = extractKnownJsonText(raw);
      if (fallback) candidates.push(fallback);
    }

    if (strings.length >= MAX_STATE_STRINGS) break;
  }

  if (strings.length) {
    const text = finalizeBlocks(strings, MAX_TEXT_CHARS);
    if (text.length >= MIN_REAL_CONTENT) candidates.push({ text, kind: 'spa-state' });
  }

  candidates.sort((a, b) => scoreText(b.text, b.kind) - scoreText(a.text, a.kind));
  return candidates[0] || null;
}

function extractTagBlocks(html, tag, attrRegex = null, maxMatches = 80) {
  const blocks = [];
  const re = attrRegex
    ? new RegExp(`<${tag}\\b[^>]*${attrRegex}[\\s\\S]*?<\\/${tag}>`, 'gi')
    : new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'gi');

  for (const m of String(html).matchAll(re)) {
    const raw = attrRegex ? (m[0] || '') : (m[1] || '');
    const text = cleanText(raw, MAX_TEXT_CHARS);
    if (text.length >= 40) blocks.push(text);
    if (blocks.length >= maxMatches) break;
  }
  return blocks;
}

function extractBestContentWindow(html) {
  let source = String(html)
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<template\b[^>]*>[\s\S]*?<\/template>/gi, ' ')
    .replace(/<(nav|footer|header|aside|form|menu|dialog|svg|canvas|iframe|select|option|button)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ');

  const records = [];
  const re = /<(p|h1|h2|h3|h4|h5|h6|blockquote|li)\b([^>]*)>([\s\S]*?)<\/\1>/gi;
  for (const m of source.matchAll(re)) {
    const raw = String(m[0] || '');
    const text = cleanText(m[3], 9000);
    if (!text || text.length < 18) continue;
    const words = text.split(/\s+/).filter(Boolean).length;
    const links = (String(m[3]).match(/<a\b/gi) || []).length;
    const lower = text.toLowerCase();
    const isHeading = /^h[1-6]$/i.test(m[1]);
    const isList = /^li$/i.test(m[1]);
    let weight = Math.min(14, text.length / 140) + Math.min(8, words / 18);
    if (isHeading) weight += 5;
    if (isList) weight -= 1;
    if (links >= 3 && links >= words / 5) weight -= 7;
    for (const noise of NOISE_WORDS) {
      if (lower.includes(noise)) weight -= 0.9;
    }
    if (/^[\d\W_]+$/.test(text)) weight -= 5;
    records.push({ text, weight, isHeading, rawLength: raw.length });
    if (records.length >= 2400) break;
  }

  if (!records.length) return '';

  // Maximum-subarray style search for the strongest contiguous article-like run.
  // Negative runs are allowed briefly so a short subheading/caption does not split an article.
  let bestStart = 0, bestEnd = 0, bestSum = -Infinity;
  let start = 0, sum = 0;
  let negativeStreak = 0;
  for (let i = 0; i < records.length; i++) {
    const w = records[i].weight;
    if (sum <= 0) {
      start = i;
      sum = w;
      negativeStreak = w < 0 ? 1 : 0;
    } else {
      sum += w;
      negativeStreak = w < 0 ? negativeStreak + 1 : 0;
    }

    if (sum > bestSum) {
      bestSum = sum;
      bestStart = start;
      bestEnd = i;
    }

    // A long consecutive noise run is more likely a page chrome boundary.
    if (negativeStreak >= 5) {
      start = i + 1;
      sum = 0;
      negativeStreak = 0;
    }
  }

  let selected = records.slice(bestStart, bestEnd + 1).map(r => r.text);
  if (selected.length < 3) {
    selected = records.map(r => r.text);
  }
  return finalizeBlocks(selected, MAX_TEXT_CHARS);
}

function extractParagraphs(html, { headings = true } = {}) {
  let source = String(html);
  source = source
    .replace(/<!--\[if[\s\S]*?<!\[endif\]-->/gi, ' ')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<template\b[^>]*>[\s\S]*?<\/template>/gi, ' ')
    .replace(/<(nav|footer|header|aside|form|menu|dialog|svg|canvas|iframe|select|option|button)\b[^>]*>[\s\S]*?<\/\1>/gi, ' ');

  const blocks = [];
  const re = headings
    ? /<(p|h1|h2|h3|h4|h5|h6|li|blockquote)\b[^>]*>([\s\S]*?)<\/\1>/gi
    : /<p\b[^>]*>([\s\S]*?)<\/p>/gi;

  for (const m of source.matchAll(re)) {
    const inner = headings ? m[2] : m[1];
    const text = cleanText(inner, 7000);
    const words = text.split(/\s+/).filter(Boolean);
    if (text.length >= 25 && words.length >= 5) blocks.push(text);
    if (blocks.length >= 1800) break;
  }

  return dedupeBlocks(blocks);
}

function extractClassOrIdCandidates(html) {
  const candidates = [];
  const source = String(html);
  const tags = ['article', 'main', 'section', 'div'];
  const hint = CONTENT_HINTS.map(escapeRegex).join('|');
  const noise = NOISE_HINTS.map(escapeRegex).join('|');

  // First pass: explicit content-hinted elements. Non-greedy extraction is intentional;
  // we validate and score the text rather than assuming a single exact DOM tree.
  const re = new RegExp(`<(${tags.join('|')})\\b[^>]*(?:id|class)\\s*=\\s*["'][^"']*(?:${hint})[^"']*["'][^>]*>([\\s\\S]{100,}?)<\\/\\1>`, 'gi');
  for (const m of source.matchAll(re)) {
    const text = cleanText(m[2], MAX_TEXT_CHARS);
    if (text.length >= MIN_REAL_CONTENT) {
      candidates.push({ text, kind: 'hinted-container' });
    }
    if (candidates.length >= 24) break;
  }

  // Second pass: likely semantic blocks with a large amount of paragraph text.
  const semanticRe = /<(article|main|section)\b[^>]*>([\s\S]{200,}?)<\/\1>/gi;
  for (const m of source.matchAll(semanticRe)) {
    const raw = m[2];
    const text = finalizeBlocks(extractParagraphs(raw), MAX_TEXT_CHARS);
    if (text.length >= MIN_REAL_CONTENT) candidates.push({ text, kind: `semantic-${m[1].toLowerCase()}` });
    if (candidates.length >= 32) break;
  }

  // Noise-hint filtering: reject obvious wrappers that are dominated by site chrome.
  const noiseRe = new RegExp(`(?:id|class)\\s*=\\s*["'][^"']*(?:${noise})[^"']*["']`, 'i');
  return candidates.filter(c => !noiseRe.test(c.text));
}

function extractSemanticHtml(html) {
  const candidates = [];
  const source = String(html);

  const windowText = extractBestContentWindow(source);
  if (windowText.length >= MIN_REAL_CONTENT) candidates.push({ text: windowText, kind: 'semantic-content-window' });

  const paragraphText = finalizeBlocks(extractParagraphs(source, { headings: true }), MAX_TEXT_CHARS);
  if (paragraphText.length >= MIN_REAL_CONTENT) candidates.push({ text: paragraphText, kind: 'semantic-all' });

  const containerCandidates = extractClassOrIdCandidates(source);
  candidates.push(...containerCandidates);

  // Explicit itemprop/microdata and common article roles often survive when class names are obfuscated.
  const attrPatterns = [
    /itemprop\s*=\s*["']articleBody["']/i,
    /itemprop\s*=\s*["']text["']/i,
    /role\s*=\s*["']main["']/i
  ];

  for (const attrRegex of attrPatterns) {
    const attrText = attrRegex.source.replace(/\\\\/g, '\\');
    const tagCandidates = [];
    for (const tag of ['div', 'section', 'article', 'main']) {
      const re = new RegExp(`<${tag}\\b[^>]*${attrText}[^>]*>([\\s\\S]{100,}?)<\\/${tag}>`, 'gi');
      for (const m of source.matchAll(re)) {
        const text = finalizeBlocks(extractParagraphs(m[1]), MAX_TEXT_CHARS);
        if (text.length >= MIN_REAL_CONTENT) tagCandidates.push(text);
        if (tagCandidates.length >= 8) break;
      }
    }
    for (const text of tagCandidates) candidates.push({ text, kind: 'microdata' });
  }

  candidates.sort((a, b) => scoreText(b.text, b.kind) - scoreText(a.text, a.kind));
  return candidates.slice(0, 12);
}

function extractJsonScriptObjects(html) {
  // Extract script[type=application/json] text for known state/content names without
  // walking every arbitrary script on the page.
  const out = [];
  const re = /<script\b[^>]*type=["']application\/json["'][^>]*>([\s\S]*?)<\/script>/gi;
  for (const m of String(html).matchAll(re)) {
    const raw = String(m[1] || '').trim();
    if (raw.length > 30) out.push(raw);
    if (out.length >= 20) break;
  }
  return out;
}

function extractFromJinaMarkdown(raw) {
  let text = String(raw || '');
  text = text
    .replace(/^Title:\s*.+$/im, '')
    .replace(/^URL Source:\s*\S+$/im, '')
    .replace(/^Published Time:\s*.+$/im, '')
    .replace(/^Markdown Content:\s*/im, '')
    .replace(/^Image:\s*.+$/gim, '')
    .replace(/^\s*>?\s*\[.*?\]\([^)]*\)\s*$/gm, '')
    .replace(/<\/?[^>]+>/g, ' ');

  const blocks = [];
  for (const line of text.split(/\n+/)) {
    const cleaned = cleanText(line, 8000);
    if (cleaned.length >= 25) blocks.push(cleaned);
  }
  return finalizeBlocks(blocks, MAX_TEXT_CHARS);
}

async function fetchViaJina(url, budgetMs) {
  const jinaUrl = `https://r.jina.ai/${url}`;
  const res = await fetchSafely(jinaUrl, budgetMs, {
    'Accept': 'text/plain,text/markdown;q=0.95,*/*;q=0.2',
    'X-Retain-Images': 'none'
  });
  if (!res.ok || !res.text) return null;

  const text = extractFromJinaMarkdown(res.text);
  if (text.length < MIN_REAL_CONTENT || isBotChallenge(text)) return null;

  return {
    text,
    kind: 'jina-reader',
    finalUrl: url,
    status: res.status,
    latencyMs: res.latencyMs
  };
}

function extractJsonDocumentBody(value) {
  const candidates = [];
  const raw = String(value || '').trim();
  if (!raw || !/^[\[{]/.test(raw)) return candidates;

  const seen = new Set();
  const visit = (obj, depth = 0, keyHint = '') => {
    if (depth > 9) return;
    if (typeof obj === 'string') {
      const t = cleanText(obj, MAX_TEXT_CHARS);
      const key = String(keyHint).toLowerCase();
      if (t.length >= 120 && t.split(/\s+/).length >= 25 &&
          (/article|body|content|text|description|summary|story|results?|snippet|answer|transcript/i.test(key) || t.length >= 700)) {
        const fp = candidateFingerprint(t);
        if (!seen.has(fp)) {
          seen.add(fp);
          candidates.push({ text: t, kind: 'json-document' });
        }
      }
      return;
    }
    if (Array.isArray(obj)) {
      for (const item of obj) visit(item, depth + 1, keyHint);
      return;
    }
    if (obj && typeof obj === 'object') {
      for (const [key, child] of Object.entries(obj)) {
        if (/^(image|images|icon|logo|src|href|url|css|style|script|tracking|analytics)$/i.test(key)) continue;
        visit(child, depth + 1, key);
      }
    }
  };

  try {
    const data = JSON.parse(raw);
    visit(data);
  } catch {}

  candidates.sort((a, b) => scoreText(b.text, b.kind) - scoreText(a.text, a.kind));
  return candidates.slice(0, 12);
}

function extractAllDirectCandidates(html) {
  const candidates = [];
  if (!html || html.length < 80) return candidates;

  if (/^[\[{]/.test(String(html).trim())) {
    candidates.push(...extractJsonDocumentBody(html));
  }

  if (String(html).length < 200) return candidates;

  const jsonLd = extractDeepJsonLd(html);
  if (jsonLd) candidates.push(jsonLd);

  const jsonText = extractKnownJsonText(html);
  if (jsonText) candidates.push(jsonText);

  const semantic = extractSemanticHtml(html);
  candidates.push(...semantic);

  const spa = extractDeepSpaState(html);
  if (spa) candidates.push(spa);

  // Plain-text extraction is a final direct-layer rescue. It is deliberately lower scored
  // because it often includes site chrome, but it can recover poorly structured publishers.
  const plain = finalizeBlocks(extractParagraphs(html, { headings: false }), MAX_TEXT_CHARS);
  if (plain.length >= MIN_REAL_CONTENT) candidates.push({ text: plain, kind: 'paragraph-rescue' });

  return candidates;
}

function scoreText(text, kind = '') {
  const t = cleanText(text, MAX_TEXT_CHARS);
  if (!t) return -1e9;
  const lower = t.toLowerCase();
  const words = t.split(/\s+/).filter(Boolean);
  const sentences = (t.match(/[.!?।]+(?=\s|$)/g) || []).length;
  const paragraphs = t.split(/\n{2,}/).filter(Boolean).length;
  const uniqueWordRatio = words.length ? new Set(words.map(w => w.toLowerCase())).size / words.length : 0;

  let score = Math.min(220, t.length / 160);
  score += Math.min(120, words.length / 10);
  score += Math.min(70, sentences * 1.7);
  score += Math.min(50, paragraphs * 2.5);
  score += uniqueWordRatio * 45;

  if (/json-ld/.test(kind)) score += 70;
  if (/article|main|hinted|microdata|jina|spa/.test(kind)) score += 45;
  if (t.length >= 1000) score += 30;
  if (t.length >= 5000) score += 35;
  if (t.length >= 15000) score += 45;

  for (const w of NOISE_WORDS) {
    const count = (lower.match(new RegExp(`\\b${escapeRegex(w)}\\b`, 'g')) || []).length;
    if (count) score -= Math.min(45, count * 5);
  }

  if (isBotChallenge(t)) score -= 1000;
  if (words.length < 40) score -= 80;
  if (uniqueWordRatio < 0.35) score -= 100;

  return score;
}

function candidateFingerprint(text) {
  return cleanText(text, 8000)
    .toLowerCase()
    .replace(/[^a-z0-9\u0900-\u097f]+/gi, ' ')
    .replace(/\b(?:the|and|or|a|an|of|to|in|for|on|is|are|was|were|with|that|this)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 1200);
}

function overlapRatio(a, b) {
  const ta = new Set(splitBlocks(a).map(x => candidateFingerprint(x)).filter(Boolean));
  const tb = new Set(splitBlocks(b).map(x => candidateFingerprint(x)).filter(Boolean));
  if (!ta.size || !tb.size) return 0;
  let hit = 0;
  for (const x of ta) if (tb.has(x)) hit++;
  return hit / Math.min(ta.size, tb.size);
}

function mergeComplementary(base, other) {
  if (!base) return other;
  if (!other) return base;
  const overlap = overlapRatio(base.text, other.text);
  if (overlap >= 0.85) {
    return base.text.length >= other.text.length ? base : other;
  }

  const baseBlocks = splitBlocks(base.text);
  const otherBlocks = splitBlocks(other.text);
  const seen = new Set(baseBlocks.map(candidateFingerprint));
  const additions = otherBlocks.filter(block => {
    const fp = candidateFingerprint(block);
    if (!fp || fp.length < 25) return false;
    if (seen.has(fp)) return false;
    seen.add(fp);
    return true;
  });

  // Only merge when the second source has meaningful unique article-like material.
  if (additions.length < 2) return base;
  const merged = finalizeBlocks([...baseBlocks, ...additions], MAX_TEXT_CHARS);
  return {
    ...base,
    text: merged,
    kind: `${base.kind}+supplement`,
    supplementedBy: other.kind
  };
}

function chooseBest(candidates) {
  const usable = candidates
    .filter(Boolean)
    .map(c => ({
      ...c,
      text: cleanText(c.text, MAX_TEXT_CHARS)
    }))
    .filter(c => c.text.length >= MIN_REAL_CONTENT && !isBotChallenge(c.text));

  const unique = [];
  const seen = new Set();
  for (const c of usable) {
    const fp = candidateFingerprint(c.text);
    if (seen.has(fp)) continue;
    seen.add(fp);
    c.score = scoreText(c.text, c.kind);
    unique.push(c);
    if (unique.length >= MAX_CANDIDATES) break;
  }

  unique.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return b.text.length - a.text.length;
  });

  if (!unique.length) return null;

  let best = unique[0];

  // If a second strong source contains extra article paragraphs, supplement it.
  for (let i = 1; i < Math.min(unique.length, 4); i++) {
    const second = unique[i];
    if (second.score < best.score * 0.62) continue;
    if (second.text.length < 350) continue;
    const merged = mergeComplementary(best, second);
    if (merged.text.length > best.text.length * 1.08) {
      merged.score = scoreText(merged.text, merged.kind);
      best = merged;
    }
  }

  return best;
}

function looksLikeRealPage(candidate, title = '') {
  if (!candidate) return false;
  if (candidate.text.length < MIN_REAL_CONTENT) return false;
  if (isBotChallenge(candidate.text)) return false;
  const score = scoreText(candidate.text, candidate.kind);
  const titleBoost = title && candidate.text.toLowerCase().includes(title.toLowerCase().slice(0, 35)) ? 15 : 0;
  return score + titleBoost >= 90;
}

async function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve(null), Math.max(0, ms));
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Multi-path resilient page extraction.
 *
 * @param {string} url
 * @param {number} budgetMs total wall-clock budget, clamped to keep Edge safe
 * @returns {Promise<object>}
 */
export async function scrapePage(url, budgetMs = TOTAL_BUDGET_MS) {
  if (!safeUrl(url)) {
    return {
      success: false,
      version: VERSION,
      reason: 'invalid_url',
      url: String(url || '')
    };
  }

  const started = Date.now();
  const totalBudget = clamp(budgetMs, 900, TOTAL_BUDGET_MS);
  const variants = makeUrlVariants(url);
  const allCandidates = [];
  const diagnostics = {
    directAttempts: 0,
    directSuccesses: 0,
    variantUrls: variants,
    readerStarted: false,
    bytesRead: 0,
    methods: []
  };

  const acceptCandidate = (candidateList, res, sourceUrl) => {
    if (!res?.ok || !res.text) return [];
    const title = extractTitle(res.text);
    const metaDescription = extractMetaDescription(res.text);
    const candidates = extractAllDirectCandidates(res.text).map(c => ({
      ...c,
      source: 'direct',
      requestedUrl: sourceUrl,
      finalUrl: res.finalUrl || sourceUrl,
      httpStatus: res.status,
      title,
      metaDescription
    }));
    allCandidates.push(...candidates);
    candidateList.push(...candidates);
    return candidates;
  };

  // Fast path: the original URL gets a shorter first budget. Most healthy publisher pages
  // finish here, avoiding the latency of additional variants and reader infrastructure.
  diagnostics.directAttempts++;
  const firstBudget = Math.min(3_400, Math.max(900, totalBudget - 2_000));
  const first = await fetchSafely(url, firstBudget);
  diagnostics.bytesRead += Math.min(first.text?.length || 0, MAX_PAGE_BYTES);
  if (first.ok && first.text) {
    diagnostics.directSuccesses++;
    const firstCandidates = [];
    acceptCandidate(firstCandidates, first, url);
    const firstBest = chooseBest(firstCandidates);
    if (firstBest && looksLikeRealPage(firstBest, firstBest.title) && firstBest.text.length >= MIN_GOOD_CONTENT) {
      diagnostics.methods.push(...new Set(firstCandidates.map(c => c.kind).filter(Boolean)));
      return {
        success: true,
        version: VERSION,
        url: firstBest.finalUrl || url,
        requestedUrl: url,
        method: firstBest.kind,
        content: firstBest.text,
        title: firstBest.title || '',
        description: firstBest.metaDescription || '',
        length: firstBest.text.length,
        wordCount: firstBest.text.split(/\s+/).filter(Boolean).length,
        httpStatus: firstBest.httpStatus || 200,
        latencyMs: Date.now() - started,
        extractionScore: Math.round(scoreText(firstBest.text, firstBest.kind) * 10) / 10,
        diagnostics
      };
    }
  }

  // Recovery phase: alternate public page representations + Jina run concurrently.
  const elapsed = Date.now() - started;
  const remaining = totalBudget - elapsed;
  if (remaining <= 700) {
    return {
      success: false,
      version: VERSION,
      url: first.finalUrl || url,
      requestedUrl: url,
      reason: 'content_unreachable_or_bot_challenged',
      httpStatus: first.status || 0,
      latencyMs: Date.now() - started,
      diagnostics
    };
  }

  const parallelBudget = Math.min(5_500, Math.max(900, remaining - 150));
  const alternateUrls = variants.slice(1, 4);

  const alternatePromises = alternateUrls.map(async variant => {
    diagnostics.directAttempts++;
    const res = await fetchSafely(variant, parallelBudget);
    diagnostics.bytesRead += Math.min(res.text?.length || 0, MAX_PAGE_BYTES);
    if (!res.ok || !res.text) return [];
    diagnostics.directSuccesses++;
    const local = [];
    acceptCandidate(local, res, variant);
    return local;
  });

  diagnostics.readerStarted = true;
  const jinaBudget = Math.min(READER_TIMEOUT_MS, parallelBudget);
  const jinaPromise = fetchViaJina(url, jinaBudget).catch(() => null);

  const [alternateResults, jina] = await Promise.all([
    Promise.allSettled(alternatePromises),
    jinaPromise
  ]);

  // acceptCandidate() already inserted each alternate result into allCandidates.
  // Only Jina needs to be added here.
  if (jina) allCandidates.push(jina);

  const best = chooseBest(allCandidates);
  diagnostics.methods.push(...new Set(allCandidates.map(c => c.kind).filter(Boolean)));

  const finalUrl = best?.finalUrl || first.finalUrl || url;
  const title = best?.title || (first.text ? extractTitle(first.text) : '');
  const description = best?.metaDescription || (first.text ? extractMetaDescription(first.text) : '');

  if (!looksLikeRealPage(best, title)) {
    const challenged = isBotChallenge(first.text || '') || allCandidates.some(c => isBotChallenge(c?.text || ''));
    return {
      success: false,
      version: VERSION,
      url: finalUrl,
      requestedUrl: url,
      reason: challenged ? 'content_unreachable_or_bot_challenged' : 'content_unreachable_or_empty',
      httpStatus: best?.httpStatus || first.status || 0,
      title,
      description,
      latencyMs: Date.now() - started,
      diagnostics
    };
  }

  return {
    success: true,
    version: VERSION,
    url: finalUrl,
    requestedUrl: url,
    method: best.kind,
    content: best.text,
    title,
    description,
    length: best.text.length,
    wordCount: best.text.split(/\s+/).filter(Boolean).length,
    httpStatus: best.httpStatus || first.status || 200,
    latencyMs: Date.now() - started,
    extractionScore: Math.round(scoreText(best.text, best.kind) * 10) / 10,
    supplementedBy: best.supplementedBy || null,
    diagnostics
  };
}

export default async function handler() {
  return new Response(JSON.stringify({
    ok: false,
    version: VERSION,
    error: 'Internal module. Import scrapePage() from this file.'
  }), {
    status: 403,
    headers: { 'content-type': 'application/json; charset=utf-8' }
  });
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
