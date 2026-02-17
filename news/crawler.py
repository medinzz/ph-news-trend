"""
Inquirer.net crawler using pure Scrapling — no Scrapy involved.

Two-phase approach:
  Phase 1 — Article index pages (Fetcher):
    inquirer.net/article-index is NOT behind Cloudflare, so a fast
    plain HTTP fetch is enough to collect all article URLs per date.

  Phase 2 — Individual article pages (StealthyFetcher):
    Each article page IS behind Cloudflare. StealthyFetcher runs a
    modified Firefox (Camoufox) that spoofs its fingerprint and solves
    Cloudflare challenges automatically.

No asyncio, no threading, no event loop conflicts.
"""

import time
import random
import traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse

from scrapling.fetchers import Fetcher, StealthyFetcher, StealthySession

from util.tools import setup_logger, html_to_markdown
from util.storage_backend import StorageBackend
from config import get_storage_backend_instance

logger = setup_logger()

# ── Selectors per subdomain ───────────────────────────────────────────────────
# Mirrors your original extract_* methods but as plain dicts for clarity.
# Extend these if Inquirer updates its HTML structure.

TITLE_SELECTORS = {
    'lifestyle':     'h1.elementor-heading-title::text',
    'pop':           'div.single-post-banner-inner > h1::text',
    'cebudailynews': '#landing-headline h1::text',
    'bandera':       '#landing-headline h1::text',
    '_default':      'h1.entry-title::text',
}

AUTHOR_SELECTORS = {
    'lifestyle':     'div.elementor-widget-post-info ul.elementor-post-info li span.elementor-post-info__terms-list a::text',
    'pop':           "ul.blog-meta-list a[href*='/byline/']::text",
    'cebudailynews': '.art-byline a::text',
    'bandera':       '#m-pd2 span::text',
    '_default':      'div#art_author::attr(data-byline-strips)',
}

CONTENT_SELECTORS = {
    'lifestyle':     'div.elementor-widget-theme-post-content',
    'pop':           'div#TO_target_content',
    'cebudailynews': 'div#article-content',
    'bandera':       'div#TO_target_content',
    '_default':      'div#FOR_target_content',
}

TAGS_SELECTORS = {
    'pop':      'div.tags-box span.tags-links a::attr(href)',
    '_default': 'div#article_tags a::attr(href)',
}

# HTML elements to strip before converting content to Markdown
UNWANTED_IDS = [
    'billboard_article',
    'article-new-featured',
    'taboola-mid-article-thumbnails',
    'taboola-mid-article-thumbnails-stream',
    'fb-root',
]
UNWANTED_CLASSES = ['ztoop', 'sib-form', 'cdn_newsletter']
UNWANTED_TAGS   = ['script', 'style']


# ── URL helpers ───────────────────────────────────────────────────────────────

def parse_inq_art_url(url: str) -> dict:
    """
    Parse an Inquirer article URL into its components.

    Returns:
        dict with keys: subdomain, origin, article_id, slug
    """
    parsed   = urlparse(url)
    parts    = parsed.netloc.split('.')
    subdomain = parts[0]
    origin    = parts[1] if len(parts) > 1 else ''
    path_parts = parsed.path.strip('/').split('/', 1)
    article_id = path_parts[0] if path_parts else ''
    slug       = path_parts[1] if len(path_parts) > 1 else ''
    return {
        'subdomain':  subdomain,
        'origin':     origin,
        'article_id': article_id,
        'slug':       slug,
    }


# ── Extraction helpers ────────────────────────────────────────────────────────

def _sel(selectors: dict, subdomain: str) -> str:
    """Return the selector for the given subdomain, falling back to _default."""
    return selectors.get(subdomain, selectors['_default'])


def extract_title(page, subdomain: str) -> str:
    try:
        sel = _sel(TITLE_SELECTORS, subdomain)
        el  = page.css_first(sel)
        if el:
            return el.clean()

        # cebudailynews fallback
        if subdomain == 'cebudailynews':
            el = page.css_first('#art-hgroup h1::text')
            if el:
                return el.clean()

        return 'No title'
    except Exception as e:
        logger.error(f'extract_title error: {e}')
        return 'Error extracting title'


def extract_author(page, subdomain: str) -> str:
    try:
        sel = _sel(AUTHOR_SELECTORS, subdomain)

        if subdomain == 'cebudailynews':
            # Try "By: <name>" pattern first
            span = page.css_first('#m-pd2 span')
            if span:
                match = span.re_first(r'By:\s*(.+)')
                if match:
                    return match.strip()
            el = page.css_first(sel)
            return el.clean() if el else 'No author'

        if subdomain == 'bandera':
            span = page.css_first('#m-pd2 span')
            if span:
                match = span.re_first(r'^([\w\s.]+)\s+-')
                if match:
                    return match.strip()
            return 'No author'

        # Default: try data-byline-strips attribute first, then art_plat text
        el = page.css_first(sel)
        if el:
            return el.clean()
        source = page.css('div#art_plat *::text').getall()
        return source[1].strip() if len(source) > 2 else 'No Author'

    except Exception as e:
        logger.error(f'extract_author error: {e}')
        return 'Error extracting author'


def extract_content(page, subdomain: str) -> str:
    try:
        sel = _sel(CONTENT_SELECTORS, subdomain)
        el  = page.css_first(sel)
        return el.html if el else 'Cannot extract article content'
    except Exception as e:
        logger.error(f'extract_content error: {e}')
        return 'Error extracting content'


def extract_tags(page, subdomain: str) -> str:
    try:
        # Strip ::attr(href) — use .attrib['href'] on each element instead
        sel   = _sel(TAGS_SELECTORS, subdomain).replace('::attr(href)', '')
        els   = page.css(sel)
        hrefs = [el.attrib.get('href', '') for el in els]
        tags  = [h.split('/tag/')[1] for h in hrefs if '/tag/' in h]
        return ', '.join(tags)
    except Exception as e:
        logger.error(f'extract_tags error: {e}')
        return ''


def extract_publish_time(page) -> datetime | None:
    formats = [
        ("%a, %d %b %Y %H:%M:%S", True),
        ("%Y-%m-%dT%H:%M:%S%z",   False),
        ("%Y-%m-%dT%H:%M:%S",     False),
    ]
    try:
        # ::attr(content) → use .attrib on each matched element
        meta_els = page.css('meta[property="article:published_time"]')
        for el in meta_els:
            content = el.attrib.get('content', '').strip()
            if not content:
                continue
            for fmt, strip_pst in formats:
                try:
                    val = content.replace('PST', '').strip() if strip_pst else content
                    return datetime.strptime(val, fmt)
                except ValueError:
                    continue
            try:
                return datetime.fromisoformat(content)
            except ValueError:
                continue
    except Exception as e:
        logger.error(f'extract_publish_time error: {e}')
    return None


# ── Main crawler ──────────────────────────────────────────────────────────────

def _build_article(page, url: str, category: str, current_date: str) -> dict:
    """Parse a fetched Scrapling page into a storage-ready dict."""
    meta       = parse_inq_art_url(url)
    subdomain  = meta['subdomain']
    article_id = f"{subdomain}:{meta['article_id']}:{meta['slug']}"

    raw_content     = extract_content(page, subdomain)
    cleaned_content = html_to_markdown(
        html=raw_content,
        unwanted_ids=UNWANTED_IDS,
        unwanted_classes=UNWANTED_CLASSES,
        unwanted_tags=UNWANTED_TAGS,
    )

    publish_time = extract_publish_time(page)

    return {
        'id':              article_id,
        'source':          meta['origin'],
        'url':             url,
        'category':        category,
        'title':           extract_title(page, subdomain),
        'author':          extract_author(page, subdomain),
        'date':            current_date,
        'publish_time':    publish_time.isoformat() if publish_time else None,
        'cleaned_content': cleaned_content,
        'tags':            extract_tags(page, subdomain),
    }


def _collect_article_links(date_str: str) -> list[tuple[str, str]]:
    """
    Phase 1 — fetch the article index page for one date using plain Fetcher.
    inquirer.net/article-index is not behind Cloudflare so StealthyFetcher
    is not needed here, keeping this phase fast.

    Returns:
        List of (url, category) tuples for all valid articles on that date.
    """
    index_url = f'https://www.inquirer.net/article-index/?d={date_str}'
    logger.info(f'Fetching index: {index_url}')

    results = []
    try:
        page = Fetcher.get(
            index_url,
            stealthy_headers=True,   # Realistic browser headers
            impersonate='chrome',    # Spoof Chrome TLS fingerprint
        )

        if page.status != 200:
            logger.warning(f'Index page returned HTTP {page.status} for {date_str}')
            return results

        sections = page.css('h4')
        for section in sections:
            # Scrapling uses .text or .get_all_text() — not ::text pseudo-elements
            category = section.get_all_text(strip=True) or ''

            links = section.xpath(
                'following-sibling::ul[1]/li/a/@href'
            ).getall()

            for link in links:
                if not link.startswith('https://'):
                    continue
                meta = parse_inq_art_url(link)
                # Skip cebudailynews daily-gospel articles (same filter as original)
                if meta['subdomain'] == 'cebudailynews' and 'daily-gospel' in meta['slug']:
                    continue
                results.append((link, category))

    except Exception as e:
        logger.error(f'Error fetching index for {date_str}: {e}')
        logger.debug(traceback.format_exc())

    logger.info(f'Found {len(results)} articles for {date_str}')
    return results


def _fetch_article(
    session: StealthySession,
    url: str,
    category: str,
    current_date: str,
    max_retries: int = 2,
) -> dict | None:
    """
    Phase 2 — fetch one article page via an existing StealthySession.
    Retries up to max_retries times with exponential back-off.

    Returns:
        Parsed article dict, or None on failure.
    """
    for attempt in range(1, max_retries + 1):
        try:
            page = session.fetch(
                url,
                timeout=60,
                network_idle=True,
            )

            if page.status != 200:
                logger.warning(
                    f'HTTP {page.status} on attempt {attempt}/{max_retries}: {url}'
                )
                if attempt < max_retries:
                    time.sleep(5 * attempt)
                continue

            return _build_article(page, url, category, current_date)

        except Exception as e:
            logger.error(f'Attempt {attempt}/{max_retries} failed for {url}: {e}')
            logger.debug(traceback.format_exc())
            if attempt < max_retries:
                time.sleep(5 * attempt)

    logger.error(f'Giving up after {max_retries} attempts: {url}')
    return None


def refresh_news_articles(
    start_date: str = '2001-01-01',
    end_date:   str = datetime.today().strftime('%Y-%m-%d'),
    delay:      tuple[float, float] = (5.0, 7.0),
    max_retries: int = 2,
    storage: StorageBackend = None,
) -> dict:
    """
    Full Inquirer crawl using pure Scrapling — no Scrapy involved.

    Phase 1: Fetcher collects article URLs from the index pages (fast, no Cloudflare).
    Phase 2: StealthyFetcher scrapes each article page (Cloudflare bypass).

    Args:
        start_date:  Earliest date to scrape (YYYY-MM-DD).
        end_date:    Latest date to scrape (YYYY-MM-DD), inclusive.
        delay:       (min, max) seconds to sleep between article fetches.
        max_retries: Retry attempts per article before giving up.
        storage:     Optional pre-configured StorageBackend. If None,
                     reads from config.py / .env automatically.

    Returns:
        dict with 'success', 'failed', 'skipped' counts.
    """
    fmt        = '%Y-%m-%d'
    start      = datetime.strptime(start_date, fmt)
    end        = datetime.strptime(end_date,   fmt)
    stats      = {'success': 0, 'failed': 0, 'skipped': 0}
    own_storage = storage is None

    if own_storage:
        storage = get_storage_backend_instance()

    # ── Phase 1: collect all article links across the date range ─────────────
    all_links: list[tuple[str, str, str]] = []  # (url, category, date_str)
    current = start
    while current <= end:
        date_str = current.strftime(fmt)
        links    = _collect_article_links(date_str)
        all_links.extend((url, cat, date_str) for url, cat in links)
        current += timedelta(days=1)

    logger.info(f'Total articles to scrape: {len(all_links)}')

    # ── Phase 2: scrape each article via a single StealthySession ────────────
    # StealthySession keeps the browser alive across all requests so
    # Cloudflare is solved once and cookies carry over to every fetch.
    # headless and solve_cloudflare are passed directly to the session —
    # the old constructor/configure() args are no longer supported.
    with StealthySession(
        headless=True,
        solve_cloudflare=True,
    ) as session:

        for i, (url, category, date_str) in enumerate(all_links, start=1):
            logger.info(f'[{i}/{len(all_links)}] {url}')

            article = _fetch_article(session, url, category, date_str, max_retries)

            if article is None:
                stats['failed'] += 1
            else:
                storage.insert_record(article)
                stats['success'] += 1
                logger.info(f"  ✓ {article.get('title', '')[:60]}")

            if i < len(all_links):
                sleep_for = random.uniform(*delay)
                logger.debug(f'  Sleeping {sleep_for:.1f}s...')
                time.sleep(sleep_for)

    if own_storage:
        storage.close()

    logger.info(
        f"Done — success: {stats['success']}, "
        f"failed: {stats['failed']}, "
        f"skipped: {stats['skipped']}"
    )
    return stats