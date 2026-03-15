import os
from typing import Dict, Any
from dotenv import load_dotenv
from datetime import datetime, timedelta
import traceback
import scrapy
from scrapy.crawler import CrawlerProcess
from twisted.internet import defer
from urllib.parse import urlparse

from util.tools import setup_logger
from util.storage_backend import get_storage_backend
from news.items import ArticleItem

# Load environment variables from .env file
load_dotenv()
STORAGE_BACKEND = os.getenv('STORAGE_BACKEND', 'duckdb')

logger = setup_logger()


# ── SHARED HELPERS (module-level so both spiders can use them) ─────────────────

def _parse_inq_art_url(url: str) -> dict:
    parsed = urlparse(url)
    parts = parsed.netloc.split('.')
    subdomain = parts[0]
    origin = parts[1] if len(parts) > 1 else ''
    path_parts = parsed.path.strip('/').split('/', 1)
    article_id = path_parts[0] if path_parts else ''
    slug = path_parts[1] if len(path_parts) > 1 else ''
    return {'subdomain': subdomain, 'origin': origin, 'article_id': article_id, 'slug': slug}


def _make_article_id(url_meta: dict) -> str:
    return f"{url_meta['subdomain']}:{url_meta['article_id']}:{url_meta['slug']}"


# ── PHASE 1: Collect URLs only ────────────────────────────────────────────────

class InquirerLinkSpider(scrapy.Spider):
    """
    Crawls article-index pages only. Yields stub ArticleItems with
    id/source/url/category/date populated — all other fields left absent
    so the DB can store them as NULL.

    Skips articles whose ID already exists in the DB — more granular
    than date-based skipping: partial days are handled correctly.
    """
    name = 'inquirer_links'
    allowed_domains = ['inquirer.net']

    def __init__(self, start_date: str, end_date: str = None, categories: str = None, **kwargs):
        super().__init__(**kwargs)
        self.url_dt_format = '%Y-%m-%d'
        self.start_date = datetime.strptime(start_date, self.url_dt_format)
        self.end_date = datetime.strptime(end_date, self.url_dt_format) if end_date else self.start_date
        self.categories = {c.strip().upper() for c in categories.split(',')} if categories else None

        # Load all existing Inquirer IDs from DB once at spider init.
        # Stored as a set for O(1) lookup — much faster than querying per article.
        db = get_storage_backend(backend_type=STORAGE_BACKEND)
        rows = db.fetch_all(
            f"SELECT DISTINCT id FROM {os.getenv('TABLE_NAME')} WHERE source = 'inquirer'"
        )
        self.existing_ids = {row[0] for row in rows}
        db.close()
        logger.info(f'Phase 1: {len(self.existing_ids)} existing Inquirer IDs loaded from DB.')

    def start_requests(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            date_str = current_date.strftime(self.url_dt_format)
            logger.info(f'Queuing article index for {date_str}')
            yield scrapy.Request(
                url=f'https://www.inquirer.net/article-index/?d={date_str}',
                callback=self.parse_links,
                meta={'current_date': date_str}
            )
            current_date += timedelta(days=1)

    def parse_links(self, response):
        sections = response.css('h4')
        logger.info(f'Found {len(sections)} sections on index page for {response.meta["current_date"]}')

        inserted = 0
        skipped = 0

        for section in sections:
            category = section.css('::text').get(default='').strip()

            if self.categories and category.upper() not in self.categories:
                logger.debug(f'Skipping category: {category}')
                continue

            for link in section.xpath('following-sibling::ul[1]/li/a/@href').getall():
                if not link.startswith('https://'):
                    continue

                try:
                    url_meta = _parse_inq_art_url(link)
                except Exception as e:
                    logger.warning(f'Skipping unparseable URL {link}: {e}')
                    continue
                
                if 'lotto' in url_meta['slug']:
                    continue
                
                if url_meta['subdomain'] == 'cebudailynews' and 'daily-gospel' in url_meta['slug']:
                    continue

                article_id = _make_article_id(url_meta)

                # Skip if ID already exists in DB
                if article_id in self.existing_ids:
                    skipped += 1
                    continue

                # Add to set to avoid yielding duplicates within the same run
                self.existing_ids.add(article_id)
                inserted += 1

                yield ArticleItem(
                    id=article_id,
                    source=url_meta['origin'],
                    url=link,
                    category=category,
                    date=response.meta['current_date'],
                )

        logger.info(
            f'Phase 1 [{response.meta["current_date"]}]: '
            f'{inserted} new articles queued, {skipped} already in DB.'
        )


# ── PHASE 2: Populate article details ─────────────────────────────────────────

class InquirerArticleSpider(scrapy.Spider):
    """
    Reads stub records from the DB (where title IS NULL) and fetches each
    article page to fill in the remaining fields.

    Exits immediately if there are no pending articles.
    """
    name = 'inquirer_articles'
    allowed_domains = ['inquirer.net']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db = get_storage_backend(backend_type=STORAGE_BACKEND)

    def closed(self, reason):
        self.db.close()

    def start_requests(self):
        pending = self.db.get_pending_articles()
        logger.info(f'Phase 2: Found {len(pending)} pending articles to fetch.')

        if not pending:
            logger.info('Phase 2: No pending articles — skipping article crawl.')
            return

        for row in pending:
            yield scrapy.Request(
                url=row['url'],
                callback=self.parse_article_details,
                meta={
                    'category': row['category'],
                    'current_date': row['date'],
                    'use_stealthy': True,
                }
            )

    def parse_article_details(self, response):
        url_meta = _parse_inq_art_url(response.url)

        yield ArticleItem(
            id=_make_article_id(url_meta),
            source=url_meta['origin'],
            url=response.url,
            category=response.meta['category'],
            date=response.meta['current_date'],
            title=self._extract_title(response, url_meta),
            author=self._extract_author(response, url_meta),
            publish_time=self._extract_publish_time(response),
            raw_content=self._extract_content(response, url_meta),
            tags=self._extract_tags(response, url_meta),
        )

    # ── EXTRACTORS ────────────────────────────────────────────────────────────

    def _extract_title(self, response, url_metadata) -> str:
        try:
            match url_metadata['subdomain']:
                case 'lifestyle':
                    return response.css('h1.elementor-heading-title::text').get(default='No title')
                case 'pop':
                    return response.css('div.single-post-banner-inner > h1::text').get(default='No title')
                case 'cebudailynews':
                    return (
                        response.css('#landing-headline h1::text').get()
                        or response.css('#art-hgroup h1::text').get()
                        or 'No title'
                    )
                case 'bandera':
                    return response.css('#landing-headline h1::text').get(default='No title')
                case _:
                    return response.css('h1.entry-title::text').get(default='No title')
        except Exception as e:
            logger.error(f'Error extracting title: {e}')
            logger.debug(traceback.format_exc())
            return 'Error extracting title'

    def _extract_author(self, response, url_metadata) -> str:
        try:
            match url_metadata['subdomain']:
                case 'lifestyle':
                    return response.css(
                        'div.elementor-widget-post-info ul.elementor-post-info li '
                        'span.elementor-post-info__terms-list a::text'
                    ).get(default='No author')
                case 'pop':
                    return response.css("ul.blog-meta-list a[href*='/byline/']::text").get(default='No author')
                case 'cebudailynews':
                    return (
                        response.css('#m-pd2 span::text').re_first(r'By:\s*(.+)')
                        or response.css('.art-byline a::text').get()
                        or 'No author'
                    )
                case 'bandera':
                    return response.css('#m-pd2 span::text').re_first(r'^([\w\s.]+)\s+-') or 'No author'
                case _:
                    byline = response.css('div#art_author::attr(data-byline-strips)').get()
                    if byline:
                        return byline
                    source = response.css('div#art_plat *::text').getall()
                    return source[1] if len(source) > 2 else 'No author'
        except Exception as e:
            logger.error(f'Error extracting author: {e}')
            logger.debug(traceback.format_exc())
            return 'Error extracting author'

    def _extract_content(self, response, url_metadata) -> str:
        try:
            content_selectors = {
                'lifestyle': 'div.elementor-widget-theme-post-content',
                'pop': 'div#TO_target_content',
                'cebudailynews': 'div#article-content',
                'usa': 'div#TO_target_content',
            }
            selector = content_selectors.get(url_metadata['subdomain'], 'div#FOR_target_content')
            return response.css(selector).get(default='Cannot extract article content')
        except Exception as e:
            logger.error(f'Error extracting content: {e}')
            logger.debug(traceback.format_exc())
            return 'Error extracting content'

    def _extract_tags(self, response, url_metadata) -> str:
        tags = []
        try:
            match url_metadata['subdomain']:
                case 'pop':
                    tags = response.css('div.tags-box span.tags-links a::attr(href)').getall()
                case _:
                    tags = response.css('div#article_tags a::attr(href)').getall()
            tags = [tag.split('/tag/')[1] for tag in tags if '/tag/' in tag]
        except Exception as e:
            logger.error(f'Error extracting tags: {e} on {url_metadata}')
            logger.debug(traceback.format_exc())
        finally:
            return ', '.join(tags)

    def _extract_publish_time(self, response) -> datetime | None:
        publish_time = None
        try:
            meta_tags = response.css('meta[property="article:published_time"]::attr(content)').getall()
            for content in meta_tags:
                try:
                    if ',' in content and content.split(',')[0].strip().isalpha():
                        cleaned = content.strip()
                        for tz_label in ('PST', 'PHT', 'UTC', 'GMT'):
                            cleaned = cleaned.replace(tz_label, '').strip()
                        publish_time = datetime.strptime(cleaned, "%a, %d %b %Y %H:%M:%S")
                    else:
                        publish_time = datetime.fromisoformat(content)
                    break
                except Exception:
                    continue
        except Exception as e:
            logger.error(f'Error extracting publish time: {e}')
            logger.debug(traceback.format_exc())
        finally:
            return publish_time


# ── DEBUG UTILITY ─────────────────────────────────────────────────────────────

def debug_article(url: str) -> dict:
    """
    Fetch a single article URL and run all extractors on it.
    Prints each extracted field and returns them as a dict.
    No DB writes, no full crawl — safe to run anytime.

    Usage:
        # From a Python shell or Jupyter notebook:
        from news.crawler import debug_article
        debug_article('https://newsinfo.inquirer.net/12345678/some-article-slug')

        # From the CLI:
        python main.py --debug-url "https://newsinfo.inquirer.net/12345678/some-article-slug"
    """
    import requests
    from parsel import Selector

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/122.0.0.0 Safari/537.36'
        )
    }

    print(f'\n{"=" * 60}')
    print(f'DEBUG: {url}')
    print(f'{"=" * 60}')

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f'❌ Failed to fetch URL: {e}')
        return {}

    # Wrap parsel Selector so extractor methods work identically to Scrapy
    class FakeResponse:
        def __init__(self, text, url):
            self.url = url
            self._sel = Selector(text=text)
        def css(self, query):
            return self._sel.css(query)
        def xpath(self, query):
            return self._sel.xpath(query)

    fake_response = FakeResponse(response.text, url)
    url_meta = _parse_inq_art_url(url)

    # Instantiate spider without calling __init__ (no DB connection needed)
    spider = InquirerArticleSpider.__new__(InquirerArticleSpider)

    results = {
        'url':          url,
        'url_meta':     url_meta,
        'id':           _make_article_id(url_meta),
        'title':        spider._extract_title(fake_response, url_meta),
        'author':       spider._extract_author(fake_response, url_meta),
        'publish_time': spider._extract_publish_time(fake_response),
        'tags':         spider._extract_tags(fake_response, url_meta),
        'content':      spider._extract_content(fake_response, url_meta),
    }

    for field, value in results.items():
        if field == 'content':
            preview = str(value)[:300] + '...' if value and len(str(value)) > 300 else value
            print(f'\n📄 content (first 300 chars):\n{preview}')
        else:
            print(f'\n🔹 {field}:\n  {value}')

    print(f'\n{"=" * 60}\n')
    return results


# ── SHARED SCRAPY SETTINGS ────────────────────────────────────────────────────

def _base_settings(extra_pipelines: dict = None) -> dict:
    pipelines = {
        'news.pipelines.InquirerCleaningPipeline': 200,
        'news.pipelines.DatabasePipeline': 300,
    }
    if extra_pipelines:
        pipelines.update(extra_pipelines)

    return {
        'USER_AGENT': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/122.0.0.0 Safari/537.36'
        ),
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 15,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 3.0,
        'AUTOTHROTTLE_DEBUG': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [429, 500, 502, 503, 504],
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'INFO',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
            'news.middlewares.stealthy_middleware.CloudflareBypassMiddleware': 543,
        },
        'ITEM_PIPELINES': pipelines,
    }


# ── RUNNERS ───────────────────────────────────────────────────────────────────

def collect_links(
    start_date: str = '2026-01-01',
    end_date: str = datetime.today().strftime('%Y-%m-%d'),
    categories: list[str] | None = None,
):
    """Phase 1 — fast crawl of index pages only. Stores stub records in DB."""
    process = CrawlerProcess(settings=_base_settings())
    process.crawl(
        InquirerLinkSpider,
        start_date=start_date,
        end_date=end_date,
        categories=','.join(categories) if categories else None,
    )
    try:
        process.start()
    except KeyboardInterrupt:
        logger.info('Crawler interrupted by user.')


def populate_articles():
    """Phase 2 — fetch each pending article page and fill in the stub records."""
    process = CrawlerProcess(settings=_base_settings())
    process.crawl(InquirerArticleSpider)
    try:
        process.start()
    except KeyboardInterrupt:
        logger.info('Crawler interrupted by user.')


def refresh_news_articles(
    start_date: str = '2026-01-01',
    end_date: str = datetime.today().strftime('%Y-%m-%d'),
    categories: list[str] | None = None,
):
    """Runs link collection then article population in a single reactor lifecycle."""
    process = CrawlerProcess(settings=_base_settings())

    @defer.inlineCallbacks
    def _crawl_chain():
        # Phase 1 — collect links (skips IDs already in DB)
        yield process.crawl(
            InquirerLinkSpider,
            start_date=start_date,
            end_date=end_date,
            categories=','.join(categories) if categories else None,
        )
        # Phase 2 — populate articles (skips if nothing pending)
        yield process.crawl(InquirerArticleSpider)

    _crawl_chain()
    try:
        process.start()  # blocks here until both spiders are done
    except KeyboardInterrupt:
        logger.info('Crawler interrupted by user.')