from datetime import datetime, timedelta
import traceback
import scrapy
from scrapy.crawler import CrawlerProcess

# Local lib
from util.tools import setup_logger
from news.items import ArticleItem

logger = setup_logger()


class InquirerArticlesLinksSpider(scrapy.Spider):
    name = 'inquirer_articles_from_archive'
    allowed_domains = ['inquirer.net']

    def __init__(self, start_date: str, end_date: str = None, categories: str = None, **kwargs):
        super().__init__(**kwargs)
        self.url_dt_format = '%Y-%m-%d'
        self.start_date = datetime.strptime(start_date, self.url_dt_format)
        self.end_date = datetime.strptime(end_date, self.url_dt_format) if end_date else self.start_date
        self.categories = {c.strip().upper() for c in categories.split(',')} if categories else None

    def start_requests(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            date_str = current_date.strftime(self.url_dt_format)
            url = f'https://www.inquirer.net/article-index/?d={date_str}'
            logger.info(f'Queuing article index for {date_str}')
            yield scrapy.Request(
                url=url,
                callback=self.parse_links,
                meta={'current_date': date_str}
            )
            current_date += timedelta(days=1)

    def parse_links(self, response):
        sections = response.css('h4')
        logger.info(f'Found {len(sections)} sections on index page for {response.meta["current_date"]}')

        for section in sections:
            category = section.css('::text').get(default='').strip()

            # ── CATEGORY FILTER ──────────────────────────────────────────────
            if self.categories and category.upper() not in self.categories:
                logger.debug(f'Skipping category: {category}')
                continue
            # ────────────────────────────────────────────────────────────────
            links = section.xpath('following-sibling::ul[1]/li/a/@href').getall()

            for link in links:
                # Skip non-https links
                if not link.startswith('https://'):
                    continue

                # FIX: parse each URL individually with try/except instead of inside a
                # list comprehension. A single bad URL in the old approach would cause
                # parse_inq_art_url to raise, silently emptying valid_links to [],
                # yielding zero requests, and making Scrapy close immediately.
                try:
                    url_meta = self.parse_inq_art_url(link)
                except Exception as e:
                    logger.warning(f'Skipping unparseable URL {link}: {e}')
                    continue

                # Filter out cebudailynews daily gospel articles
                if url_meta['subdomain'] == 'cebudailynews' and 'daily-gospel' in url_meta['slug']:
                    continue

                yield scrapy.Request(
                    url=link,
                    callback=self.parse_article_details,
                    meta={
                        'category': category,
                        'current_date': response.meta['current_date'],
                        'use_stealthy': True,
                    }
                )

    def parse_article_details(self, response):
        url_metadata = self.parse_inq_art_url(response.url)
        article_id = f"{url_metadata['subdomain']}:{url_metadata['article_id']}:{url_metadata['slug']}"

        item = ArticleItem(
            id=article_id,
            source=url_metadata['origin'],
            url=response.url,
            category=response.meta['category'],
            title=self.extract_title(response, url_metadata),
            author=self.extract_author(response, url_metadata),
            date=response.meta['current_date'],
            publish_time=self.extract_publish_time(response),
            raw_content=self.extract_content(response, url_metadata),
            tags=self.extract_tags(response, url_metadata)
        )

        yield item

    # ── URL TOOLS ──────────────────────────────────────────────────────────────

    def parse_inq_art_url(self, url: str) -> dict:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        parts = parsed.netloc.split('.')
        subdomain = parts[0]
        origin = parts[1] if len(parts) > 1 else ''

        path_parts = parsed.path.strip('/').split('/', 1)
        article_id = path_parts[0] if path_parts else ''
        slug = path_parts[1] if len(path_parts) > 1 else ''

        return {
            'subdomain': subdomain,
            'origin': origin,
            'article_id': article_id,
            'slug': slug,
        }

    # ── EXTRACTORS ─────────────────────────────────────────────────────────────

    def extract_title(self, response, url_metadata) -> str:
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

    def extract_author(self, response, url_metadata) -> str:
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

    def extract_content(self, response, url_metadata) -> str:
        try:
            content_selectors = {
                'lifestyle': 'div.elementor-widget-theme-post-content',
                'pop': 'div#TO_target_content',
                'cebudailynews': 'div#article-content',
                'bandera': 'div#TO_target_content',
            }
            selector = content_selectors.get(url_metadata['subdomain'], 'div#FOR_target_content')
            return response.css(selector).get(default='Cannot extract article content')
        except Exception as e:
            logger.error(f'Error extracting content: {e}')
            logger.debug(traceback.format_exc())
            return 'Error extracting content'

    def extract_tags(self, response, url_metadata) -> str:
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

    def extract_publish_time(self, response) -> datetime | None:
        publish_time = None
        try:
            meta_tags = response.css('meta[property="article:published_time"]::attr(content)').getall()
            for content in meta_tags:
                try:
                    if ',' in content and content.split(',')[0].strip().isalpha():
                        # RFC 2822: "Sat, 19 Apr 2025 09:18:07 PST"
                        cleaned = content.strip()
                        for tz_label in ('PST', 'PHT', 'UTC', 'GMT'):
                            cleaned = cleaned.replace(tz_label, '').strip()
                        publish_time = datetime.strptime(cleaned, "%a, %d %b %Y %H:%M:%S")
                    else:
                        # ISO 8601: "2025-04-19T09:18:07+08:00"
                        publish_time = datetime.fromisoformat(content)
                    break
                except Exception:
                    continue
        except Exception as e:
            logger.error(f'Error extracting publish time: {e}')
            logger.debug(traceback.format_exc())
        finally:
            return publish_time


# ── RUNNER ─────────────────────────────────────────────────────────────────────

def refresh_news_articles(
    start_date: str = '2026-01-01',
    end_date: str = datetime.today().strftime('%Y-%m-%d'),
    categories: list[str] | None = None   # e.g. ['NEWS', 'SPORTS', 'TECHNOLOGY']
):
    process = CrawlerProcess(
        settings={
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
            'ITEM_PIPELINES': {
                'news.pipelines.InquirerCleaningPipeline': 200,
                'news.pipelines.DatabasePipeline': 300,
            },
        }
    )

    process.crawl(
        InquirerArticlesLinksSpider,
        start_date=start_date,
        end_date=end_date,
        categories=','.join(categories) if categories else None,
    )

    process.start()