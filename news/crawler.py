from datetime import datetime, timedelta
from doctest import debug
import traceback
import scrapy
from scrapy.crawler import CrawlerProcess

# Local lib
from util.tools import parse_inq_art_url, setup_logger
from news.items import ArticleItem

debug_log = setup_logger()


class InquirerArticlesLinksSpider(scrapy.Spider):
    """
    `InquirerArticlesLinksSpider` is a Scrapy spider for scraping article links and metadata
    from the Philippine Daily Inquirer website based on a specified date range.

    Attributes:
        name (str): The name of the spider.
        allowed_domains (list): A list of domains that the spider is allowed to crawl.
        start_date (datetime): The starting date for scraping articles.
        end_date (datetime): The ending date for scraping articles.
        news_articles (list): A list to store scraped article metadata.

    Methods:
        __init__(start_date: str, end_date: str, **kwargs):
            Initializes the spider with a start date and an optional end date.

        start_requests():
            Generates initial requests for each date in the specified range,
            targeting the article index page for that date.

        parse(response):
            Extracts sections, article links, and metadata from the article index page.
            Writes the extracted links to a JSON file.

        parse_article_details(response):
            Extracts detailed metadata for each article, including title, author,
            publication date, and content. Cleans unwanted elements from the article content.

        closed(reason):
            Saves the scraped article metadata to a JSON file when the spider finishes.
    """
    name = 'inquirer_articles_from_archive'
    allowed_domains = ['inquirer.net']

    def __init__(self, start_date: str, end_date: str, **kwargs):
        super().__init__(**kwargs)
        self.url_dt_format = '%Y-%m-%d'

        self.start_date = datetime.strptime(start_date, self.url_dt_format)
        self.end_date = datetime.strptime(
            end_date, self.url_dt_format) if end_date else self.start_date
        self.news_articles = []

    def start_requests(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            url = f'https://www.inquirer.net/article-index/?d={current_date.strftime(self.url_dt_format)}'
            yield scrapy.Request(
                url=url,
                callback=self.parse_links,
                meta={'current_date': current_date.strftime(self.url_dt_format)}
            )
            current_date += timedelta(days=1)
            debug_log.info(f'Scraping articles from {current_date.strftime(self.url_dt_format)}')

    def parse_links(self, response):
        sections = response.css('h4')
        for section in sections:
            category = section.css('::text').get(default='').strip()
            links = section.xpath('following-sibling::ul[1]/li/a/@href').getall()

            valid_links = [
                link for link in links
                if link.startswith('https://') and not (
                    'daily-gospel' in parse_inq_art_url(link)['slug'] and
                    parse_inq_art_url(link)['subdomain'] == 'cebudailynews'
                )
            ]

            for link in valid_links:
                yield scrapy.Request(
                    url=link,
                    callback=self.parse_article_details,
                    meta={
                        'category': category,
                        'current_date': response.meta['current_date']
                    }
                )

    def parse_article_details(self, response):
        url_metadata = parse_inq_art_url(response.url)
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

    def extract_title(self, response, url_metadata):
        try:
            match url_metadata['subdomain']:
                case 'lifestyle':
                    return response.css('h1.elementor-heading-title::text').get(default='No title')
                case 'pop':
                    return response.css('div.single-post-banner-inner > h1::text').get()
                case 'cebudailynews':
                    return response.css(
                        '#landing-headline h1::text'
                    ).get(default=response.css('#art-hgroup h1::text').get(default='No title'))
                case 'bandera':
                    return response.css('#landing-headline h1::text').get(default='No title')
                case _:
                    return response.css('h1.entry-title::text').get(default='No title')
        except Exception as e:
            debug_log.error(f'Error extracting title: {e}')
            debug_log.debug(traceback.format_exc())
            return 'Error extracting title'

    def extract_author(self, response, url_metadata):
        try:
            match url_metadata['subdomain']:
                case 'lifestyle':
                    return response.css(
                        'div.elementor-widget-post-info ul.elementor-post-info li span.elementor-post-info__terms-list a::text'
                    ).get(default='No author')
                case 'pop':
                    return response.css("ul.blog-meta-list a[href*='/byline/']::text").get()
                case 'cebudailynews':
                    return response.css('#m-pd2 span::text').re_first(r'By:\s*(.+)') or response.css(
                        '.art-byline a::text').get(default='No author')
                case 'bandera':
                    return response.css('#m-pd2 span::text').re_first(r'^([\w\s.]+)\s+-')
                case _:
                    source = response.css('div#art_plat *::text').getall()
                    return response.css(
                        'div#art_author::attr(data-byline-strips)'
                    ).get(default=source[1] if len(source) > 2 else 'No Author')
        except Exception as e:
            debug_log.error(f'Error extracting author: {e}')
            debug_log.debug(traceback.format_exc())
            return 'Error extracting author'

    def extract_content(self, response, url_metadata):
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
            debug_log.error(f'Error extracting content: {e}')
            debug_log.debug(traceback.format_exc())
            return 'Error extracting content'
        
    def extract_tags(self, response, url_metadata):
        tags = []
        try:
            match url_metadata['subdomain']:
                case 'pop':
                    tags = response.css('div.tags-box span.tags-links a::attr(href)').getall()
                case _:
                    tags = response.css('div#article_tags a::attr(href)').getall()
                    
            tags = [tag.split('/tag/')[1] for tag in tags if '/tag/' in tag]
                    
        except Exception as e:
            debug_log.error(f'Error extracting tags: {e} on {url_metadata}')
            debug_log.debug(traceback.format_exc())
        
        finally:
            return ', '.join(tags)
    
    def extract_publish_time(self, response):
        publish_time = None
        try:
            meta_tags = response.css('meta[property="article:published_time"]::attr(content)').getall()
            for content in meta_tags:
                try:
                    # Prefer the format with weekday (e.g., "Sat, 19 Apr 2025 09:18:07 PST")
                    if ',' in content and content.split(',')[0].strip().isalpha():
                        if 'PST' in content:
                            content = content.replace('PST', '')
                        publish_time = datetime.strptime(content.strip(), "%a, %d %b %Y %H:%M:%S")
                    else:
                        publish_time = datetime.fromisoformat(content).striptime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    continue
                
        except Exception as e:
            debug_log.error(f'Error extracting publish time: {e}')
            debug_log.debug(traceback.format_exc())
            return 'Error extracting publish time'

        finally:
            return publish_time


def refresh_news_articles(start_date: str = '2001-01-01', end_date: str = datetime.today().strftime('%Y-%m-%d')):
    process = CrawlerProcess(
        settings={
            'USER_AGENT': 'Mozilla/5.0',
            'DOWNLOAD_DELAY': 1,
            'LOG_LEVEL': 'INFO',
            'ROBOTSTXT_OBEY': True,
            'ITEM_PIPELINES': {
                'news.pipelines.InquirerCleaningPipeline': 200,
                'news.pipelines.SQLitePipeline': 300,
            },
        }
    )

    process.crawl(
        InquirerArticlesLinksSpider,
        start_date=start_date,
        end_date=end_date
    )

    process.start()
