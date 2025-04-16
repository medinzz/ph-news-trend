import scrapy
import json
import traceback

from scrapy.crawler import CrawlerProcess
from datetime import datetime, timedelta

# Local lib
from util.tools import (
    parse_inq_art_url,
    clean_article
)
from util.logger import setup_logger

debug_log = setup_logger()


class InquirerArticlesLinksSpider(scrapy.Spider):
    '''
    `InquirerArticlesLinksSpider` is a Scrapy spider for scraping article links from the Philippine Daily Inquirer website 
    based on a specified date range.

    Attributes:
        name (str): The name of the spider.
        allowed_domains (list): A list of domains that the spider is allowed to crawl.
        start_date (datetime): The starting date for scraping articles.
        end_date (datetime): The ending date for scraping articles.
        news_articles (list): A list to store scraped article metadata.

    Methods:
        __init__(start_date='2025-01-01', end_date=None, **kwargs):
            Initializes the spider with a start date and an optional end date.

        start_requests():
            Generates initial requests for each date in the specified range, 
            targeting the article index page for that date.

        parse(response):
            Extracts article links and metadata from the article index page.

        parse_article_details(response):
            Extracts detailed metadata for each article.

        closed(reason):
            Saves the scraped article metadata to a JSON file when the spider finishes.
    '''
    name = 'inquirer_articles_from_archive'
    allowed_domains = ['inquirer.net']

    def __init__(self, start_date: str = '2025-01-01', end_date: str = None, **kwargs):
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
                callback=self.parse,
                meta={'current_date': current_date.strftime(
                    self.url_dt_format)}
            )
            current_date += timedelta(days=1)

    def parse(self, response):
        # Extract sections and their corresponding article links
        for section in response.css('h4'):
            category = section.css('::text').get().strip()
            ul = section.xpath('following-sibling::ul[1]')
            links = ul.css('li a::attr(href)').getall()

            for link in links:
                if link.startswith('https://'):
                    parsed_link = parse_inq_art_url(link)

                    # Excluding daily gospel 
                    if 'daily-gospel' in parsed_link['slug'] and parsed_link['subdomain'] == 'cebudailynews':
                        continue

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
        category = response.meta.get('category')
        current_date = response.meta.get('current_date')
        article_id = f"{url_metadata['subdomain']}:{url_metadata['article_id']}:{url_metadata['slug']}"

        unwanted_ids = ['billboard_article', 'article-new-featured', 'taboola-mid-article-thumbnails', 'taboola-mid-article-thumbnails-stream', 'fb-root']
        unwanted_classes = ['.ztoop', '.sib-form', '.cdn_newsletter']
        unwanted_elements = ['script', 'style']

        try:
            # Extract article details based on subdomain
            if url_metadata['subdomain'] == 'lifestyle':
                debug_log.info(f"starting to process fields from {url_metadata['subdomain']}")
                title = response.css('h1.elementor-heading-title::text').get(default='No title')
                author = response.css('div.elementor-widget-post-info ul.elementor-post-info li span.elementor-post-info__terms-list a::text').get(default='No author')
                article_content = response.css('div.elementor-widget-theme-post-content').get(default='Cannot extract article content')
                                   
            elif url_metadata['subdomain'] == 'pop':
                debug_log.info(f"starting to process fields from {url_metadata['subdomain']}")
                title = response.css('div.single-post-banner-inner > h1::text').get()
                author = response.css("ul.blog-meta-list a[href*='/byline/']::text").get()
                article_content = response.css('div#TO_target_content').get(default='Cannot extract article content')

            elif url_metadata['subdomain'] == 'cebudailynews':
                debug_log.info(f"starting to process fields from {url_metadata['subdomain']}")
                title = response.css('#landing-headline h1::text').get(default=response.css('#art-hgroup h1::text').get(default='No title'))
                author = response.css('#m-pd2 span::text').re_first(r'By:\s*(.+)') or response.css('.art-byline a::text').get(default='No author')
                article_content = response.css('div#article-content').get(default='Cannot extract article content')
                
            elif url_metadata['subdomain'] == 'bandera':
                debug_log.info(f"starting to process fields from {url_metadata['subdomain']}")
                title = response.css('#landing-headline h1::text').get(default='No title')
                author = response.css('#m-pd2 span::text').re_first(r'^([\w\s.]+)\s+-')
                date_str = response.css('#m-pd2 span::text').re_first(r'(\w+ \d{1,2}, \d{4} - \d{1,2}:\d{2} [APMapm]{2})')
                date_time = datetime.strptime(date_str, '%B %d, %Y - %I:%M %p')
                article_content = response.css('div#TO_target_content').get(default='Cannot extract article content')

            else:
                debug_log.info(f"starting to process fields from {url_metadata['subdomain']} in ELSE")
                title = response.css('h1.entry-title::text').get(default='No title')
                source = response.css('div#art_plat *::text').getall()
                author = response.css('div#art_author::attr(data-byline-strips)').get(default=source[1] if len(source) > 2 else 'No Author')

                article_content = response.css('div#FOR_target_content').get(default='Cannot extract article content')
            
            article_content = clean_article(
                    article=article_content,
                    unwanted_ids=unwanted_ids,
                    unwanted_classes=unwanted_classes,
                    unwanted_elements=unwanted_elements
                )
            
            article_data = {
                'id': article_id,
                'url': response.url,
                'category': category,
                'title': title.strip(),
                'author': author.strip(),
                'date': current_date,
                'article_content': article_content
            }

            self.news_articles.append(article_data)

            with open('inquirer.json', 'a', encoding='utf-8') as f:
                f.write(json.dumps(article_data, ensure_ascii=False) + "\n")

            debug_log.info(f'Article details extracted: {article_id} - {title}')

        except Exception as e:
            debug_log.error(f'Error parsing article details: {e} for {response.url} \n {traceback.format_exc()}')
            self.logger.error(f'Error parsing article details: {e} \n {traceback.format_exc()}') 
            
            with open('failed_extractions.json', 'a', encoding='utf-8') as f:
                f.write(json.dumps({
                'url': response.url,
                'category': category,
                'date': current_date
            }, ensure_ascii=False) + "\n")


    def closed(self, reason):
        with open('news_articles.json', 'w') as f:
            json.dump(self.news_articles, f, indent=4)

def refresh_news_articles(start_date: str = '2025-01-01', end_date: str = None):
    process = CrawlerProcess(
        settings={
            'USER_AGENT': 'Mozilla/5.0',
            'DOWNLOAD_DELAY': 1,
            'LOG_LEVEL': 'INFO',
            'ROBOTSTXT_OBEY': True
        })

    process.crawl(
        InquirerArticlesLinksSpider,
        start_date=start_date,
        end_date=end_date)

    process.start()
