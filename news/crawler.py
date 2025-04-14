import scrapy
from scrapy.crawler import CrawlerProcess

import json
from datetime import datetime, timedelta

from util.tools import (
    parse_inq_art_url,
    find_date_in_list
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
                    if 'daily-gospel' in parsed_link['slug'] and 'cebudailynews' in parsed_link['subdomain']:
                        continue

                    # generating links data
                    # self.news_articles.append({
                    #     'category': category,
                    #     'date': response.meta['current_date'],
                    #     'article_id': parsed_link['article_id'],
                    #     'slug': parsed_link['slug'],
                    #     'subdomain': parsed_link['subdomain'],
                    #     'origin': parsed_link['origin'],
                    #     'url': link
                    # })

                    yield scrapy.Request(
                        url=link,
                        callback=self.parse_article_details,
                        meta={
                            'category': category,
                            'current_date': response.meta['current_date']
                        }
                    )

    def parse_article_details(self, response):

        try:
            url_metadata = parse_inq_art_url(response.url)
            category = response.meta.get('category')
            current_date = response.meta.get('current_date')
            article_id = f"{url_metadata['subdomain']}:{url_metadata['article_id']}:{url_metadata['slug']}"

            # Extract article details based on subdomain
            if url_metadata['subdomain'] == 'lifestyle':
                title = response.css(
                    'h1.elementor-heading-title::text').get(default='No title')
                author = response.css(
                    'div.elementor-widget-post-info ul.elementor-post-info li span.elementor-post-info__terms-list a::text'
                ).get(default='No author')
                date = response.css(
                    '.elementor-post-info__item--type-date::text').get()
                time = response.css(
                    '.elementor-post-info__item--type-time::text').get()
                date_time = f'{time} {date}' if time and date else 'No date'
            elif url_metadata['subdomain'] == 'pop':
                title = response.css(
                    'div.single-post-banner-inner > h1::text').get()
                author = response.css(
                    "ul.blog-meta-list a[href*='/byline/']::text").get()
                date_str = response.css(
                    'ul.blog-meta-list li.dot-shape a::text').re_first(r'\d{1,2} \w+, \d{4}')
                date_time = datetime.strptime(date_str, '%d %B, %Y')
            elif url_metadata['subdomain'] == 'cebudailynews':
                title = response.css(
                    '#landing-headline h1::text').get(
                        default=response.css('#art-hgroup h1::text').get(default='No title'))
                author = response.css(
                    '#m-pd2 span::text').re_first(r'By:\s*(.+)') or response.css('.art-byline a::text').get(default='No author')
                date_str = response.css(
                    '#m-pd2 span::text').re_first(r'(\w+ \d{1,2},\d{4} - \d{2}:\d{2} [APMapm]{2})') or response.css('.art-byline span::text').re_first(r'([A-Za-z]+\s+\d{1,2},\s+\d{4})')
                date_time = datetime.strptime(date_str, '%B %d,%Y - %I:%M %p')
            elif url_metadata['subdomain'] == 'bandera':
                title = response.css(
                    '#landing-headline h1::text').get(default='No title')
                author = response.css(
                    '#m-pd2 span::text').re_first(r'^([\w\s.]+)\s+-')
                date_str = response.css(
                    '#m-pd2 span::text').re_first(r'(\w+ \d{1,2}, \d{4} - \d{1,2}:\d{2} [APMapm]{2})')
                date_time = datetime.strptime(date_str, '%B %d, %Y - %I:%M %p')
            else:
                title = response.css(
                    'h1.entry-title::text').get(default='No title')
                source = response.css('div#art_plat *::text').getall()
                author = response.css('div#art_author::attr(data-byline-strips)').get(
                    default=source[1] if len(source) > 2 else 'No Author'
                )
                date_time = find_date_in_list(source)

            self.news_articles.append({
                'id': article_id,
                'url': response.url,
                'title': title,
                'author': author,
                'datetime': str(date_time),
                'category': category,
                'date': current_date
            })

            self.logger.info(f'''
                article_id: {article_id}
                ur: {response.url}
                title: {title}
                author: {author}
                date_time: {date_time}
                category: {category}
                current_date: {current_date}
            ''')
        except Exception as e:
            debug_log.error(
                f'Error parsing article details: {e} at {response.url}')
            self.logger.error(f'Error parsing article details: {e}')

    def closed(self, reason):
        with open('news_articles.json', 'w') as f:
            json.dump(self.news_articles, f, indent=4)


class InquirerArticleSpider(scrapy.Spider):
    '''
    `InquirerArticleSpider` is a Scrapy spider designed to scrape articles from the Inquirer website.
    Attributes:
        name (str): The name of the spider, used by Scrapy to identify it.
        start_urls (list): A list of URLs to start scraping from, initialized via the 'urls' parameter.
    Methods:
        __init__(urls, **kwargs):
            Initializes the spider with a list of URLs or a single URL.
            Args:
                urls (str or list): A single URL as a string or a list of URLs to scrape.
            Raises:
                ValueError: If the 'urls' parameter is not a string or a list of strings.
        parse(response):
            Parses the response from the given URL to extract article details.
            Args:
                response (scrapy.http.Response): The response object containing the HTML content of the page.
            Yields:
                dict: A dictionary containing the following keys:
                    - 'url': The URL of the article.
                    - 'title': The title of the article.
                    - 'author': The author of the article (default is 'No author' if not found).
                    - 'date_time': The date and time of the article (raw text from the page).
                    - 'body': The main content of the article, concatenated into a single string.
    '''
    name = 'inquirer_article'

    def __init__(self, urls, **kwargs):
        super().__init__(**kwargs)
        if isinstance(urls, str):
            self.start_urls = [urls]
        elif isinstance(urls, list):
            self.start_urls = urls
        else:
            raise ValueError(
                'The `urls` parameter must be a string or a list of strings.')

    def parse(self, response):
        article_dt_format = '%I:%M %p %B %d, %Y'
        url_metadata = parse_inq_art_url(response.url)

        # TODO: not going to work on the ff. categories:
        #   - lifestyle
        #   - pop
        #   - bandera
        #   - cebu daily news
        match url_metadata['subdomain']:
            case 'lifestyle':
                title = response.css(
                    'h1.elementor-heading-title::text').get(default='No title')
                author = response.css(
                    'div.elementor-widget-post-info ul.elementor-post-info li span.elementor-post-info__terms-list a::text'
                ).get(default='No author')

                date = response.css(
                    '.elementor-post-info__item--type-date::text').get()
                time = response.css(
                    '.elementor-post-info__item--type-time::text').get()

                date_time = datetime.strftime(
                    f'{time} {date}', article_dt_format)
            case _:
                title = response.css(
                    'h1.entry-title::text').get(default='No title')
                # Extract the date from the <div id='art_plat'> tag
                source = response.css('div#art_plat *::text').getall()
                author = response.css('div#art_author::attr(data-byline-strips)').get(
                    default=source[1] if 2 < len(source) else 'No Author')

                date_time = find_date_in_list(
                    source, date_format=article_dt_format)

        # article contents from <p> element.
        paragraphs = response.css('div#article_content p::text').getall()

        # Join all the paragraph texts into a single string
        body = '\n'.join([p.strip() for p in paragraphs if p.strip()])

        data = {
            'url': response.url,
            'title': title,
            'author': author,
            'date_time': date_time,
            'body': body
        } | url_metadata
        yield data


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


def get_article_content(article_links: list[str] = [], end_date: str = None):
    process = CrawlerProcess(
        settings={
            'USER_AGENT': 'Mozilla/5.0',
            'DOWNLOAD_DELAY': 1,
            'LOG_LEVEL': 'INFO',
            'ROBOTSTXT_OBEY': True,
            'FEEDS': {
                'article_content.csv': {'format': 'csv'}
            }
        })

    process.crawl(
        InquirerArticleSpider,
        urls=article_links)

    process.start()
