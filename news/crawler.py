import scrapy
from scrapy.crawler import CrawlerProcess

import json
from collections import defaultdict
from datetime import datetime, timedelta

from util.tools import (
    parse_inq_art_url,
    find_date_in_list
)


class InquirerArticlesLinksSpider(scrapy.Spider):
    '''
    InquirerArticlesLinksSpider is a Scrapy spider designed to scrape article links 
    from the Philippine Daily Inquirer website (inquirer.net) based on a specified date range.

    Attributes:
        name (str): The name of the spider.
        allowed_domains (list): A list of domains that the spider is allowed to crawl.
        start_date (datetime): The starting date for scraping articles.
        end_date (datetime): The ending date for scraping articles.
        news_articles (defaultdict): A nested dictionary to store scraped article links 
            categorized by date and category.

    Methods:
        __init__(start_date='2025-01-01', end_date=None, **kwargs):
            Initializes the spider with a start date and an optional end date.
        
        start_requests():
            Generates initial requests for each date in the specified range, 
            targeting the article index page for that date.

        parse(response):
            Parses the response from the article index page, extracting article links 
            categorized by their respective sections.

        closed(reason):
            Called when the spider finishes its execution. Saves the scraped article 
            links to a JSON file named 'news_articles.json'.
    '''
    name = 'inquirer_by_date'
    allowed_domains = ['inquirer.net']

    def __init__(self, start_date: str = '2025-01-01', end_date: str = None, **kwargs):
        super().__init__(**kwargs)
        self.dt_format = '%Y-%m-%d'
        self.start_date = datetime.strptime(start_date, self.dt_format)
        self.end_date = datetime.strptime(
            end_date, self.dt_format) if end_date else self.start_date
        self.news_articles = defaultdict(lambda: defaultdict(list))

    def start_requests(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            url = f'https://www.inquirer.net/article-index/?d={current_date.strftime(self.dt_format)}'
            yield scrapy.Request(url=url, callback=self.parse, meta={'current_date': current_date.strftime(self.dt_format)})
            current_date += timedelta(days=1)

    def parse(self, response):
        # Iterate over each h4 element, which is followed by a <ul> with article links
        for section in response.css('h4'):
            # get the category name
            category = section.css('::text').get().strip()
            # get the first <ul> after the <h4>
            ul = section.xpath('following-sibling::ul[1]')

            # extract all hrefs from <a> inside <li>
            links = ul.css('li a::attr(href)').getall()
            for link in links:
                if link.startswith('https://'):
                    self.news_articles[category][response.meta['current_date']].append(
                        link)

    def closed(self, reason):
        with open('news_articles.json', 'w') as f:
            json.dump(self.news_articles, f)


class InquirerArticleSpider(scrapy.Spider):
    '''
    InquirerArticleSpider is a Scrapy spider designed to scrape articles from the Inquirer website.
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
            raise ValueError('The `urls` parameter must be a string or a list of strings.')

    def parse(self, response):
        url_metadata = parse_inq_art_url(response.url)

        title = response.css('h1.entry-title::text').get()

        # TODO: not going to work on the ff. categories: 
        #   - lifestyle
        #   - pop
        #   - bandera
        #   - cebu daily news 
        
        source = response.css('div#art_plat *::text').getall() # Extract the date from the <div id='art_plat'> tag
        author = response.css('div#art_author::attr(data-byline-strips)').get(
            default = source[1] if 2 < len(source) else 'No Author')

        
        date_time = find_date_in_list(source)

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
        yield  data 


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
