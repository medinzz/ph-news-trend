import scrapy
from scrapy.crawler import CrawlerProcess

import json
from collections import defaultdict
from datetime import datetime, timedelta


class InquirerArticlesLinksSpider(scrapy.Spider):
    name = "inquirer_by_date"
    allowed_domains = ["inquirer.net"]

    def __init__(self, start_date="2025-01-01", end_date=None, **kwargs):
        super().__init__(**kwargs)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(
            end_date, "%Y-%m-%d") if end_date else self.start_date
        self.news_articles = defaultdict(lambda: defaultdict(list))

    def start_requests(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            url = f"https://www.inquirer.net/article-index/?d={current_date.strftime('%Y-%m-%d')}"
            yield scrapy.Request(url=url, callback=self.parse, meta={'current_date': current_date.strftime('%Y-%m-%d')})
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
                if link.startswith("https://"):
                    self.news_articles[category][response.meta['current_date']].append(
                        link)

    def closed(self, reason):
        with open('news_articles.json', 'w') as f:
            json.dump(self.news_articles, f)


class InquirerArticleSpider(scrapy.Spider):
    name = 'inquirer_article'
    start_urls = [
        'https://newsinfo.inquirer.net/2021698/ecowaste-to-nazarene-devotees-keep-our-environment-clean'
    ]

    def parse(self, response):
        title = response.css('h1.entry-title::text').get()

        author = response.css('div#art_author::attr(data-byline-strips)').get(default='No author')

        # Extract the date from the <div id='art_plat'> tag
        date_time = response.css('div#art_plat::text').get()
        # Clean up and extract the date part (remove the source text part)

        paragraphs = response.css('div#article_content p::text').getall()

        # Join all the paragraph texts into a single string
        body = '\n'.join([p.strip() for p in paragraphs if p.strip()])

        self.logger.info(f'Body: {body}')
        yield {
            'url': response.url,
            'title': title,
            'author': author,
            'date_time': date_time,
            'body': body
        }


def refresh_news_articles(start_date="2025-01-01", end_date="2025-04-09"):
    process = CrawlerProcess(
        settings={
            "USER_AGENT": "Mozilla/5.0",
            "DOWNLOAD_DELAY": 1,
            "LOG_LEVEL": "INFO",
            "ROBOTSTXT_OBEY": True,
        })

    process.crawl(
        InquirerArticlesLinksSpider,
        start_date=start_date,
        end_date=end_date)

    process.start()
