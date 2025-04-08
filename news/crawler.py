import scrapy
from scrapy.crawler import CrawlerProcess

import json
from collections import defaultdict
from datetime import datetime, timedelta


class InquirerArticlesSpider(scrapy.Spider):
    name = "inquirer_by_date"
    allowed_domains = ["inquirer.net"]

    def __init__(self, start_date="2025-04-05", end_date=None, **kwargs):
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


def refresh_news_articles():
    process = CrawlerProcess(
        settings={
            "USER_AGENT": "Mozilla/5.0",
            "DOWNLOAD_DELAY": 1,
            "LOG_LEVEL": "INFO",
            "ROBOTSTXT_OBEY": True,
        })

    process.crawl(
        InquirerArticlesSpider, 
        start_date="2025-04-05",
        end_date="2025-04-08")
    
    process.start()

