import scrapy
from scrapy.crawler import CrawlerProcess
from datetime import datetime, timedelta


class InquirerIndexSpider(scrapy.Spider):
    name = "inquirer_by_date"
    allowed_domains = ["inquirer.net"]

    def __init__(self, start_date="2025-04-05", end_date=None, **kwargs):
        super().__init__(**kwargs)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else self.start_date
        self.output_file = open("inquirer_links.txt", "w", encoding="utf-8")

    def start_requests(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            url = f"https://www.inquirer.net/article-index/?d={current_date.strftime('%Y-%m-%d')}"
            yield scrapy.Request(url=url, callback=self.parse)
            current_date += timedelta(days=1)

    def parse(self, response):
        links = response.css('ul li a::attr(href)').getall()
        for link in links:
            if link.startswith("https://"):
                self.output_file.write(link + "\n")

    def closed(self, reason):
        self.output_file.close()

process = CrawlerProcess(settings={
    "USER_AGENT": "Mozilla/5.0",
    "DOWNLOAD_DELAY": 1,
    "LOG_LEVEL": "INFO",
    "ROBOTSTXT_OBEY": True,
})

process.crawl(InquirerIndexSpider, start_date="2025-04-05", end_date="2025-04-07")
process.start()