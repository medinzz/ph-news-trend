import scrapy


class ArticleItem(scrapy.Item):
    id = scrapy.Field()
    source = scrapy.Field()
    url = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    date = scrapy.Field()
    publish_time = scrapy.Field()
    raw_content = scrapy.Field()
    cleaned_content = scrapy.Field()
    tags = scrapy.Field()