from bs4 import BeautifulSoup
from util.sqlite import SQLiteConnection
from util.tools import html_to_markdown


class InquirerCleaningPipeline:
    """Remove unwanted tags/ids/classes and convert HTML to Markdown."""

    def __init__(self, unwanted_ids, unwanted_classes, unwanted_tags):
        self.unwanted_ids = unwanted_ids
        self.unwanted_classes = unwanted_classes
        self.unwanted_tags = unwanted_tags

    @classmethod
    def from_crawler(cls, crawler):
        # Pass your unwanted lists via settings or hard-code
        return cls(
            unwanted_ids = ['billboard_article', 'article-new-featured', 'taboola-mid-article-thumbnails', 'taboola-mid-article-thumbnails-stream', 'fb-root'],
            unwanted_classes = ['ztoop', 'sib-form', 'cdn_newsletter'],
            unwanted_tags = ['script', 'style']
        )

    def process_item(self, item, spider):
        
        ################### CONVERT HTML RAW CONTENT TO MARKDOWN ###################
        html = item['raw_content']
        soup = BeautifulSoup(html, 'html.parser')

        # Remove by id
        for uid in self.unwanted_ids:
            for tag in soup.select(f'#{uid}'):
                tag.decompose()

        # Remove by class
        for cls_ in self.unwanted_classes:
            for tag in soup.select(f'.{cls_}'):
                tag.decompose()

        # Remove by tag name
        for t in self.unwanted_tags:
            for tag in soup.find_all(t):
                tag.decompose()

        cleaned_html = str(soup)
        item['cleaned_content'] = html_to_markdown(cleaned_html)
        
        return item


class SQLitePipeline:
    """Store each item into a SQLite database."""
        
    def open_spider(self, spider):
         self.sqlite = SQLiteConnection('articles.db', 'articles')

    def close_spider(self, spider):
        self.sqlite.close()

    def process_item(self, item, spider):
        self.sqlite.insert_record(item)
        return item