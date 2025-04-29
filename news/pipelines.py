
from util.sqlite import SQLiteConnection
from util.tools import html_to_markdown


class InquirerCleaningPipeline:
    """Remove unwanted tags/ids/classes and convert HTML to Markdown."""

    def __init__(self):
        self.unwanted_ids = ['billboard_article', 'article-new-featured', 'taboola-mid-article-thumbnails', 'taboola-mid-article-thumbnails-stream', 'fb-root']
        self.unwanted_classes = ['ztoop', 'sib-form', 'cdn_newsletter']
        self.unwanted_tags = ['script', 'style']

    def process_item(self, item, spider):
        item['cleaned_content'] = html_to_markdown(
            html = item['raw_content'], 
            unwanted_ids = self.unwanted_ids,
            unwanted_classes = self.unwanted_classes,
            unwanted_tags = self.unwanted_tags
        )
        
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