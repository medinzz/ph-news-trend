

from util.tools import html_to_markdown
from util.storage_backend import get_storage_backend, StorageBackend

# Global storage backend - will be set by get_all_articles
storage: StorageBackend = None

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


class DatabasePipeline:
    """Store each item into a SQLite database."""
        
    def open_spider(self, spider):
         self.storage = get_storage_backend(backend_type = 'sqlite')

    def close_spider(self, spider):
        self.storage.close()

    def process_item(self, item, spider):
        self.storage.insert_record(item)
        return item