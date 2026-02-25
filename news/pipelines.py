from bs4 import BeautifulSoup
from util.storage_backend import get_storage_backend
from util.tools import html_to_markdown


class InquirerCleaningPipeline:
    """Remove unwanted tags/ids/classes and convert HTML to Markdown."""

    def __init__(self, unwanted_ids, unwanted_classes, unwanted_tags):
        self.unwanted_ids = unwanted_ids
        self.unwanted_classes = unwanted_classes
        self.unwanted_tags = unwanted_tags

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            unwanted_ids=['billboard_article', 'article-new-featured',
                          'taboola-mid-article-thumbnails',
                          'taboola-mid-article-thumbnails-stream', 'fb-root'],
            unwanted_classes=['ztoop', 'sib-form', 'cdn_newsletter'],
            unwanted_tags=['script', 'style'],
        )

    def process_item(self, item, spider):
        # Phase 1 stub items have no raw_content — nothing to clean, pass through
        if not item.get('raw_content'):
            return item

        html = item['raw_content']
        soup = BeautifulSoup(html, 'html.parser')

        for uid in self.unwanted_ids:
            for tag in soup.select(f'#{uid}'):
                tag.decompose()

        for cls_ in self.unwanted_classes:
            for tag in soup.select(f'.{cls_}'):
                tag.decompose()

        for t in self.unwanted_tags:
            for tag in soup.find_all(t):
                tag.decompose()

        item['cleaned_content'] = html_to_markdown(str(soup))
        return item


class DatabasePipeline:
    """
    Phase 1 — INSERT stub records (url + metadata, content fields NULL).
    Phase 2 — UPSERT: update existing stubs with full content on id match.

    Relies on your storage backend's upsert_record(item) method.
    See note below if you only have insert_record.
    """

    def open_spider(self, spider):
        self.db = get_storage_backend(backend_type='duckdb')

    def close_spider(self, spider):
        self.db.close()

    def process_item(self, item, spider):
        self.db.upsert_record(item)
        return item