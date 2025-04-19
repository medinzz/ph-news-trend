import sqlite3
from bs4 import BeautifulSoup
import html2text


class InquirerCleaningPipeline:
    """Remove unwanted tags/ids/classes and convert HTML to Markdown."""

    def __init__(self, unwanted_ids, unwanted_classes, unwanted_tags):
        self.unwanted_ids = unwanted_ids
        self.unwanted_classes = unwanted_classes
        self.unwanted_tags = unwanted_tags
        self.html2md = html2text.HTML2Text()
        self.html2md.body_width = 0

    @classmethod
    def from_crawler(cls, crawler):
        # Pass your unwanted lists via settings or hard-code
        return cls(
            unwanted_ids = ['billboard_article', 'article-new-featured', 'taboola-mid-article-thumbnails', 'taboola-mid-article-thumbnails-stream', 'fb-root'],
            unwanted_classes = ['ztoop', 'sib-form', 'cdn_newsletter'],
            unwanted_tags = ['script', 'style']
        )

    def process_item(self, item, spider):
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
        item['cleaned_content'] = self.html2md.handle(cleaned_html)
        return item


class SQLitePipeline:
    """Store each item into a SQLite database."""

    def open_spider(self, spider):
        self.conn = sqlite3.connect('articles.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                id TEXT PRIMARY KEY,
                url TEXT,
                category TEXT,
                title TEXT,
                author TEXT,
                date TEXT,
                content TEXT
            )
        ''')
        self.conn.commit()

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        self.cursor.execute('''
            INSERT OR REPLACE INTO articles
            (id,url,category,title,author,date,content)
            VALUES (?,?,?,?,?,?,?)
        ''', (
            item['id'], item['url'], item['category'],
            item['title'], item['author'], item['date'],
            item['cleaned_content']
        ))
        self.conn.commit()
        return item