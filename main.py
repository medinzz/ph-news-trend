from datetime import datetime
from news.crawler import refresh_news_articles

refresh_news_articles(end_date=datetime.today().strftime('%Y-%m-%d'))