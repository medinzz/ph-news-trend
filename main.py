from datetime import datetime, timedelta
from news.crawler import refresh_news_articles

########## Getting news articles ################
refresh_news_articles(
    start_date='2025-04-25',
    end_date=datetime.today().strftime('%Y-%m-%d')
)


