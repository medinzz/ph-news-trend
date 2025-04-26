from datetime import datetime, timedelta
from news.crawler import refresh_news_articles
from news.apis.abscbn import get_articles

start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

articles = get_articles(
    start_date=start_date
)

print(articles)

########## Getting news articles from inquirer ################
# refresh_news_articles(
#     start_date=start_date,
#     end_date=datetime.today().strftime('%Y-%m-%d')
# )