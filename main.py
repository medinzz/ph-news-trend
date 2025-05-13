from datetime import datetime, timedelta
from news.crawler import refresh_news_articles
from news.apis import get_all_articles


start_date = (datetime.today()).strftime('%Y-%m-%d')

################ Getting news articles from news outlets' APIs ################
get_all_articles(start_date=start_date)

################ Getting news articles from inquirer ################
# Inquirer API is poorly maintained and does not have all the articles
refresh_news_articles(
    start_date=start_date,
    end_date=datetime.today().strftime('%Y-%m-%d')
)