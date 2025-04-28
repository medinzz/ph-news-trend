from datetime import datetime, timedelta
from news.crawler import refresh_news_articles
from news.apis.abscbn import get_abscbn_articles


start_date = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')

################ Getting news articles from abscbn ################
get_abscbn_articles(start_date=start_date)

################ Getting news articles from inquirer ################
refresh_news_articles(
    start_date=start_date,
    end_date=datetime.today().strftime('%Y-%m-%d')
)