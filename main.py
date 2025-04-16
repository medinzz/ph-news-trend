import pandas as pd
import json

from datetime import datetime
from news.crawler import refresh_news_articles

########## Getting news articles ################
refresh_news_articles(end_date=datetime.today().strftime('%Y-%m-%d'))


