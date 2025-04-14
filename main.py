import pandas as pd
import json

from datetime import datetime
from news.crawler import refresh_news_articles, get_article_content

########## Getting news articles ################
refresh_news_articles(
    start_date=datetime.today().strftime('%Y-%m-%d'), 
    end_date=datetime.today().strftime('%Y-%m-%d'))


########## Extracting article content ##############
# with open('news_articles.json', 'r') as f:
#     data = json.load(f)

# # Step 2: Flatten the structure
# records = []
# for category, dates in data.items():
#     for date, urls in dates.items():
#         for url in urls:
#             records.append({
#                 'category': category,
#                 'date': date,
#                 'url': url
#             })

# # Step 3: Convert to DataFrame
# df = pd.DataFrame(records)
# urls = df['url'].to_list()

# get_article_content(article_links=urls)


