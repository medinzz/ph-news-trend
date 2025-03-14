import pandas as pd
from util.api import get_news

# articles = pd.DataFrame(get_news(query='duterte'))
articles = pd.read_json('results.json')

print(articles.keys())