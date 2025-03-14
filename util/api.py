import requests
import json
from util.logger import setup_logger
from util.const import news_api_key


logger = setup_logger()


def get_news(query='tech'): 
    url = 'https://newsapi.org/v2/everything'

    params = {
        'apiKey': news_api_key,
        'q': query,
        'searchIn': 'title',
        'sortBy':'popularity'
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        status = data['status']
        num_results = data['totalResults']
        articles = data['articles']
        
        logger.info(f'util: get_news({query}) status: {status} articles found: {num_results}')
        with open('results.json', 'w') as file:
            json.dump(articles, file, indent=1)
    else:
        logger.error('Error:', response.json())
    return articles