from venv import logger
import requests
import datetime 

from util.tools import setup_logger

logger = setup_logger()

def get_articles(start_date: str, end_date: str) -> list:
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else start_date
    
    url = 'https://od2-content-api.abs-cbn.com/prod/latest'
    limit = 1000
    offset = 0
    params = {
        'sectionId': 'news',
        'brand': 'OD',
        'limit': limit,
        'offset': offset,
    }
    
    articles = []
    while end_date < start_date:
        response = requests.get(url, params=params)
        try:
            if response.status_code == 200:
                data = response.json().get('listItem', [])
                
                for article in data:
                    article_date = datetime.strptime(article['createdDateFull'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    if start_date <= article_date <= end_date:
                        articles.append({
                            'id': article['id'],
                            'source': 'abs-cbn',
                            'url': 'https://www.abs-cbn.com/' + article['url'],
                            'category': article['category'].upper(),
                            'title': article['title'],
                            'author': article['author'],
                            'date': article_date.strftime('%Y-%m-%d'),
                            'publish_time': article_date.striptime('%Y-%m-%d %H:%M:%S'),
                            'tags': article['tags'],
                            'content': article['abstract'],
                        })
            else:
                logger.error(f"Error: {response.status_code}")
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            break
        
        
                