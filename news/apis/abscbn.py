from math import log
from venv import logger
import requests
from datetime import datetime

from util import sqlite
from util.tools import setup_logger
from util.sqlite import SQLiteConnection

logger = setup_logger()

def get_articles(start_date: str) -> list:
    url = 'https://od2-content-api.abs-cbn.com/prod/latest'
    limit = 1000
    offset = 0
    params = {
        'sectionId': 'news',
        'brand': 'OD',
        'partner': 'imp-01',
        'limit': limit,
        'offset': offset,
    }
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    article_date = datetime.now()
    
    sqlite_conn = SQLiteConnection('articles.db', 'articles')
    
    articles = []
    while article_date >= start_date:
        response = requests.get(url, params=params)
        try:
            if response.status_code == 200:
                data = response.json().get('listItem', [])
                logger.info(f'Fetched {len(data)} articles from ABS-CBN. at date: {article_date}')
                for article in data:
                    logger.info(f'Processing article: {article["title"]}')
                    article_date = datetime.strptime(article['createdDateFull'], '%Y-%m-%dT%H:%M:%SZ')
                    sqlite_conn.insert_record({
                        'id': article['id'],
                        'source': 'abs-cbn',
                        'url': 'https://www.abs-cbn.com/' + article['slugline_url'],
                        'category': article['category'].upper(),
                        'title': article['title'],
                        'author': article['author'],
                        'date': article_date.strftime('%Y-%m-%d'),
                        'publish_time': article_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'tags': article['tags'],
                        'cleaned_content': None,
                    })
                    logger.info(f'Inserted article: {article["title"]} into database.')
                offset += limit
                
            else:
                logger.error(f'Error: {response.status_code}')
        except Exception as e:
            logger.error(f'Error parsing response: {e}')
            break
    
    logger.info(f'Finished fetching articles from ABS-CBN.')
    sqlite_conn.close()