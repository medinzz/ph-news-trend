import asyncio
import aiohttp

from datetime import datetime

from util.tools import setup_logger, async_get, html_to_markdown
from util.sqlite import SQLiteConnection

logger = setup_logger()

async def get_articles(start_date: str) -> None:
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
    created_date = datetime.now()
    article_info_base_url = 'https://od2-content-api.abs-cbn.com/prod/item?url='

    sqlite = SQLiteConnection('articles.db', 'articles')

    async with aiohttp.ClientSession() as session:
        
        while created_date >= start_date:
            params['offset'] = offset
            data = await async_get(session, url, params=params)
            articles = data.get('listItem', [])
            logger.info(f'Fetched {len(articles)} articles from ABS-CBN. Offset: {offset}')

            if not articles:
                logger.info('No more articles found.')
                break

            # Filter articles by date
            filtered_articles = []
            for article in articles:
                created_date = datetime.strptime(
                    article.get(
                        'createdDateFull', 
                        ''), 
                    '%Y-%m-%dT%H:%M:%SZ')
                if created_date < start_date:
                    logger.info('Reached articles older than start_date')
                    break
                filtered_articles.append((article, created_date))
            

            # Prepare async requests for article details
            tasks = [
                async_get(
                    session,
                    url = article_info_base_url + item.get('slugline_url', 'no_url'),
                    id = item.get('_id'),
                    source = 'abs-cbn',
                    slugline_url = item.get('slugline_url'),
                    category = item.get('category').upper(),
                    title = item.get('title'),
                    author = item.get('author'),
                    date = created_date.strftime('%Y-%m-%d'),
                    publish_time = created_date.strftime('%Y-%m-%d %H:%M:%S'),
                    tags = item.get('tags'),
                )
                for item, created_date in filtered_articles if item.get('slugline_url')
            ]
            details = await asyncio.gather(*tasks)

            # Log articles and their details
            for article in  details:
                article_content = html_to_markdown(article['data'].get('body_html') if article.get('data') else 'No content found')
                sqlite.insert_record({
                    'id': article.get('id'),
                    'source': article.get('source'),
                    'url': 'https://www.abs-cbn.com/' + article.get('slugline_url'),
                    'category': article.get('category'),
                    'title': article.get('title'),
                    'author': article.get('author'),
                    'date': article.get('date'),
                    'publish_time': article.get('publish_time'),
                    'tags': article.get('tags'),
                    'cleaned_content': article_content,
                })
            
            logger.info(f'Inserted {len(details)} articles into the database.')

            offset += limit

def get_abscbn_articles(start_date: str) -> None:
    """
    Fetch articles from ABS-CBN and store them in a SQLite database.
    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_articles(start_date))

# # Synchronous version of the function for reference
# def get_articles(start_date: str) -> list:
#     url = 'https://od2-content-api.abs-cbn.com/prod/latest'
#     limit = 1000
#     offset = 0
#     params = {
#         'sectionId': 'news',
#         'brand': 'OD',
#         'partner': 'imp-01',
#         'limit': limit,
#         'offset': offset,
#     }
#     start_date = datetime.strptime(start_date, '%Y-%m-%d')
#     article_date = datetime.now()
    
#     sqlite_conn = SQLiteConnection('articles.db', 'articles')
    
#     articles = []
#     while article_date >= start_date:
#         response = requests.get(url, params=params)
#         try:
#             if response.status_code == 200:
#                 data = response.json().get('listItem', [])
#                 logger.info(f'Fetched {len(data)} articles from ABS-CBN. at date: {article_date}')
#                 for article in data:
#                     article_date = datetime.strptime(article['createdDateFull'], '%Y-%m-%dT%H:%M:%SZ')
#                     sqlite_conn.insert_record({
#                         'id': article['_id'],
#                         'source': 'abs-cbn',
#                         'url': 'https://www.abs-cbn.com/' + article['slugline_url'],
#                         'category': article['category'].upper(),
#                         'title': article['title'],
#                         'author': article['author'],
#                         'date': article_date.strftime('%Y-%m-%d'),
#                         'publish_time': article_date.strftime('%Y-%m-%d %H:%M:%S'),
#                         'tags': article['tags'],
#                         'cleaned_content': None,
#                     })
#                 offset += limit
                
#             else:
#                 logger.error(f'Error: {response.status_code}')
#         except Exception as e:
#             logger.error(f'Error parsing response: {e}')
#             break
    
#     logger.info(f'Finished fetching articles from ABS-CBN.')
#     sqlite_conn.close()
