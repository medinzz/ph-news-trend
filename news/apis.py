import asyncio
import aiohttp

from datetime import datetime

from util.tools import setup_logger, async_get, html_to_markdown
from util.sqlite import SQLiteConnection

logger = setup_logger()

async def abscbn_articles(start_date: str) -> None:
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
                
                article_content_raw = article['data'].get('body_html') if article.get('data') else 'No content found'
                article_content = html_to_markdown(
                    article_content_raw,
                    unwanted_tags=['img', 'figure', 'iframe']
                )
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

async def mb_articles(start_date: str) -> None:
    url = 'https://admin.mb.com.ph/api/articles'
    page = 1
    params = {
        'pagination[pageSize]': 100,
        'sort[0]': 'publishedAt:desc',
        'populate': '*'
    }
    sqlite = SQLiteConnection('articles.db', 'articles')

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    publish_date = datetime.now()

    async with aiohttp.ClientSession() as session:
        
        while publish_date >= start_date:
            params['pagination[page]'] = page
            data = await async_get(session, url, params=params)
            articles = data.get('data', [])
            logger.info(f'Fetched {len(articles)} articles from Manila Bulletin. Page: {page}')

            if not articles:
                logger.info('No more articles found.')
                break

            for article in articles:
                attributes = article.get('attributes', {})
                primary_category = attributes.get('category_primary', {}) \
                    .get('data', {}).get('attributes', {}) \
                    .get('name', '').upper()
                
                author = attributes.get('author', {}) \
                    .get('data', {}).get('attributes', {}) \
                    .get('name', '')
                
                tags = [tag.get('attributes', {}).get('slug', '') \
                        for tag in attributes.get('tags', {}).get('data', [])]

                publish_date = datetime.strptime(
                    attributes.get(
                        'publishedAt', 
                        ''), 
                    '%Y-%m-%dT%H:%M:%S.%fZ')
                created_date = datetime.strptime(
                    attributes.get(
                        'createdAt', 
                        ''), 
                    '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y/%m/%d')
                
                article_content_raw = attributes.get('body', 'No content found')
                article_content = html_to_markdown(
                    article_content_raw,
                    unwanted_tags=['img', 'figure', 'iframe']
                )
                
                
                # Publish date is used for filtering as the earliest create date 
                # for every articles is: 2023-03-26T16:55:02.086Z
                if publish_date < start_date:
                    logger.info('Reached articles older than start_date')
                    break

                sqlite.insert_record({
                    'id': article.get('id'),
                    'source': 'manila bulletin',
                    'url': 'https://mb.com.ph/' + created_date + '/' + attributes.get('slug'),
                    'category': primary_category.upper(),
                    'title': attributes.get('title'),
                    'author': author,
                    'date': publish_date.strftime('%Y-%m-%d'),
                    'publish_time': publish_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'tags': ','.join(tags),
                    'cleaned_content': article_content,
                })
            
            page += 1


def get_all_articles(start_date: str) -> None:
    """
    Fetch articles from ABS-CBN and store them in a SQLite database.
    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mb_articles(start_date))
    loop.run_until_complete(abscbn_articles(start_date))
