import traceback
import asyncio
import aiohttp

from datetime import datetime
from urllib.parse import urlparse

from util.tools import setup_logger, async_get, html_to_markdown
from util.storage_backend import get_storage_backend, StorageBackend


logger = setup_logger()

# Global storage backend - will be set by get_all_articles
storage: StorageBackend = None


async def abscbn_articles(start_date: str) -> None:
    """
    Fetches and stores ABS-CBN news articles published since a given start date.
    Uses the global storage backend (SQLite or BigQuery).
    """
    url = 'https://od2-content-api.abs-cbn.com/prod/latest'
    limit = 100
    offset = 0
    params = {
        'brand': 'OD',
        'partner': 'imp-01',
        'limit': limit,
        'offset': offset,
    }
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    created_date = datetime.now()
    article_info_base_url = 'https://od2-content-api.abs-cbn.com/prod/item?url='

    async with aiohttp.ClientSession() as session:
        while created_date >= start_date:
            params['offset'] = offset
            data = await async_get(session, url, params=params)
            articles = data.get('listItem', [])
            logger.info(
                f'Fetched {len(articles)} articles from ABS-CBN. Offset: {offset}')

            if not articles:
                logger.info('No more articles found.')
                break

            # Filter articles by date
            filtered_articles = []
            for article in articles:
                created_date = datetime.strptime(
                    article.get('createdDateFull', ''),
                    '%Y-%m-%dT%H:%M:%S.%fZ')
                if created_date < start_date:
                    logger.info('Reached articles older than start_date')
                    break
                filtered_articles.append((article, created_date))

            # Prepare async requests for article details
            tasks = [
                async_get(
                    session,
                    url=article_info_base_url + item.get('slugline_url', 'no_url'),
                    id=item.get('_id'),
                    source='abs-cbn',
                    slugline_url=item.get('slugline_url'),
                    category=item.get('category').upper(),
                    title=item.get('title'),
                    author=item.get('author'),
                    date=created_date.strftime('%Y-%m-%d'),
                    publish_time=created_date.strftime('%Y-%m-%d %H:%M:%S'),
                    tags=item.get('tags'),
                )
                for item, created_date in filtered_articles if item.get('slugline_url')
            ]
            details = await asyncio.gather(*tasks)

            # Insert articles using the storage backend
            for article in details:
                article_content = html_to_markdown(
                    article['data'].get('body_html') if article.get('data') else 'No content found',
                    unwanted_tags=['img', 'figure', 'iframe']
                )
                storage.insert_record({
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

            logger.info(f'Inserted {len(details)} ABS-CBN articles into storage.')
            offset += limit


async def manila_bulletin_articles(start_date: str, section_ids: list = None) -> None:
    """
    Fetches articles from Manila Bulletin's API and stores them using the global storage backend.
    """
    if section_ids is None:
        section_ids = [25, 26, 27, 28, 29, 30, 31]
    
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    current_article = {}

    async with aiohttp.ClientSession() as session:
        for section_id in section_ids:
            page = 1
            logger.info(f'Starting to fetch articles from section_id: {section_id}')
            
            while True:
                try:
                    articles_url = 'https://mb.com.ph/api/pb/fetch-articles-paginated'
                    params = {
                        'page': page,
                        'section_id': section_id
                    }
                    
                    response = await async_get(session, articles_url, params=params)
                    
                    if not response or response.get('response') != 'success':
                        logger.warning(f'No more articles found for section_id {section_id}, page {page}')
                        break
                    
                    articles = response.get('data', [])
                    
                    if not articles:
                        logger.info(f'No more articles for section_id {section_id}')
                        break
                    
                    logger.info(f'Fetched {len(articles)} articles from Manila Bulletin. Section: {section_id}, Page: {page}')
                    
                    # Filter articles by date
                    filtered_articles = []
                    for article in articles:
                        publish_time = article.get('publish_time', '')
                        if publish_time:
                            article_datetime = datetime.strptime(publish_time, '%Y-%m-%d %H:%M:%S')
                            if article_datetime >= start_datetime:
                                filtered_articles.append(article)
                    
                    if not filtered_articles:
                        logger.info(f'All articles in page {page} are before start_date. Moving to next section.')
                        break
                    
                    logger.info(f'Processing {len(filtered_articles)} articles after date filter')
                    
                    # Fetch full article details
                    for article_summary in filtered_articles:
                        try:
                            cms_article_id = article_summary.get('cms_article_id')
                            
                            if not cms_article_id:
                                logger.warning(f'Missing cms_article_id for article: {article_summary.get("title")}')
                                continue
                            
                            article_detail_url = f'https://mb.com.ph/api/pb/article/{cms_article_id}'
                            article_detail = await async_get(session, article_detail_url)
                            
                            if not article_detail or article_detail.get('response') != 'success':
                                logger.warning(f'Failed to fetch article details for cms_article_id: {cms_article_id}')
                                continue
                            
                            article_data = article_detail.get('data', {})
                            
                            # Convert HTML content to Markdown
                            article_body = article_data.get('body', '')
                            
                            if article_body:
                                article_content = html_to_markdown(
                                    article_body,
                                    unwanted_tags=['img', 'figure', 'iframe']
                                )
                            else:
                                article_content = article_data.get('summary', 'No content found')
                                
                            # Extract tags
                            tags = article_data.get('cf_article_tags', '')
                            if isinstance(tags, str):
                                tags_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
                            else:
                                tags_list = []
                            
                            # Insert using storage backend
                            storage.insert_record({
                                'id': article_data.get('cms_article_id'),
                                'source': 'manila_bulletin',
                                'url': article_data.get('link', ''),
                                'category': article_data.get('section_name', 'Unknown'),
                                'title': article_data.get('title', 'No title found'),
                                'author': article_data.get('author_name', 'Unknown'),
                                'date': article_data.get('publish_time', '').split(' ')[0] if article_data.get('publish_time') else None,
                                'publish_time': article_data.get('publish_time', ''),
                                'tags': ','.join(tags_list),
                                'cleaned_content': article_content,
                            })
                            
                        except Exception as e:
                            logger.error(f'Error processing article cms_article_id {cms_article_id}: {e}')
                            logger.error(traceback.format_exc())
                            continue
                    
                    page += 1
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error('############ Error Occurred ############')
                    logger.error(f'Error fetching page {page} for section_id {section_id}: {e}')
                    logger.error(traceback.format_exc())
                    break

    logger.info('Completed fetching all Manila Bulletin articles')


async def rappler_articles(start_date: str) -> None:
    """
    Fetches articles from Rappler's API and stores them using the global storage backend.
    """
    url = 'https://www.rappler.com/wp-json/wp/v2/posts'
    page = 1
    params = {
        'page': page,
        'per_page': 10,
        'after': datetime.strptime(start_date, '%Y-%m-%d').isoformat(),
    }

    current_article = {}

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params['page'] = page
                articles = await async_get(session, url, params=params)
                logger.info(f'Fetched {len(articles)} articles from Rappler. Page: {page}')

                for article in articles:
                    article_content = html_to_markdown(
                        article.get('content', {}).get('rendered', 'No content found'),
                        unwanted_tags=['img', 'figure', 'iframe']
                    )
                    tags_tasks = [
                        async_get(
                            session,
                            url=f'https://www.rappler.com/wp-json/wp/v2/tags/{tag_id}')
                        for tag_id in article.get('tags', [])
                    ]
                    tags = await asyncio.gather(*tags_tasks)

                    storage.insert_record({
                        'id': article.get('id'),
                        'source': 'rappler',
                        'url': article.get('link'),
                        'category': urlparse(article.get('link')).path.split('/')[1],
                        'title': article.get('title', {}).get('rendered', 'No title found'),
                        'author': None,
                        'date': article.get('date').split('T')[0],
                        'publish_time': datetime.strptime(
                            article.get('date', ''),
                            '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'),
                        'tags': ','.join(tag.get('slug', '') for tag in tags if tag),
                        'cleaned_content': article_content,
                    })

                page += 1
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error('############ Error Occurred ############')
                logger.error(e)
                logger.error(traceback.format_exc())
                break


def get_all_articles(start_date: str, backend: str = 'sqlite', **backend_kwargs) -> None:
    """
    Fetch articles from multiple news sources and store them using the specified backend.
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        backend: Storage backend to use ('sqlite' or 'bigquery')
        **backend_kwargs: Additional arguments for the storage backend
            For SQLite: db_path='articles.db', table_name='articles'
            For BigQuery: dataset_id='news_data', table_name='articles'
    """
    global storage
    
    # Initialize storage backend
    storage = get_storage_backend(backend, **backend_kwargs)
    logger.info(f"Using {backend} storage backend")
    
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(abscbn_articles(start_date))
        loop.run_until_complete(rappler_articles(start_date))
        loop.run_until_complete(manila_bulletin_articles(start_date))
    finally:
        # Clean up storage connection
        storage.close()
        loop.close()