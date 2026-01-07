import traceback
import asyncio
import aiohttp

from datetime import datetime
from urllib.parse import urlparse

from util.tools import setup_logger, async_get, html_to_markdown
from util.sqlite import SQLiteConnection


logger = setup_logger()
sqlite = SQLiteConnection('articles.db', 'articles')

async def abscbn_articles(start_date: str) -> None:
    """
    Fetches and stores ABS-CBN news articles published since a given start date.
    This asynchronous function retrieves articles from the ABS-CBN content API, filters them by the specified start date,
    fetches detailed article information, converts the article body to Markdown (excluding unwanted tags), and inserts the
    processed articles into a local SQLite database.
    Args:
        start_date (str): The earliest publication date (inclusive) for articles to fetch, in 'YYYY-MM-DD' format.
    Returns:
        None
    Behavior:
        - Fetches articles in batches using pagination.
        - Processes articles with a 'slugline_url'.
        - The function logs progress and stops when articles older than the start date are encountered.
    """
    url = 'https://od2-content-api.abs-cbn.com/prod/latest'
    limit = 100
    offset = 0
    params = {
        # 'sectionId': 'news',
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
                    article.get(
                        'createdDateFull',
                        ''),
                    '%Y-%m-%dT%H:%M:%S.%fZ')
                if created_date < start_date:
                    logger.info('Reached articles older than start_date')
                    break
                filtered_articles.append((article, created_date))

            # Prepare async requests for article details
            tasks = [
                async_get(
                    session,
                    url=article_info_base_url +
                    item.get('slugline_url', 'no_url'),
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

            # Log articles and their details
            for article in details:
                article_content = html_to_markdown(
                    article['data'].get('body_html') if article.get(
                        'data') else 'No content found',
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

            logger.info(f'Inserted {len(details)} ABS-CBN articles into the database.')

            offset += limit


async def mb_articles(start_date: str) -> None:
    """
    Fetches articles from the Manila Bulletin API starting from a specified date, processes them, and stores them in a local SQLite database.
    Args:
        start_date (str): The earliest publish date (in 'YYYY-MM-DD' format) for articles to fetch.
    Returns:
        None
    Behavior:
        - Iteratively fetches paginated articles from the Manila Bulletin API, starting from the most recent and moving backwards in time.
        - For each article, extracts relevant fields such as category, author, tags, publish date, and content.
        - Converts HTML content to Markdown, removing unwanted tags.
        - Stops fetching when articles older than the specified start_date are reached.
        - Stores each processed article as a record in the 'articles' table of the 'articles.db' SQLite database.
        - Logs progress and status throughout the process.
    """
    url = 'https://admin.mb.com.ph/api/articles'
    page = 1
    params = {
        'pagination[pageSize]': 100,
        'sort[0]': 'publishedAt:desc',
        'populate': '*'
    }

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    publish_date = datetime.now()

    current_article = {}

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

                tags = [tag.get('attributes', {}).get('slug', '')
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

                article_content = html_to_markdown(
                    attributes.get('body', 'No content found'),
                    unwanted_tags=['img', 'figure', 'iframe']
                )

                # Publish date is used for filtering as the earliest create date
                # for every articles is: 2023-03-26T16:55:02.086Z
                if publish_date < start_date:
                    logger.info('Reached articles older than start_date')
                    break
            
                current_article = {
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
                }
                sqlite.insert_record(current_article)
            page += 1


async def rappler_articles(start_date: str) -> None:
    """
    Fetches articles from Rappler's public WordPress API starting from a given date, processes their content, and stores them in a local SQLite database.
    Args:
        start_date (str): The start date in 'YYYY-MM-DD' format from which to fetch articles.
    Returns:
        None
    Behavior:
        - Iteratively fetches paginated articles from Rappler's API after the specified start date.
        - Converts article HTML content to Markdown, removing unwanted tags.
        - Fetches and aggregates tag slugs for each article.
        - Extracts relevant metadata (ID, source, URL, category, title, date, publish time, tags, cleaned content).
        - Inserts processed articles into a local SQLite database.
        - Logs progress and errors.
    """

    url = 'https://www.rappler.com/wp-json/wp/v2/posts'
    page = 1
    params = {
        'page': page,
        'per_page': 100,
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
                        article.get('content', {}).get(
                            'rendered', 'No content found'),
                        unwanted_tags=['img', 'figure', 'iframe']
                    )
                    tags_tasks = [
                        async_get(
                            session,
                            url=f'https://www.rappler.com/wp-json/wp/v2/tags/{tag_id}')
                        for tag_id in article.get('tags', [])
                    ]
                    tags = await asyncio.gather(*tags_tasks)

                    current_article = {
                        'id': article.get('id'),
                        'source': 'rappler',
                        'url': article.get('link'),
                        'category': urlparse(article.get('link')).path.split('/')[1],
                        'title': article.get('title', {}).get('rendered', 'No title found'),
                        'author': None,
                        'date': article.get('date').split('T')[0],
                        'publish_time': datetime.strptime(
                            article.get(
                                'date',
                                ''),
                            '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'),
                        'tags': ','.join(tag.get('slug', '') for tag in tags if tag),
                        'cleaned_content': article_content,
                    }

                    sqlite.insert_record(current_article)

                page += 1
            except Exception as e:
                logger.error(e)
                logger.error(traceback.format_exc())
                logger.error('############ Error Occurred ############')


async def inquirer_articles(start_date: str) -> None:
    """
    Fetches articles from Inquirer API starting from a specified date, processes them, and stores them in a local SQLite database.
    Args:
        start_date (str): The earliest publish date (in 'YYYY-MM-DD' format) for articles to fetch.
    Returns:
        None
    Behavior:
        - Iteratively fetches paginated articles from the Inquirer API, starting from the most recent and moving backwards in time.
        - For each article, extracts relevant fields such as category, author, tags, publish date, and content.
        - Converts HTML content to Markdown, removing unwanted tags.
        - Stops fetching when articles older than the specified start_date are reached.
        - Stores each processed article as a record in the 'articles' table of the 'articles.db' SQLite database.
        - Logs progress and status throughout the process.
    """
    subdomains = [
        'newsinfo',
        'globalnation',
        'business',
        'lifestyle',
        'entertainment',
        'technology',
        'sports',
        'esports',
        'opinion',
        'usa',
        'bandera',
        'cebudailynews',
        'pop'
        ]
    
    # For content cleaning
    unwanted_ids = ['billboard_article', 'article-new-featured', 'taboola-mid-article-thumbnails', 'taboola-mid-article-thumbnails-stream', 'fb-root']
    unwanted_classes = ['ztoop', 'sib-form', 'cdn_newsletter']
    unwanted_tags = ['script', 'style']
    
    current_article = {}
    
    async with aiohttp.ClientSession() as session:
        for subdomain in subdomains:
            base_url = f'https://{subdomain}.inquirer.net/wp-json/wp/v2/'
            page = 1
            params = {
                'page': page,
                'per_page': 100,
                'after': datetime.strptime(start_date, '%Y-%m-%d').isoformat(),
            }
            while True:
                try:
                    params['page'] = page
                    articles = await async_get(
                        session, 
                        url = f'{base_url}posts', 
                        params=params)
                    logger.info(f'Fetched {len(articles)} articles from Inquirer at {subdomain} subdomain. Page: {page}')

                    for article in articles:
                        article_content = html_to_markdown(
                            article.get('content', {}).get(
                                'rendered', 'No content found'),
                            unwanted_tags=['img', 'figure', 'iframe']
                        )

                        tags_tasks = [
                            async_get(
                                session,
                                url=f'{base_url}tags/{tag_id}')
                            for tag_id in article.get('tags', [])
                        ]
                        tags = await asyncio.gather(*tags_tasks)

                        byline_tasks = [
                            async_get(
                                session,
                                url=f'{base_url}byline/{byline_id}')
                            for byline_id in article.get('byline', [])
                        ]
                        bylines = await asyncio.gather(*byline_tasks)

                        current_article = {
                            'id': article.get('id'),
                            'source': 'inquirer',
                            'url': article.get('link'),
                            'category': subdomain,
                            'title': article.get('title', {}).get('rendered', 'No title found'),
                            'author': ','.join(byline.get('name', '') for byline in bylines if byline),
                            'date': article.get('date').split('T')[0],
                            'publish_time': datetime.strptime(
                                article.get(
                                    'date',
                                    ''),
                                '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'),
                            'tags': ','.join(tag.get('slug', '') for tag in tags if tag),
                            'cleaned_content': article_content,
                        }

                        sqlite.insert_record(current_article)

                    page += 1
                except Exception as e:
                    logger.error(e)
                    logger.error(traceback.format_exc())
                    logger.error('############ Error Occurred ############')
                    break
    
            page = 1 # reset page number for the next subdomain
            logger.info(f'Finished fetching articles from {subdomain} subdomain.')
    
    
def get_all_articles(start_date: str) -> None:
    """
    Fetch articles from ABS-CBN and store them in a SQLite database.
    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(abscbn_articles(start_date))
    loop.run_until_complete(rappler_articles(start_date))
    loop.run_until_complete(inquirer_articles(start_date))
    loop.close()


