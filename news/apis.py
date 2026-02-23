import traceback
import asyncio
import aiohttp
import sys

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
    Skips articles that already exist in storage.
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
            logger.info(f'Fetched {len(articles)} articles from ABS-CBN. Offset: {offset}')

            if not articles:
                logger.info('No more ABS-CBN articles found.')
                break

            # Filter articles by date and skip existing records
            filtered_articles = []
            reached_old = False
            for article in articles:
                created_date = datetime.strptime(
                    article.get('createdDateFull', ''),
                    '%Y-%m-%dT%H:%M:%S.%fZ')
                if created_date < start_date:
                    logger.info('Reached ABS-CBN articles older than start_date.')
                    reached_old = True
                    break
                # Skip if already in DB — no point fetching the detail page
                if storage.record_exists(str(article.get('_id'))):
                    logger.debug(f'Skipping existing ABS-CBN record: {article.get("_id")}')
                    continue
                filtered_articles.append((article, created_date))

            if not filtered_articles:
                if reached_old:
                    break
                offset += limit
                continue

            # Fetch all article details concurrently
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
                    date=cd.strftime('%Y-%m-%d'),
                    publish_time=cd.strftime('%Y-%m-%d %H:%M:%S'),
                    tags=item.get('tags'),
                )
                for item, cd in filtered_articles if item.get('slugline_url')
            ]
            details = await asyncio.gather(*tasks)

            inserted = 0
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
                inserted += 1

            logger.info(f'Inserted {inserted} new ABS-CBN articles.')

            if reached_old:
                break

            offset += limit


async def manila_bulletin_articles(start_date: str, section_ids: list = None) -> None:
    """
    Fetches articles from Manila Bulletin's API.
    - Skips detail API calls for articles already in storage.
    - Stops pagination early when all articles on a page already exist (caught up).
    - Stops pagination when articles older than start_date are found.
    """
    if section_ids is None:
        section_ids = [25, 26, 27, 28, 29, 30, 31]

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')

    async with aiohttp.ClientSession() as session:
        for section_id in section_ids:
            page = 1
            logger.info(f'Fetching Manila Bulletin section_id: {section_id}')

            while True:
                try:
                    response = await async_get(
                        session,
                        'https://mb.com.ph/api/pb/fetch-articles-paginated',
                        params={'page': page, 'section_id': section_id}
                    )

                    if not response or response.get('response') != 'success':
                        logger.warning(f'No response for section {section_id}, page {page}')
                        break

                    articles = response.get('data', [])
                    if not articles:
                        logger.info(f'No more articles for section {section_id}')
                        break

                    logger.info(f'Fetched {len(articles)} articles — section: {section_id}, page: {page}')

                    # ── Date filter + early exit checks ───────────────────────
                    reached_old_articles = False
                    filtered_articles = []

                    for article in articles:
                        publish_time = article.get('publish_time', '')
                        if not publish_time:
                            continue
                        article_datetime = datetime.strptime(publish_time, '%Y-%m-%d %H:%M:%S')

                        if article_datetime < start_datetime:
                            # Articles are newest-first — everything after this is older
                            reached_old_articles = True
                            break

                        filtered_articles.append(article)

                    if not filtered_articles:
                        logger.info(f'No in-range articles for section {section_id}, page {page}. Stopping.')
                        break

                    # ── Caught-up check: if every article on this page already exists,
                    #    there's nothing new to fetch — stop this section entirely. ──
                    all_exist = all(
                        storage.record_exists(str(a.get('cms_article_id')))
                        for a in filtered_articles
                    )
                    if all_exist:
                        logger.info(
                            f'All {len(filtered_articles)} articles on page {page} '
                            f'already exist. Caught up for section {section_id}.'
                        )
                        break

                    # ── Fetch detail pages concurrently, skipping known records ──
                    async def fetch_detail(article_summary):
                        cms_id = article_summary.get('cms_article_id')
                        if not cms_id:
                            return None
                        # Skip expensive detail call if already stored
                        if storage.record_exists(str(cms_id)):
                            logger.debug(f'Skipping existing MB record: {cms_id}')
                            return None
                        try:
                            detail = await async_get(
                                session,
                                f'https://mb.com.ph/api/pb/article/{cms_id}'
                            )
                            if detail and detail.get('response') == 'success':
                                return detail.get('data', {})
                        except Exception as e:
                            logger.error(f'Failed to fetch detail for cms_id {cms_id}: {e}')
                        return None

                    details = await asyncio.gather(*[fetch_detail(a) for a in filtered_articles])

                    inserted = 0
                    for article_data in details:
                        if not article_data:
                            continue
                        try:
                            article_content = html_to_markdown(
                                article_data.get('body', '') or article_data.get('summary', 'No content found'),
                                unwanted_tags=['img', 'figure', 'iframe']
                            )
                            tags_raw = article_data.get('cf_article_tags', '')
                            tags = ','.join(
                                t.strip() for t in tags_raw.split(',') if t.strip()
                            ) if isinstance(tags_raw, str) else ''

                            storage.insert_record({
                                'id': article_data.get('cms_article_id'),
                                'source': 'manila_bulletin',
                                'url': article_data.get('link', ''),
                                'category': article_data.get('section_name', 'Unknown'),
                                'title': article_data.get('title', 'No title found'),
                                'author': article_data.get('author_name', 'Unknown'),
                                'date': article_data.get('publish_time', '').split(' ')[0],
                                'publish_time': article_data.get('publish_time', ''),
                                'tags': tags,
                                'cleaned_content': article_content,
                            })
                            inserted += 1
                        except Exception as e:
                            logger.error(f'Error inserting MB article {article_data.get("cms_article_id")}: {e}')
                            logger.error(traceback.format_exc())

                    logger.info(f'Inserted {inserted} new articles — section: {section_id}, page: {page}')

                    if reached_old_articles:
                        logger.info(f'Reached old articles in section {section_id}. Moving on.')
                        break

                    page += 1
                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f'Error on section {section_id}, page {page}: {e}')
                    logger.error(traceback.format_exc())
                    break

    logger.info('Completed fetching all Manila Bulletin articles.')


async def rappler_articles(start_date: str) -> None:
    """
    Fetches articles from Rappler's API.
    Skips articles already present in storage.
    """
    url = 'https://www.rappler.com/wp-json/wp/v2/posts'
    page = 1
    params = {
        'page': page,
        'per_page': 10,
        'after': datetime.strptime(start_date, '%Y-%m-%d').isoformat(),
    }

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params['page'] = page
                articles = await async_get(session, url, params=params)
                logger.info(f'Fetched {len(articles)} articles from Rappler. Page: {page}')

                inserted = 0
                for article in articles:
                    article_id = str(article.get('id'))

                    # Skip if already stored
                    if storage.record_exists(article_id):
                        logger.debug(f'Skipping existing Rappler article: {article_id}')
                        continue

                    article_content = html_to_markdown(
                        article.get('content', {}).get('rendered', 'No content found'),
                        unwanted_tags=['img', 'figure', 'iframe']
                    )
                    tags_tasks = [
                        async_get(session, url=f'https://www.rappler.com/wp-json/wp/v2/tags/{tag_id}')
                        for tag_id in article.get('tags', [])
                    ]
                    tags = await asyncio.gather(*tags_tasks)

                    storage.insert_record({
                        'id': article_id,
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
                    inserted += 1

                logger.info(f'Inserted {inserted} new Rappler articles on page {page}.')
                page += 1
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error('############ Rappler Error ############')
                logger.error(e)
                logger.error(traceback.format_exc())
                break


async def get_all_articles_async(start_date: str, backend: str = 'sqlite', **backend_kwargs) -> None:
    global storage

    storage = get_storage_backend(backend, **backend_kwargs)
    logger.info(f'Using {backend} storage backend')

    if 'main' in sys.modules:
        sys.modules['main'].storage_instance = storage

    try:
        await asyncio.gather(
            abscbn_articles(start_date),
            rappler_articles(start_date),
            manila_bulletin_articles(start_date)
        )
    finally:
        storage.close()


def get_all_articles(start_date: str, backend: str = 'sqlite', **backend_kwargs) -> None:
    """Fetch articles from all sources and store using the specified backend."""
    asyncio.run(get_all_articles_async(start_date, backend, **backend_kwargs))