import traceback
import asyncio
import aiohttp
import sys
import html

from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup

from util.tools import setup_logger, async_get, html_to_markdown
from util.storage_backend import get_storage_backend, StorageBackend


logger = setup_logger()

# Global storage backend - will be set by get_all_articles
storage: StorageBackend = None

# ── SHARED HEADERS ─────────────────────────────────────────────────────────────

GMA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:155.0) Gecko/20100101 Firefox/155.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://www.gmanetwork.com',
    'Referer': 'https://www.gmanetwork.com/',
}

GMA_BASE_URL      = 'https://www.gmanetwork.com/news/'
GMA_TRACKER_URL   = 'https://data.gmanetwork.com/gno/widgets/grid_reverse_listing/just_in/tracker.gz'
GMA_LIST_BASE_URL = 'https://data.gmanetwork.com/gno/widgets/grid_reverse_listing/just_in/{count}.gz'


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
                storage.upsert_record({
                    'id': article.get('id'),
                    'source': article.get('source'),
                    'url': 'https://www.abs-cbn.com/' + article.get('slugline_url'),
                    'category': article.get('category'),
                    'title': html.unescape(article.get('title')),
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
                            reached_old_articles = True
                            break

                        filtered_articles.append(article)

                    if not filtered_articles:
                        logger.info(f'No in-range articles for section {section_id}, page {page}. Stopping.')
                        break

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

                    async def fetch_detail(article_summary):
                        cms_id = article_summary.get('cms_article_id')
                        if not cms_id:
                            return None
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

                            storage.upsert_record({
                                'id': article_data.get('cms_article_id'),
                                'source': 'manila_bulletin',
                                'url': article_data.get('link', ''),
                                'category': article_data.get('section_name', 'Unknown'),
                                'title': html.unescape(article_data.get('title', 'No title found')),
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

                    storage.upsert_record({
                        'id': article_id,
                        'source': 'rappler',
                        'url': article.get('link'),
                        'category': urlparse(article.get('link')).path.split('/')[1],
                        'title': html.unescape(article.get('title', {}).get('rendered', 'No title found')),
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


async def _gma_fetch_content(session: aiohttp.ClientSession, article_url: str) -> str:
    """
    Fetch a single GMA article page and extract the content from div.story_main.
    Returns the content as markdown, or a fallback string on failure.
    """
    try:
        async with session.get(
            article_url,
            headers={**GMA_HEADERS},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as response:
            if response.status != 200:
                logger.warning(f'GMA content fetch returned {response.status} for {article_url}')
                return 'Cannot extract article content'

            html_text = await response.text()
            soup = BeautifulSoup(html_text, 'html.parser')
            story_div = soup.select_one('div.story_main')

            if not story_div:
                logger.warning(f'div.story_main not found for {article_url}')
                return 'Cannot extract article content'

            # Remove unwanted elements inside the content div
            for tag in story_div.find_all(['script', 'style', 'iframe', 'figure', 'img']):
                tag.decompose()

            return html_to_markdown(str(story_div), unwanted_tags=['img', 'figure', 'iframe'])

    except Exception as e:
        logger.error(f'Error fetching GMA content for {article_url}: {e}')
        return 'Cannot extract article content'


async def gma_articles(start_date: str) -> None:
    """
    Fetches and stores GMA News articles published since a given start date.

    Flow:
        1. GET tracker.gz → get current count
        2. GET {count}.gz → get article batch, parse nested structure
        3. Filter by publish_timestamp, skip existing IDs
        4. Decrement count and repeat until reaching articles older than start_date
        5. For each new article batch, concurrently fetch article pages for content
    """
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')

    async with aiohttp.ClientSession() as session:

        # ── Step 1: Get current count ─────────────────────────────────────
        try:
            async with session.get(
                GMA_TRACKER_URL,
                headers=GMA_HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                tracker_data = await resp.json(content_type=None)
                current_count = tracker_data.get('count')
                if not current_count:
                    logger.error('GMA tracker returned no count. Aborting.')
                    return
                logger.info(f'GMA tracker count: {current_count}')
        except Exception as e:
            logger.error(f'Failed to fetch GMA tracker: {e}')
            return

        # ── Step 2: Paginate backwards through article batches ────────────
        count = current_count
        reached_old = False
        total_inserted = 0

        while not reached_old and count > 0:
            try:
                list_url = GMA_LIST_BASE_URL.format(count=count)
                async with session.get(
                    list_url,
                    headers=GMA_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f'GMA list returned {resp.status} for count {count}. Stopping.')
                        break
                    list_data = await resp.json(content_type=None)

            except Exception as e:
                logger.error(f'Error fetching GMA list at count {count}: {e}')
                break

            articles = list_data.get('data', [])
            if not articles:
                logger.info(f'GMA: empty batch at count {count}. Stopping.')
                break

            logger.info(f'GMA: fetched {len(articles)} items at count {count}')

            # ── Step 3: Filter by date and existing IDs ───────────────────
            new_articles = []
            for article in articles:
                try:
                    publish_dt = datetime.strptime(
                        article.get('publish_timestamp', ''),
                        '%Y-%m-%d %H:%M:%S'
                    )
                except ValueError:
                    continue

                # Articles are newest-first — stop when we pass start_date
                if publish_dt < start_datetime:
                    logger.info(
                        f'GMA: reached articles older than {start_date}. Stopping.')
                    reached_old = True
                    break

                article_id = f"gma:{article.get('id')}"

                if storage.record_exists(article_id):
                    logger.debug(f'Skipping existing GMA record: {article_id}')
                    continue

                new_articles.append({
                    'id': article_id,
                    'raw_id': article.get('id'),
                    'title': html.unescape(article.get('title', 'No title')),
                    'author': article.get('author', 'No author') or 'No author',
                    'category': (
                        article.get('subsection', {}).get('ssec_name')
                        or article.get('section', {}).get('sec_name')
                        or 'Unknown'
                    ),
                    'url': GMA_BASE_URL + article.get('link', ''),
                    'date': publish_dt.strftime('%Y-%m-%d'),
                    'publish_time': publish_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'tags': ','.join(
                        t.strip().replace(' ', '_')
                        for t in article.get('tags', '').split()
                        if t.strip()
                    ),
                })

            # ── Step 4: Fetch content concurrently for new articles ───────
            if new_articles:
                content_tasks = [
                    _gma_fetch_content(session, a['url'])
                    for a in new_articles
                ]
                contents = await asyncio.gather(*content_tasks)

                inserted = 0
                for article, content in zip(new_articles, contents):
                    try:
                        storage.upsert_record({
                            'id': article['id'],
                            'source': 'gma',
                            'url': article['url'],
                            'category': article['category'],
                            'title': article['title'],
                            'author': article['author'],
                            'date': article['date'],
                            'publish_time': article['publish_time'],
                            'tags': article['tags'],
                            'cleaned_content': content,
                        })
                        inserted += 1
                    except Exception as e:
                        logger.error(
                            f'Error inserting GMA article {article["id"]}: {e}')
                        logger.error(traceback.format_exc())

                total_inserted += inserted
                logger.info(
                    f'GMA: inserted {inserted} articles from count {count}.')

            # ── Step 5: Decrement count for next batch ────────────────────
            count -= 1
            await asyncio.sleep(0.3)

        logger.info(f'GMA: completed. Total inserted: {total_inserted}.')


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
            manila_bulletin_articles(start_date),
            gma_articles(start_date)
        )
    finally:
        storage.close()


def get_all_articles(start_date: str, backend: str = 'sqlite', **backend_kwargs) -> None:
    """Fetch articles from all sources and store using the specified backend."""
    asyncio.run(get_all_articles_async(start_date, backend, **backend_kwargs))