import asyncio
import logging
import sys
import time
import concurrent.futures
from scrapy.http import HtmlResponse

logger = logging.getLogger(__name__)

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)


async def _async_fetch(url: str) -> tuple[str, int]:
    """
    Uses Camoufox async API. We own the event loop here,
    so ProactorEventLoop is guaranteed on Windows.
    """
    from camoufox.async_api import AsyncCamoufox

    async with AsyncCamoufox(
        headless=True, 
        humanize=True,
        i_know_what_im_doing=True  # silences the LeakWarning
    ) as browser:
        page = await browser.new_page()
        response = await page.goto(url, timeout=30000)
        status = response.status if response else 0

        # Wait out Cloudflare challenge
        for _ in range(15):
            title = await page.title()
            if 'just a moment' not in title.lower() and 'attention required' not in title.lower():
                break
            # Try clicking Turnstile checkbox if present
            for frame in page.frames:
                if 'challenges.cloudflare.com' in frame.url:
                    try:
                        bbox = await frame.frame_element()
                        bbox = await bbox.bounding_box()
                        if bbox:
                            await page.mouse.click(
                                bbox['x'] + bbox['width'] / 9,
                                bbox['y'] + bbox['height'] / 2,
                            )
                            await asyncio.sleep(3)
                    except Exception:
                        pass
            await asyncio.sleep(1)

        await page.wait_for_load_state('networkidle', timeout=15000)
        html = await page.content()
        return html, status


def _fetch_in_thread(url: str) -> tuple[str, int]:
    """
    Runs in a ThreadPoolExecutor worker.
    We set the event loop policy BEFORE asyncio.run() so that
    asyncio.run() creates a ProactorEventLoop on Windows — 
    which is required for subprocess support (Playwright needs it).
    """
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    return asyncio.run(_async_fetch(url))


class CloudflareBypassMiddleware:
    def process_request(self, request, spider):
        if not request.meta.get('use_stealthy'):
            return None

        logger.debug(f'CloudflareBypassMiddleware fetching: {request.url}')
        try:
            future = _executor.submit(_fetch_in_thread, request.url)
            html, status = future.result(timeout=60)
            return HtmlResponse(
                url=request.url,
                status=200,
                body=html.encode('utf-8'),
                encoding='utf-8',
                request=request,
            )
        except concurrent.futures.TimeoutError:
            logger.error(f'Camoufox timed out for {request.url}')
        except Exception as e:
            logger.error(f'Camoufox failed for {request.url}: {e}')

        return None