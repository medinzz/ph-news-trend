import asyncio
import logging
import sys
import concurrent.futures
import random
from scrapy.http import HtmlResponse
from camoufox.async_api import AsyncCamoufox

logger = logging.getLogger(__name__)

# One shared executor — Camoufox is only launched when actually needed
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Subdomains confirmed to be behind Cloudflare — skip fast path entirely for these
_CF_PROTECTED_SUBDOMAINS = {'bandera', 'business', 'opinion', 'globalnation', 'usa'}

# ─── Realistic browser headers ────────────────────────────────────────────────
_BASE_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}

_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
]


def _get_subdomain(url: str) -> str:
    try:
        return url.split('//')[1].split('.')[0]
    except Exception:
        return ''


def _is_cloudflare_blocked(response) -> bool:
    """Detect Cloudflare challenge from a `requests` library response."""
    # FIX: `requests` uses .status_code, not .status (which is Scrapy's attribute)
    if response.status_code in (403, 503):
        return True
    body = response.text[:2000].lower()
    return (
        'just a moment' in body
        or 'cf-browser-verification' in body
        or 'attention required' in body
    )


# ─── Camoufox fallback ────────────────────────────────────────────────────────

async def _async_fetch_camoufox(url: str) -> tuple[str, int]:

    async with AsyncCamoufox(
        headless=True,
        humanize=True,
        i_know_what_im_doing=True
    ) as browser:
        page = await browser.new_page()

        # OPTIMIZATION 1: Block heavy media & ad resources to speed up loads by 3-5x
        async def _block_heavy_resources(route):
            if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", _block_heavy_resources)

        # OPTIMIZATION 2: Increase navigation timeout to avoid abrupt driver disconnects
        try:
            response = await page.goto(url, timeout=15000, wait_until="domcontentloaded")
            status = response.status if response else 0
        except Exception as e:
            logger.warning(f"[Camoufox] page.goto warning for {url}: {e}")
            status = 0

        # Solve Cloudflare Turnstile if present
        for _ in range(10):
            try:
                title = await page.title()
            except Exception:
                break

            if 'just a moment' not in title.lower() and 'attention required' not in title.lower():
                break

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
                            await asyncio.sleep(2)
                    except Exception:
                        pass
            await asyncio.sleep(1)

        try:
            html = await page.content()
        except Exception:
            html = ""

        return html, status


def _fetch_camoufox_in_thread(url: str) -> tuple[str, int]:
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    return asyncio.run(_async_fetch_camoufox(url))


# ─── Main Middleware ───────────────────────────────────────────────────────────

class CloudflareBypassMiddleware:
    """
    Reactive Middleware:
    Allows Scrapy to fetch pages via fast native downloader.
    Intercepts responses and triggers Camoufox ONLY when Cloudflare blocks
    a request with a 403, 503, or challenge page.
    """

    def process_response(self, request, response, spider):
        # 1. Check if response is normal (200 OK and no challenge text)
        is_cf_blocked = response.status in (403, 503) or 'just a moment' in response.text[:2000].lower()
        
        if not is_cf_blocked:
            return response

        url = request.url
        logger.warning(f'[CF Block] Status {response.status} on {url}. Triggering Camoufox fallback...')

        # 2. Escalate ONLY blocked requests to Camoufox
        try:
            future = _executor.submit(_fetch_camoufox_in_thread, url)
            html, status = future.result(timeout=90)
            
            if html and 'just a moment' not in html[:2000].lower():
                logger.info(f'[Camoufox] Bypassed Cloudflare for {url}')
                return HtmlResponse(
                    url=url,
                    status=200,
                    body=html.encode('utf-8'),
                    encoding='utf-8',
                    request=request,
                )
            else:
                logger.error(f'[Camoufox] Challenge remained unsolved for {url}')
        except concurrent.futures.TimeoutError:
            logger.error(f'[Camoufox] Timed out for {url}')
        except Exception as e:
            logger.error(f'[Camoufox] Failed for {url}: {e}')

        return response