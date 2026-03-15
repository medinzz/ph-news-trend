import asyncio
import logging
import sys
import concurrent.futures
import random
from scrapy.http import HtmlResponse

logger = logging.getLogger(__name__)

# One shared executor — Camoufox is only launched when actually needed
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

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
    from camoufox.async_api import AsyncCamoufox

    async with AsyncCamoufox(
        headless=True,
        humanize=True,
        i_know_what_im_doing=True
    ) as browser:
        page = await browser.new_page()
        response = await page.goto(url, timeout=6000)
        status = response.status if response else 0

        # Solve Cloudflare Turnstile if present
        for _ in range(15):
            title = await page.title()
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
                            await asyncio.sleep(3)
                    except Exception:
                        pass
            await asyncio.sleep(1)

        # FIX: Use 'domcontentloaded' instead of 'networkidle'.
        # Inquirer pages have too many persistent ad/tracker requests so they
        # never reach networkidle, causing the 15000ms timeout errors you saw.
        # domcontentloaded fires as soon as the HTML is parsed — all we need.
        try:
            await page.wait_for_load_state('domcontentloaded', timeout=10000)
        except Exception:
            pass  # If even this times out, we still grab whatever HTML is available

        html = await page.content()
        return html, status


def _fetch_camoufox_in_thread(url: str) -> tuple[str, int]:
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    return asyncio.run(_async_fetch_camoufox(url))


# ─── Main Middleware ───────────────────────────────────────────────────────────

class CloudflareBypassMiddleware:
    """
    Smart two-path middleware:
      1. Fast path  — plain requests.get(), handles non-CF subdomains in <1s
      2. Slow path  — Camoufox headless browser, for CF-protected subdomains
                      or when fast path detects a challenge page

    Known CF-protected subdomains are routed directly to Camoufox to avoid
    the wasted fast-path attempt + fallback round-trip.

    Only activates for Scrapy requests with meta flag `use_stealthy: True`.
    """

    def process_request(self, request):
        if not request.meta.get('use_stealthy'):
            return None

        url = request.url
        subdomain = _get_subdomain(url)

        # Skip fast path for known CF-protected subdomains
        if subdomain not in _CF_PROTECTED_SUBDOMAINS:
            headers = {**_BASE_HEADERS, 'User-Agent': random.choice(_USER_AGENTS)}
            try:
                import requests as req_lib
                resp = req_lib.get(url, headers=headers, timeout=15, allow_redirects=True)

                if not _is_cloudflare_blocked(resp):
                    logger.debug(f'[FastPath] OK {resp.status_code} {url}')
                    return HtmlResponse(
                        url=resp.url,
                        status=resp.status_code,
                        body=resp.content,
                        encoding=resp.encoding or 'utf-8',
                        request=request,
                    )
                else:
                    logger.warning(f'[FastPath] CF detected on {url}, adding to CF list and falling back...')
                    _CF_PROTECTED_SUBDOMAINS.add(subdomain)  # remember for future requests

            except Exception as e:
                logger.warning(f'[FastPath] Failed for {url}: {e}, falling back to Camoufox...')

        else:
            logger.debug(f'[Camoufox] Known CF subdomain "{subdomain}", skipping fast path for {url}')

        # ── Camoufox slow path ────────────────────────────────────────────────
        try:
            future = _executor.submit(_fetch_camoufox_in_thread, url)
            html, status = future.result(timeout=90)
            logger.info(f'[Camoufox] Fetched {url} with status {status}')
            return HtmlResponse(
                url=url,
                status=200,
                body=html.encode('utf-8'),
                encoding='utf-8',
                request=request,
            )
        except concurrent.futures.TimeoutError:
            logger.error(f'[Camoufox] Timed out for {url}')
        except Exception as e:
            logger.error(f'[Camoufox] Failed for {url}: {e}')

        return None