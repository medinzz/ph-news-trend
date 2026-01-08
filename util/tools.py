################### LOGGER ###################
def setup_logger(log_file="app.log"):
    """
    Set up and return a logger that writes logs to both a file and the console.
    """
    import logging


    logger = logging.getLogger("custom_logger")
    logger.setLevel(logging.DEBUG)

    # Prevent adding multiple handlers if logger is already set up
    if not logger.hasHandlers():
        # File Handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


################### ASYNC HTTP REQUESTS ###################
async def async_get(
        session,
        url: str,
        params: dict[str, str | int] = {},
        headers: dict[str, str | int] = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br', # aiohttp handles decompression automatically
            },
        cookies: dict[str, str | int] = {},
        **kwargs):
    async with session.get(url, params=params, headers=headers, cookies=cookies) as response:
        if response.status == 200:
            result = await response.json()
        
            # If there are any kwargs passed, update the result
            if kwargs:
                result.update(kwargs)
            
            return result
        else:
            raise Exception(f"Error fetching {url}: Status: {response.status}, params: {params}")
################### CONVERT HTML RAW CONTENT TO MARKDOWN ###################
def html_to_markdown(
        html_content: str,  # Changed from 'html' to 'html_content'
        unwanted_ids: list[str] = [],
        unwanted_classes: list[str] = [],
        unwanted_tags: list[str] = []) -> str:
    
    import html
    import html2text
    from bs4 import BeautifulSoup
    import re
    
    # Unescape HTML entities
    unescaped_html = html.unescape(html_content)  # Now using html_content parameter
    
    soup = BeautifulSoup(unescaped_html, 'html.parser')

    # Remove by id
    for id in unwanted_ids:
        for tag in soup.select(f'#{id}'):
            tag.decompose()

    # Remove by class
    for html_class in unwanted_classes:
        for tag in soup.select(f'.{html_class}'):
            tag.decompose()

    # Remove by tag name
    for tag_str in unwanted_tags:
        for tag in soup.find_all(tag_str):
            tag.decompose()

    # Convert to markdown
    html2md = html2text.HTML2Text()
    html2md.body_width = 0
    html2md.ignore_links = True
    html2md.ignore_images = True
    html2md.ignore_emphasis = True
    html2md.skip_internal_links = True
    
    markdown_content = html2md.handle(str(soup))
    
    # Clean up extra whitespace
    markdown_content = re.sub(r'\n{3,}', '\n\n', markdown_content)
    markdown_content = re.sub(r' +', ' ', markdown_content)
    
    return markdown_content.strip()