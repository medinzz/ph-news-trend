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


# ASYNC HTTP REQUESTS
async def async_get(session, url, params=None, **kwargs):
    async with session.get(url, params=params) as response:
        if response.status == 200:
            result = await response.json()
        
            # If there are any kwargs passed, update the result
            if kwargs:
                result.update(kwargs)
            
            return result
        else:
            raise Exception(f"Error fetching {url}: {response.status}")
        
# HTML 2 MARKDOWN CONVERTER

def html_to_markdown(html: str) -> str:
    import html2text


    html2md = html2text.HTML2Text()
    html2md.ignore_images = True
    html2md.body_width = 0

    return html2md.handle(html)