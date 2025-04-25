from urllib.parse import urlparse
import logging


def setup_logger(log_file="app.log"):
    """
    Set up and return a logger that writes logs to both a file and the console.
    """
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

# URL TOOLS
def parse_inq_art_url(url) -> dict:
    parsed = urlparse(url)
    # Extract subdomain (e.g., 'pop', 'globalnation', 'business')
    subdomain = parsed.netloc.split('.')[0]
    origin = parsed.netloc.split('.')[1] if len(parsed.netloc.split('.')) > 1 else ''

    # Split the path: ['', 'article_id', 'article-slug']
    path_parts = parsed.path.strip('/').split('/', 1)

    article_id = path_parts[0] if path_parts else ''
    slug = path_parts[1] if len(path_parts) > 1 else ''

    return {
        'subdomain': subdomain,
        'origin': origin,
        'article_id': article_id,
        'slug': slug
    }