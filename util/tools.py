# URL TOOLS
from urllib.parse import urlparse

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

# MISC 
from datetime import datetime

def find_date_in_list(items, date_format='%I:%M %p %B %d, %Y'):
    for item in items:
        try:
            return datetime.strptime(item.replace('/', '').strip(), date_format)
        except ValueError:
            continue
    return None