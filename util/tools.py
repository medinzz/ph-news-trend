import html2text

from urllib.parse import urlparse
from bs4 import BeautifulSoup


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

# MISC 
def clean_article(
        article: str, 
        unwanted_ids: list = [], 
        unwanted_classes: list = [], 
        unwanted_elements: list = ['script', 'style']) -> str:
    '''
    Cleans an HTML article by removing unwanted elements, IDs, and classes, 
    and converts the cleaned HTML into Markdown format.
    Args:
        article (str): The HTML content of the article to be cleaned.
        unwanted_ids (list, optional): A list of element IDs to remove from the HTML. Defaults to an empty list.
        unwanted_classes (list, optional): A list of CSS class selectors to remove from the HTML. Defaults to an empty list.
        unwanted_elements (list, optional): A list of HTML tag names to remove from the HTML. Defaults to ['script', 'style'].
    Returns:
        str: The cleaned article content converted to Markdown format.
    '''
    
    soup = BeautifulSoup(article, 'html.parser')

    # Remove unwanted elements by ID
    for unwanted_id in unwanted_ids:
        tag = soup.find(id=unwanted_id)
        if tag:
            tag.decompose()
    
    for tag in soup(unwanted_elements):
        tag.decompose()

    # Remove unwantend elements by CLASS
    for cls in unwanted_classes:
        for tag in soup.select(cls):
            tag.decompose()

    # Convert cleaned HTML to Markdown
    clean_html = str(soup)
    markdown = html2text.html2text(clean_html, bodywidth = 0)

    return markdown
