import requests
import json
from google import genai

# Local libraries
from util.logger import setup_logger
from util.const import (
    NEWS_API_KEY, 
    
    #gemini constants
    GEMINI_API_KEY,
    GEMINI_MODEL
)

logger = setup_logger()


def get_news(query='tech'): 
    url = 'https://newsapi.org/v2/everything'

    params = {
        'apiKey': NEWS_API_KEY,
        'q': query,
        'searchIn': 'title',
        'sortBy':'popularity'
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        status = data['status']
        num_results = data['totalResults']
        articles = data['articles']
        
        logger.info(f'util: get_news({query}) status: {status} articles found: {num_results}')
        with open('results.json', 'w') as file:
            json.dump(articles, file, indent=1)
    else:
        logger.error('Error:', response.json())
    return articles


def get_gemini_output(prompt):
    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt
    )
    return response

