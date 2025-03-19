import pandas as pd
from util.api import get_news, get_gemini_output

# articles = pd.DataFrame(get_news(query='duterte'))
articles = pd.read_json('results.json')
summarize_article = get_gemini_output(f'''
can you provide a summarization of this news article? url: {articles.loc[0, 'url']}
''')
print(summarize_article)