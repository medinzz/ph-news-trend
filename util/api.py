from google import genai
# Local libraries
from util.tools import setup_logger
from util.const import (
    # gemini constants
    GEMINI_API_KEY,
    GEMINI_MODEL,
)

logger = setup_logger()

def get_gemini_output(prompt):
    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
    )
    return response

