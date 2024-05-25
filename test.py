from dotenv import load_dotenv

import os


load_dotenv()

url: str = os.getenv('SUPABASE_URL')
key: str = os.getenv('SUPABASE_KEY')

print(url)