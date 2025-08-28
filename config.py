import os

# Simplified, robust project root discovery
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, 'DATABASE')
# Backwards compatibility names
MARKET_ASSISTANTS_DIR = BASE_DIR
PARENT_DIR = os.path.dirname(BASE_DIR)