import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'host': os.environ.get('POSTGRES_HOST', 'localhost'),
    'port': os.environ.get('POSTGRES_PORT', 5433),
    'user': os.environ.get('POSTGRES_USER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres_password'),
    'dbname': os.environ.get('POSTGRES_DB', 'default_db')
}
