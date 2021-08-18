import os

from databases import DatabaseURL
from starlette.datastructures import CommaSeparatedStrings

PROJECT_NAME = os.getenv("PROJECT_NAME")
API_VERSION = os.getenv("API_VERSION")
LOG_LEVEL = os.getenv("LOG_LEVEL")
DEBUG = os.getenv("LOG_LEVEL", "").strip().lower() == "debug"
ALLOWED_HOSTS = CommaSeparatedStrings(os.getenv("ALLOWED_HOSTS", "*"))

DATABASE_URL = os.getenv("DATABASE_URL")  # deploying without docker-compose
if not DATABASE_URL:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")

    DATABASE_URL = DatabaseURL(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    )
else:
    DATABASE_URL = DatabaseURL(DATABASE_URL)

MAX_CONNECTIONS_COUNT = int(os.getenv("MAX_CONNECTIONS_COUNT", 10))
MIN_CONNECTIONS_COUNT = int(os.getenv("MIN_CONNECTIONS_COUNT", 10))

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_TIMEOUT = 20
