import os

from starlette.datastructures import CommaSeparatedStrings

PROJECT_NAME = os.getenv("PROJECT_NAME")
API_VERSION = os.getenv("API_VERSION")
LOG_LEVEL = os.getenv("LOG_LEVEL")
DEBUG = os.getenv("LOG_LEVEL", "").strip().lower() == "debug"
ALLOWED_HOSTS = CommaSeparatedStrings(os.getenv("ALLOWED_HOSTS", "*"))

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
ELASTICSEARCH_TIMEOUT = 20
