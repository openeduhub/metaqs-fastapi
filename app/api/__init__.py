from importlib import import_module

from app.core.config import API_VERSION

api = import_module(f".{API_VERSION}.api", package=__name__)

router = api.router
