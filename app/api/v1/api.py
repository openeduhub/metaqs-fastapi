from fastapi import APIRouter

from .endpoints.demo import router as demo_router
from .endpoints.collections import router as collections_router


router = APIRouter()
router.include_router(demo_router)
router.include_router(collections_router)
