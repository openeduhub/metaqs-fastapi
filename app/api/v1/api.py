from fastapi import APIRouter

from .endpoints.demo import router as demo_router


router = APIRouter()
router.include_router(demo_router)
