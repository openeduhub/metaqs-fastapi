from fastapi import APIRouter

from .endpoints.collections import router as collections_router
from .endpoints.demo import router as demo_router
from .endpoints.learning_materials import router as materials_router
from .endpoints.stats import router as stats_router
from .endpoints.dbt_analytics import router as analytics_router


router = APIRouter()
router.include_router(materials_router)
router.include_router(collections_router)
router.include_router(stats_router)
router.include_router(analytics_router, prefix="/analytics")
router.include_router(demo_router)
