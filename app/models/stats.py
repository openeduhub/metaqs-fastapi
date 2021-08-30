from datetime import datetime
from typing import Dict

from .base import (
    BaseModel,
    ResponseModel,
)


class MaterialStats(BaseModel):
    __root__: Dict[str, int]


class StatsResponse(ResponseModel):
    timestamp: datetime
    stats: Dict[str, Dict[str, MaterialStats]]
