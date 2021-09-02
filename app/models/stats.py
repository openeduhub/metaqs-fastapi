from datetime import datetime
from typing import Dict

from .base import ResponseModel


class StatsResponse(ResponseModel):
    derived_at: datetime
    stats: Dict[str, Dict[str, Dict[str, int]]]
