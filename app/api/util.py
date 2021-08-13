from typing import Optional

from fastapi import (
    Path,
    Query,
)
from pydantic import BaseModel
from starlette.responses import Response

from app.models import (
    CollectionAttribute,
    MaterialAttribute,
)
from app.crud import (
    MissingCollectionAttributeFilter,
    MissingMaterialAttributeFilter,
)


def collections_filter_params(
    *, missing_attr: CollectionAttribute = Path(...)
) -> MissingCollectionAttributeFilter:
    return MissingCollectionAttributeFilter(attr=missing_attr)


def materials_filter_params(
    *, missing_attr: MaterialAttribute = Path(...)
) -> MissingMaterialAttributeFilter:
    return MissingMaterialAttributeFilter(attr=missing_attr)


# def sort_params(*, _sort: str = Query(None), _order: str = Query(None)):
#     if _sort or _order:
#         return OrderByParams(column=_sort, direction=_order)


def pagination_params(
    *, _start: int = Query(0), _end: int = Query(None), response: Response
):
    return PaginationParams(start=_start, stop=_end, response=response)


class PaginationParams(BaseModel):
    start: int = 0
    stop: Optional[int] = None
    response: Response

    class Config:
        arbitrary_types_allowed = True

    def __call__(self, records: list):
        self.response.headers["X-Total-Count"] = str(len(records))
        return records[self.start : self.stop]
