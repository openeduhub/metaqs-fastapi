from enum import Enum
from typing import (
    List,
    Tuple,
)
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.sql import ClauseElement
from sqlalchemy.dialects.postgresql import pypostgresql
from starlette.exceptions import HTTPException
from starlette.status import HTTP_404_NOT_FOUND

from app.models.collection import (
    Collection,
    PortalTreeNode,
)

dialect = pypostgresql.dialect(paramstyle="pyformat")
dialect.implicit_returning = True
dialect.supports_native_enum = True
dialect.supports_smallserial = True
dialect._backslash_escapes = False
dialect.supports_sane_multi_rowcount = True
dialect._has_native_hstore = True


class CollectionNotFoundException(HTTPException):
    def __init__(self, noderef_id):
        super().__init__(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Collection with id '{noderef_id}' not found",
        )


def compile_query(query: ClauseElement) -> Tuple[str, list, tuple]:
    compiled = query.compile(dialect=dialect)
    compiled_params = sorted(compiled.params.items())

    mapping = {key: "$" + str(i) for i, (key, _) in enumerate(compiled_params, start=1)}
    compiled_query = compiled.string % mapping

    processors = compiled._bind_processors
    params = [
        processors[key](val) if key in processors else val
        for key, val in compiled_params
    ]

    return compiled_query, params, compiled._result_columns


class OrderByDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class OrderByParams(BaseModel):
    column: str
    direction: OrderByDirection = OrderByDirection.ASC

    def __call__(self, query: ClauseElement):
        col = query.columns[self.column]
        if self.direction == OrderByDirection.DESC:
            query = query.order_by(col.desc())
        else:
            query = query.order_by(col.asc())
        return query


async def build_portal_tree(
    portals: List[Collection], root_noderef_id: UUID
) -> List[PortalTreeNode]:
    lut = {str(root_noderef_id): []}

    for portal in portals:
        portal_node = PortalTreeNode(
            noderef_id=portal.noderef_id, title=portal.title, children=[],
        )

        try:
            lut[str(portal.parent_id)].append(portal_node)
        except KeyError:
            lut[str(portal.parent_id)] = [portal_node]

        lut[str(portal.noderef_id)] = portal_node.children

    return lut.get(str(root_noderef_id), [])
