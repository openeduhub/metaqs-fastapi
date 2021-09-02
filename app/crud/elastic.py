from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import create_model

from app.elastic import (
    qterm,
    qmatch,
)
from app.models.elastic import ElasticResourceAttribute


class ResourceType(str, Enum):
    COLLECTION = "COLLECTION"
    MATERIAL = "MATERIAL"


base_filter = [
    qterm(qfield=ElasticResourceAttribute.PERMISSION_READ, value="GROUP_EVERYONE"),
    qterm(qfield=ElasticResourceAttribute.EDU_METADATASET, value="mds_oeh"),
    qterm(qfield=ElasticResourceAttribute.PROTOCOL, value="workspace"),
]

type_filter = {
    ResourceType.COLLECTION: [
        qterm(qfield=ElasticResourceAttribute.TYPE, value="ccm:map"),
    ],
    ResourceType.MATERIAL: [
        qterm(qfield=ElasticResourceAttribute.TYPE, value="ccm:io"),
    ],
}


def get_many_base_query(
    resource_type: ResourceType, ancestor_id: Optional[UUID] = None,
) -> dict:
    query_dict = {"filter": [*base_filter, *type_filter[resource_type]]}

    if ancestor_id:
        prefix = "collections." if resource_type == ResourceType.MATERIAL else ""
        query_dict["should"] = [
            qmatch(**{f"{prefix}path": ancestor_id}),
            qmatch(**{f"{prefix}nodeRef.id": ancestor_id}),
        ]
        query_dict["minimum_should_match"] = 1

    return query_dict
