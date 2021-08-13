from enum import Enum
from typing import Optional

from app.elastic import (
    qterm,
    qmatch,
)


class ResourceType(str, Enum):
    COLLECTION = "COLLECTION"
    MATERIAL = "MATERIAL"


base_filter = [
    qterm(**{"permissions.read.keyword": "GROUP_EVERYONE"}),
    qterm(**{"properties.cm:edu_metadataset.keyword": "mds_oeh"}),
    qterm(**{"nodeRef.storeRef.protocol.keyword": "workspace"}),
]

type_filter = {
    ResourceType.COLLECTION: [qterm(type="ccm:map"),],
    ResourceType.MATERIAL: [qterm(type="ccm:io"),],
}


def get_many_base_query(
    resource_type: ResourceType, ancestor_id: Optional[str] = None,
) -> dict:
    query_dict = {"filter": type_filter[resource_type]}
    query_dict["filter"].extend(base_filter)
    if ancestor_id:
        prefix = "collections." if resource_type == ResourceType.MATERIAL else ""
        query_dict["should"] = [
            qmatch(**{f"{prefix}path": ancestor_id}),
            qmatch(**{f"{prefix}nodeRef.id": ancestor_id}),
        ]
        query_dict["minimum_should_match"] = 1
    return query_dict
