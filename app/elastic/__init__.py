from typing import (
    List,
    Union,
)

from elasticsearch_dsl import *
from elasticsearch_dsl import Search as ElasticSearch
from elasticsearch_dsl.aggs import Agg
from elasticsearch_dsl.query import Query

from .fields import (
    Field,
    FieldType,
)


def _extract_terms_key(field: Union[Field, str]) -> str:
    if isinstance(field, Field):
        field_key = field.path
        if field.field_type is FieldType.TEXT:
            field_key = f"{field_key}.keyword"
        return field_key
    else:
        return field


class Search(ElasticSearch):
    def __init__(self, index="workspace", **kwargs):
        super(Search, self).__init__(index=index, **kwargs)

    def source(self, source_fields=None, **kwargs):
        if source_fields:
            source_fields = [
                (field.path if isinstance(field, Field) else field)
                for field in source_fields
            ]
        return super(Search, self).source(source_fields, **kwargs)


def qterm(field: Union[Field, str], value, **kwargs) -> Query:
    kwargs[_extract_terms_key(field)] = value
    return Q("term", **kwargs)


def qterms(field: Union[Field, str], values: list, **kwargs) -> Query:
    kwargs[_extract_terms_key(field)] = values
    return Q("terms", **kwargs)


def qmatch(**kwargs) -> Query:
    return Q("match", **kwargs)


def qwildcard(field: Union[Field, str], value, **kwargs) -> Query:
    if isinstance(field, Field):
        field = field.path
    kwargs[field] = value
    return Q("wildcard", **kwargs)


def qbool(**kwargs) -> Query:
    return Q("bool", **kwargs)


def qexists(field: Union[Field, str], **kwargs) -> Query:
    if isinstance(field, Field):
        field = field.path
    return Q("exists", field=field, **kwargs)


def qnotexists(field: str) -> Query:
    return qbool(must_not=qexists(field))


def qboolor(conditions: List[Query]) -> Query:
    return qbool(should=conditions, minimum_should_match=1,)


def aterms(field: Union[Field, str], **kwargs) -> Agg:
    kwargs["field"] = _extract_terms_key(field)
    return A("terms", **kwargs)


def acomposite(sources: List[Union[Query, dict]], **kwargs) -> Agg:
    return A("composite", sources=sources, **kwargs)


def abucketsort(sort: List[Union[Query, dict]], **kwargs) -> Agg:
    return A("bucket_sort", sort=sort, **kwargs)


def script(source: str, params: dict = None) -> dict:
    snippet = {
        "source": source,
    }
    if params:
        snippet["params"] = params
    return snippet
