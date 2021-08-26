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


def _extract_terms_key(qfield: Union[Field, str]) -> str:
    if isinstance(qfield, Field):
        qfield_key = qfield.path
        if qfield.field_type is FieldType.TEXT:
            qfield_key = f"{qfield_key}.keyword"
        return qfield_key
    else:
        return qfield


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


def qsimplequerystring(query: str, qfields: List[Union[Field, str]], **kwargs) -> Query:
    kwargs["query"] = query
    kwargs["fields"] = [
        (qfield.path if isinstance(qfield, Field) else qfield) for qfield in qfields
    ]
    return Q("simple_query_string", **kwargs)


def qterm(qfield: Union[Field, str], value, **kwargs) -> Query:
    kwargs[_extract_terms_key(qfield)] = value
    return Q("term", **kwargs)


def qterms(qfield: Union[Field, str], values: list, **kwargs) -> Query:
    kwargs[_extract_terms_key(qfield)] = values
    return Q("terms", **kwargs)


def qmatch(**kwargs) -> Query:
    return Q("match", **kwargs)


def qwildcard(qfield: Union[Field, str], value, **kwargs) -> Query:
    if isinstance(qfield, Field):
        qfield = qfield.path
    kwargs[qfield] = value
    return Q("wildcard", **kwargs)


def qbool(**kwargs) -> Query:
    return Q("bool", **kwargs)


def qexists(qfield: Union[Field, str], **kwargs) -> Query:
    if isinstance(qfield, Field):
        qfield = qfield.path
    return Q("exists", field=qfield, **kwargs)


def qnotexists(qfield: str) -> Query:
    return qbool(must_not=qexists(qfield))


def qboolor(conditions: List[Query]) -> Query:
    return qbool(should=conditions, minimum_should_match=1,)


def aterms(qfield: Union[Field, str], **kwargs) -> Agg:
    kwargs["field"] = _extract_terms_key(qfield)
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
