from typing import List

from elasticsearch_dsl import *
from elasticsearch_dsl import Search as ElasticSearch
from elasticsearch_dsl.aggs import Agg
from elasticsearch_dsl.query import Query


class Search(ElasticSearch):
    def __init__(self, index="workspace", **kwargs):
        super(Search, self).__init__(index=index, **kwargs)


def qterms(**kwargs) -> Query:
    return Q("terms", **kwargs)


def qterm(**kwargs) -> Query:
    return Q("term", **kwargs)


def qmatch(**kwargs) -> Query:
    return Q("match", **kwargs)


def qbool(**kwargs) -> Query:
    return Q("bool", **kwargs)


def qexists(field: str, **kwargs) -> Query:
    return Q("exists", field=field, **kwargs)


def qnotexists(field: str) -> Query:
    return qbool(must_not=qexists(field))


def qboolor(conditions: List[Query]) -> Query:
    return qbool(should=conditions, minimum_should_match=1,)


def aterms(**kwargs) -> Agg:
    return A("terms", **kwargs)


def acomposite(sources: List[Query], **kwargs) -> Agg:
    return A("composite", sources=sources, **kwargs)


def abucketsort(sort: List[Query], **kwargs) -> Agg:
    return A("bucket_sort", sort=sort, **kwargs)


def script(source: str, params: list = None) -> dict:
    snippet = {
        "source": source,
    }
    if params:
        snippet["params"] = params
    return snippet
