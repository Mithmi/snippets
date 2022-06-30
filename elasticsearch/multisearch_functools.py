import functools
import logging
import multiprocessing
import os
from itertools import chain

import elasticsearch
from urllib3.exceptions import NewConnectionError

logger = logging.getLogger(__name__)


def prepare_docid_query(_id):
    query = {"query": {"bool": {"must": [{"match_phrase": {"_id": _id}}]}}}
    return query


def prepare_msearch_docids(docid):
    query = prepare_docid_query(_id=docid)
    return [{}, query]


def extract_source(item, field):
    try:
        _source = item["hits"]["hits"][0]["_source"]
    except (KeyError, IndexError) as _source_error:
        return None
    if _source and field in _source:
        return _source[field]
    else:
        return None


def multi_search(es_client, index, prepared_msearch, retry_attempts=0):
    """
    Обёртка для функции es.msearch, делает мультисёрч запрос по подготовленному запросу
    :param es: elasticsearch client
    :param prepared_msearch: prepared query request for multi-search
    :param retry_attempts: number of allowed retry attempts
    :return: elasticsearch multi-search response
    """
    result = None
    while retry_attempts < 20:
        try:
            result = es_client.msearch(index=index, body=prepared_msearch, request_timeout=3000)
            break
        except (
            elasticsearch.exceptions.ConnectionTimeout,
            elasticsearch.exceptions.ConnectionError,
            elasticsearch.exceptions.TransportError,
            NewConnectionError,
        ) as error:
            retry_attempts += 1
            logger.warning("{}".format(error))
        except Exception as e:
            logging.critical("msearch error {} on query {}".format(e, prepared_msearch))
            raise
    return result


es_host = os.getenv("ELASTIC_HOST")

es = elasticsearch.Elasticsearch(
    hosts=[{"host": es_host, "port": 9200}], timeout=60, max_retries=20, retry_on_timeout=True,
)
index = ""  # ES index
docids = []  # list of ids for docs in ES
field = ""  # some field inside doc source
prepared_msearch = map(prepare_msearch_docids, docids)
prepared_msearch = list(chain.from_iterable(prepared_msearch))
docs = multi_search(es_client=es, index=index, prepared_msearch=prepared_msearch, retry_attempts=10)
with multiprocessing.Pool(processes=4) as pool:
    sources = pool.map(functools.partial(extract_source(), field=field), docs["responses"])
sources = list(chain.from_iterable(sources))  # needs to flatten lists from [[]] to []
