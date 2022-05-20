import multiprocessing
import os
from itertools import chain
import timeit
from tqdm import tqdm
import elasticsearch
import pandas as pd
import functools
from elasticsearch.helpers import scan

es_host1 = os.getenv("ELASTIC_HOST1")
es_host2 = os.getenv("ELASTIC_HOST2")
es_host3 = os.getenv("ELASTIC_HOST3")

es = elasticsearch.Elasticsearch(
    hosts=[{"host": es_host1, "port": 9200}, {"host": es_host2, "port": 9200}, {"host": es_host3, "port": 9200}],
    timeout=30,
    max_retries=10,
    retry_on_timeout=True,
)


def extract_data(terms, _id):
    return {"id_": _id, "term": terms.get("term"), "count": terms.get("count")}


def process_doc(item):
    """
    Важно:
    В других индексах вместо article_body должно быть нужное поле с текстом обработанным на термины.
    Также как и нужный язык вместо en. В русских текстах анлийские термины например искать бессмысленно.

    :param item:
    :return:
    """
    total = []
    _id = item.get("_id", None)
    try:
        raw_terms = item["_source"]["phrases"]["en"]["part"]["article_body"]["NP"]
        extracted_terms = map(functools.partial(extract_data, _id=_id), raw_terms)
    except KeyError:
        # Не все тексты могут быть обработаны на термины в текущий момент.
        print("No fields with terms for document: {_id}".format(_id=_id))
        extracted_terms = []
    total.extend(extracted_terms)
    return total


query = {
    "track_total_hits": True,
    "fields": [{"field": "unified_date", "format": "strict_date_optional_time"}],
    "script_fields": {},
    "stored_fields": ["*"],
    "_source": True,
    "query": {
        "bool": {
            "must": [{"query_string": {"fields": ["*.en"], "query": "hydrogen"}}],
            "filter": [{"range": {"unified_date": {"gte": "2015-12-31", "lte": "2022-05-16", "format": "yyyy-MM-dd"}}}],
            "should": [],
            "must_not": [],
        }
    },
}

if __name__ == "__main__":
    index = "*md_*_v1.*"
    start = timeit.timeit()

    docs = scan(client=es, index=index, query=query, request_timeout=30000, scroll="1m", size=10000)
    chunk = []
    unified_results = []
    dataset_ = None
    for doc in tqdm(docs):
        chunk.append(doc)
        if len(chunk) >= 10000:
            print("Collected batch of documents: {size}".format(size=len(chunk)))
            with multiprocessing.Pool(processes=4) as pool:
                map_results = pool.map(process_doc, chunk)
            chunk = []
            results = chain.from_iterable(map_results)
            if dataset_ is None:
                dataset_ = pd.DataFrame(results)
            else:
                step_dataset_ = pd.DataFrame(results)
                dataset_ = dataset_.append(step_dataset_, ignore_index=True)
            print("Size of dataframe on step: {size}".format(size=len(dataset_.index)))

    if len(chunk) < 10000:
        print("Collected batch of documents: {size}".format(size=len(chunk)))
        with multiprocessing.Pool(processes=4) as pool:
            map_results = pool.map(process_doc, chunk)
        chunk = []
        results = chain.from_iterable(map_results)
        step_dataset_ = pd.DataFrame(results)
        dataset_ = dataset_.append(step_dataset_, ignore_index=True)
    print("Final size of dataframe: {size} rows".format(size=len(dataset_.index)))
    dataset_.to_csv("result_dataset.csv", index_label=False, index=False)
    end = timeit.timeit()
    print("Took time:", end - start)
