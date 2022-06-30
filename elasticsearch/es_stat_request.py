import os

import elasticsearch
import pandas as pd

es_host = os.getenv("ELASTIC_HOST")

es = elasticsearch.Elasticsearch(
    hosts=[{"host": es_host, "port": 9200}], timeout=60, max_retries=20, retry_on_timeout=True,
)


stats = es.cat.indices(v=True, format="json")
es_stats = pd.DataFrame(stats)
es_stats.to_csv("Index.csv")
