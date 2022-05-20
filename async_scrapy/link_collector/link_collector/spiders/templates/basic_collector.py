import json
import logging
import re
from time import sleep

import pika
import requests
import six
from elasticsearch import Elasticsearch
from scrapy.http import XmlResponse
from scrapy.spiders import Spider
from scrapy.utils.gz import gunzip, is_gzipped
from elasticsearch.exceptions import ConnectionTimeout as ES_ConnectionTimeout
from urllib3.exceptions import ReadTimeoutError


from etl_scrapy_spiders.config import (
    articles_queue,
    es_host,
    es_port,
    publisher_queue_size,
    publisher_timeout,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)
logging.getLogger("basic_producer").setLevel(logging.WARNING)
logging.getLogger("pika.callback").setLevel(logging.ERROR)
logging.getLogger("pika.connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.blocking_connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.select_connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.base_connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.utils.io_services_utils").setLevel(logging.ERROR)
logging.getLogger("pika.channel").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


def regex(x):
    if isinstance(x, six.string_types):
        return re.compile(x)
    return x


def iterloc(it, alt=False):
    for d in it:
        yield d["loc"]
        # Also consider alternate URLs (xhtml:link rel="alternate")
        if alt and "alternate" in d:
            for l in d["alternate"]:
                yield l


class BasicProducer(Spider):
    """
    Basic producer class
    """

    def parse(self, response):
        pass

    def __init__(
        self,
        settings_file=None,
        settings_type=None,
        unwanted_links=[],
        unwanted_link_pattern="",
        es_index=None,
        check_index_exists=False,
        ignore_test=False,
        *a,
        **kw
    ):
        super(BasicProducer, self).__init__(*a, **kw)
        try:
            self.unwanted_links = unwanted_links.split(",")
        except AttributeError:
            self.unwanted_links = []

        self.unwanted_link_pattern = unwanted_link_pattern
        self.settings_file = settings_file
        self._cbs = []
        self.es_index = es_index
        self.settings_type = settings_type
        self.ignore_test = ignore_test

    def try_connect(self):
        self.es = Elasticsearch(hosts=[{"host": es_host, "port": es_port}])
        while True:
            try:
                self.es.cluster.health()
                break
            except (ConnectionError, TimeoutError, ES_ConnectionTimeout, ReadTimeoutError) as connection_issues:
                logger.error("Problem with connection occurred during producer runtime", connection_issues)

    def index_exists(self, check_index_exists):
        """
        Checks if index exists in elastic instead of creating new one.
        :param check_index_exists:
        :return:
        """
        if self.es_index is None or self.es_index == "":
            logger.error("Elasticsearch index variable is empty. Provided value = %s. Aborting all actions", self.es_index)
            raise ValueError
        else:
            if check_index_exists:
                self.try_connect()
                if not self.es.indices.exists(self.es_index):
                    logger.error("Elasticsearch index variable consist of index that do not exists. Provided value = %s. Aborting all actions",
                                 self.es_index)
                    raise ValueError

    def read_instruction(self, settings_file, settings_type):
        """
        reads instruction from file or string argument provided to producer
        :param settings_file:
        :param settings_type:
        :return:
        """
        if settings_type == "file":
            with open(settings_file, "r") as file:
                request_rpc = json.load(file)
        elif settings_type == "str":
            request_rpc = json.loads(settings_file)
        else:
            logger.error("Provided type for settings is wrong. Aborting all actions")
            raise TypeError
        return request_rpc

    def rabbit_declare(self):
        self.channel.exchange_declare(exchange=articles_queue, exchange_type="direct")
        self.channel.queue_declare(
            queue=articles_queue,
            durable=True,
            arguments={"x-max-length": publisher_queue_size, "x-overflow": "reject-publish"},
        )
        self.channel.queue_bind(exchange=articles_queue, queue=articles_queue, routing_key="")
        self.channel.confirm_delivery()

    def prepare_message(self, request_rpc, link):
        """
        prepares message for RabbitMQ queue
        :param settings_file:
        :param settings_type:
        :param link:
        :return:
        """
        if request_rpc:
            request_rpc["params"]["parse"]["link"] = link
            request_rpc["params"]["parse"]["index"] = self.es_index
            try:
                self.channel.basic_publish(
                    exchange="",
                    routing_key=articles_queue,
                    body=json.dumps(request_rpc, ensure_ascii=False),
                    properties=pika.BasicProperties(delivery_mode=2,),  # make message persistent
                    mandatory=True,
                )
            except pika.exceptions.NackError:
                self.logger.warning("Queue is full, waiting.")
                sleep(publisher_timeout)
                self.prepare_message(request_rpc, link)

    def closed(self, reason):
        self.connection.close()

    def check_item_exists(self, loc):
        """
        Checks if item already exists in elasticsearch index
        :param loc:
        :return:
        """
        self.try_connect()
        retry_attempts = 0
        while True:
            if retry_attempts > 20:
                return None
            try:
                url = 'http://{ES_HOST}:{ES_PORT}/{es_index}/_search?q=link:"{link}"'.format(
                    ES_HOST=es_host, ES_PORT=es_port, es_index=self.es_index, link=loc
                )
                check_link = requests.get(url)
                check_response = json.loads(check_link.text)
                return check_response
            except (ConnectionError, TimeoutError, ES_ConnectionTimeout, ReadTimeoutError) as connection_issues:
                logger.error("Problem with connection occurred during producer runtime", connection_issues)
                retry_attempts += 1



    def _get_sitemap_body(self, response):
        """Return the sitemap body contained in the given response,
        or None if the response is not a sitemap.
        Returns response in case sitemap ends with .txt
        due to some chinese practices.
        """
        if isinstance(response, XmlResponse):
            return response.body
        elif is_gzipped(response):
            return gunzip(response.body)
        elif response.url.endswith(".xml"):
            return response.body
        elif response.url.endswith(".txt"):
            return response
        elif response.url.endswith(".xml.gz"):
            return gunzip(response.body)