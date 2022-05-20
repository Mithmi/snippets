import logging
import re

import pika
import six
from scrapy.http import Request
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots

from etl_scrapy_spiders.config import (
    rabbit_host,
    rabbit_pass,
    rabbit_user,
    rabbit_port,
    headers,
)
from etl_scrapy_spiders.spiders.templates.basic_producer import BasicProducer, iterloc, regex

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)
logging.getLogger("producer_sitemaps").setLevel(logging.WARNING)
logging.getLogger("pika.callback").setLevel(logging.ERROR)
logging.getLogger("pika.connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.blocking_connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.select_connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.base_connection").setLevel(logging.ERROR)
logging.getLogger("pika.adapters.utils.io_services_utils").setLevel(logging.ERROR)
logging.getLogger("pika.channel").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


class ProducerSitemaps(BasicProducer):
    name = "producer_sitemaps"
    custom_settings = headers

    def __init__(
        self,
        sitemap_urls=[],
        settings_file=None,
        settings_type=None,
        unwanted_links=[],
        unwanted_link_pattern="",
        es_index=None,
        check_index_exists=True,
        ignore_test=False,
        *a,
        **kw
    ):
        super(ProducerSitemaps, self).__init__(*a, **kw)
        try:
            self.sitemap_urls = sitemap_urls.split(",")
        except AttributeError:
            self.sitemap_urls = []
        try:
            self.unwanted_links = unwanted_links.split(",")
        except AttributeError:
            self.unwanted_links = []

        self.sitemap_rules = [("", "parse")]
        self.sitemap_follow = [""]
        self.sitemap_alternate_links = False
        self.url_custom_list = []

        self.unwanted_link_pattern = unwanted_link_pattern
        self.settings_file = settings_file
        self._cbs = []
        self.es_index = es_index
        self.settings_type = settings_type
        self.ignore_test = ignore_test
        self.index_exists(check_index_exists)

        for r, c in self.sitemap_rules:
            if isinstance(c, six.string_types):
                c = getattr(self, c)
            self._cbs.append((regex(r), c))

        self._follow = [regex(x) for x in self.sitemap_follow]

        self.credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, heartbeat=0, credentials=self.credentials)
        )

        self.channel = self.connection.channel()
        self.rabbit_declare()
        self.rpc_request = self.read_instruction(self.settings_file, self.settings_type)
        if not self.sitemap_urls:
            self.sitemap_urls = self.rpc_request["params"]["parse"]["start_page"]

    def start_requests(self):
        for url in self.sitemap_urls:
            yield Request(url, self._parse_sitemap)

    def _parse_sitemap(self, response):
        if not self.ignore_test:
            self.already_parsed_elastic = []
        if response.url.endswith("/robots.txt"):
            for url in sitemap_urls_from_robots(response.text):
                yield Request(url, callback=self._parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                logger.warning("Ignoring invalid sitemap: %(response)s", {"response": response}, extra={"spider": self})
                return
            try:
                s = Sitemap(body)
                if s.type == "sitemapindex":
                    if self.unwanted_link_pattern != "":
                        rxc = re.compile(self.unwanted_link_pattern)
                        for loc in list(filter(rxc.match, iterloc(s, self.sitemap_alternate_links))):
                            if any(x.search(loc) for x in self._follow):
                                yield Request(loc, callback=self._parse_sitemap)
                            else:
                                continue
                    else:
                        for loc in iterloc(s, self.sitemap_alternate_links):
                            if any(x.search(loc) for x in self._follow) and loc not in self.unwanted_links:
                                yield Request(loc, callback=self._parse_sitemap)
                            else:
                                continue
                elif s.type == "urlset":
                    for loc in iterloc(s):
                        if not self.ignore_test:
                            try:
                                if isinstance(loc, tuple):
                                    loc = loc[0][0]
                                check_item_exists_response = self.check_item_exists(loc)
                                if (check_item_exists_response is not None) and (check_item_exists_response["hits"]["total"]["value"] >= 1):
                                    self.already_parsed_elastic.append(loc)
                            except Exception as error:
                                self.logger.error(error)
                            if loc in self.already_parsed_elastic:
                                self.logger.info("url %s already exists in index, skipping", loc)
                                continue
                        for r, c in self._cbs:
                            if r.search(loc):
                                self.prepare_message(self.rpc_request, loc)
                                break
            except ValueError:
                list_of_urls = []
                raw_list_of_urls = body.xpath("//body//text()").extract()
                for item in raw_list_of_urls:
                    list_of_urls += item.split("\n")
                for loc in list_of_urls:
                    if loc in self.already_parsed_elastic:
                        self.logger.info("url %s already exists in index, skipping", loc)
                        continue
                    if "http" in loc:
                        self.prepare_message(self.rpc_request, loc)