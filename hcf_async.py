# coding: utf8
"""
Yet another extension for dealing with the Hubstorage Crawl Frontier. Designed
to be an asynchronous alternative to the `scrapylib.hcf.HcfMiddleware`.
Supposed to integrate better with the `scrapy` framework.
"""

try:
    import ujson as json
except ImportError:
    import json

import scrapy
import scrapy.xlib.pydispatch.dispatcher
import scrapy.signals
import scrapy.utils.request


class HcfAsync(object):
    """The asynchronous HCF extension class."""
    ids_to_delete = None
    num_links_to_delete = 0
    new_job_scheduled = False
    link_buf = {}

    def __init__(
            self, num_links_to_fetch=0, num_links_batch=1000, consume_from=None,
            new_link_callback=None, start_new_job=False, slot_buf_size=4096,
            debug_mode=False, hs_project_id=None, hs_auth=None,
            hs_endpoint='http://storage.scrapinghub.com/'):
        """Initializes the class.

        Parameters:
            `num_links_to_fetch`: (_int_) Number of links to read from HCF.
                Defaults to `0`.
            `num_links_batch`: (_int_) Number of links to read in a batch.
                Defaults to `1000`.
            `consume_from`: (_tuple_ or _list_) Contains the name of frontier
                and slot to read from. E.g. `('frontier_name', 'slot_name')`.
                Defaults to `None`.
            `new_link_callback`: (_callable_) A function to be called after
                retrieving new links from HCF. It shall accept one parameter
                which is a list of `[fingerprint, qdata]`. Its return value
                shall be a `scrapy.Item`, a `scrapy.Request`, or `None`
                (exactly the same as the `callback` parameter of
                `scrapy.Request`). Defaults to `None`.
            `start_new_job`: (_boolean_) Whether to start a new job after this
                one. Defaults to `False`.
            `slot_buf_size`: (_int_) Size of the per-slot buffer when storing
                links to HCF. Defaults to `4096`.
            `debug_mode`: (_boolean_) For debugging purpose. If set to `True`,
                links would not be deleted from HCF, while at most
                `num_links_batch` links woul be read from HCF. Defaults to
                `False`.
            `hs_project_id`: (_string_) The project ID on dash.
            `hs_auth`: (_string_) The API key.
            `hs_endpoint`: (_string_) The API endpoint.
        """
        self.num_links_to_fetch = num_links_to_fetch
        # This number 10000 is hardcoded in the `hubstorage` package. See:
        # https://github.com/scrapinghub/hubstorage/blob/master/servlet/src/main/webapp/hstorage/apis/frontier/crawlfrontier.py
        self.num_links_batch = min(num_links_batch, 10000)
        for key in (
                'num_links_to_fetch', 'consume_from', 'new_link_callback',
                'start_new_job', 'slot_buf_size', 'debug_mode',
                'hs_project_id', 'hs_auth', 'hs_endpoint'):
            setattr(self, key, locals()[key])

        scrapy.xlib.pydispatch.dispatcher.connect(
            self.on_idle, scrapy.signals.spider_idle)

    @staticmethod
    def url_component_join(*args):
        """Joins URL components."""
        return '/'.join(x.strip('/') for x in args)

    def parse_delete_from_queue(self, response):
        """Parses a delete operation."""
        assert response.status == 200
        scrapy.log.msg('Deleted %d IDs (%d links) from HCF' % (
            response.meta['num_ids'], response.meta['num_links']))

    def parse_read_queue(self, response):
        """Parses a read operation."""
        assert response.status == 200
        self.ids_to_delete = []
        links = []
        for line in response.body.strip().splitlines():
            jdata = json.loads(line)
            self.ids_to_delete.append(jdata['id'])
            links.extend(jdata['requests'])
        self.num_links_to_delete = len(links)
        self.num_links_to_fetch -= self.num_links_to_delete
        self.num_links_to_fetch = max(self.num_links_to_fetch, 0)
        if not links:
            # The slot is now empty
            self.num_links_to_fetch = 0
        if self.debug_mode:
            # Don't load more links from HCF
            self.num_links_to_fetch = 0
        scrapy.log.msg('Obtained %d IDs (%d links) from HCF' % (
            len(self.ids_to_delete), self.num_links_to_delete))
        for ret in self.new_link_callback(links):
            yield ret

    def parse_add_queue(self, response):
        """Parses an add operation."""
        assert response.status == 200
        jdata = json.loads(response.body)
        scrapy.log.msg('Trying to add %d links to HCF (%d added)' % (
            response.meta['num_links'], jdata['newcount']))

    def parse_schedule_job(self, response):
        """Parses a schedule operation."""
        assert response.status == 200
        scrapy.log.msg('Scheduled new job: %s' % response.body)

    def on_idle(self, spider):
        """Invokes on spider idle. Works like a finite state machine to rotate
        the spiders status."""
        if self.ids_to_delete and not self.debug_mode:
            # Issue a request to delete stored IDs
            frontier, slot = self.consume_from
            url = self.url_component_join(
                self.hs_endpoint, 'hcf', self.hs_project_id, frontier, 's',
                slot, 'q', 'deleted'
            )
            request = scrapy.Request(
                url=url,
                method='POST',
                body='\n'.join('"%s"' % x for x in self.ids_to_delete),
                meta={
                    'num_ids': len(self.ids_to_delete),
                    'num_links': self.num_links_to_delete,
                },
                dont_filter=True,
                callback=self.parse_delete_from_queue,
            )
            scrapy.utils.request.request_authenticate(request, self.hs_auth, '')
            spider.crawler.engine.crawl(request, spider)
            self.num_links_to_delete = 0
            self.ids_to_delete = None
            raise scrapy.exceptions.DontCloseSpider
        elif self.num_links_to_fetch:
            # Issue a request to fetch new links
            frontier, slot = self.consume_from
            url = self.url_component_join(
                self.hs_endpoint, 'hcf', self.hs_project_id, frontier, 's',
                slot, 'q'
            )
            request = scrapy.FormRequest(
                url=url,
                method='GET',
                formdata={
                    'mincount': str(min(
                        self.num_links_batch, self.num_links_to_fetch)),
                },
                dont_filter=True,
                callback=self.parse_read_queue,
            )
            scrapy.utils.request.request_authenticate(request, self.hs_auth, '')
            spider.crawler.engine.crawl(request, spider)
            raise scrapy.exceptions.DontCloseSpider
        elif self.link_buf:
            # Issue some requests to flush the link buffer
            for frontier, frontier_buf in self.link_buf.items():
                for slot, slot_buf in frontier_buf.items():
                    if slot_buf:
                        request = self._get_add_queue_request(
                            frontier, slot, slot_buf)
                        spider.crawler.engine.crawl(request, spider)
            self.link_buf = {}
            raise scrapy.exceptions.DontCloseSpider
        elif self.start_new_job and not self.new_job_scheduled:
            # Issue a request to schedule a new job
            request = scrapy.FormRequest(
                url='https://dash.scrapinghub.com/api/schedule.json',
                method='POST',
                formdata={
                    'project': self.hs_project_id,
                    'spider': spider.name,
                },
                dont_filter=True,
                callback=self.parse_schedule_job
            )
            scrapy.utils.request.request_authenticate(request, self.hs_auth, '')
            spider.crawler.engine.crawl(request, spider)
            self.new_job_scheduled = True
            raise scrapy.exceptions.DontCloseSpider

    def _get_add_queue_request(self, frontier, slot, links):
        """Build a `scrapy.Request` object to add `links` into the specified
        `frontier` and `slot` on HCF."""
        url = self.url_component_join(
            self.hs_endpoint, 'hcf', self.hs_project_id, frontier, 's', slot
        )
        request = scrapy.Request(
            url=url,
            method='POST',
            body='\n'.join(json.dumps(x) for x in links),
            meta={
                'num_links': len(links),
            },
            dont_filter=True,
            callback=self.parse_add_queue
        )
        scrapy.utils.request.request_authenticate(request, self.hs_auth, '')
        return request

    def add_link_to_hcf(
            self, frontier, slot, fingerprint,
            qdata=None, fdata=None, p=None):
        """Adds a link to HCF. Returns a `scrapy.Request` object if the slot
        buffer is full or `None`.

        Parameters:
            `fingerprint`: (_string_) Request fingerprint. Typically the request
                URL itself.
            `qdata`: (_object_) Data to be stored along with the fingerprint in
                the request queue.
            `fdata`: (_object_) Data to be stored along with the fingerprint in
                the fingerprint set.
            `p`: (_int_) Priority: lower priority numbers are returned first
                (default is 0).
        """
        link_data = {'fp': fingerprint}
        for key in ('qdata', 'fdata', 'p'):
            if locals()[key] is not None:
                link_data.update({key: locals()[key]})
        frontier_buf = self.link_buf.setdefault(frontier, {})
        slot_buf = frontier_buf.setdefault(slot, [])
        slot_buf.append(link_data)
        if len(slot_buf) >= self.slot_buf_size:
            request = self._get_add_queue_request(frontier, slot, slot_buf)
            frontier_buf.pop(slot)
            if not frontier_buf:
                self.link_buf.pop(frontier)
            return request
