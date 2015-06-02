# coding: utf8
"""Only testing."""

import hcf_async
import scrapy


class SampleSpider(scrapy.Spider):
    """A sample spider."""

    name = 'sample_spider'
    start_urls = ['http://httpbin.org/get?baz=%d' % i for i in range(32)]

    def __init__(self, *args, **kwargs):
        """Init"""
        super(SampleSpider, self).__init__(*args, **kwargs)
        self.hcf = hcf_async.HcfAsync(
            num_links_to_fetch=100,
            new_link_callback=self.new_link_callback,
            consume_from=('test0', 'test0'),
            slot_buf_size=1024,
            hs_auth=API_KEY_HERE,
            hs_project_id=PROJECT_ID_HERE
        )

    def new_link_callback(self, links):
        """Callback for new links from HCF."""
        # Process links from HCF
        for fingerprint, qdata in links:
            yield scrapy.Request(fingerprint)

    def parse(self, response):
        """Parses default response."""
        # Store links to HCF
        yield self.hcf.add_link_to_hcf('test0', 'test1', response.url + '&foo=bar')
