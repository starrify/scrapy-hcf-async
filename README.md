# scrapy-hcf-async

Yet another extension for dealing with the Hubstorage Crawl Frontier. Designed to be an asynchronous alternative to the `scrapylib.hcf.HcfMiddleware`. Supposed to integrate better with the `scrapy` framework.

This extension is currently at its very early stage of development. Suggestions and criticisms are always welcomed!

### Documentations

Not available yet. :( Please read the docstrings in `hcf_async.py` instead.

### Sample Usage

See `sample_spider.py`. You could run it with `scrapy runspider sample_spider.py` (please first edit the file to add an API key and a project ID)

### Motivations

As I've been using `scrapylib.hcf.HcfMiddleware` recently, it's been observed that the middleware sometimes doesn't meet my expectations. Here are some examples:
- Sometimes adding links to HCF may become the bottleneck of the whole spider, e.g. when accessing a large sitemap and trying to store all the links inside to HCF.
- Sometimes requests to the hubstorage server may fail, and such failed requests shall be retried (there is [a pull request](https://github.com/scrapinghub/scrapylib/pull/61) to add an option for retrying)
- `scrapylib.hcf.HcfMiddleware` assumes the `fingerprint` is always the request URL. However in some scenarios it's better pick something like an ID as the `fingerprint` (and store URL in `qdata`), e.g. a place of some website may have a URL like this: `http://somedomain.com/<name-of-the-place>/<id-of-the-place>`, while the name of a place may change (the ID does not).
- Due to the design of the `hubstorage` server, it's hard for `scrapylib.hcf.HcfMiddleware` to load more than 10000 links from HCF.

### Comparasions

- The `hcf_async` extension uses `scrapy.Request` for all its requests (while `scrapylib.hcf.HcfMiddleware` relies on `requests`).
- The `hcf_async` extension is able to load more than 10000 links from HCF.
- The `hcf_async` extension works in an asynchronous manner, which greatly increases the speed of storing links to HCF.
- The `hcf_async` extension allows users to build requests from HCF-loaded links via `new_link_callback`, which is more flexible.
