
import seldonite.helpers.utils as utils

import pytest

@pytest.mark.parametrize("crawl_name", [("CC-MAIN-2017-13")])
def test_get_crawl_listing(crawl_name):
    listing = utils.get_crawl_listing(crawl_name)
    assert len(listing) > 0
    assert all(entry.startswith('s3://commoncrawl') for entry in listing)
    assert all(crawl_name in entry for entry in listing)


@pytest.mark.parametrize("sites, limit, true_query", 
    [(["cbc.ca"], 10, "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2021-39' AND subset = 'wet' AND url_host_registered_domain IN ('cbc.ca') LIMIT 10"),
     (["cbc.ca", "apnews.com"], 100, "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2021-39' AND subset = 'wet' AND url_host_registered_domain IN ('cbc.ca', 'apnews.com') LIMIT 100")])
def test_cc_index_query_builder(sites, limit, true_query):
    query = utils.construct_query(sites, limit)
    assert query == true_query
