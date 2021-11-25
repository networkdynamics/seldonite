import datetime

import seldonite.helpers.utils as utils

import pytest

@pytest.mark.parametrize("crawl_name", [("CC-MAIN-2017-13")])
def test_get_crawl_listing(crawl_name):
    listing = utils.get_crawl_listing(crawl_name)
    assert len(listing) > 0
    assert all(entry.startswith('s3://commoncrawl') for entry in listing)
    assert all(crawl_name in entry for entry in listing)

def test_most_recent_cc_crawl():
    crawl = utils.most_recent_cc_crawl()
    assert 'CC-MAIN' in crawl

@pytest.mark.parametrize("date, earliest_crawl",
    [(datetime.date(2019, 6, 23), 'CC-MAIN-2019-30'),
     (datetime.date(2015, 11, 23), 'CC-MAIN-2016-07'),
     (datetime.date(2014, 5, 11), 'CC-MAIN-2014-23'),
     (datetime.date(2013, 2, 14), 'CC-MAIN-2014-10'),
     (datetime.date(2012, 1, 29), 'CC-MAIN-2013-20'),
     (datetime.date(2011, 2, 10), 'CC-MAIN-2012'),
     (datetime.date(2010, 4, 12), 'CC-MAIN-2012'),
     (datetime.date(2008, 12, 23), 'CC-MAIN-2009-2010')])
def test_get_cc_crawls_since(date, earliest_crawl):
    crawls = utils.get_cc_crawls_since(date)
    assert len(crawls) >= 23
    assert crawls[-1] == earliest_crawl

@pytest.mark.parametrize("sites, limit, true_query", 
    [(["cbc.ca"], 10, "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2021-39' AND subset = 'wet' AND url_host_registered_domain IN ('cbc.ca') LIMIT 10"),
     (["cbc.ca", "apnews.com"], 100, "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2021-39' AND subset = 'wet' AND url_host_registered_domain IN ('cbc.ca', 'apnews.com') LIMIT 100")])
def test_cc_index_query_builder(sites, limit, true_query):
    query = utils.construct_query(sites, limit)
    assert query == true_query
