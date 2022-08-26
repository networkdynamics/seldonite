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


@pytest.mark.parametrize("start_date, end_date",
    [(None, None),
     (datetime.date(2017, 1, 1), datetime.date(2017, 6, 1)),
     (datetime.date(2018, 3, 1), datetime.date(2018, 9, 1))])
def test_get_news_crawl_listing_dates(start_date, end_date):
    paths = utils.get_news_crawl_listing(start_date=start_date, end_date=end_date)
    assert paths
    assert len(paths) > 0
    assert all(path.startswith('s3://commoncrawl/crawl-data/') for path in paths)
    assert all(path.endswith('.warc.gz') for path in paths)

def test_map_col_with_index():
    data = [{'index': 0, 'col': 1}]
    iter = (item for item in data)
    def func(items):
        return items
    res_gen = utils.map_col_with_index(iter, 'index', 'col', 'new_col', func)
    res = list(res_gen)
    assert len(res) == len(data)

def test_construct_db_uri():
    connection_str = 'mongodb+srv://user:pass@mongodb-svc.default.svc.cluster.local/admin?ssl=false'
    database = 'my_db'
    collection = 'my_collection'
    desired_uri = 'mongodb+srv://user:pass@mongodb-svc.default.svc.cluster.local/my_db.my_collection?ssl=false'

    uri = utils.construct_db_uri(connection_str, database, collection)
    assert uri == desired_uri

@pytest.mark.parametrize("url",
    [("https://www.reuters.com/world/europe/two-more-ships-depart-ukraine-turkeys-defence-ministry-2022-08-12/"),
     ("https://www.reuters.com/markets/commodities/ukraine-says-it-can-export-3-million-tonnes-grain-ports-next-month-2022-08-16/"),
     ("https://www.reuters.com/business/palladium-sheds-nearly-13-worries-over-china-demand-hit-2022-04-25/")])
def test_link_to_article(url):
    article = utils.link_to_article(url)
    assert article.meta_data is not None