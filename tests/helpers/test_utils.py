
import seldonite.helpers.utils as utils

import pytest

@pytest.mark.parametrize("crawl_name", [("CC-MAIN-2017-13")])
def test_get_crawl_listing(crawl_name):
    listing = utils.get_crawl_listing(crawl_name)
    assert len(listing) > 0
    assert all(entry.startswith('s3://commoncrawl') for entry in listing)
    assert all(crawl_name in entry for entry in listing)

@pytest.mark.parametrize("sites, query", [(["cbc.ca"], "")])
def test_cc_index_query_builder(sites, true_query):
    query = utils.construct_query(sites, 10)
    assert query == true_query
