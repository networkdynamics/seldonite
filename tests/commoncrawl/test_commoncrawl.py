import os

import pandas as pd
import pytest
from seldonite.helpers import utils
from seldonite.commoncrawl.cc_index_fetch_news import CCIndexFetchNewsJob
from seldonite.commoncrawl.fetch_news import FetchNewsJob

import mocks

def test_query_set_correctly():
    spark_master_url = "k8s://https://10.140.16.25:6443"
    sites = ["cbc.ca"]
    job = CCIndexFetchNewsJob(spark_master_url=spark_master_url, sites=sites)
    assert job.query is not None


@pytest.mark.parametrize(
    "csv_path, site_name", 
    [
        ('apnews_2021_39_index_limit_10.csv', 'https://apnews.com'), 
        ('vox_records.csv', 'https://www.vox.com')
    ]
)
def test_articles_pulled_from_url_via_index(csv_path, site_name):

    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    fixture_dir_path = os.path.join(this_dir_path, '..', 'fixtures')
    warc_index_path = os.path.join(fixture_dir_path, csv_path)

    warc_index = pd.read_csv(warc_index_path)
    warc_rdd = mocks.MockRDD(warc_index)

    access_key = os.environ['AWS_ACCESS_KEY']
    secret_key = os.environ['AWS_SECRET_KEY']
    job = CCIndexFetchNewsJob(access_key, secret_key)
    job.records_processed = mocks.MockAccumulator()
    job.warc_input_processed = mocks.MockAccumulator()
    job.warc_input_failed = mocks.MockAccumulator()
    job.set_constraints([], None, None)
    job.features = ['url', 'text', 'title', 'publish_date', 'meta_keywords']
    articles = job.fetch_process_warc_records(warc_rdd)
    articles = list(articles)

    for article in articles:
        assert 'text' in article
        assert article['text']
        assert 'title' in article
        assert article['title']
        assert 'url' in article
        assert site_name in article['url']
        assert 'publish_date' in article
        assert article['publish_date']

def test_articles_pulled_from_url_via_listing():

    warc_urls = utils.get_news_crawl_listing()[-1:]

    master_url = ""
    job = FetchNewsJob(spark_master_url=master_url, num_input_partitions=1)
    job.records_processed = mocks.MockAccumulator()
    job.warc_input_processed = mocks.MockAccumulator()
    job.warc_input_failed = mocks.MockAccumulator()

    limit = 10
    sites = ['thehour.com']
    job.set_constraints(limit, [], sites, None, None)
    job.url_only = False
    articles_gen = job.process_warcs(warc_urls)

    articles = list(articles_gen)
    assert len(articles) <= limit
    for article in articles:
        assert 'text' in article
        assert 'title' in article
        assert 'url' in article
        assert 'publish_date' in article