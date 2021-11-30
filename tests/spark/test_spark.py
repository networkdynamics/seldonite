import os

import pandas as pd

from seldonite.helpers import utils
from seldonite.spark.cc_index_fetch_news import CCIndexFetchNewsJob
from seldonite.spark.fetch_news import FetchNewsJob

import mocks

def test_query_set_correctly():
    spark_master_url = "k8s://https://10.140.16.25:6443"
    sites = ["cbc.ca"]
    job = CCIndexFetchNewsJob(spark_master_url=spark_master_url, sites=sites)
    assert job.query is not None


def test_articles_pulled_from_url_via_index():

    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    fixture_dir_path = os.path.join(this_dir_path, '..', 'fixtures')
    warc_index_path = os.path.join(fixture_dir_path, 'apnews_2021_39_index_limit_10.csv')

    warc_index = pd.read_csv(warc_index_path)
    warc_rdd = mocks.MockRDD(warc_index)

    master_url = ""
    job = CCIndexFetchNewsJob(spark_master_url=master_url)
    job.records_processed = mocks.MockAccumulator()
    job.warc_input_processed = mocks.MockAccumulator()
    job.warc_input_failed = mocks.MockAccumulator()
    job.set_constraints(None, [], [], [], None, None)
    articles = job.fetch_process_warc_records(warc_rdd)

    for article in articles:
        assert 'text' in article
        assert article['text']
        assert 'title' in article
        assert article['title']
        assert 'url' in article
        assert 'https://apnews.com' in article['url']
        assert 'publish_date' in article
        assert article['publish_date']

def test_articles_pulled_from_url_via_listing():

    warc_urls = utils.get_news_crawl_listing()[-1:]

    master_url = ""
    job = FetchNewsJob(spark_master_url=master_url)
    job.records_processed = mocks.MockAccumulator()
    job.warc_input_processed = mocks.MockAccumulator()
    job.warc_input_failed = mocks.MockAccumulator()
    job.set_constraints(None, [], [], None, None)
    articles = job.process_warcs(warc_urls)

    for article in articles:
        assert 'text' in article
        assert article['text']
        assert 'title' in article
        assert article['title']
        assert 'url' in article
        assert 'publish_date' in article
        assert article['publish_date']