import os

import pandas as pd

from seldonite.spark.cc_index_fetch_news import CCIndexFetchNewsJob

import mocks

def test_query_set_correctly():
    spark_master_url = "k8s://https://10.140.16.25:6443"
    sites = ["cbc.ca"]
    job = CCIndexFetchNewsJob(spark_master_url=spark_master_url, sites=sites)
    assert job.query is not None


def test_articles_pulled_from_url_via_warc():

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