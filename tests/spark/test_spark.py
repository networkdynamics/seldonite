import os
import sys
import zipfile

import pandas as pd
import pyspark.sql as psql

from seldonite import filters
from seldonite.helpers import utils
from seldonite.commoncrawl.cc_index_fetch_news import CCIndexFetchNewsJob
from seldonite.commoncrawl.fetch_news import FetchNewsJob

def test_preprocess_political_filter():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    fixture_path = os.path.join(this_dir_path, '..', 'fixtures')
    data_path = os.path.join(fixture_path, 'to_preprocess_political.csv')
    p_df = pd.read_csv(data_path)

    # make sure the file are where we need them
    filters.political.ensure_zip_exists()
    zip_path = os. path.join(this_dir_path, '..', '..', 'seldonite', 'filters', 'pon_classifier.zip')
    utils.unzip(zip_path, fixture_path)

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    #Create PySpark SparkSession
    master = 'local[1]'
    spark = psql.SparkSession.builder \
        .master(master) \
        .appName("test_political_filter") \
        .getOrCreate()
    #Create PySpark DataFrame from Pandas
    df = spark.createDataFrame(p_df[['url', 'all_text']])

    fetch_news_job = FetchNewsJob()
    fetch_news_job.political_filter_path = fixture_path

    tokens_df = fetch_news_job.preprocess_text(spark, df)

    try:
        assert tokens_df.count() > 0
    finally:
        # clean up files
        os.remove(os.path.join(fixture_path, 'model.h5'))
        os.remove(os.path.join(fixture_path, 'tokenizer.json'))