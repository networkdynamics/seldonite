from seldonite.spark.cc_index_fetch_news import CCIndexFetchNewsJob

def test_query_set_correctly():
    spark_master_url = "k8s://https://10.140.16.25:6443"
    sites = ["cbc.ca"]
    job = CCIndexFetchNewsJob(spark_master_url=spark_master_url, sites=sites)
    assert job.query is not None