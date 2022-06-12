import os
import sys

import pandas as pd
import pyspark.sql as psql

from seldonite import embed

def test_accumulate_embedding():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    #Create PySpark SparkSession
    master = 'local[1]'
    spark = psql.SparkSession.builder \
        .master(master) \
        .appName("test_accumulate_embedding") \
        .getOrCreate()
    #Create PySpark DataFrame from Pandas

    article_data = {
        'id': [1, 1, 2, 2, 3, 3, 4, 4],
        'token': ['c', 'c', 'c', 'd', 'd', 'c', 'd', 'd']
    }
    article_p_df = pd.DataFrame(article_data)
    article_df = spark.createDataFrame(article_p_df)

    embeddings = {'c': [0,2], 'd': [1,0]}
    embedding_data = {
        'token': list(embeddings.keys()),
        'token_embedding': list(embeddings.values())
    }
    embedding_p_df = pd.DataFrame(embedding_data)
    embedding_df = spark.createDataFrame(embedding_p_df)

    article_embeddings_df = embed.accumulate_embeddings(article_df, embedding_df, 2)

    article_embeddings = article_embeddings_df.collect()

    for article in article_embeddings:
        assert article['embedding'] == [x + y for x,y in zip(embeddings[article['a']], embeddings[article['b']])]

