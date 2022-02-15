from typing import List

import pyspark.sql as psql

from seldonite import collect

class Analyze():
    collector: collect.Collector

    def __init__(self, collector):
        self.collector = collector


    def process(self, spark_manager):
        df = self.collector.process(spark_manager)

        df = self.collector._fetch(spark_manager)
        if self.keywords_over_time_flag:
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))
            for keyword in keywords:
                df.withColumn(keyword, df['all_text'].like(f"%{keyword}%"))

    def keywords_over_time(self, keywords: List[str]):
        self.keywords_over_time_flag = True
        self.keywords = keywords

        return self