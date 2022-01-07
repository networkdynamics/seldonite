from typing import List

import pyspark.sql as psql

from seldonite import collect

class Analyze():
    collector: collect.Collector

    def __init__(self, collector):
        self.collector = collector

    def keywords_over_time(self, keywords: List[str]):
        self.collector._check_args()

        spark_builder = self.collector._get_spark_builder()
        with spark_builder.start_session() as spark_manager:
            df = self.collector._fetch(spark_manager)
            
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))
            for keyword in keywords:
                df.withColumn(keyword, df['all_text'].like(f"%{keyword}%"))

            #df.groupby

        return df