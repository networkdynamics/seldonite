from typing import List
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StringType, MapType, IntegerType

import pyspark.sql as psql

from seldonite import collect

from geotext import GeoText
from geopy import geocoders

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


        def proportion_of_countries(self):
            self.collector._check_args()

            spark_builder = self.collector._get_spark_builder()

            with spark_builder.start_session() as spark_manager:
                df = self.collector._fetch(spark_manager)

                df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

                def countries(x):
                    country_list = GeoText(x).countries
                    liste = []
                    if not country_list:
                        return []
                    else:
                        for country in country_list:
                            liste.append(country)
                        return liste

                udfCountry = udf(countries, ArrayType(StringType(), True))
                df = df.withColumn('countries', udfCountry(df.all_text))

                
                key = df.select(explode(df.countries).alias("key"))
                df_with_key = df.withColumn("key", lit(key))

                # TODO: what should I return?
                key.groupBy(col("key")).count().show()
        

        def keywords_over_time_delal(self, year):
            self.collector._check_args()

            spark_builder = self.collector._get_spark_builder()

            with spark_builder.start_session() as spark_manager:
                df = self.collector._fetch(spark_manager)

                df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

                year_date = year(col("date")).alias("date_year")
                month_date = month(col("date")).alias("date_month")
                sparkDF = sparkDF.withColumn('date_year', lit(year_date))
                sparkDF = sparkDF.withColumn('date_month', lit(month_date))

                d = {}

                for i in keywords:
                    d[i] = 0

                def count_keywords(x):
                    # key events
                    keywords = ["Trump"]
                    
                    for event in keywords:
                        if event in x:
                            
                            d[event] += 1
                    return d

                udfKeywords = udf(count_keywords, MapType(StringType(), IntegerType()))
                df = sparkDF.withColumn('counts', udfKeywords(sparkDF.all_text))

                # TODO: what should I return?
                