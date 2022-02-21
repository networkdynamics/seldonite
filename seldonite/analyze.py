import subprocess
from typing import List
import urllib

from pyspark.sql.functions import *
import pyspark.sql.functions as sfuncs
from pyspark.sql.types import ArrayType, StringType, MapType, IntegerType
import pyspark.sql as psql
import geograpy

from seldonite import collect



class Analyze():
    collector: collect.Collector

    def __init__(self, collector):
        self.collector = collector
        self.do_articles_over_time = False
        self.do_article_domains = False

    def _process(self, spark_manager):
        df = self.collector._process(spark_manager)

        if self.do_articles_over_time:
            df = self._process_articles_over_time(df)
        elif self.do_article_domains:
            df = self._process_article_domains(df)

        return df

    def articles_over_time(self, period):
        assert period == 'year' or period == 'month'
        self.do_articles_over_time = True
        self.articles_over_time_period = period

    def _process_articles_over_time(self, df):

        df = df.withColumn('date_year', sfuncs.year(col("publish_date")).alias("date_year"))

        if self.articles_over_time_period == 'month':
            df = df.withColumn('date_month', sfuncs.month(col("publish_date")).alias("date_month"))

        group_cols = ('date_year', 'date_month') if self.articles_over_time_period == 'month' else ('date_year')
        df = df.groupby(*group_cols).count()

        return df

    def article_domains(self):
        self.do_article_domains = True

    def _process_article_domains(self, df: psql.DataFrame):
        
        def get_domain(url):
            return urllib.parse.urlparse(url).netloc
        domain_udf = udf(get_domain, StringType())

        df = df.select(domain_udf('url').alias('domain')).distinct()

        return df



    def keywords_over_time(self, keywords: List[str]):
        self.keywords_over_time_flag = True
        self.keywords = keywords


    def proportion_of_countries(self, df):
        df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

        def countries(text):
            try:
                countries = geograpy.get_geoPlace_context(text).countries
            except Exception:
                subprocess.call('geograpy-nltk')
                countries = geograpy.get_geoPlace_context(text).countries
                
            return countries if countries else []

        udfCountry = udf(countries, ArrayType(StringType(), True))
        df = df.withColumn('countries', udfCountry(df.all_text))

        
        key = df.select(explode(df.countries).alias("key"))

        # TODO: what should I return?
        key.groupBy(col("key")).count()
    

    def keywords_over_time_delal(self, df):

        df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

        year_date = sfuncs.year(col("date")).alias("date_year")
        month_date = sfuncs.month(col("date")).alias("date_month")
        df = df.withColumn('date_year', lit(year_date))
        df = df.withColumn('date_month', lit(month_date))


        for keyword in keywords:
            df = df.withColumn(keyword, sfuncs.col('all_text').like(f"%{keyword}%"))

        df = df.groupby('date_year', 'date_month').count(*keywords)

        return df
            
