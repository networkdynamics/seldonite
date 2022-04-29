from typing import List
import urllib

import pyspark.sql.functions as sfuncs
import pyspark.sql as psql

from seldonite import base
from seldonite.helpers import utils


class Analyze(base.BaseStage):
    input: base.BaseStage

    def __init__(self, input):
        self.input = input
        self._do_articles_over_time = False
        self._do_article_domains = False
        self._do_publish_dates = False

    def _process(self, spark_manager):
        df = self.input._process(spark_manager)

        if self._do_articles_over_time:
            df = self._process_articles_over_time(df)
        elif self._do_article_domains:
            df = self._process_article_domains(df)
        elif self._keywords_over_time_flag:
            df = self._keywords_over_time(df)

        return df

    def articles_over_time(self, period):
        assert period == 'year' or period == 'month'
        self._do_articles_over_time = True
        self.articles_over_time_period = period
        return self

        return self

    def _process_articles_over_time(self, df):

        df = df.withColumn('date_year', sfuncs.year(sfuncs.col("publish_date")).alias("date_year"))

        if self.articles_over_time_period == 'month':
            df = df.withColumn('date_month', sfuncs.month(sfuncs.col("publish_date")).alias("date_month"))

        group_cols = ('date_year', 'date_month') if self.articles_over_time_period == 'month' else ('date_year')
        df = df.groupby(*group_cols).count()

        return df

    def article_domains(self):
        self._do_article_domains = True

        return self

    def _process_article_domains(self, df: psql.DataFrame):
        
        def get_domain(url):
            return urllib.parse.urlparse(url).netloc
        domain_udf = sfuncs.udf(get_domain, StringType())

        df = df.select(domain_udf('url').alias('domain')).distinct()

        return df

    def keywords_over_time(self, keywords: List[str]):
        self._keywords_over_time_flag = True
        self._keywords = keywords

        return self


    def proportion_of_countries(self, df):
        df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

        udfCountry = sfuncs.udf(utils.get_countries, ArrayType(StringType(), True))
        df = df.withColumn('countries', udfCountry(df.all_text))

        key = df.select(sfuncs.explode(df.countries).alias("key"))

        # TODO: what should I return?
        key.groupBy(col("key")).count()
    

    def _keywords_over_time(self, df):

        df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))

        year_date = sfuncs.year(col("date")).alias("date_year")
        month_date = sfuncs.month(col("date")).alias("date_month")
        df = df.withColumn('date_year', lit(year_date))
        df = df.withColumn('date_month', lit(month_date))

        for keyword in self._keywords:
            df = df.withColumn(keyword, sfuncs.col('all_text').like(f"%{keyword}%"))

        df = df.groupby('date_year', 'date_month').count(*self._keywords)

        return df
            
