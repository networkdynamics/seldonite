import os

import pyspark.sql as psql
import pyspark.sql.functions as sfuncs

from seldonite import filters
from seldonite.helpers import utils
from seldonite.sources import news
from seldonite.spark import spark_tools


class Collector:
    '''
    Class to search through a source for desired news articles

    Can use a variety of search methods
    '''
    _source: news.BaseSource

    def __init__(self, source):
        self._source = source

        self._keywords = None
        self._url_only_val = False
        self._max_articles = None
        self._political_filter = False
        self._get_distinct_articles = False
        self._get_sample = False
        self._filter_countries = False
        self.sites=[]

    def in_date_range(self, start_date, end_date):
        '''
        Set dates to fetch news from, range is inclusive
        '''
        self._source.set_date_range(start_date, end_date)
        return self

    def by_keywords(self, keywords):
        '''
        Set some keywords to filter by
        '''

        self._keywords = keywords
        self._source.set_keywords(keywords)

        return self

    def only_political_articles(self, threshold=0.5):
        self._political_filter = True
        self._political_filter_threshold = threshold
        return self

    def on_sites(self, sites):
        self._source.set_sites(sites)
        return self

    def limit_num_articles(self, limit):
        self._max_articles = limit
        return self

    def url_only(self, set=True):
        self._url_only_val = set
        return self

    def in_language(self, lang='eng'):
        self._source.set_language(lang)
        return self

    def exclude_in_url(self, url_wildcards):
        self._source.set_url_blacklist(url_wildcards)
        return self

    def distinct(self):
        self._get_distinct_articles = True
        return self

    def sample(self, num_articles):
        self._get_sample = True
        self._num_sample_articles = num_articles
        return self

    def mentions_countries(self, countries=None, min_num_countries=None):
        self._mentions_countries = countries
        self._min_num_countries = min_num_countries
        self._filter_countries = True

    def _set_spark_options(self, spark_builder: spark_tools.SparkBuilder):

        if self._political_filter:
            spark_builder.use_bigdl()

            this_dir_path = os.path.dirname(os.path.abspath(__file__))
            political_classifier_path = os.path.join(this_dir_path, 'filters', 'pon_classifier.zip')
            pol_class_archive = f"{political_classifier_path}#pon_classifier"
            spark_builder.add_archive(pol_class_archive)

        self._source._set_spark_options(spark_builder)

    def _process(self, spark_manager):
        self._check_args()
        df = self._source.fetch(spark_manager, self._max_articles, url_only=self._url_only_val)

        if self._get_distinct_articles:
            df = df.drop_duplicates(['url'])

        if self._filter_countries:
            udfCountry = sfuncs.udf(utils.get_countries, psql.types.ArrayType(psql.types.StringType(), True))
            df = df.withColumn('countries', udfCountry(df.all_text))
            if self._min_num_countries:
                df = df.where(sfuncs.size(sfuncs.col('countries')) >= self._min_num_countries)
            if self._mentions_countries:
                for country in self._mentions_countries:
                    df = df.where(sfuncs.array_contains('countries', country))

            df = df.drop('countries')

        if self._political_filter:

            # create concat of title and text
            df = df.filter(df['title'].isNotNull())
            df = df.filter(df['text'].isNotNull())
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit('. '), df['text']))

            # tokenize text
            spark_session = spark_manager.get_spark_session()
            df = filters.political.preprocess_text(spark_session, df)
            df = df.drop('all_text')

            # drop invalid rows
            df = df.filter(df['tokens'].isNotNull())
            df = df.filter(psql.functions.size('tokens') > 0)

            # increase num partitions ready for predict to avoid errors
            num_partitions = df.rdd.getNumPartitions()
            df = df.repartition(num_partitions * 16)

            # get political predictions
            pred_df = None
            for df_batch in spark_tools.batch(df, max_rows=100000):

                pred_col = 'political_pred'
                df_batch = filters.political.spark_predict(df_batch, 'tokens', pred_col)
                df_batch = df_batch.drop('tokens')

                if pred_df is None:
                    pred_df = df_batch
                else:
                    pred_df = pred_df.union(df_batch)

            df = pred_df

            # filter where prediction is higher than threshold
            df = df.where(f"{pred_col} > {self._political_filter_threshold}")

        if self._get_sample:
            num_rows = df.count()
            df = df.sample(fraction=(self._num_sample_articles + 1) / num_rows).limit(self._num_sample_articles)

        if self._max_articles:
            return df.limit(self._max_articles)
        else:
            return df


    def _check_args(self):
        if self._url_only_val and self._political_filter:
            raise ValueError('Cannot check political articles and get only URLs. Please remove one of the options.')
