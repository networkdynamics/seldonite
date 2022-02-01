import os

import pyspark.sql as psql

from seldonite import filters, sources
from seldonite.spark import spark_tools


class Collector:
    '''
    Class to search through a source for desired news articles

    Can use a variety of search methods
    '''
    source: sources.BaseSource

    def __init__(self, source):
        self.source = source

        self.keywords = None
        self.url_only_val = False
        self.max_articles = None
        self.political_filter = False
        self.sites=[]

    def in_date_range(self, start_date, end_date):
        '''
        Set dates to fetch news from, range is inclusive
        '''
        self.source.set_date_range(start_date, end_date)
        return self

    def by_keywords(self, keywords):
        '''
        Set some keywords to filter by
        '''

        self.keywords = keywords
        self.source.set_keywords(keywords)

        return self

    def only_political_articles(self, threshold=0.5):
        self.political_filter = True
        self.political_filter_threshold = threshold
        return self

    def on_sites(self, sites):
        self.source.set_sites(sites)
        return self

    def limit_num_articles(self, limit):
        self.max_articles = limit
        return self

    def url_only(self, set=True):
        self.url_only_val = set
        return self

    def in_language(self, lang='eng'):
        self.source.set_language(lang)
        return self

    def exclude_in_url(self, url_wildcards):
        self.source.set_url_blacklist(url_wildcards)
        return self


    def _set_spark_options(self, spark_builder: spark_tools.SparkBuilder):

        if self.political_filter:
            spark_builder.use_bigdl()

            this_dir_path = os.path.dirname(os.path.abspath(__file__))
            political_classifier_path = os.path.join(this_dir_path, 'filters', 'pon_classifier.zip')
            pol_class_archive = f"{political_classifier_path}#pon_classifier"
            spark_builder.add_archive(pol_class_archive)

        self.source._set_spark_options(spark_builder)

    def process(self, spark_manager):
        self._check_args()
        df  = self.source.fetch(spark_manager, self.max_articles, url_only=self.url_only_val)
        spark_session = spark_manager.get_spark_session()
        if self.political_filter:
            # create concat of title and text
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))
            # tokenize text
            tokens_df = filters.political.preprocess_text(spark_session, df.select('url', 'all_text'))
            df = df.join(tokens_df, 'url').drop('all_text')

            # drop invalid rows
            df = df.filter(df['tokens'].isNotNull())
            df = df.filter(psql.functions.size('tokens') > 0)

            # get political predictions
            pred_col = 'political_pred'
            df = filters.political.spark_predict(df, 'tokens', pred_col)
            df = df.drop('tokens')
            # filter where prediction is higher than threshold
            df = df.where(f"{pred_col} > {self.political_filter_threshold}")

        if self.max_articles:
            return df.limit(self.max_articles)
        else:
            return df


    def _check_args(self):
        if self.url_only_val and self.political_filter:
            raise ValueError('Cannot check political articles and get only URLs. Please remove one of the options.')
