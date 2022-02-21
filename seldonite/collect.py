import os

from gensim import models, corpora
import pyspark.sql as psql

from seldonite import filters, sources
from seldonite.helpers import preprocess
from seldonite.spark import spark_tools


class Collector:
    '''
    Class to search through a source for desired news articles

    Can use a variety of search methods
    '''
    source: sources.BaseSource

    def __init__(self, source, master_url=None, num_executors=1, executor_cores=16, executor_memory='160g', spark_conf={}):
        self.source = source
        self.spark_master_url = master_url
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.spark_conf = spark_conf

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

    # TODO split arguments into methods
    def fetch(self):
        '''
        :return: Pandas dataframe
        '''
        self._check_args()

        spark_builder = self._get_spark_builder()
        with spark_builder.start_session() as spark_manager:
            df = self._fetch(spark_manager)
            df = df.toPandas()

        return df

    def _get_spark_builder(self):
        use_bigdl = self.political_filter
        if self.political_filter:
            this_dir_path = os.path.dirname(os.path.abspath(__file__))
            political_classifier_path = os.path.join(this_dir_path, 'filters', 'pon_classifier.zip')
            archives = [ political_classifier_path ]
        else:
            archives = []

        source_spark_conf = self.source.get_source_spark_conf()
        for source_conf_key in source_spark_conf:
            if source_conf_key not in self.spark_conf:
                self.spark_conf[source_conf_key] = source_spark_conf[source_conf_key]
            else:
                raise NotImplementedError('This behaviour needs to be worked out')

        spark_builder = spark_tools.SparkBuilder(self.spark_master_url, use_bigdl=use_bigdl, archives=archives,
                                                 executor_cores=self.executor_cores, executor_memory=self.executor_memory, num_executors=self.num_executors,
                                                 spark_conf=self.spark_conf)

        return spark_builder

    def _fetch(self, spark_manager: spark_tools.SparkManager):
        df  = self.source.fetch(spark_manager, self.max_articles, url_only=self.url_only_val)

        if self.political_filter:

            # create concat of title and text
            df = df.filter(df['title'].isNotNull())
            df = df.filter(df['text'].isNotNull())
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit('. '), df['text']))

            # tokenize text
            spark_session = spark_manager.get_spark_session()
            tokens_df = filters.political.preprocess_text(spark_session, df.select('url', 'all_text'))
            df = df.join(tokens_df, 'url').drop('all_text')

            # drop invalid rows
            df = df.filter(df['tokens'].isNotNull())
            df = df.filter(psql.functions.size('tokens') > 0)

            # increase num partitions ready for predict to avoid errors
            num_partitions = df.rdd.getNumPartitions()
            df = df.repartition(num_partitions * 16)

            # get political predictions
            num_partitions = max(1, int(df.count() / 100000))

            df = df.withColumn('_row_id', psql.functions.monotonically_increasing_id())
            # Using ntile() because monotonically_increasing_id is discontinuous across partitions
            df = df.withColumn('_partition', psql.functions.ntile(num_partitions).over(psql.window.Window.orderBy(df._row_id))) 

            pred_df = None
            for i in range(num_partitions):
                df_batch = df.filter(df._partition == i+1).drop('_row_id', '_partition')

                pred_col = 'political_pred'
                df_batch = filters.political.spark_predict(df_batch, 'tokens', pred_col)
                df_batch = df_batch.drop('tokens')

                if pred_df is None:
                    pred_df = df_batch
                else:
                    pred_df = pred_df.union(df_batch)

            df = pred_df

            # filter where prediction is higher than threshold
            df = df.where(f"{pred_col} > {self.political_filter_threshold}")

        if self.max_articles:
            return df.limit(self.max_articles)
        else:
            return df


    def send_to_database(self, connection_string, database, table):
        self._check_args()
        spark_builder = self._get_spark_builder()
        spark_builder.set_output_database(connection_string)
        with spark_builder.start_session() as spark_manager:
            df = self._fetch(spark_manager)
            df.write \
                .format("mongo") \
                .mode("append") \
                .option("database", database) \
                .option("collection", table) \
                .save()

    def _check_args(self):
        if self.url_only_val and self.political_filter:
            raise ValueError('Cannot check political articles and get only URLs. Please remove one of the options.')

    def find_topics(self, batch_size=1000):
        articles = self.fetch()
        prepro = preprocess.Preprocessor()

        more_articles = True
        model = None
        dictionary = None
        while more_articles:
            batch_idx = 0
            content_batch = []

            while batch_idx < batch_size:
                try:
                    article = next(articles)
                    content_batch.append(article.text)
                    batch_idx += 1
                except StopIteration:
                    more_articles = False
                    break

            # TODO add bigrams
            docs = list(prepro.preprocess(content_batch))

            if not dictionary:
                # TODO consider using hashdictionary
                dictionary = corpora.Dictionary(docs)

                no_below = max(1, batch_size // 100)
                dictionary.filter_extremes(no_below=no_below, no_above=0.9)
            
            corpus = [dictionary.doc2bow(doc) for doc in docs]

            if not model:
                # need to 'load' the dictionary
                dictionary[0]
                # TODO use ldamulticore for speed
                model = models.LdaModel(corpus, 
                                        id2word=dictionary.id2token, 
                                        num_topics=10)
            else:
                model.update(corpus)

        return model, dictionary
