import os

import pyspark.sql as psql

from seldonite import filters
from seldonite.spark.sparkcc import CCSparkJob
from seldonite.helpers import utils, heuristics


class FetchNewsJob(CCSparkJob):
    """ News articles from from texts in Common Crawl WET files"""

    name = "FetchNewsJob"

    def run(self, listing, limit=None, keywords=[], sites=[], start_date=None, end_date=None, political_filter=False, **kwargs):
        self.set_constraints(keywords, start_date, end_date, political_filter)
        self.limit = limit
        self.sites = sites
        return super().run(listing, **kwargs)

    def set_constraints(self, keywords, start_date, end_date, political_filter):
        self.keywords = keywords
        self.start_date = start_date
        self.end_date = end_date
        self.political_filter = political_filter
        if self.political_filter:
            self.political_filter_path = os.path.join('.', 'pon_classifier')
            self.use_bigdl = True

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return None
        if not self.is_html(record):
            return None

        url = record.rec_headers.get_header('WARC-Target-URI')

        if self.sites and not filters.check_url_from_sites(url, self.sites):
            return None

        if self.url_only:
            return url

        page = record.content_stream().read()

        try:
            article = utils.html_to_article(url, page)
        except Exception as e:
            self.get_logger().error("Error converting HTML to article for {}: {}",
                                    record.rec_headers['WARC-Target-URI'], e)
            self.records_parsing_failed.add(1)
            return None

        if not heuristics.og_type(article):
            return None

        publish_date = article.publish_date.date()
        if (self.start_date and publish_date < self.start_date) or (self.end_date and publish_date > self.end_date):
            return None

        if self.keywords and not filters.contains_keywords(article, self.keywords):
            return None

        return psql.Row(title=article.title, text=article.text, url=url, publish_date=article.publish_date)

    def preprocess_text_to_list(self, texts, **kwargs):
        return (array.tolist() for array in filters.political.preprocess(texts, **kwargs))

    def preprocess_text_partition(self, iter):
        return utils.map_col_with_index(iter, 'url', 'all_text', 'tokens', self.preprocess_text_to_list, tokenizer_path=self.political_filter_path)

    def preprocess_text(self, session, df):
        tokens_rdd = df.rdd \
                       .mapPartitions(self.preprocess_text_partition)
        
        schema = psql.types.StructType([
            psql.types.StructField("url", psql.types.StringType(), True),
            psql.types.StructField("tokens", psql.types.ArrayType(psql.types.IntegerType()), True)
        ])
        return session.createDataFrame(tokens_rdd, schema)

    def process_dataset(self, session, rdd):

        df = rdd.toDF()
        if self.political_filter:
            # create concat of title and text
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))
            # tokenize text
            tokens_df = self.preprocess_text(session, df.select('url', 'all_text'))
            df = df.join(tokens_df, 'url')
            # get political predictions
            df = filters.political.spark_predict(df, 'tokens', 'pred')
            # filter where prediction is higher than threshold
            THRESHOLD = 0.5
            df = df.filter(df.pred > THRESHOLD)

        return df