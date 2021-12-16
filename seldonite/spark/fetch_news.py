import pyspark.sql as psql

from seldonite import filters
from seldonite.spark.sparkcc import CCSparkJob
from seldonite.helpers import utils, heuristics


class FetchNewsJob(CCSparkJob):
    """ News articles from from texts in Common Crawl WET files"""

    name = "FetchNewsJob"

    def run(self, listing, limit=None, keywords=[], sites=[], start_date=None, end_date=None, political_filter=False, **kwargs):
        self.set_constraints(limit, keywords, sites, start_date, end_date, political_filter)
        return super().run(listing, **kwargs)

    def set_constraints(self, limit, keywords, sites, start_date, end_date, political_filter):
        self.limit = limit
        self.keywords = keywords
        self.sites = sites
        self.start_date = start_date
        self.end_date = end_date
        self.political_filter = political_filter
        if self.political_filter:
            self.use_orca = True

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return None
        if not self.is_html(record):
            return None
        
        # TODO filter by language

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

        if (self.start_date and article.publish_date < self.start_date) or (self.end_date and article.publish_date > self.end_date):
            return None

        if self.keywords and not filters.contains_keywords(article, self.keywords):
            return None

        return psql.Row(title=article.title, text=article.text, url=url, publish_date=article.publish_date)


    def process_dataset(self, rdd):

        df = rdd.toDF()
        if self.political_filter:
            # create concat of title and text
            df = df.withColumn('all_text', psql.functions.concat(psql.functions.col('title'), psql.functions.lit(' '), psql.functions.col('text')))
            # tokenize text
            tokens_df = df.select('all_text').rdd.mapPartitions(filters.political.preprocess)
            df = df.withColumn('tokens', tokens_df)
            # get political predictions
            preds = filters.political.spark_predict(df.select('tokens'))
            df = df.withColumn('prediction', preds)
            # filter where prediction is higher than threshold
            THRESHOLD = 0.5
            df = df.filter(df.prediction > THRESHOLD)

        rdd = df.rdd.map(lambda row: {'title': row['title'], 'text': row['text'], 'url': row['url'], 'publish_date': row['publish_date']})

        if self.limit:
            return rdd.take(self.limit)
        else:
            return rdd.collect()