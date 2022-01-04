
import pyspark.sql as psql

from seldonite import filters
from seldonite.commoncrawl.sparkcc import CCIndexWarcSparkJob
from seldonite.commoncrawl.fetch_news import FetchNewsJob
from seldonite.helpers import heuristics, utils


class CCIndexFetchNewsJob(CCIndexWarcSparkJob, FetchNewsJob):
    """ News articles from WARC records matching a SQL query
        on the columnar URL index """

    name = "CCIndexFetchNewsJob"

    records_parsing_failed = None
    records_non_html = None
        
    def run(self, keywords=[], sites=[], start_date=None, end_date=None, **kwargs):
        self.set_constraints(keywords, sites, start_date, end_date)
        return super().run(**kwargs)

    def set_query_options(self, sites=[], crawls=[], lang=None, limit=None, path_black_list=[]):
        self.query = utils.construct_query(sites, limit, crawls=crawls, lang=lang, path_black_list=path_black_list)

    def init_accumulators(self):
        super().init_accumulators()

        sc = self.spark_manager.get_spark_context()
        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def log_aggregators(self):
        super().log_aggregators()

        sc = self.spark_manager.get_spark_context()
        self.log_aggregator(sc, self.records_parsing_failed,
                            'records failed to parse = {}')
        self.log_aggregator(sc, self.records_non_html,
                            'records not HTML = {}')


    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return None
        if not self.is_html(record):
            return None

        url = record.rec_headers.get_header('WARC-Target-URI')

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
