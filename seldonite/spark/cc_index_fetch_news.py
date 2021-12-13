
from seldonite.spark.sparkcc import CCIndexWarcSparkJob
from seldonite.spark.fetch_news import FetchNewsJob
from seldonite.helpers import utils


class CCIndexFetchNewsJob(CCIndexWarcSparkJob, FetchNewsJob):
    """ News articles from WARC records matching a SQL query
        on the columnar URL index """

    name = "CCIndexFetchNewsJob"

    records_parsing_failed = None
    records_non_html = None
        
    def run(self, url_only=False, limit=None, keywords=[], sites=[], crawls=None, start_date=None, end_date=None):
        query = utils.construct_query(sites, limit, crawls=crawls)
        self.set_constraints(limit, keywords, sites, start_date, end_date)
        return super().run(query, url_only)

    def init_accumulators(self, sc):
        super().init_accumulators(sc)

        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def log_aggregators(self, sc):
        super().log_aggregators(sc)

        self.log_aggregator(sc, self.records_parsing_failed,
                            'records failed to parse = {}')
        self.log_aggregator(sc, self.records_non_html,
                            'records not HTML = {}')


    
