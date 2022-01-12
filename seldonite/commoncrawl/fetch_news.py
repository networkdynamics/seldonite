import pyspark.sql as psql

from seldonite import filters
from seldonite.commoncrawl.sparkcc import CCSparkJob
from seldonite.helpers import utils, heuristics


class FetchNewsJob(CCSparkJob):
    """ News articles from from texts in Common Crawl WET files"""

    name = "FetchNewsJob"

    def run(self, spark_manager, listing, limit=None, keywords=[], sites=[], start_date=None, end_date=None, **kwargs):
        self.set_constraints(keywords, start_date, end_date)
        self.limit = limit
        self.sites = sites
        return super().run(spark_manager, listing, **kwargs)

    def set_constraints(self, keywords, start_date, end_date):
        self.keywords = keywords
        self.start_date = start_date
        self.end_date = end_date

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

        return self._process_record(url, record)

    def _process_record(self, url, record):
        page = record.content_stream().read()

        try:
            article = utils.html_to_article(url, page)
        except Exception as e:
            self.get_logger().error("Error converting HTML to article for {}: {}",
                                    record.rec_headers['WARC-Target-URI'], e)
            self.records_parsing_failed.add(1)
            return None

        if (not article.title) or (not article.text) or (not article.publish_date):
            return None

        if not heuristics.og_type(article):
            return None

        if self.start_date or self.end_date:
            publish_date = article.publish_date.date()
            if (self.start_date and publish_date < self.start_date) or (self.end_date and publish_date > self.end_date):
                return None

        if self.keywords and not filters.contains_keywords(article, self.keywords):
            return None

        return psql.Row(title=article.title, text=article.text, url=url, publish_date=article.publish_date)

    