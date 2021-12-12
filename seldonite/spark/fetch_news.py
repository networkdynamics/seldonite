from seldonite.spark.sparkcc import CCSparkJob
from seldonite.helpers import utils, filter, heuristics


class FetchNewsJob(CCSparkJob):
    """ News articles from from texts in Common Crawl WET files"""

    name = "FetchNewsJob"

    def run(self, listing, url_only=False, limit=None, keywords=[], sites=[], start_date=None, end_date=None):
        self.set_constraints(limit, keywords, sites, start_date, end_date)
        return super().run(url_only=url_only, input_file_listing=listing)

    def set_constraints(self, limit, keywords, sites, start_date, end_date):
        self.limit = limit
        self.keywords = keywords
        self.sites = sites
        self.start_date = start_date
        self.end_date = end_date

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return None
        if not self.is_html(record):
            return None
        
        # TODO filter by language

        url = record.rec_headers.get_header('WARC-Target-URI')

        if self.sites and not filter.check_url_from_sites(url, self.sites):
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

        if self.keywords and not filter.contains_keywords(article, self.keywords):
            return None

        return { "title": article.title, "text": article.text, "url": url, "publish_date": article.publish_date }
