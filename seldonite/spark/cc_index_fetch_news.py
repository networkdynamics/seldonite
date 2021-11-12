from collections import Counter

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from seldonite.spark.sparkcc import CCIndexWarcSparkJob
from seldonite.spark.fetch_news import FetchNewsJob
from seldonite.helpers import utils


class CCIndexFetchNewsJob(CCIndexWarcSparkJob, FetchNewsJob):
    """ Word count (frequency list) from WARC records matching a SQL query
        on the columnar URL index """

    name = "CCIndexWordCount"

    records_parsing_failed = None
    records_non_html = None

    def __init__(self, sites=[], limit=None, **kwargs):
        super().__init__(**kwargs)

        self.query = utils.construct_query(sites, limit)

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

    @staticmethod
    def reduce_by_key_func(a, b):
        # sum values of tuple <term_frequency, document_frequency>
        return ((a[0] + b[0]), (a[1] + b[1]))

    def html_to_text(self, page, record):
        try:
            encoding = record.rec_headers['WARC-Identified-Content-Charset']
            if not encoding:
                for encoding in EncodingDetector(page, is_html=True).encodings:
                    # take the first detected encoding
                    break
            soup = BeautifulSoup(page, 'lxml', from_encoding=encoding)
            for script in soup(['script', 'style']):
                script.extract()
            return soup.get_text(' ', strip=True)
        except Exception as e:
            self.get_logger().error("Error converting HTML to text for {}: {}",
                                    record.rec_headers['WARC-Target-URI'], e)
            self.records_parsing_failed.add(1)
            return ''

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            self.records_non_html.add(1)
            return
        page = record.content_stream().read()
        text = self.html_to_text(page, record)

        return utils.html_to_article("", text)
