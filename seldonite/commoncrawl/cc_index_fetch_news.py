
from seldonite.commoncrawl.sparkcc import CCIndexWarcSparkJob
from seldonite.commoncrawl.fetch_news import FetchNewsJob
from seldonite.helpers import worker_utils


class CCIndexFetchNewsJob(CCIndexWarcSparkJob, FetchNewsJob):
    """ News articles from WARC records matching a SQL query
        on the columnar URL index """

    name = "CCIndexFetchNewsJob"

    records_parsing_failed = None
    records_non_html = None
        
    def run(self, spark_manager, features=['title', 'text', 'url', 'publish_date'], keywords=[], start_date=None, end_date=None, **kwargs):
        self.set_constraints(keywords, start_date, end_date)
        return super().run(spark_manager, features, **kwargs)

    def set_query_options(self, urls=[], sites=[], crawls=[], lang=None, limit=None, url_black_list=[], start_date=None, end_date=None):

        lang_map = {
            'en': 'eng',
            'fr': 'fra',
            'de': 'deu',
            'es': 'spa',
            'zh': 'zho',
            'it': 'ita',
            'el': 'ell',
            'no': 'nor',
            'sv': 'swe',
            'da': 'dan',
            'pt': 'por',
            'ja': 'jpn',
            'ko': 'kor'
        }

        if lang:
            if lang not in lang_map:
                exception = KeyError("Please add country code mapping for this language")
                if len(lang) == 3:
                    potential_three_lang = lang
                    if potential_three_lang in lang_map.values():
                        three_lang = potential_three_lang
                    else:
                        raise exception
                else:
                    raise exception
            else:
                three_lang = lang_map[lang]
        else:
            three_lang = None

        self.query = worker_utils.construct_query(
            urls, sites, limit, 
            crawls=crawls, lang=three_lang, url_black_list=url_black_list, start_date=start_date, end_date=end_date
        )

    def init_accumulators(self, spark_manager):
        super().init_accumulators(spark_manager)

        sc = spark_manager.get_spark_context()
        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def log_aggregators(self, spark_manager):
        super().log_aggregators(spark_manager)

        self.log_aggregator(spark_manager, self.records_parsing_failed,
                            'records failed to parse = {}')
        self.log_aggregator(spark_manager, self.records_non_html,
                            'records not HTML = {}')


    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return None
        if not self.is_html(record):
            return None

        url = record.rec_headers.get_header('WARC-Target-URI')

        return self._process_record(url, record)
