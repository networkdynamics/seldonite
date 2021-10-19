from seldonite.helpers import heuristics, utils
from seldonite.spark.cc_index_fetch_news import CCIndexFetchNewsJob

from googleapiclient.discovery import build as gbuild

# TODO make abstract
class Source:
    '''
    Base class for a source

    A source can be anything from a search engine, to an API, to a dataset
    '''

    # TODO make abstract
    def __init__(self):
        # flag to show this source returns in a timely fashion without callbacks, unless overriden
        self.uses_callback = False
        self.can_keyword_filter = False
        # we need to filter for only news articles by default
        self.news_only = False

    def set_date_range(self, start_date, end_date, strict=True):
        '''
        params:
        start_date: (if None, any date is OK as start date), as datetime
        end_date: (if None, any date is OK as end date), as datetime
        strict: if date filtering is strict and the date of an article could not be detected, the article will be discarded
        '''
        self.start_date = start_date
        self.end_date = end_date
        self.strict = strict

    def fetch(self):
        articles = self._fetch()

        for article in articles:
            if self.news_only:
                yield article
            # apply newsplease heuristics to get only articles
            else:
                if heuristics.og_type(article):
                    yield article

    def _fetch(self):
        raise NotImplementedError()

class WebWideSource(Source):
    '''
    Parent class for web wide sources
    '''

    def __init__(self, hosts=[]):
        '''
        params:
        hosts: If None or empty list, any host is OK. Example: ['cbc.ca']
        '''
        super().__init__()

        self.hosts = hosts
        self.keywords = []

    def set_keywords(self, keywords=[]):
        self.keywords = keywords

class CommonCrawl(WebWideSource):
    '''
    Source that uses Spark to search CommonCrawl
    '''

    def __init__(self, ip='localhost', port=8080, hosts=[]):
        '''
        params:
        '''
        super().__init__(hosts)

        self.spark_master_url = f"{ip}:{port}"
        self.can_keyword_filter = True
        self.crawl_version = "CC-MAIN-2017-13"

    def _fetch(self):

        # get wet file listings from common crawl
        listing = utils.get_crawl_listing(self.crawl_version)

        # create the spark job
        job = CCIndexFetchNewsJob(spark_master_url=self.spark_master_url)
        job.run()

class SearchEngineSource(WebWideSource):

    # TODO this is incorrect syntax for param expansion, fix
    def __init__(self, hosts):
        super().__init__(hosts)

        self.can_keyword_filter = True

        

class Google(SearchEngineSource):
    '''
    Source that uses Google's Custom Search JSON API
    '''

    def __init__(self, dev_key, engine_id, hosts=[], max_requests=10):
        super().__init__(hosts)

        self.dev_key = dev_key
        self.engine_id = engine_id
        self.max_requests = max_requests

    def _fetch(self):

        service = gbuild("customsearch", "v1",
            developerKey=self.dev_key)

        # construct keywords into query
        query = ' '.join(self.keywords)

        # using siterestrict allows more than 10000 calls per day
        # note both methods still require payment for more than 100 requests a day
        if self.hosts:
            method = service.cse()
        else:
            method = service.cse().siterestrict()

        # google custom search returns max of 100 results
        # each page contains max 10 results

        num_pages = self.max_requests

        # TODO add hosts to query
        for page_num in range(num_pages):
            results = method.list(
                q=query,
                cx=self.engine_id,
                start=str((page_num * 10) + 1)
            ).execute()

            items = results['items']

            for item in items:
                link = item['link']
                yield utils.link_to_article(link)


class Bing(SearchEngineSource):
    def __init__(self):
        raise NotImplementedError()