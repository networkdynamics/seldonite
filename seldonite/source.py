import datetime

from seldonite.commoncrawl.cc_index_fetch_news import CCIndexFetchNewsJob
from seldonite.commoncrawl.fetch_news import FetchNewsJob
from seldonite.commoncrawl.sparkcc import CCIndexSparkJob
from seldonite.helpers import utils
from seldonite.model import Article
from seldonite.spark import spark_tools

from googleapiclient.discovery import build as gbuild
import pandas as pd

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

        self.start_date = None
        self.end_date = None

        self.can_lang_filter = True
        self.can_path_black_list = True

    def set_date_range(self, start_date, end_date, strict=True):
        '''
        params:
        start_date: (if None, any date is OK as start date), as date
        end_date: (if None, any date is OK as end date), as date
        strict: if date filtering is strict and the date of an article could not be detected, the article will be discarded
        '''
        self.start_date = start_date
        self.end_date = end_date
        self.strict = strict

    def set_language(self, language):
        if self.can_lang_filter:
            self.lang = language
        else:
            raise NotImplementedError('This source cannot filter language, please try another source.')

    def set_path_blacklist(self, path_black_list):
        if self.can_path_black_list:
            self.path_black_list = path_black_list
        else:
            raise NotImplementedError('This source cannot blacklist paths, please try another source.')

    def set_keywords(self, keywords):
        if self.can_keyword_filter:
            self.keywords = keywords
        else:
            raise NotImplemented('This source cannot filter by keywords, please try another source.')

    def set_sites(self, sites):
        self.sites = sites

    def fetch(self, *args, **kwargs):
        raise NotImplementedError()

class WebWideSource(Source):
    '''
    Parent class for web wide sources
    '''

    def __init__(self):
        super().__init__()

        self.keywords = []

    def set_keywords(self, keywords=[]):
        self.keywords = keywords

class CommonCrawl(WebWideSource):
    '''
    Source that uses Spark to search CommonCrawl
    '''

    def __init__(self):
        '''
        params:
        '''
        super().__init__()

        self.can_keyword_filter = True
        # we apply newsplease heuristics in spark job
        self.news_only = True
        self.can_lang_filter = True
        self.lang = None
        self.can_path_black_list = True
        self.path_black_list = []

    def set_crawls(self, crawl):
        if crawl == 'latest':
            self.crawls = [ utils.most_recent_cc_crawl() ]
        elif crawl == 'all':
            self.crawls = utils.get_all_cc_crawls()
        else:
            self.crawls = [ crawl ]

    def fetch(self, spark_manager, max_articles, url_only=False):
        # only need to look at crawls that are after the start_date of the search
        if self.start_date is not None:
            self.crawls = utils.get_cc_crawls_since(self.start_date)

        if self.crawls is None:
            raise ValueError('Set crawls either using `set_crawls` or `in_date_range`')

        # create the spark job
        job = CCIndexFetchNewsJob()
        job.set_query_options(sites=self.sites, crawls=self.crawls, lang=self.lang, limit=max_articles, path_black_list=self.path_black_list)
        return job.run(spark_manager, url_only=url_only, keywords=self.keywords, 
                       start_date=self.start_date, end_date=self.end_date)
        

    def query_index(self, spark_master_url, query):
        spark_builder = spark_tools.SparkBuilder(spark_master_url)

        with spark_builder.start_session() as spark_manager:
            job = CCIndexSparkJob()
            return job.run(spark_manager, query).toPandas()

class NewsCrawl(WebWideSource):
    '''
    Source that uses Spark to search CommonCrawl's NewsCrawl dataset
    '''

    def __init__(self):
        '''
        params:
        '''
        super().__init__()
        self.can_keyword_filter = True

        # we apply newsplease heuristics in spark job
        self.news_only = True
        self.can_political_filter = True
        self.political_filter = False


    def fetch(self, spark_manager, max_articles, url_only=False):

        # get wet file listings from common crawl
        listings = utils.get_news_crawl_listing(start_date=self.start_date, end_date=self.end_date)

        # create the spark job
        job = FetchNewsJob()
        return job.run(spark_manager, listings, url_only=url_only, keywords=self.keywords, limit=max_articles, sites=self.sites)

class SearchEngineSource(WebWideSource):

    # TODO this is incorrect syntax for param expansion, fix
    def __init__(self):
        super().__init__()

        self.can_keyword_filter = True

class Google(SearchEngineSource):
    '''
    Source that uses Google's Custom Search JSON API
    '''

    def __init__(self, dev_key, engine_id):
        super().__init__()

        self.dev_key = dev_key
        self.engine_id = engine_id

    def fetch(self, spark_manager, max_articles, url_only=False):

        service = gbuild("customsearch", "v1",
            developerKey=self.dev_key)

        # construct keywords into query
        query = ' '.join(self.keywords)

        # add sites
        if self.sites:
            query += " " + " OR ".join([f"site:{site}" for site in self.sites])

        if self.start_date:
            pre_start_date = self.start_date - datetime.timedelta(days=1)
            query += f" after:{pre_start_date.strftime('%Y-%m-%d')}"

        if self.end_date:
            post_end_date = self.end_date + datetime.timedelta(days=1)
            query += f" before:{post_end_date.strftime('%Y-%m-%d')}"

        # using siterestrict allows more than 10000 calls per day
        # note both methods still require payment for more than 100 requests a day
        # if sites:
        #     method = service.cse()
        # else:
        #     method = service.cse().siterestrict()
        method = service.cse()

        # google custom search returns max of 100 results
        # each page contains max 10 results

        num_pages = max_articles // 10

        if url_only:
            df = pd.DataFrame(columns=['url'])
        else:
            df = pd.DataFrame(columns=['title', 'text', 'url', 'publish_date'])

        for page_num in range(num_pages):
            results = method.list(
                q=query,
                cx=self.engine_id,
                start=str((page_num * 10) + 1)
            ).execute()

            if int(results['searchInformation']['totalResults']) == 0:
                raise ValueError("Query to Google Search produced no results.")

            items = results['items']

            for item in items:
                link = item['link']
                # TODO convert for spark
                if url_only:
                    df.append({'url': link}, ignore_index=True)
                else:
                    article = utils.link_to_article(link)
                    df.append({ 'text': article.text, 'title': article.title, 'url': link, 'publish_date': article.publish_date }, ignore_index=True)

        spark_session = spark_manager.get_spark_session()
        return spark_session.createDataFrame(df)
