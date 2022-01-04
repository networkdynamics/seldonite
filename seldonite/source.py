import datetime
import os

from seldonite.commoncrawl.cc_index_fetch_news import CCIndexFetchNewsJob
from seldonite.commoncrawl.fetch_news import FetchNewsJob
from seldonite.commoncrawl.query_index import QueryIndexJob
from seldonite.helpers import utils
from seldonite.model import Article
from seldonite.spark import spark_tools

from googleapiclient.discovery import build as gbuild
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

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

    def fetch(self):
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

    def fetch(self, max_articles, url_only=False):
        # only need to look at crawls that are after the start_date of the search
        if self.start_date is not None:
            self.crawls = utils.get_cc_crawls_since(self.start_date)

        if self.crawls is None:
            raise ValueError('Set crawls either using `set_crawls` or `in_date_range`')

        # create the spark job
        job = CCIndexFetchNewsJob()
        job.set_query_options(sites=self.sites, crawls=self.crawls, lang=self.lang, limit=max_articles, path_black_list=self.path_black_list)
        return job.run(url_only=url_only, keywords=self.keywords, 
                       start_date=self.start_date, end_date=self.end_date)
        

    def query_index(self, spark_master_url, query):
        spark_builder = spark_tools.SparkBuilder(spark_master_url)

        with spark_builder.start_session() as spark_manager:
            job = QueryIndexJob()
            return job.run(query).toPandas()

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


    def fetch(self, max_articles, url_only=False):

        # get wet file listings from common crawl
        listings = utils.get_news_crawl_listing(start_date=self.start_date, end_date=self.end_date)

        # create the spark job
        job = FetchNewsJob()
        return job.run(listings, url_only=url_only, keywords=self.keywords, limit=max_articles, sites=self.sites)

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

    def fetch(self, max_articles, url_only=False):
        raise NotImplementedError()
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

        # TODO add sites to query
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
                    yield Article(link, init=False)
                else:
                    yield utils.link_to_article(link, )

class Eureka(SearchEngineSource):
    def __init__(self, chromedriver, eureka_url, username=None, password=None, show_browser=False, **kwargs):
        super().__init__(**kwargs)

        self.news_only = True

        self.chromedriver = chromedriver
        self.eureka_url = eureka_url

        # only needed if sign in necessary
        self.username = username
        self.password = password

        self.show_browser = show_browser

    def fetch(self, only_url=False):
        raise NotImplementedError()
        if only_url:
            raise ValueError('Eureka is unable to fetch article URLs.')

        chrome_options = None
        if not self.show_browser:
            chrome_options = webdriver.chrome.options.Options()
            chrome_options.headless = True

        # open up Eureka webpage
        with webdriver.Chrome(self.chromedriver, chrome_options=chrome_options) as driver:
            driver.get(self.eureka_url)

            if not driver.find_elements_by_css_selector('#advLink'):

                if self.username is None or self.password is None:
                    raise ValueError('Username and password must be provided.')

                # deal with login page
                username_enter = driver.find_element_by_css_selector("[name=j_username]")
                password_enter = driver.find_element_by_css_selector("[name=j_password]")

                username_enter.send_keys(self.username)
                password_enter.send_keys(self.password)

                # click login
                enter_button = driver.find_element_by_css_selector("[name=_eventId_proceed]")
                enter_button.click()

            # go to advanced search
            delay = 5
            advanced_search_link = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.ID, 'advLink')))
            advanced_search_link.click()
            
            # Find the search box  
            textbox = driver.find_element_by_id("Keywords")
            
            # Create query for search
            #textbox.send_keys('TEXT= "Afghanistan"& TEXT= ("withdrawal"|"withdraw"|"U.S.")')
            textbox.send_keys(f'TEXT= "{" ".join(self.keywords)}"')
        
            # Select English Sources only
            # TODO change this
            select_source = Select(driver.find_element_by_id('CriteriaSet'))
            select_source.select_by_value('147890')
        
            # Select articles from in date range
            select_date_type = Select(driver.find_element_by_id('DateFilter_DateRange'))

            if self.start_date and self.end_date:
                # date range
                select_date_type.select_by_value('10')

                start_day_picker, end_day_picker = [Select(element) for element in driver.find_elements_by_css_selector("#periodRange .day")]
                start_month_picker, end_month_picker = [Select(element) for element in driver.find_elements_by_css_selector("#periodRange .month")]
                start_year_picker, end_year_picker = [Select(element) for element in driver.find_elements_by_css_selector("#periodRange .year")]

                start_day_picker.select_by_value(self.start_date.day)
                end_day_picker.select_by_value(self.end_date.day)

                start_month_picker.select_by_value(self.start_date.month)
                end_month_picker.select_by_value(self.end_date.month)

                start_year_picker.select_by_value(self.start_date.year)
                end_year_picker.select_by_value(self.end_date.year)
            else:
                # in all archives
                select_date_type.select_by_value('9')

            # Complete the search
            textbox.send_keys(Keys.RETURN)

            # Sort search results by relevance
            select_sort = Select(driver.find_element_by_id('ddlSort'))
            select_sort.select_by_value('1')

            # Wait to load page -- adjust as required
            delay = 10
            doc0 = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.ID, 'doc0')))

            # get first document
            doc0 = doc0.find_element_by_css_selector('.docList-links')
            doc0.click()
            article_title = ""

            while True:

                # get article title
                delay = 10

                # janky but it works
                def page_updated(arg):
                    elements = driver.find_elements_by_css_selector('.titreArticleVisu')
                    # check that title exists and that its different from the last title
                    return elements and len(elements[0].text) > 0 and elements[0].text != article_title
                
                WebDriverWait(driver, delay).until(page_updated)
                article_title_element = driver.find_element_by_css_selector('.titreArticleVisu')
                article_title = article_title_element.text

                article = utils.html_to_article(driver.current_url, driver.page_source, title=article_title)

                # TODO convert to spark dataframe
                yield article

                next_doc_button = driver.find_element_by_id('nextDoc')
                if not next_doc_button.is_displayed():
                    break

                next_doc_button.click()

