import datetime
import gzip
import re

from seldonite.model import Article

import requests

def link_to_article(link):
    article = Article(link)
    article.download()
    article.parse()

    return article

def html_to_article(url, html, title=None):
    article = Article(url)
    article.download(input_html=html)
    article.parse()

    if title is not None:
        article.set_title(title)

    return article

def dict_to_article(dict):
    article = Article(dict['url'])
    article.set_title(dict['title'])
    article.set_text(dict['text'])
    article.publish_date = dict['publish_date']
    return article

def get_crawl_listing(crawl, data_type="wet"):
    url = f"https://commoncrawl.s3.amazonaws.com/crawl-data/{crawl}/{data_type}.paths.gz"
    res = requests.get(url)
    txt_listing = gzip.decompress(res.content).decode("utf-8")
    listing = txt_listing.splitlines()
    return ['s3://commoncrawl/' + entry for entry in listing]

def most_recent_cc_crawl():
    url = 'https://index.commoncrawl.org/collinfo.json'
    res = requests.get(url)
    crawls = res.json()
    return crawls[0]['id']

def get_cc_crawls_since(date):
    url = 'https://index.commoncrawl.org/collinfo.json'
    res = requests.get(url)
    crawls = res.json()

    year_regex = r'[0-9]{4}'
    month_regex = r'January|February|March|April|May|June|July|August|September|October|November|December'
    crawl_ids = []
    for crawl in crawls:
        crawl_years = [int(year) for year in re.findall(year_regex, crawl['name'])]
        crawl_year = min(crawl_years)
        if crawl_year > date.year:
            crawl_ids.append(crawl['id'])
        elif crawl_year == date.year:
            crawl_month_match = re.search(month_regex, crawl['name'])
            if not crawl_month_match:
                continue

            crawl_month = crawl_month_match.group()
            crawl_month_date = datetime.datetime.strptime(crawl_month, '%B')
            crawl_month_num = crawl_month_date.month
            if crawl_month_num > date.month:
                crawl_ids.append(crawl['id'])

    return crawl_ids

def construct_query(sites, limit, crawl=None, type=None):
    #TODO automatically get most recent crawl
    query = "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE subset = 'warc'"

    if crawl:
        # 
        query += f" AND crawl = '{crawl}'"

    # site restrict
    if not all("." in domain for domain in sites):
        raise ValueError("Sites should be the full registered domain, i.e. cbc.ca instead of just cbc")

    if sites:
        site_list = ', '.join([f"'{site}'" for site in sites])
        query += f" AND url_host_registered_domain IN ({site_list})"

    # set limit to sites if needed
    if limit:
        query += f" LIMIT {str(limit)}"

    return query
