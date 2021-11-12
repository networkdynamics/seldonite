import gzip

from seldonite.model import Article

import requests

def link_to_article(link):

    article = Article(link)
    article.download()
    article.parse()

    return article

def get_crawl_listing(crawl, data_type="wet"):
    url = f"https://commoncrawl.s3.amazonaws.com/crawl-data/{crawl}/{data_type}.paths.gz"
    res = requests.get(url)
    txt_listing = gzip.decompress(res.content).decode("utf-8")
    listing = txt_listing.splitlines()
    return ['s3://commoncrawl/' + entry for entry in listing]

def construct_query(sites, limit):
    #TODO automatically get most recent crawl
    query = "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2021-39' AND subset = 'wet'"
    
    # site restrict
    if not all("." in domain for domain in sites):
        raise ValueError("Sites should be the full registered domain, i.e. cbc.ca instead of just cbc")

    if sites:
        site_list = ', '.join([f"'{site}'" for site in sites])
        query += f" AND url_host_registered_domain IN ({site_list})"

    # set limit to sites if needed
    if limit is not None:
        query += f" LIMIT {str(limit)}"

    return query
