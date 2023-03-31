from newspaper import Article, ArticleException

def link_to_article(link):
    article = Article(link)
    article.download()

    try:
        article.parse()
    except ArticleException as e:
        raise ValueError(str(e))

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

def construct_query(urls, sites, limit, crawls=None, lang='eng', url_black_list=[]):
    #TODO automatically get most recent crawl
    query = "SELECT url, url_path, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE subset = 'warc'"

    if crawls:
        # 
        if crawls == 'all':
            pass
        elif len(crawls) == 1:
            query += f" AND crawl = '{crawls[0]}'"
        else:
            crawl_list = ', '.join([f"'{crawl}'" for crawl in crawls])
            query += f" AND crawl IN ({crawl_list})"

    # site restrict
    if not all("." in domain for domain in sites):
        raise ValueError("Sites should be the full registered domain, i.e. cbc.ca instead of just cbc")

    if sites:
        site_list = ', '.join([f"'{site}'" for site in sites])
        query += f" AND url_host_registered_domain IN ({site_list})"

    if urls:
        url_list = ', '.join([f"'{url}'" for url in urls])
        query += f" AND url IN ({url_list})"

    # Language filter
    if lang:
        query += f" AND (content_languages IS NULL OR (content_languages IS NOT NULL AND content_languages = '{lang}'))"

    # url must have a path longer than /, otherwise its probably not an article
    query += " AND LENGTH(url_path) > 1"

    if url_black_list:
        # replace wildcards with %
        url_black_list = [url_wildcard.replace('*', '%') for url_wildcard in url_black_list]
        clause = " OR ".join((f"url_path LIKE '{url_wildcard}'" for url_wildcard in url_black_list))
        query += f" AND NOT ({clause})"

    # set limit to sites if needed
    if limit:
        query += f" LIMIT {str(limit)}"

    return query