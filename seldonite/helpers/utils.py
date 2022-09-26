import collections
import datetime
import gzip
import re
import subprocess
import zipfile

import botocore
import boto3
import nltk
import pyspark.sql as psql
import requests
import pyspark.ml as sparkml
import sparknlp



def get_crawl_listing(crawl, data_type="wet"):
    url = f"https://commoncrawl.s3.amazonaws.com/crawl-data/{crawl}/{data_type}.paths.gz"
    res = requests.get(url)
    txt_listing = gzip.decompress(res.content).decode("utf-8")
    listing = txt_listing.splitlines()
    return ['s3://commoncrawl/' + entry for entry in listing]

def get_news_crawl_listing(start_date=None, end_date=None):
    no_sign_request = botocore.client.Config(
        signature_version=botocore.UNSIGNED)
    s3client = boto3.client('s3', config=no_sign_request)
    s3_paginator = s3client.get_paginator('list_objects_v2')

    def keys(bucket_name, prefix='/', delimiter='/', start_after=''):
        prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
        start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
        for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
            for content in page.get('Contents', ()):
                yield content['Key']

    warc_paths = []
    if not start_date and not end_date:
        for key in keys('commoncrawl', prefix='/crawl-data/CC-NEWS/'):
            warc_paths.append(key)

    else:
        if not end_date:
            end_date = datetime.date.today()
        elif not start_date:
            start_date = datetime.date.min
        
        delta = end_date - start_date

        # pad ending files to account for time that pages spend in sitemap and rss feed
        # normally roughly 30 days
        sitemap_pad = 30

        days = []
        for i in range(delta.days + 1 + sitemap_pad):
            days.append(start_date + datetime.timedelta(days=i))

        for day in days:
            date_path = day.strftime('%Y/%m/CC-NEWS-%Y%m%d')
            for key in keys('commoncrawl', prefix=f'/crawl-data/CC-NEWS/{date_path}'):
                    warc_paths.append(key)

    return [f's3://commoncrawl/{path}' for path in warc_paths]

def get_all_cc_crawls():
    url = 'https://index.commoncrawl.org/collinfo.json'
    res = requests.get(url)
    crawls = res.json()
    return [crawl['id'] for crawl in crawls]

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


def map_col_with_index(iter, index_name, col_name, mapped_name, func, **kwargs):
    index = []
    col = []
    for item in iter:
        index.append(item[index_name])
        col.append(item[col_name])
    mapped_col = func(col, **kwargs)
    for idx, mapped_item in zip(index, mapped_col):
        row_values = collections.OrderedDict()
        row_values[index_name] = idx
        row_values[mapped_name] = mapped_item
        yield psql.Row(**row_values)


def unzip(from_zip, to_path):
    with zipfile.ZipFile(from_zip, 'r') as zip_ref:
        zip_ref.extractall(to_path)


def construct_db_uri(connection_string, database, collection):
    if '?' in connection_string:
        url_path, query_string = connection_string.split('?')
        query_string = f"?{query_string}"
    else:
        url_path = connection_string
        query_string = ''

    if url_path.count('/') > 2:
        url_path = '/'.join(url_path.split('/')[:-1])

    return f"{url_path}/{database}.{collection}{query_string}"

def get_countries(text):
    geotext = GeoText()
    places = geotext.extract(input_text=text)
        
    return list(places['countries'].keys())


def tokenize(df):

    df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit('. '), df['text']))

    try:
        eng_stopwords = nltk.corpus.stopwords.words('english')
    except LookupError as e:
        nltk.download('stopwords')
        eng_stopwords = nltk.corpus.stopwords.words('english')

    stages = []
    cols_to_drop = []

    text_col = 'all_text'
    doc_out_col = f"{text_col}_document"
    document_assembler = sparknlp.base.DocumentAssembler() \
        .setInputCol(text_col) \
        .setOutputCol(doc_out_col)
        
    tokenizer_out_col = f"{text_col}_token"
    tokenizer = sparknlp.annotator.Tokenizer() \
        .setInputCols(doc_out_col) \
        .setOutputCol(tokenizer_out_col)
        
    # note normalizer defaults to changing all words to lowercase.
    # Use .setLowercase(False) to maintain input case.
    normalizer_out_col = f"{text_col}_normalized"
    normalizer = sparknlp.annotator.Normalizer() \
        .setInputCols(tokenizer_out_col) \
        .setOutputCol(normalizer_out_col) \
        .setLowercase(True)
        
    # note that lemmatizer needs a dictionary. So I used the pre-trained
    # model (note that it defaults to english)
    lemma_out_col = f"{text_col}_lemma"
    lemmatizer = sparknlp.annotator.LemmatizerModel.pretrained() \
        .setInputCols(normalizer_out_col) \
        .setOutputCol(lemma_out_col)
        
    cleaner_out_col = f"{text_col}_clean_lemma"
    stopwords_cleaner = sparknlp.annotator.StopWordsCleaner() \
        .setInputCols(lemma_out_col) \
        .setOutputCol(cleaner_out_col) \
        .setCaseSensitive(False) \
        .setStopWords(eng_stopwords)# finisher converts tokens to human-readable output

    finisher_out_col = "tokens"
    finisher = sparknlp.base.Finisher() \
        .setInputCols(cleaner_out_col) \
        .setOutputCols(finisher_out_col) \
        .setCleanAnnotations(False)

    cols_to_drop.extend([
        doc_out_col,
        tokenizer_out_col,
        normalizer_out_col,
        lemma_out_col,
        cleaner_out_col
    ])

    stages.extend([
        document_assembler,
        tokenizer,
        normalizer,
        lemmatizer,
        stopwords_cleaner,
        finisher
    ])

    pipeline = sparkml.Pipeline() \
        .setStages(stages)

    # tokenize, lemmatize, remove stop words
    df = pipeline.fit(df) \
                 .transform(df)

    df = df.drop(*cols_to_drop)

    return df