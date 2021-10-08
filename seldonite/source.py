import newsplease

# TODO make abstract
class Source:
    '''
    Base class for a source

    A source can be anything from a search engine, to an API, to a dataset
    '''

    # TODO make abstract
    def __init__(self):
        pass


class CommonCrawlSource(Source):
    '''
    Source that uses the news-please library to search CommonCrawl
    '''