from seldonite import filter

class Collector:
    '''
    Class to search through a source for desired news articles

    Can use a variety of search methods
    '''
    def __init__(self, source):
        self.source = source
        self.uses_callback = self.source.uses_callback

    def set_date_range(self, start_date, end_date):
        '''
        Set dates to fetch news from, range is inclusive
        '''
        self.source.set_date_range(start_date, end_date)

    def by_keywords(self, keywords):
        '''
        Set some keywords to filter by
        '''

        self.keywords = keywords

        if self.source.can_keyword_filter:
            self.source.set_keywords(keywords)

    def fetch(self, cb=None):

        if self.source.uses_callback and cb is None:
            raise TypeError('Source used requires a callback function to be provided.')

        self.caller_cb = cb

        if self.source.uses_callback:
            self.source.fetch(self.accumulate)
        else:
            articles = self.source.fetch()

        if self.keywords and not self.source.can_keyword_filter:
            for article in articles:
                if filter.contains_keywords(article, self.keywords):
                    yield article

        return articles

    def accumulate(self, article):

        if self.keywords and not self.source.can_keyword_filter:
            if filter.contains_keywords(article, self.keywords):
                self.caller_cb(article)

        else:
            self.caller_cb(article)
