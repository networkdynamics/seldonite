from seldonite import sources

class MockSource(sources.BaseSource):
    def __init__(self, can_keyword_filter=False, articles=None):
        super().__init__()

        self.can_keyword_filter = can_keyword_filter
        self.articles = articles

    def set_keywords(self, keywords):
        if self.can_keyword_filter:
            self.keywords = keywords
        else:
            raise AttributeError()

    def fetch(self):
        if self.articles:
            return self.articles
        if self.can_keyword_filter:
            return [{'url': 'https://cbc.ca/thing/happened/', 'text': ' '.join(self.keywords)}] * 10
        else:
            return []