from seldonite.helpers import filter, preprocess

from gensim import models, corpora

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

    def fetch(self):

        articles = self.source.fetch()

        if self.keywords and not self.source.can_keyword_filter:
            for article in articles:
                if filter.contains_keywords(article, self.keywords):
                    yield article

        for article in articles:
            yield article


    def find_topics(self, batch_size=1000):
        articles = self.fetch()
        prepro = preprocess.Preprocessor()

        more_articles = True
        model = None
        dictionary = None
        while more_articles:
            batch_idx = 0
            content_batch = []

            while batch_idx < batch_size:
                try:
                    article = next(articles)
                    content_batch.append(article.text)
                    batch_idx += 1
                except StopIteration:
                    more_articles = False
                    break

            # TODO add bigrams
            docs = prepro.preprocess(content_batch)

            if not dictionary:
                # TODO consider using hashdictionary
                dictionary = corpora.Dictionary(docs)
                dictionary.filter_extremes(no_below=5, no_above=0.8)
            
            corpus = [dictionary.doc2bow(doc) for doc in docs]

            if not model:
                # TODO use ldamulticore for speed
                model = models.LdaModel(corpus, 
                                        id2word=dictionary.id2token, 
                                        num_topics=10)
            else:
                model.update(corpus)

        return model, dictionary
