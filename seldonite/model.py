


class NewsArticle(dict):
    def __init__(self, title=None, content=None):
        dict.__init__(self, authors=None,
                            domain=None,
                            content=content,
                            title=title,
                            url=None)

    def __getattr__(self, key):
        val = dict.__getitem__(self, key)
        return val

    def __setattr__(self, key, val):
        dict.__setitem__(self, key, val)

    def __repr__(self):
        dictrepr = dict.__repr__(self)
        return '%s(%s)' % (type(self).__name__, dictrepr)
