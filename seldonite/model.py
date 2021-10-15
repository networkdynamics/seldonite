
class NewsArticle:
    def __init__(self, title=None, content=None):
        self.authors = []
        self.date_download = None
        self.date_modify = None
        self.date_publish = None
        self.language = None
        self.source_domain = None
        self.content = content
        self.title = title
        self.url = None