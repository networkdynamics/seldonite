from newspaper.article import Article as NPArticle

class Article(NPArticle):
    def __init__(self, url, *args, text = '', init=True, **kwargs):
        self.source_url = url
        self.text = text
        
        if init:
            super().__init__(url, *args, **kwargs)

    def to_dict(self):
        article_dict = {}
        article_dict["title"] = self.title
        article_dict["text"] = self.text
        article_dict["authors"] = self.authors
        article_dict["url"] = self.url
        article_dict["publish_date"] = self.publish_date

        return article_dict
