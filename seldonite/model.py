from newspaper.article import Article as NPArticle

class Article(NPArticle):
    def __init__(self, *args, text = '', **kwargs):
        super().__init__(*args, **kwargs)
        self.text = text

    def to_dict(self):
        article_dict = {}
        article_dict["title"] = self.title
        article_dict["text"] = self.text
        article_dict["authors"] = self.authors
        article_dict["url"] = self.url
        article_dict["publish_date"] = self.publish_date

        return article_dict
