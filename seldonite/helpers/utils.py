from seldonite.model import Article

def link_to_article(link):

    article = Article(link)
    article.download()
    article.parse()

    return article