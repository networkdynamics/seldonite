from seldonite import model

import newspaper

def link_to_article(link):

    np_article = newspaper.Article(link)
    np_article.download()
    np_article.parse()

    article = model.NewsArticle()

    article.authors = np_article.authors
    article.title = np_article.title
    article.content = np_article.text
    article.domain = np_article.source_url
    article.url = np_article.url

    return article