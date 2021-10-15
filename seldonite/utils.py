from seldonite import article

import newspaper

def link_to_article(link):

    np_article = newspaper.Article(link)
    np_article.download()
    np_article.parse()

    this_article = article.NewsArticle()

    this_article.authors = np_article.authors
    this_article.title = np_article.title
    this_article.text = np_article.text

    return this_article