import json

from seldonite.model import NewsArticle

import pytest

@pytest.mark.parametrize("article_data",
    [({"authors": ["Someone", "Someone Else"], "domain": "cbc.ca", "content": "Something happened", "title": "Woah!", "url": "cbc.ca/thing"})])
def test_json(article_data):
    article = NewsArticle()
    article.authors = article_data["authors"]
    article.domain = article_data["domain"]
    article.content = article_data["content"]
    article.title = article_data["title"]
    article.url = article_data["url"]

    json_articles = json.dumps([ article ])
    json_article_data = json.dumps([ article_data ])
    assert json_articles == json_article_data
