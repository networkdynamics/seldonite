import json

from seldonite.model import Article

import pytest

@pytest.mark.parametrize("article_data",
    [({"title": "Woah!", "text": "Something happened", "authors": ["Someone", "Someone Else"], "url": "https://cbc.ca/thing"})])
def test_json(article_data):
    article = Article(article_data["url"])
    article.authors = article_data["authors"]
    article.text = article_data["text"]
    article.title = article_data["title"]

    json_articles = json.dumps([ article.to_dict() ])
    json_article_data = json.dumps([ article_data ])
    assert json_articles == json_article_data
