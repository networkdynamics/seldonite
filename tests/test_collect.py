import pytest

from seldonite import model
from seldonite import collect

import mock_source

@pytest.mark.parametrize("uses_callback, can_keyword_filter, articles",
    [(False, False, [model.Article('https://cbc.ca/thing/happened/', title='', text=keyword) for keyword in ['policy', 'notpolicy']]),
     (False, True, None)])
def test_fetch(uses_callback, can_keyword_filter, articles):
    source = mock_source.MockSource(uses_callback=uses_callback,
                                    can_keyword_filter=can_keyword_filter,
                                    articles=articles)

    collector = collect.Collector(source)
    collector.by_keywords(['policy'])
    news = collector.fetch()

    policy_articles = list('policy' in article.text for article in news)
    assert len(policy_articles) > 0
    assert all(policy_articles)
    