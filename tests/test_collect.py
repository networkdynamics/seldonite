import pytest

from seldonite import collect

import mock_source

@pytest.mark.parametrize("can_keyword_filter, articles",
    [(False, [{'url': 'https://cbc.ca/thing/happened/', 'title': '', 'text': keyword} for keyword in ['policy', 'notpolicy']]),
     (True, None)])
def test_fetch(can_keyword_filter, articles):
    source = mock_source.MockSource(can_keyword_filter=can_keyword_filter,
                                    articles=articles)

    collector = collect.Collector(source)
    collector.by_keywords(['policy'])
    news = collector.fetch()

    policy_articles = list('policy' in article.text for article in news)
    assert len(policy_articles) > 0
    assert all(policy_articles)

@pytest.mark.parametrize("articles",
    [([{'url': 'https://cbc.ca/thing/happened/', 'title': '', 'text': content} for content in ['economic policy happened in government',
                                                                                               'social policy prime minister',
                                                                                               'tax cuts for rich',
                                                                                               'social security increase for elderly',
                                                                                               'central bank adjusts interest rate',
                                                                                               'travel restrictions for unvaccinated',
                                                                                               'immigration program announced for foreign workers',
                                                                                               'subsidies for farmers',
                                                                                               'renewable energy investment for municipalities',
                                                                                               'funding increase for hospitals in city']])])
def test_model_topics(articles):
    source = mock_source.MockSource(articles=articles)

    collector = collect.Collector(source)
    lda, dictionary = collector.find_topics(batch_size=5)

    assert lda
    assert dictionary
    