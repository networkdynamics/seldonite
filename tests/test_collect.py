from seldonite import collect
import mock_source

def test_fetch():
    source = mock_source.MockSource()

    collector = collect.Collector(source)
    collector.by_keyword('policy')
    news = collector.fetch()

    assert all('policy' in article.content for article in news)