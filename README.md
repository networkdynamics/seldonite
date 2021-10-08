# Seldonite
### A News Event Representation Library

Define a news source, set your search method, and create event representations.

Usage (WIP):
```python
from seldonite import source, collect, represent

sites = ['cbc.ca', 'bbc.com']
source = source.CommonCrawl(sites)

collector = collect.Collector(source)
collector.by_keywords(['afghanistan', 'withdrawal'])
news = collector.fetch()

timeline = represent.Timeline(news)
timeline.show()
```

Please see the wiki for more detail on sources and methods

## Setup

Just run:
`pip install -r requirements.txt`

Or if you have make:
`make setup`

## Tests

We use `pytest` here.

To run tests:
`pytest`

Or:
`make test`
