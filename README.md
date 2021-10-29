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

To install dependencies via pip:
```
pip install -r requirements.txt
```
Install seldonite locally to use it:
```
pip install --editable .
```
To use NLP methods that require the use of spacy:
```
python -m spacy download en_core_web_sm
```
To use sources that require Selenium, download the chromedriver and place is on your path. For me this goes like this:
```
curl -LO https://chromedriver.storage.googleapis.com/94.0.4606.61/chromedriver_linux64.zip
unzip ./chromedriver_linux64.zip
mv ./chromedriver /path/to/bin
rm chromedriver_linux64.zip
```

Or if you have `make`:
```
make setup
```

## Tests

We use `pytest`.

To run tests, run these commands from the top level directory:

```
pip install --edit .
pytest
```

Or: 
```
make test
```

## Credits

* Spark based pipeline based on Sebastien Nagel's [cc-pyspark](https://github.com/commoncrawl/cc-pyspark) library. 
* Heuristics for News Articles adapted from [newsplease](https://github.com/fhamborg/news-please)
