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

To install seldonite as editable, and dependencies via conda:
```
conda env update -f ./environment.yml
```

This library uses a variety of third party libraries, please see limited setup instructions below:

### Spacy

To use NLP methods that require the use of spacy:
```
python -m spacy download en_core_web_sm
```

### Selenium

To use sources that require Selenium, download the chromedriver and place is on your path. For me this goes like this:
```
curl -LO https://chromedriver.storage.googleapis.com/94.0.4606.61/chromedriver_linux64.zip
unzip ./chromedriver_linux64.zip
mv ./chromedriver /path/to/bin
rm chromedriver_linux64.zip
```

### Spark

To make Python dependencies available to Spark executors, use the dependency packaging script:
```
bash ./seldonite/spark/package_pyspark_deps.sh
```

### Horovod



## Tests

We use `pytest`.

To run tests, run these commands from the top level directory:

```
pytest
```

## Credits

* Spark based pipeline based on Sebastien Nagel's [cc-pyspark](https://github.com/commoncrawl/cc-pyspark) library. 
* Heuristics for News Articles adapted from [newsplease](https://github.com/fhamborg/news-please)
* Political news classifier taken from [Political-News-Filter](https://github.com/lukasgebhard/Political-News-Filter)
