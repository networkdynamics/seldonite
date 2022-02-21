# Seldonite
### A News Event Representation Library

Define a news source, set your search method, and create event representations.

Usage (WIP):
```python
from seldonite import source, collect, analyze

sites = ['cbc.ca', 'bbc.com']
source = source.CommonCrawl(sites)

collector = collect.Collector(source) \
    .by_keywords(['afghanistan', 'withdrawal'])

analysis = analyze.Analyze(collector) \
    .timeline()

timeline = runner.Runner(analysis)
    .to_pandas()

timeline.plot()
```

Please see the wiki for more detail on sources and methods

## Setup

To install seldonite as editable, and dependencies via conda:
```
conda env create -f ./environment.yml
```

This library uses a variety of third party libraries, please see limited setup instructions below:

### Spacy

To use NLP methods that require the use of spacy:
```
python -m spacy download en_core_web_sm
```

### Spark

To make Python dependencies available to Spark executors, use the dependency packaging script:
```
bash ./seldonite/spark/package_pyspark_deps.sh
```


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
