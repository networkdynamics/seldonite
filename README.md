# Seldonite
### A News Collection Library

Define a news source, set your search method, and collect news articles or create news graphs.

Usage (WIP):
```python
import os

from seldonite import sources, collect, run

aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']

source = sources.news.CommonCrawl(aws_access_key, aws_secret_key)

collector = collect.Collector(source) \
    .on_sites(['cbc.ca', 'bbc.com']) \
    .by_keywords(['afghanistan', 'withdrawal'])

analysis = analyze.Analyze(collector) \
    .timeline()

timeline = run.Runner(analysis)
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
