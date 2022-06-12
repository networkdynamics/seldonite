import argparse
import json

from seldonite.sources import news
from seldonite import collect, run


def main(args):

    mongo_source = news.MongoDB(args.connection_string, args.database, args.collection)

    collector = collect.Collector(mongo_source) \
        .limit_num_articles(10)
    runner = run.Runner(collector)
    df = runner.to_pandas()

    df.to_csv(args.out)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-string')
    parser.add_argument('--database')
    parser.add_argument('--collection')
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)