import argparse
import json

from seldonite.sources import news
from seldonite import collect, run


def main(args):

    sites = [args.site]
    google_source = news.Google(dev_key=args.dev_key, engine_id=args.engine_id)

    collector = collect.Collector(google_source) \
                    .on_sites([args.site]) \
                    .by_keywords([args.keyword])
    runner = run.Runner(collector)
    df = runner.to_pandas()

    df.to_csv(args.out)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev-key')
    parser.add_argument('--engine-id')
    parser.add_argument('--site')
    parser.add_argument('--keyword')
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)