import argparse
import json

from seldonite import source
from seldonite import collect


def main(args):

    hosts = [args.host]
    google_source = source.Google(dev_key=args.dev_key, engine_id=args.engine_id, hosts=hosts)

    collector = collect.Collector(google_source)
    collector.by_keywords([args.keyword])
    articles = collector.fetch()

    with open(args.out, 'w') as f:
        f.write(json.dumps(list(articles)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev-key')
    parser.add_argument('--engine-id')
    parser.add_argument('--host')
    parser.add_argument('--keyword')
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)