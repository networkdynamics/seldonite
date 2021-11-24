import argparse
import json

from seldonite import source
from seldonite import collect


def main(args):

    sites = [args.site]
    google_source = source.Google(dev_key=args.dev_key, engine_id=args.engine_id, sites=sites, max_requests=1)

    collector = collect.Collector(google_source)
    collector.by_keywords([args.keyword])
    articles = collector.fetch()

    json_articles = json.dumps([article.to_dict() for article in articles], indent=2)
    if args.out:
        with open(args.out, 'w') as f:
            f.write(json_articles)
    else:
        print(json_articles)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev-key')
    parser.add_argument('--engine-id')
    parser.add_argument('--site')
    parser.add_argument('--keyword')
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)