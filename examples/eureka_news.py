import argparse
import json

from seldonite import source
from seldonite import collect


def main(args):

    google_source = source.Eureka(args.chrome_driver, args.eureka_url, username=args.username, password=args.password)

    collector = collect.Collector(google_source)
    collector.by_keywords([args.keyword])
    articles = collector.fetch()

    # get first 10 articles
    article_dicts = []
    while len(article_dicts) < 5:
        article_dicts.append(next(articles).to_dict())

    json_articles = json.dumps(article_dicts, indent=2)
    if args.out:
        with open(args.out, 'w') as f:
            f.write(json_articles)
    else:
        print(json_articles)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--chrome-driver')
    parser.add_argument('--eureka-url')
    parser.add_argument('--username')
    parser.add_argument('--password')
    parser.add_argument('--keyword')
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)