import argparse
import json
import os

from seldonite import source
from seldonite import collect


def main(args):

    if args.site is None:
        sites = []
    else:
        sites = [args.site]
        
    google_source = source.CommonCrawl(master_url=args.master_url, sites=sites)

    collector = collect.Collector(google_source)
    collector.by_keywords([args.keyword])
    articles = collector.fetch(max_articles=None)

    json_articles = json.dumps([article.to_dict() for article in articles], indent=2)
    if args.out:
        out_path = os.path.abspath(args.out)
        out_dir = os.path.dirname(out_path)

        if not os.path.exists(out_dir):
            os.mkdir(out_dir)

        with open(out_path, 'w') as f:
            f.write(json_articles)
    else:
        print(json_articles)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--master-url')
    parser.add_argument('--site')
    parser.add_argument('--keyword')
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)