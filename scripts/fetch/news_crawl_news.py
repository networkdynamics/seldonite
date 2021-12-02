import argparse
import datetime
import json
import os

from seldonite import source
from seldonite import collect


def main(args):

    if args.site is None:
        sites = []
    else:
        sites = [args.site]
        
    cc_source = source.NewsCrawl(master_url=args.master_url)

    collector = collect.Collector(cc_source)
    collector.set_date_range(datetime.date(2021, 12, 1), datetime.date(2021, 12, 1))
    articles = collector.fetch(max_articles=10, sites=sites)

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
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)