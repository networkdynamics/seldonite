import argparse
import datetime
import json
import os

from seldonite.sources import news
from seldonite import collect, run


def main(args):

    if args.site is None:
        sites = []
    else:
        sites = [args.site]
        
    cc_source = news.NewsCrawl(master_url=args.master_url)

    collector = collect.Collector(cc_source) \
                    .in_date_range(datetime.date(2021, 12, 1), datetime.date(2021, 12, 1)) \
                    .on_sites(sites) \
                    .limit_num_articles(10)
    runner = run.Runner(collector)
    article_df = runner.to_pandas()

    if args.out:
        out_path = os.path.abspath(args.out)
        out_dir = os.path.dirname(out_path)

        if not os.path.exists(out_dir):
            os.mkdir(out_dir)

        article_df.to_csv(out_path)
    else:
        print(article_df)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--master-url')
    parser.add_argument('--site')
    parser.add_argument('--out')
    args = parser.parse_args()

    main(args)