import argparse
import json

from seldonite import source
from seldonite import collect


def main(args):

    google_source = source.Eureka(args.chrome_driver, args.eureka_url, username=args.username, password=args.password)

    collector = collect.Collector(google_source)
    collector.by_keywords([args.keyword])
    lda = collector.find_topics()

    print(lda.show_topics())

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