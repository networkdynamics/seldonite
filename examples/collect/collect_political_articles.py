import argparse

from seldonite import collect, run, nlp
from seldonite.sources import news

def main(args):

    csv_source = news.CSV(args.input)
    collector = collect.Collector(csv_source) \
        .only_political_articles()

    runner = run.Runner(collector) 
    df = runner.to_pandas()

    df.to_csv(args.output)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-o', '--output')
    args = parser.parse_args()

    main(args)