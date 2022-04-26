import argparse

from seldonite import collect, run, nlp
from seldonite.sources import news

def main(args):

    csv_source = news.CSV(args.input)
    collector = collect.Collector(csv_source)

    nl_processor = nlp.NLP(collector) \
        .top_tfidf(6, save_path=args.model)

    runner = run.Runner(nl_processor) 
    df = runner.to_pandas()

    df.to_csv(args.output)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-o', '--output')
    parser.add_argument('-m', '--model')
    args = parser.parse_args()

    main(args)