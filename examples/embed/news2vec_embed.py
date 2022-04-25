import argparse
import os

from seldonite import embed, collect, nlp, run
from seldonite.sources import news

def main(args):
    source = news.CSV(args.input)
    collector = collect.Collector(source)
    collector.limit_num_articles(10)

    nl_processor = nlp.NLP(collector) \
        .top_tfidf(20)#, load_path=args.tfidf)

    graph_constructor = embed.Embed(nl_processor) \
        .news2vec_embed(embedding_path=args.embeddings)

    runner = run.Runner(graph_constructor)
    embeddings = runner.to_pandas()

    embeddings.to_csv(args.output)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--tfidf')
    parser.add_argument('--embeddings')
    parser.add_argument('--output')
    args = parser.parse_args()

    main(args)