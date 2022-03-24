import argparse

import networkx as nx

from seldonite import graphs, sources, collect, nlp, run

def main(args):
    source = sources.CSV(args.input)
    collector = collect.Collector(source)
    collector.limit_num_articles(10)

    nl_processor = nlp.NLP(collector)
    nl_processor.top_tfidf(10)

    graph_constructor = graphs.Graph(nl_processor)
    graph_constructor.build_news2vec_graph()

    runner = run.Runner(graph_constructor)
    G, map_df = runner.get_obj()

    nx.write_weighted_edgelist(G, args.graph)
    map_df.to_csv(args.map, index=False, sep=' ', header=False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-g', '--graph')
    parser.add_argument('-m', '--map')
    args = parser.parse_args()

    main(args)