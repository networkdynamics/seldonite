import argparse

from seldonite import graphs, collect, nlp, run
from seldonite.sources import news

def main(args):
    source = news.CSV(args.input)
    collector = collect.Collector(source) \
        .limit_num_articles(40)

    nl_processor = nlp.NLP(collector) \
        .get_entities(blacklist_entities=['Reuters'])

    graph_constructor = graphs.Graph(nl_processor) \
        .build_entity_dag()

    runner = run.Runner(graph_constructor)
    nodes, edges = runner.to_pandas()

    nodes.to_csv(args.nodes, index=False)
    edges.to_csv(args.edges, index=False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-g', '--edges')
    parser.add_argument('-m', '--nodes')
    args = parser.parse_args()

    main(args)