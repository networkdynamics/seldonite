import argparse

import networkx as nx

from seldonite import graphs, sources, collect, nlp, run

def main(args):
    source = sources.CSV(args.input)
    collector = collect.Collector(source)

    graph_constructor = graphs.Graph(collector)
    graph_constructor.build_entity_dag()

    runner = run.Runner(graph_constructor)
    G, map_df = runner.get_obj()

    nx.write_weighted_edgelist(G, args.graph)
    map_df.to_csv(args.map, index=False, sep=' ')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-g', '--graph')
    parser.add_argument('-m', '--map')
    args = parser.parse_args()

    main(args)