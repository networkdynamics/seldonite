import argparse

from seldonite import graphs, sources, collect, nlp, run, visualize

def main(args):
    source = sources.CSV(args.input)
    collector = collect.Collector(source)

    nl_processor = nlp.NLP(collector) \
        .get_entities()

    graph_constructor = graphs.Graph(nl_processor) \
        .build_entity_dag()

    visualizer = visualize.Visualize(graph_constructor) \
        .show_entity_dag_graph()

    runner = run.Runner(visualizer)
    runner.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    args = parser.parse_args()

    main(args)