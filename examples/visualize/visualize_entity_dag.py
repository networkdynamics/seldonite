import argparse

from seldonite import graphs, collect, nlp, run, visualize
from seldonite.sources import other

def main(args):
    graph_source = other.GraphCSV(args.nodes_path, args.edges_path)

    visualizer = visualize.Visualize(graph_source) \
        .show_entity_dag_graph()

    runner = run.Runner(visualizer)
    runner.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--nodes-path')
    parser.add_argument('-e', '--edges-path')
    args = parser.parse_args()

    main(args)