import matplotlib.pyplot as plt
import networkx as nx

from seldonite import base

class Visualize(base.BaseStage):
    def __init__(self, input):
        super().__init__(input)
        self.do_show_entity_dag_graph = False

    def show_entity_dag_graph(self):
        self.do_show_entity_dag_graph = True
        return self

    def _show_entity_dag_graph(self, graph):
        nodes_df, edges_df = graph

        nodes = nodes_df.collect()
        edges = edges_df.collect()

        G = nx.DiGraph()

        for node in nodes:
            G.add_node(node['id'], title=node['title'])

        for edge in edges:
            G.add_edge(edge['old_id'], edge['new_id'], entity=edge['entity'])

        pos = nx.spring_layout(G)
        nx.draw(
            G, pos, edge_color='black', width=1, linewidths=1,
            node_size=500, node_color='pink', alpha=0.9,
            labels={node: G.nodes[node]['title'] for node in G.nodes()}
        )
        nx.draw_networkx_edge_labels(
            G, pos,
            edge_labels={edge: G.edges[edge]['entity'] for edge in G.edges()},
            font_color='red'
        )
        plt.show()


    def _process(self, spark_manager):
        res = self.input._process(spark_manager)

        if self.do_show_entity_dag_graph:
            self._show_entity_dag_graph(res)



