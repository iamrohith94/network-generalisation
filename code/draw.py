# -*- coding: utf-8 -*-
"""
Created on Sun May  8 22:02:53 2016

@author: rohithreddy
"""

import networkx as nx
from common import *;
"""G=nx.Graph()
G.add_node("a")
G.add_nodes_from(["b","c"])

G.add_edge(1,2)
edge = ("d", "e")
G.add_edge(*edge)
edge = ("a", "b")
G.add_edge(*edge)

print("Nodes of graph: ")
print(G.nodes())
print("Edges of graph: ")
print(G.edges())


G.add_edges_from([("a","c"),("c","d"), ("a",1), (1,"d"), ("a",2)])"""
#G = nx.balanced_tree(2, 4);

"""dic = nx.drawing.nx_agraph.graphviz_layout(G);
print dic
#print G.edges()
for edge in G.edges():
    print "src: ",edge[0], ";target: ",edge[1], ";(x1,y1): ",dic[edge[0]], ";(x2,y2): ",dic[edge[1]]"""
n=10
#BARBELL GRAPH
"""G = nx.barbell_graph(n,1);
table_name="barbell_graph" """

#COMPLETE GRAPH
"""G = nx.complete_graph(n);
table_name="complete_graph" """

#COMPLETE BIPARTITE GRAPH
"""G = nx.complete_bipartite_graph(n,n);
table_name="complete_bipartite_graph" """


#LADDER GRAPH
"""G = nx.ladder_graph(n);
table_name="ladder_graph" """


#WHEEL GRAPH
"""G = nx.wheel_graph(n);
table_name="wheel_graph" """


#BALANCED TREE
""" G = nx.balanced_tree(2,n);
table_name="balanced_tree" """

#GRID GRAPH
p = {}
p['n'] = n
G = nx.grid_2d_graph(n,n);
p['graph'] = G
G = tuple_to_id(p)
table_name="grid_graph"


parameters = {}
parameters['graph'] = G
parameters['db'] = "paper"
parameters['table'] = table_name
graph_to_edge_table(parameters);
nx.drawing.nx_agraph.write_dot(G, table_name+".dot")

#G = nx.barbell_graph(3,2);
#G = nx.path_graph(3);
#G = nx.complete_bipartite_graph(2,3);
#G = nx.circular_ladder_graph(4);
#G = nx.dorogovtsev_goltsev_mendes_graph(3);
#G = nx.grid_2d_graph(3,3);

#G = nx.ladder_graph(4);
#G = nx.lollipop_graph(4,4);
#G = nx.circular_ladder_graph(4);
#nx.draw_graphviz(G)
#nx.drawing.nx_agraph.write_dot(G, table_name+".dot")
#print(G.edges())
#plt.savefig(table_name+".png") # save as png
#plt.show() # display
#print "initial: ",G.edges()
#G1 = tuple_to_id(parameters)
#print "final: ",G1.edges()
#xy_dict = nx.drawing.nx_agraph.graphviz_layout(G1);
#print xy_dict 