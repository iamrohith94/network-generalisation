import networkx as nx
from common import *;

parameters = {}
parameters['db'] = "gachibowli"
parameters['table'] = "ways"
G = edge_table_to_graph(parameters)


print sorted(nx.connected_components(G), key = len, reverse=True)

