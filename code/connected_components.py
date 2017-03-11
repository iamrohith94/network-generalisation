# -*- coding: utf-8 -*-
"""
@author: rohithreddy

This script contains helper functions which are
used to make a graph strongly connected
"""
import networkx as nx
from common import *;
from graphs import *;
import numpy as np
def connect_components(parameters):
	conn = parameters['conn']
	super_node_map = {}
	G = edge_table_to_graph_level(parameters);
	#print "Initial Skeleton: "
	#print G.edges()
	#parameters['table'] = parameters['table_v']
	#parameters['column'] = "id";
	#nodes = get_column_values(parameters);
	for node in G.nodes():
		super_node_map[node] = [node]
	edge_additions = make_strongly_connected(G, super_node_map)
	return edge_additions
	

def make_strongly_connected(G, super_node_map):
	node_to_component = {}
	edge_additions = []
	temp_super_node_map = {}
	initial_connected_components =  sorted(nx.strongly_connected_components(G), key = len, reverse = True)
	initial_num_connected_comp = len(initial_connected_components)
	

	# Return an empty list if it is already strongly connected
	if initial_num_connected_comp == 1:
		return edge_additions

	"""
	#print "Super Node map: "
	for super_node in super_node_map.keys():
		print str(super_node)+": "
		print super_node_map[super_node]

	#print "Initial Connected Components:"
	for i in xrange(initial_num_connected_comp):
		print str(i) + ": "
		print initial_connected_components[i]
	"""

	for i in xrange(initial_num_connected_comp):
		temp_super_node_map[i] = [v for x in initial_connected_components[i] for v in super_node_map[x]]	

	else:
		# Mapping every node to its connected component
		for i in xrange(initial_num_connected_comp):
			for v in initial_connected_components[i]:
				node_to_component[v] = i

		DAG = nx.DiGraph();

		
		#print "Generating DAG"
		# Generating the DAG with every strongly connected component as node
		#Adding nodes
		for key in node_to_component.keys():
			DAG.add_node(node_to_component[key])
		#Adding edges
		for edge in G.edges():
			DAG_src = node_to_component[edge[0]]
			DAG_target = node_to_component[edge[1]]
			if  DAG_src != DAG_target:
				DAG.add_edge(int(DAG_src), int(DAG_target))
		"""
		print "Node to connected component"
		print node_to_component

		print "DAG: ";
		for edge in DAG.edges():
			print str(edge[0]) + ", "+ str(edge[1])
		"""
		for v in DAG.nodes():
			#print "node: " + str(v);
			add_inc = False
			add_out = False
			# fetching the first node in the intial strongly connected component
			# v -> DAG node
			# n -> first neighbor of v in the DAG
			# poi -> first node(in original graph) of super node v
			# connect_poi -> first node(in original graph) of super node n
			if DAG.in_degree(v) == 0 and DAG.out_degree(v) == 0:
				add_inc = add_out = True
				n = 0
				#break;
			elif DAG.in_degree(v) == 0 or DAG.out_degree(v) == 0:
				n = nx.all_neighbors(DAG, v).next()
				# add an incoming edge from one of its neighbors
				if DAG.in_degree(v) == 0:
					add_inc = True
				
				if DAG.out_degree(v) == 0:
					add_out = True
				#break;

			else:
				continue

			poi = temp_super_node_map[v][0];
			connect_poi = temp_super_node_map[n][0]
			#add necessary edges between n and v in DAG
			#add necessary edges between poi and connect_poi in G
			if add_inc == True:
				#print "DAG connection between " + str(n) + " and " + str(v);
				#print "G connection between " + str(connect_poi) + " and " + str(poi);
				DAG.add_edge(n, v);
				edge_additions.append((connect_poi, poi))
			if add_out == True:
				#print "DAG connection between " + str(v) + " and " + str(n);
				#print "G connection between " + str(connect_poi) + " and " + str(poi);
				DAG.add_edge(v, n);
				edge_additions.append((poi, connect_poi))

		#deleting the unwanted variables
		del super_node_map
		del node_to_component
		del initial_connected_components

		additions = make_strongly_connected(DAG, temp_super_node_map)
		for addition in additions:
			edge_additions.append(addition)

		return edge_additions
