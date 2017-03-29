import psycopg2
import collections
from path_class import *
import time
from psycopg2.extensions import AsIs
from heapq import heappush, heappop
class ShortestPath:
	'Class for extracting shortest path'
	user = 'postgres'
	password = "postgres"
	host="127.0.0.1"
	port="5432"
	vertex_table = "cleaned_ways_vertices_pgr"
	edge_table = "cleaned_ways"
	target_level = 1
	time_source_to_skeleton = 0.00
	time_skeleton_to_target = 0.00
	time_on_skeleton = 0.00
	time_gen_path = 0.00
	time_original_path = 0.00

	def __init__(self, db):
		self.db = db
		self.conn = psycopg2.connect(database=db, user=self.user, password=self.password, host=self.host, port=self.port)
		self.cur = self.conn.cursor()

	def get_node_level(self, node_id, level_column):
		query = "SELECT %s FROM %s WHERE id = %s"
		self.cur.execute(query, (AsIs(level_column), AsIs(self.vertex_table), node_id,))
		rows = self.cur.fetchall()
		for row in rows:
			return row[0]

	def get_next_node(self, curr_node_id, target_node_id, level_column, visited_nodes, forward):
		if self.get_node_level(curr_node_id, level_column) == self.target_level:
			return curr_node_id
		if forward:
			return self.forward_level_heuristic(curr_node_id, target_node_id, level_column, visited_nodes)
		return self.backward_level_heuristic(curr_node_id, target_node_id, level_column, visited_nodes)

	def update_path_cost(self, path):
		query_1 = "SELECT cost FROM %s WHERE id = %s AND source = %s"
		query_2 = "SELECT reverse_cost FROM %s WHERE id = %s AND target = %s"
		for entry in path.get_path():
			if entry['edge'] != -1:
				self.cur.execute(query_1, (AsIs(self.edge_table), entry['edge'], entry['node']))
				rows = self.cur.fetchall()
				for row in rows:
					entry['cost'] = row[0]
				self.cur.execute(query_2, (AsIs(self.edge_table), entry['edge'], entry['node']))
				rows = self.cur.fetchall()
				for row in rows:
					entry['cost'] = row[0]
		return path


	def get_path_to_target_level(self, curr_node_id, target_node_id, level_column, forward = True):
		path = Path()
		visited = []
		if not forward:
			path.add_entry({'node': curr_node_id, 'edge': -1, 'cost': 0.0000})
		#print curr_node_id
		temp_node_id = curr_node_id
		while self.get_node_level(temp_node_id, level_column) != self.target_level:
			visited.append(temp_node_id)
			temp_node_id, entry = self.get_next_node(temp_node_id, target_node_id, level_column, visited, forward)
			path.add_entry(entry)
			print entry
			print temp_node_id
		if forward:
			path.add_entry({'node': temp_node_id, 'edge': -1, 'cost': 0.0000})
		else:
			path.get_path().reverse()
		return path

	def get_path_on_skeleton(self, source, target, level_column):
		path = Path()
		inner_query = "SELECT id, source, target, cost, reverse_cost \
		FROM "+self.edge_table+" WHERE "+level_column+" <= "+str(self.target_level)
		query = "SELECT node, edge, cost FROM pgr_dijkstra(%s, %s, %s)"
		self.cur.execute(query, (inner_query, source, target))
		rows = self.cur.fetchall()
		for row in rows:
			path.add_entry({'node': row[0], 'edge': row[1], 'cost': row[2]})
		return path

	def get_generalised_path(self, source, target, level_column):
		print source, target
		print level_column
		self.time_source_to_skeleton = time.time()
		source_to_skeleton = self.astar_path(source, target, level_column)
		self.time_source_to_skeleton = time.time() - self.time_source_to_skeleton
		print "Source node on skeleton: ", source_to_skeleton.get_last_node()
		self.update_path_cost(source_to_skeleton)
		
		print "Source to skeleton: "+str(len(source_to_skeleton)-1)
		print "Path: ", source_to_skeleton
		
		self.time_skeleton_to_target = time.time()
		skeleton_to_target = self.astar_path(target, source_to_skeleton.get_last_node(), level_column, True)
		self.time_skeleton_to_target = time.time() - self.time_skeleton_to_target
		print "Target node on skeleton: ", skeleton_to_target.get_start_node()
		self.update_path_cost(skeleton_to_target)
		"""
		print "Skeleton to target: "+str(len(skeleton_to_target)-1)
		print skeleton_to_target
		"""
		self.time_on_skeleton = time.time()
		on_skeleton = self.get_path_on_skeleton(source_to_skeleton.get_last_node(), skeleton_to_target.get_start_node(), level_column)
		self.time_on_skeleton = time.time() - self.time_on_skeleton
		"""
		print "On Skeleton: "+str(len(on_skeleton))
		print on_skeleton
		"""
		self.time_gen_path = self.time_source_to_skeleton+self.time_on_skeleton+self.time_skeleton_to_target
		
		final_path = Path()
		final_path.add_path(source_to_skeleton)
		final_path.add_path(on_skeleton)
		final_path.add_path(skeleton_to_target)

		#print "Final Path: "+str(len(final_path))
		#print final_path

		return final_path


	def get_original_path(self, source, target):
		path = Path()
		inner_query = "SELECT id, source, target, cost, reverse_cost \
		FROM "+self.edge_table
		query = "SELECT node, edge, cost FROM pgr_dijkstra(%s, %s, %s)"
		self.time_original_path = time.time()
		self.cur.execute(query, (inner_query, source, target))
		rows = self.cur.fetchall()
		self.time_original_path = time.time() - self.time_original_path
		for row in rows:
			path.add_entry({'node': row[0], 'edge': row[1], 'cost': row[2]})
		return path

	def euclidean_distance(self, source, target):
		#print source, target
		query = "SELECT ST_Distance(v1.the_geom, v2.the_geom) FROM %s AS v1, %s AS v2 \
		WHERE v1.id = %s AND v2.id = %s"
		self.cur.execute(query, (AsIs(self.vertex_table), AsIs(self.vertex_table), source, target))
		rows = self.cur.fetchall()
		for row in rows:
			dist = row[0]
		return dist

	def get_out_neighbors(self, node):
		query_1 = "SELECT id, target, cost FROM %s WHERE cost > 0 AND source = %s"
		query_2 = "SELECT id, source, reverse_cost FROM %s WHERE reverse_cost > 0 AND target = %s"
		neighbors = set()
		self.cur.execute(query_1, (AsIs(self.edge_table), node, ))
		rows = self.cur.fetchall()
		for row in rows:
			neighbors.add((row[0], row[1], row[2],))
		self.cur.execute(query_2, (AsIs(self.edge_table), node, ))
		rows = self.cur.fetchall()
		for row in rows:
			neighbors.add((row[0], row[1], row[2],))
		return list(neighbors)

	def get_in_neighbors(self, node):
		query_1 = "SELECT id, source, cost FROM %s WHERE cost > 0 AND target = %s"
		query_2 = "SELECT id, target, reverse_cost FROM %s WHERE reverse_cost > 0 AND source = %s"
		neighbors = set()
		self.cur.execute(query_1, (AsIs(self.edge_table), node, ))
		rows = self.cur.fetchall()
		for row in rows:
			neighbors.add((row[0], row[1], row[2],))
		self.cur.execute(query_2, (AsIs(self.edge_table), node, ))
		rows = self.cur.fetchall()
		for row in rows:
			neighbors.add((row[0], row[1], row[2],))
		return list(neighbors)


	def astar_path(self, source, target, level_column, reverse = False):
		#print source, target
		heuristic = self.euclidean_distance
		path = Path()
		explored_nodes = {}
		nodes_in_queue = {}
		#[node_level, total_dist, dist_from_source, node, parent_node, parent_edge]
		source_level = self.get_node_level(source, level_column)
		queue = [(source_level, 0, 0, source, None, None)]
		push = heappush
   		pop = heappop
		while queue:
			#print "queue", queue
			level, _, dist_from_source, currnode, parent_node, parent_edge = pop(queue)
			
			print "Exploring Node:"
			print "node: "+str(currnode)
			print "level: "+str(level)
			print "parent_node: "+str(parent_node)
			print "parent_edge: "+str(parent_edge)
			
			#if the currnode is target we backtrack using the parents and extract the path
			if currnode == target or self.get_node_level(currnode, level_column) == self.target_level:
				if reverse == True:
					node = currnode
					explored_nodes[node] = (parent_node, parent_edge)
					while explored_nodes[node][1] != None:
						path.add_entry({'node': node, 'edge': explored_nodes[node][1], 'cost': 0.0000})
						node = explored_nodes[node][0]
					path.add_entry({'node': node, 'edge': -1, 'cost': 0.0000})
				else:
					path.add_entry({'node': currnode, 'edge': -1, 'cost': 0.0000})
					node = currnode
					explored_nodes[node] = (parent_node, parent_edge)
					while explored_nodes[node][0] != None:
						path.add_entry({'node': explored_nodes[node][0], 'edge': explored_nodes[node][1], 'cost': 0.0000})
						node = explored_nodes[node][0]
						edge = explored_nodes[node][1]
					path.reverse_path()
				return path

			#if the current node is already explored we ignore it
			if currnode in explored_nodes:
				continue

			#we put the node into the set of explored nodes
			explored_nodes[currnode] = (parent_node, parent_edge)
			#print "Adding neighbors:"
			#if the current node is not explored we add the neighbors to the queue
			
			if reverse == True:
				to_explore = self.get_in_neighbors(currnode)
			else:
				to_explore = self.get_out_neighbors(currnode)

			#print "neighbors: ", to_explore
			for edge_id, neighbor, weight in to_explore:
				if neighbor in explored_nodes:
					continue;
				ncost = dist_from_source + weight;
				if neighbor in nodes_in_queue:
					qcost, h = nodes_in_queue[neighbor];
					if qcost <= ncost:
						continue;
				else:
					h = heuristic(neighbor, target);
				nodes_in_queue[neighbor] = ncost, h;
				#print "Pushing ", neighbor
				push(queue, (self.get_node_level(neighbor, level_column), ncost+h, ncost, neighbor, currnode, edge_id));
		return path


		

