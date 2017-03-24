# -*- coding: utf-8 -*-
"""
@author: rohithreddy

This script contains statistical helper functions
"""
from common import *
import pandas as pd
def update_quantile(parameters):
	print "Updating quantile ranges...."
	conn = parameters['conn']
	cur = conn.cursor();
	ranges = pd.qcut(parameters['series'], parameters['level_count']);
	classes = {}
	temp = []
	ind = 1;
	for i in pd.value_counts(ranges).keys():
		s = '['
		s += i[1:-1]
		s += ']'
		temp.append(eval(s));
	temp = sorted(temp, key=lambda x: x[0], reverse = True)
	for i in temp:
		parameters['level'] = ind;
		parameters['level_vertices'] = [int(x) for x in i]
		parameters['promoted_level_column'] = "promoted_"+parameters['level_column'];
		populate_levels(parameters);
		ind = ind + 1;

def update_top_k(parameters):
	print "Updating ranges for "+str(parameters['k'])+"%....";
	conn = parameters['conn']
	cur = conn.cursor();
	num_levels = parameters['num_levels']
	bucket_size = parameters['bucket_size'];
	level_column = "level_"+str(parameters['k']);
	for level in xrange(1, num_levels+1):
		parameters['query'] = "SELECT id \
		FROM "+parameters['table_e']+" \
		WHERE "+level_column+" >= "+str(level)+" \
		AND "+parameters['count_column']+" > 0 \
		ORDER BY "+parameters['count_column']+" \
		DESC LIMIT "+str(bucket_size);
		level_edges = get_selected_columns(parameters);
		level_edges = [int(x[0]) for x in level_edges];
		print "level_edges", level_edges
		parameters['level'] = level;
		parameters['level_column'] = level_column
		parameters['level_edges'] = level_edges
		parameters['promoted_level_column'] = "promoted_"+level_column
		populate_edge_levels(parameters)
		#print level_edges;
		conn.commit()

def update_with_intervals(parameters):
	"""
	Updates the level column of the edge and vertex table given the interval ranges
	"""
	conn = parameters['conn']
	cur = conn.cursor()
	query = "UPDATE %s SET %s = %s, %s = %s WHERE %s >= %s AND %s < %s"
	curr_level = 1
	for start in xrange(0, len(parameters['intervals'])-1):
		end = start + 1
		#print start, end
		if parameters['intervals'][start] != parameters['intervals'][end]:
			cur.execute(query, (AsIs(parameters['table_e']), 
				AsIs(parameters['level_column']), curr_level, 
				AsIs(parameters['promoted_level_column']), curr_level,
				AsIs(parameters['count_column']), parameters['intervals'][end],
				AsIs(parameters['count_column']), parameters['intervals'][start], ))
			if cur.rowcount > 0:
				curr_level = curr_level + 1
	populate_vertex_levels(parameters)

	"""
	Update those edges where source vertex and target vertex are of level 1 but
	edge is not level 1
	"""
	skeleton_level = 1
	u_query = "UPDATE e SET e.%s = %s \
	FROM %s AS e, %s AS v1, %s AS v2 \
	WHERE v1.id = e.source AND v2.id = e.target AND v1.%s = %s AND v2.%s = %s"

	cur.execute(u_query, (AsIs(parameters['promoted_level_column']), skeleton_level, 
		AsIs(parameters['table_e']), AsIs(parameters['table_v']), AsIs(parameters['table_v']), 
		AsIs(parameters['promoted_level_column']), skeleton_level, 
		AsIs(parameters['promoted_level_column']), skeleton_level))
	conn.commit()


