# -*- coding: utf-8 -*-
"""
@author: rohithreddy

This script contains statistical helper functions
"""
from db_functions import populate_vertex_levels
from db_functions import AsIs

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


