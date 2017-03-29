# -*- coding: utf-8 -*-
"""
@author: rohithreddy

This script makes the skeletons of different levels
strongly connected by promoting some edges and vertices
"""
from graphs import *
from common import *
from connected_components import *
db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'
breaks = [10, 20, 30, 40, 50]
d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn

#All edges and vertices whose level <= skeleton_level are included in the skeleton
#skeleton_level = 1
d['level'] = 1;
d['directed'] = True;

for k in breaks[0:1]:
	#Making the initial skeleton strongly connected
	print "Making level "+str(k)+"th skeleton strongly connected....."
	d['level_column'] =  "level_"+str(k);
	d['promoted_level_column'] = "promoted_"+d['level_column'];
	path_additions = connect_components(d);
	d['path_additions'] = path_additions;

	#Populating the promoted vertex and edge levels
	update_level_skeleton(d);


update_query = "UPDATE %s SET promoted_level_%s = %s \
WHERE id IN (SELECT e.id FROM %s AS e WHERE e.promoted_level_%s < e.level_%s AND e.promoted_level_%s = 1);"
cur = conn.cursor()
for index, curr_level in enumerate(breaks[1:]):
	
	#Adding the promoted_edges of the curr_skeleton to the next skeleton
	#Adding promoted edges of skeleton_10 to skeleton_20 and so on .....
	prev_level = breaks[index]
	print "curr_level: ", curr_level
	print "prev_level: ", prev_level
	cur.execute(update_query, (AsIs(d['table_e']),
		curr_level, 1, 
		AsIs(d['table_e']),
		prev_level, prev_level, prev_level,
		))
	#print cur.rowcount


	#Making the initial skeleton strongly connected
	print "Making level "+str(curr_level)+"th skeleton strongly connected....."
	d['level_column'] =  "level_"+str(curr_level);
	d['promoted_level_column'] = "promoted_"+d['level_column'];
	path_additions = connect_components(d);
	d['path_additions'] = path_additions;

	#Populating the promoted vertex and edge levels
	update_level_skeleton(d);

conn.commit()
conn.close()
	