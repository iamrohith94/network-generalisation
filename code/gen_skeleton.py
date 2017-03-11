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
breaks = [10, 15, 20, 25, 30, 35, 40, 45, 50]
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

for k in breaks:
	#Making the initial skeleton strongly connected
	print "Making level "+str(k)+"th skeleton strongly connected....."
	d['level_column'] =  "level_"+str(k);
	d['promoted_level_column'] = "promoted_"+d['level_column'];
	path_additions = connect_components(d);
	d['path_additions'] = path_additions;

	#Populating the promoted vertex and edge levels
	update_level_skeleton(d);
	
	