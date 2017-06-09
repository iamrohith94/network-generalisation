# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10

@author: rohithreddy

This script calculates the level parameter of each edge 
for different interval sizes given a betweenness distribution and
stores it in the database.
"""

import math
import pandas as p
import numpy as np
from statistical_functions import *
from graphs import *
from common import *
db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn

#Fetching betweenness values
d['query'] = "SELECT betweenness FROM "+d['table_e'];
d['betweenness_values'] = [int(x[0]) for x in get_selected_columns(d) if x[0] > 0]

#Storing the betweenness values as a series
#s = p.Series(d['betweenness_values'])
s = sorted(d['betweenness_values'])
#s.sort(ascending = False)

#print "Series: ", s

#Generating intervals of different length
n_percentile = [10, 20, 30, 40, 50]
for n in n_percentile:
	temp = 100
	bucket_size = n
	intervals = []
	d["count_column"]="betweenness"
	d['level_column'] = "level_"+str(bucket_size);
	d['promoted_level_column'] = "promoted_level_"+str(bucket_size);
	print "Working on bucket_size of ", bucket_size

	#print "bucket_size: ", bucket_size

	while temp >= 0:
		#print "temp: ",temp
		#value = math.ceil(s.quantile(temp))
		value = math.ceil(np.percentile(s, temp))
		#print value
		intervals.append(value)
		temp = temp - n

	d['num_levels'] = len(intervals)


	#Reversing the intervals, descending order
	#intervals.reverse()


	# Adding the corresponding level column to the tables
	# level_column and promoted_level_column
	
	d['query'] = "ALTER TABLE "+table_e+" \
		ADD COLUMN "+d['level_column']+" BIGINT DEFAULT "+str(d['num_levels']+1);
	run_query(d);
	d['query'] = "ALTER TABLE "+table_e+" \
		ADD COLUMN promoted_"+d['level_column']+" BIGINT DEFAULT "+str(d['num_levels']+1)
	run_query(d);

	d['query'] = "ALTER TABLE "+table_v+" \
		ADD COLUMN "+d['level_column']+" BIGINT DEFAULT "+str(d['num_levels']+1)
	run_query(d);
	d['query'] = "ALTER TABLE "+table_v+" \
		ADD COLUMN promoted_"+d['level_column']+" BIGINT DEFAULT "+str(d['num_levels']+1)
	run_query(d);
	
	intervals.append(0);
	intervals[0] = intervals[0]+1
	#intervals.insert(0, max(d['betweenness_values'])+1)
	print "Intervals ", intervals

	
	#Updating the levels according to the intervals
	d['intervals'] = intervals;
	update_with_intervals(d);

conn.commit()
conn.close()