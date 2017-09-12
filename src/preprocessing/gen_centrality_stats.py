# -*- coding: utf-8 -*-
"""
Created on Tue May 10 14:17:22 2016

@author: rohithreddy

This script calculates the betweenness paramter for each edge and
stores it in the database.
"""
from graphs import *
from common import *
from plot_functions import *
db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["fraction"] = 2
d["table_e"] = table_e
d["table_v"] = table_v
d["column"] = "id";
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn
cur = conn.cursor()
# Generating edge count based on betweenness
print "Generating edge count...."
d["column"] = "id";
d['table'] = "contracted_ways_vertices_pgr";
d['num_vertices'] = get_count(d);
d['table'] = "contracted_ways";
d['num_edges'] = get_count(d);
d['num_pairs'] = long(d['fraction']*d['num_vertices']);

d["contracted_table_e"] = "contracted_ways"
d["contracted_table_v"] = "contracted_ways_vertices_pgr"

print "Number of vertex pairs: ", d['num_pairs']

sources, targets = generate_random_source_target(d)

print "Generated random pairs"


update_query = "UPDATE %s SET %s = %s + %s WHERE id = %s"

curr = 0
batch_size = 1000
while curr < d['num_pairs']:
	d['sources'] = sources[curr: curr+batch_size]
	d['targets'] = targets[curr: curr+batch_size]
	count_temp = generate_edge_count_m_m(d)
	for eid in count_temp.keys():
		cur.execute(update_query, (AsIs(table_e), AsIs("betweenness"), AsIs("betweenness"), count_temp[eid], eid, ))
	conn.commit()
	curr += 1000
	print "Done with ", curr

#Saving the betweenness distribution
d['betweenness_values'] = [c for c in d['count'].values() if c > 0]
d['column'] = 'betweenness'
betweenness_distribution(d)


conn.commit()
conn.close()


#Now our skeleton is ready






