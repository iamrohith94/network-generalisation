import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
db = sys.argv[1] 
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

sp = ShortestPath(db) 

d = {}
d["db"] = db 
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn 

cur = conn.cursor()

# Generating vertexx pairs
print "Generating vertex pairs...."
d['num_pairs'] = 100
pairs = generate_random_pairs(d) 
levels = [10, 20, 30, 40, 50]
for pair in pairs:
	path_query = "INSERT INTO %s(source, target, level, actual_time, contracted_time, actual_distance, contracted_distance) \
	VALUES(%s, %s, %s, %s, %s, %s, %s)"
	#print "source, target " , pair[0], pair[1]
	orig_dist = sp.get_original_path(pair[0], pair[1]).get_path_cost()*111*1000
	orig_time = sp.time_original_path
	cur.execute(path_query, (AsIs("time_stats"), pair[0], pair[1], 100, orig_time, orig_time, orig_dist, orig_dist))
	for level in levels:
		#print "level: ", level
		gen_dist = sp.get_generalised_path(pair[0], pair[1], "promoted_level_" + str(level)).get_path_cost()*111*1000
		gen_time = sp.time_gen_path
		#print "Path: ", g_path
		cur.execute(path_query, (AsIs("time_stats"), pair[0], pair[1], level, orig_time, gen_time,orig_dist, gen_dist))

conn.commit()