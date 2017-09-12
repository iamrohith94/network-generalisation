import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

sp = ShortestPath(db);

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn;
d['num_pairs'] = 1000
cur = conn.cursor()

store_random_pairs(d)


equal = 0;
non_equal = 0;
limit = 1000
pairs_sql = "SELECT source, target FROM random_pairs LIMIT %s"

cur.execute(pairs_sql, (limit, ))

pairs = cur.fetchall()
random_pairs = []

for pair in pairs:
	random_pairs.append([pair[0], pair[1]])


levels = [10, 20, 30, 40, 50]
original_graph_size = get_graph_size(d)
query = "INSERT INTO %s(source, target, level, actual_distance, contracted_distance, original_graph_edges, "\
		"original_graph_vertices, contracted_graph_edges, contracted_graph_vertices) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
count = 1
for pair in random_pairs:
	#print pair
	if count%100 == 0:
		conn.commit()
		print count
	orig_path = sp.get_original_path(pair[0], pair[1])
	orig_edges = orig_path.get_edge_set()
	orig_dist = orig_path.get_path_cost()*111*1000
	cur.execute(query, (AsIs("paths"), pair[0], pair[1], 100, orig_dist, orig_dist, original_graph_size[0], original_graph_size[1],
		original_graph_size[0], original_graph_size[1],))
#geom_query = "UPDATE %s SET %s = (SELECT ST_UNION(edge_table.the_geom) FROM %s AS edge_table WHERE edge_table.id = ANY(%s)) WHERE source = %s AND target = %s AND level = %s"
	for level in levels:
		#print "Working on level: ", level
		#print pair
		#print "source===,target " , pair[0], pair[1]
		d['level'] = level
		g_path = sp.get_connected_comp_path(pair[0], pair[1], level, d['conn'])
		g_edges = g_path.get_edge_set()
		g_dist = g_path.get_path_cost()*111*1000
		skeleton_size = get_skeleton_size_by_level(d)
		d['vertex'] = pair[0]
		comp_info_source = get_comp_size(d)
		d['vertex'] = pair[1]
		comp_info_target = get_comp_size(d)
		source_comp_id = comp_info_source[2]
		target_comp_id = comp_info_target[2]
		if source_comp_id == target_comp_id:
			contracted_graph_edges = comp_info_source[0]+skeleton_size[0]
			contracted_graph_vertices = comp_info_source[1]+skeleton_size[1]
		else:
			contracted_graph_edges = comp_info_source[0]+comp_info_target[0]+skeleton_size[0]
			contracted_graph_vertices = comp_info_source[1]+comp_info_target[1]+skeleton_size[1]	
		cur.execute(query, (AsIs("paths"), pair[0], pair[1], level, orig_dist, g_dist, original_graph_size[0], original_graph_size[1], 
			contracted_graph_edges, contracted_graph_vertices, ))
	count += 1
conn.commit()
