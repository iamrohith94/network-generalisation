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
conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn;

cur = conn.cursor()

equal = 0;
non_equal = 0;

# Generating vertexx pairs
print "Generating vertex pairs...."
d['num_pairs'] = 2000;
actual_path_costs = [];
diff_cost = [];

d['level_column'] = "promoted_level_10";
d['level'] = 1;
pairs = generate_random_pairs(d);
level_column = "promoted_level_20";
levels = [10,20,30,40,50]
for pair in pairs:
	query = "INSERT INTO %s(source, target, level, actual_distance, contracted_distance) VALUES(%s, %s, %s, %s, %s)"
	geom_query = "UPDATE %s SET %s = (SELECT ST_UNION(edge_table.the_geom) FROM %s AS edge_table WHERE edge_table.id = ANY(%s)) \
	WHERE source = %s AND target = %s AND level = %s"
	#print pair
	print "source===,target " , pair[0], pair[1]
	orig_path = sp.get_original_path(pair[0], pair[1])
	orig_edges = orig_path.get_edge_set()
	if len(orig_edges) <= 2 :
		continue
	orig_dist = orig_path.get_path_cost()*111*1000
	cur.execute(query, (AsIs("paths"), pair[0], pair[1], 100, orig_dist, orig_dist))
	cur.execute(geom_query, (AsIs("paths"), AsIs("the_geom"), AsIs(table_e), list(orig_edges), pair[0], pair[1], 100))
	for level in levels:
		print "level: ", level
		g_path = sp.get_generalised_path(pair[0], pair[1], "promoted_level_" + str(level))
		print "Path: ", g_path
		g_edges = g_path.get_edge_set()
		if len(g_edges) <= 1:
			continue
		g_dist = g_path.get_path_cost()*111*1000
		cur.execute(query, (AsIs("paths"), pair[0], pair[1], level, orig_dist, g_dist))
		print list(g_edges)
		cur.execute(geom_query, (AsIs("paths"), AsIs("the_geom"), AsIs(table_e), list(g_edges), pair[0], pair[1], level))

conn.commit()
