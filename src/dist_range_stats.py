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
d['is_max'] = True
max_dist = get_distance(d)
d['is_max'] = False
min_dist = get_distance(d)

interval_size = 5

print "Max Distance: ", max_dist
print "Min Distance: ", min_dist

ranges = np.arange(min_dist, max_dist, interval_size)
d['num_pairs'] = 10
pairs = []

print "Generating vertex pairs...."
for i in xrange(0, len(ranges)-1):
	d['range'] = [ranges[i], ranges[i+1]]
	for pair in generate_random_pairs_dist(d):
		pairs.append(pair)

actual_path_costs = []
diff_cost = []
levels = [10, 20, 30, 40, 50]

query = "INSERT INTO %s(source, target, level, actual_distance, contracted_distance) VALUES(%s, %s, %s, %s, %s)"

for pair in pairs:		
	orig_dist = sp.get_original_path(pair[0], pair[1]).get_path_cost()*111
	cur.execute(query, (AsIs("paths"), pair[0], pair[1], 100, orig_dist, orig_dist))

for level in levels:
	print "level: ", level
	for pair in pairs:		
		orig_dist = sp.get_original_path(pair[0], pair[1]).get_path_cost()*111
		g_dist = sp.get_generalised_path(pair[0], pair[1], "promoted_level_" + str(level)).get_path_cost()*111
		cur.execute(query, (AsIs("paths"), pair[0], pair[1], level, orig_dist, g_dist))
conn.commit()
