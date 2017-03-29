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



equal = 0;
non_equal = 0;

# Generating vertexx pairs
print "Generating vertex pairs...."
d['num_pairs'] = 1;
actual_path_costs = [];
diff_cost = [];

d['level_column'] = "promoted_level_10";
d['level'] = 1;
#pairs = generate_random_level_pairs(d);
level_column = "promoted_level_20";
pairs = [[725, 794]]
for pair in pairs:
	print pair
	print sp.get_generalised_path(pair[0], pair[1], level_column)
	"""
	orig_dist = sp.get_original_path(pair[0], pair[1]).get_path_cost();
	g_dist = sp.get_path_on_skeleton(pair[0], pair[1], level_column).get_path_cost();
	if orig_dist > 0.000 and g_dist > 0.00:
		actual_path_costs.append(orig_dist);
		diff_cost.append(g_dist - orig_dist);
		if g_dist - orig_dist <= 0.0001:
			equal = equal + 1;
		else:
			non_equal = non_equal + 1;
	"""
"""
plt.xlabel('Actual Distance')
plt.ylabel('Approximate Distance - Actual Distance')
plt.title(level_column+"\n equal_paths: "+str(equal)+"\n non_equal_paths: "+str(non_equal));
plt.plot(actual_path_costs, diff_cost, 'ro')
plt.axis([0, 0.011, 0, 0.011])
plt.show()
"""