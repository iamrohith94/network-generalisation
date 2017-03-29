import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
from shortest_path_class import *
db = "test_gachibowli"
sp = ShortestPath(db);
p = Path();
db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn;

d['table'] = table_v
num_vertices = get_count(d);

# Generating vertex pairs
print "Generating vertex pairs...."
d['num_pairs'] = 400;
d['level'] = 1;

pairs = generate_random_pairs(d);



#breaks = [2, 3, 5, 7, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]
breaks = [10, 20, 30, 40, 50]
results = {}

#Run shortest path for each pair
for p in breaks:
	print "Working on skeleton "+str(p)+"%"
	dist_diff = []
	actual_dist = []
	pair_count = 0;
	d['level_column'] = "promoted_level_"+str(p);
	d['level'] = 1;	
	for pair in pairs:
		print pair
		if pair_count > 200:
			break;
		d['source'] = pair[0];
		d['target'] = pair[1];
		orig_path = sp.get_original_path(d['source'], d['target']);
		if len(orig_path) == 0:
			continue;
		gen_path = sp.get_generalised_path(d['source'], d['target'], d['level_column']);
		gen_path_len = gen_path.get_path_cost();
		orig_path_len = orig_path.get_path_cost();
		dist_diff.append(gen_path_len-orig_path_len);
		actual_dist.append(orig_path_len);
		pair_count = pair_count + 1;
	plt.xlabel('Actual Distance');
	plt.ylabel('Difference in dist');
	print "num_samples: "+str(len(actual_dist));
	#plt.title(level_column+"\n equal_paths: "+str(equal)+"\n non_equal_paths: "+str(non_equal));
	plt.plot(actual_dist, dist_diff, 'ro')
	plt.axis([0, 0.01, 0, 0.01])
	plt.savefig('../images/result_level_'+str(p)+'.png',facecolor='white')
	plt.clf();

