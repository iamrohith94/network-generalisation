import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
import numpy as numpy
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
 
fig, ax = plt.subplots()
cur = conn.cursor()

d['is_max'] = True
max_dist = get_distance(d)
d['is_max'] = False
min_dist = get_distance(d)

interval_size = 5

ranges = np.arange(min_dist, max_dist, interval_size)

print "Max Distance: ", max_dist
print "Min Distance: ", min_dist

levels = [10, 20, 30, 40, 50]
for level in levels:
	avg_deviations = []
	for i in xrange(0, len(ranges)-1):
		query = "SELECT AVG(contracted_distance - actual_distance) \
		FROM paths WHERE actual_distance >= %s AND actual_distance <= %s \
		AND level = %s"
		cur.execute(query, (ranges[i], ranges[i+1], level))
		rows = cur.fetchall()
		for row in rows:
			avg_deviations.append(row[0])
	print ranges
	print avg_deviations
	ax.plot(ranges, avg_deviations, color = 'blue')
	plt.xlabel('Distance')
	plt.ylabel('Deviation')
	plt.title("Chandigarh")
	fig.savefig('../images/level_'+str(level))
plt.show()

