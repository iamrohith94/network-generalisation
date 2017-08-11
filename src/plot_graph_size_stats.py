import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
import numpy as np
from math import floor
import matplotlib.patches as mpatches
db = sys.argv[1]
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn
threshold = 1000.00
cur = conn.cursor()
fig = plt.figure()
ax = fig.add_subplot(111)

colors = ['r', 'g', 'b', 'm', 'c']
interval_size = 5

def get_average_percent(averages, threshold, level): 
	print "level: ", level
	print "averages: ", averages
	#objects = ('0-10', '10-20', '20-30', '30-40', '40-50')
	objects = [str(x)+'-'+str(eval(str(x)+'+'+str(interval_size))) for x in xrange(0, 50, interval_size)]
	y_pos = np.arange(len(objects))
	ax.bar(y_pos, averages, align='center', color = colors[level/10-1])
	plt.xticks(y_pos, objects)
	
fig,ax = plt.subplots()
levels = [10, 20, 30, 40, 50]
intervals = [x for x in xrange(0, 50, interval_size)]
for level in levels:
	averages = {} 
	#create_query = "SELECT comp_id_%s, count(*) AS size INTO comp_freq_%s FROM %s GROUP BY comp_id_%s ORDER BY count(*)"	
	#cur.execute(create_query, (level, level, AsIs(d["table_v"]), level, ))
	if level == 10:
		max_percent = 15
	else:
		max_percent = level
	query = "SELECT ((contracted_distance - actual_distance)*100.00)/actual_distance, (contracted_graph_edges*100.00)/original_graph_edges FROM paths WHERE level = %s AND actual_distance >= %s AND (contracted_graph_edges*100.00)/original_graph_edges <= %s"
	cur.execute(query, (level, threshold, max_percent, ))
	rows = cur.fetchall()
	dist_diff = []
	graph_percent = []
	for row in rows:
		key = int(floor(row[1]) - floor(row[1])%5)
		try:
			averages[key].append(row[0])
		except KeyError:
			averages[key] = [row[0]]
	
	for key in averages.keys():
		print key, len(averages[key])
		averages[key] = np.mean(averages[key])

	for x in intervals:
		if x not in averages.keys():
			averages[x] = 0.00
	values = []
	for x in intervals:
		values.append(averages[x])
	get_average_percent(values, threshold, level)

plt.xlabel('Percentage of Graph')
plt.ylabel('Path Exactness Error')
#plt.title('Threshold: '+str(threshold))
plt.tick_params(axis='both', which='major', labelsize=7)
level_10 = mpatches.Patch(color='r', label='Level-10')
level_20 = mpatches.Patch(color='g', label='Level-20')
level_30 = mpatches.Patch(color='b', label='Level-30')
level_40 = mpatches.Patch(color='m', label='Level-40')
level_50 = mpatches.Patch(color='c', label='Level-50')

plt.legend(handles=[level_10, level_20, level_30, level_40, level_50])

plt.savefig('../images/'+db+'_graph_size_stats.png',facecolor='white')
print averages
conn.commit()
