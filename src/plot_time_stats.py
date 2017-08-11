import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
import numpy as np
db = sys.argv[1]
import matplotlib.patches as mpatches

table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn
threshold = 0001.00
cur = conn.cursor()


levels = [10, 20, 30, 40, 50]
#dist_query = "SELECT actual_distance FROM paths WHERE level = %s AND source = %s AND target = %s AND actual_distance != 0 AND actual_distance < 4000"
exec_time_query = "SELECT actual_build_time, contracted_build_time, actual_avg_execution_time, contracted_avg_execution_time FROM time_stats WHERE level = %s AND source = %s AND target = %s"
#build_time_query = "SELECT actual_build_time, contracted_build_time FROM time_stats WHERE level = %s AND source = %s AND target = %s"


query = "SELECT source, target, actual_distance FROM paths WHERE level = %s  AND actual_distance != 0"# AND actual_distance < 12000"

bucket_size = 1000

main_time = False

colors = ['r', 'g', 'y', 'm', 'c']
fig,ax = plt.subplots()
for level in levels:
	distance_buckets = {}
	actual_avg_execution_bucket = {}
	contracted_avg_execution_bucket = {}
	actual_build_times = []
	contracted_build_times = []
	
	cur.execute(query, (level, ))
	rows = cur.fetchall()

	for row in rows:
		bucket = int(row[2]/bucket_size)
		print bucket
		cur.execute(exec_time_query, (level, row[0], row[1], ))
		times = cur.fetchall()
		for time in times:
			try:			
				actual_avg_execution_bucket[bucket].append(time[2])
			except KeyError:
				actual_avg_execution_bucket[bucket] = [time[2]]

			try:			
				contracted_avg_execution_bucket[bucket].append(time[3])
			except KeyError:
				contracted_avg_execution_bucket[bucket] = [time[3]]

	for key in actual_avg_execution_bucket.keys():
		actual_avg_execution_bucket[key] = np.mean(actual_avg_execution_bucket[key])

	for key in contracted_avg_execution_bucket.keys():
		contracted_avg_execution_bucket[key] = np.mean(contracted_avg_execution_bucket[key])

	print "Distances size: ", len(actual_avg_execution_bucket.keys())
	#del actual_avg_execution_bucket[10]
	#del contracted_avg_execution_bucket[10]
	#print "build times size: ", len(actual_build_times)

	if not main_time:
		main_time = True
		ax.plot(actual_avg_execution_bucket.keys(), actual_avg_execution_bucket.values(), color= 'blue')

	ax.plot(contracted_avg_execution_bucket.keys(), contracted_avg_execution_bucket.values(), color= colors[(level/10)-1])



level_10 = mpatches.Patch(color='r', label='Level-10')
level_20 = mpatches.Patch(color='g', label='Level-20')
level_30 = mpatches.Patch(color='y', label='Level-30')
level_40 = mpatches.Patch(color='m', label='Level-40')
level_50 = mpatches.Patch(color='c', label='Level-50')


plt.legend(handles=[level_10, level_20, level_30, level_40, level_50], prop={'size':8})


#ax.legend(tuple(colors), ('Level-10', 'Level-20', 'Level-30', 'Level-40', 'Level-50') )
#plt.title('Time stats at level: '+str(level))
plt.xlabel('Distance')
plt.ylabel('Average Time Taken for path computation')
plt.savefig('../images/'+db+'_time_stats.png',facecolor='white')
conn.commit()
