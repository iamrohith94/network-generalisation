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
 
fig,ax = plt.subplots()
cur = conn.cursor()
s = 121
 
query = "SELECT source,target,COUNT(*)  FROM time_stats where actual_distance > 2000 and actual_distance < 5000 GROUP BY source,target limit 30"
cur.execute(query)
rows = cur.fetchall()
for row in rows:
	level = []
	distance = []
	query = "SELECT contracted_time, level FROM time_stats where source = %s and target = %s ORDER BY level DESC"
	cur.execute(query, (row[0],row[1]))
	sd_rows = cur.fetchall()
	for sd in sd_rows:
		level.append(sd[1])
		distance.append(sd[0])
	c = numpy.random.rand(3,1)
	ax.scatter(distance, level, color= c)
	ax.plot(distance, level, color = c)
plt.xlabel('Time')
plt.ylabel('level')
plt.title("Chandigarh"+"\n : "+str(len(rows)) +" sd pairs "  )
# plt.axis([3000,7000 , 0, 120])
plt.show()

# levels = {100 : [], 50 : [], 40 :[], 30 : [], 20 : [], 10 : [] }
# diff_dist_level = {100 : [], 50 : [], 40 :[], 30 : [], 20 : [], 10 : [] }
# for row in rows:
# 	query = "SELECT contracted_distance, actual_distance, level FROM paths where source = %s and target = %s ORDER BY level DESC"
# 	cur.execute(query, (row[0],row[1]))
# 	sd_rows = cur.fetchall()
# 	for sd in sd_rows:
# 		levels[sd[2]].append(sd[1])
# 		diff_dist_level[sd[2]].append(((sd[0] - sd[1])/sd[1])*100)

# ll = [10, 20, 30, 40, 50, 100]
# # for l in ll:
# # 	c = numpy.random.rand(3,1)
# # 	ax.scatter( levels[l], diff_dist_level[l], color= c)
# # 	ax.plot( levels[l], diff_dist_level[l], color = c)
# l =30
# c = numpy.random.rand(3,1)
# ax.scatter( levels[l], diff_dist_level[l], color= c)
# # ax.plot( levels[l], diff_dist_level[l], color = c)

# plt.xlabel(' Distance')
# plt.ylabel('difference')
# plt.title("Gachibwoli"+"\n : "+str(len(rows)) +" sd pairs "  )
# plt.axis([0,10000 , 0, 50])
# plt.show()



