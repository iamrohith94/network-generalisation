import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn;

cur = conn.cursor()
fig,ax = plt.subplots()
levels = [10, 20, 30, 40, 50]
for level in levels:
	#create_query = "SELECT comp_id_%s, count(*) AS size INTO comp_freq_%s FROM %s GROUP BY comp_id_%s ORDER BY count(*)"	
	#cur.execute(create_query, (level, level, AsIs(d["table_v"]), level, ))
	size_query = "SELECT size FROM comp_freq_%s"
	cur.execute(size_query, (level, ))
	rows = cur.fetchall()
	sizes = []
	freq = []
	for row in rows:
		sizes.append(row[0])
	plt.hist(sizes, bins = 50, rwidth = 0.8)
	plt.xlabel('Connected Component Size')
	plt.ylabel('Frequency')
	plt.title('Level '+str(level))
	plt.savefig('../images/comp_freq_'+str(level)+'.png',facecolor='white')
	plt.clf();
conn.commit()
