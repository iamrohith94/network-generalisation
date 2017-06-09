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
d['num_pairs'] = 1000
cur = conn.cursor()

#store_random_pairs_into_table(d)

equal = 0;
non_equal = 0;
limit = 1000
pairs_sql = "SELECT source, target FROM random_pairs LIMIT %s"
time_sql = "SELECT source, target, build_time, avg_execution_time FROM pgr_conn_compQuery_time(%s, %s, %s, %s, %s)"
insert_sql = "INSERT INTO %s(source, target, level, contracted_build_time, contracted_avg_execution_time) VALUES(%s, %s, %s, %s, %s)"

cur.execute(pairs_sql, (limit, ))

pairs = cur.fetchall()
sources = []
targets = []
count = 0
for pair in pairs:
	count += 1
	if count % 100 == 0:
		print count
		#print sources
		#print targets
		levels = [10, 20, 30, 40, 50]
		for level in levels:
			cur.execute(time_sql, (table_e, table_v, sources, targets, level))
			rows = cur.fetchall()
			for row in rows:
				cur.execute(insert_sql, (AsIs("time_stats"), row[0], row[1], level, row[2], row[3]))
		conn.commit()
	else:
		sources.append(pair[0])
		targets.append(pair[1])
conn.commit()
