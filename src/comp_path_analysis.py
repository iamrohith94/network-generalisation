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
levels = [10, 20, 30, 40, 50]

for level in levels:
	print "Working on level ", level
	d['level'] = level
	store_random_pairs_from_components(d)