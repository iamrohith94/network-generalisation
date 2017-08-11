import numpy as np
import matplotlib.pyplot as plt
from itertools import izip
import sys
import psycopg2

db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn;

N = 5
ind = np.arange(N)  # the x locations for the groups
width = 0.30       # the width of the bars
colors = ['g', 'b', 'r']
cur = conn.cursor()
levels = [10, 20, 30, 40, 50]
intervals = [0, 50, 100, 1000]
fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_ylabel('Frequency')
labels = ['JFVDNSV GFKB GVF ', 'Level-10',' Level-20','Level-30','Level-40','Level-50']
ax.xaxis.set_ticks_position('none') 
ax.xaxis.label.set_size(10)
ax.set_xticklabels(labels)
#ax.tick_params(axis='x', which='major', pad=15)
ax.tick_params(pad = 1)



for tick in ax.xaxis.get_majorticklabels():
    tick.set_horizontalalignment("left")


#ax.set_xlim(xmin=1)

#ax.xaxis.set_major_formatter(plt.NullFormatter())
#ax.set_xlabel('Level')
legend_colors = []
#ax.legend( (rects1[0], rects2[0], rects3[0]), ('0-50', '50-100', '>100') )
for x in zip(intervals,intervals[1:]):
	size_query = "SELECT count(*) FROM comp_freq_%s WHERE size >= %s AND size < %s"
	y_values = []
	for level in levels:
		cur.execute(size_query, (level, x[0], x[1]))
		rows = cur.fetchall()
		for row in rows:
			y_values.append(row[0])
	i = x[0]/50
	print i
	rect = ax.bar(ind+width*i, y_values, width, color=colors[i])
	if len(legend_colors) < 3:
		legend_colors.append(rect[0])

ax.legend(tuple(legend_colors), ('0-50', '50-100', '>100') )

plt.show()

"""
for ind in intervals:
	
"""


