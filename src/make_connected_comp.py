#assigninig unique id to disconnected componennts
from common import *
from graphs import *
import sys
parameters = {} ;
db = sys.argv[1]
cleaned_table_e = "cleaned_ways";
cleaned_table_v = "cleaned_ways_vertices_pgr";

parameters['db'] = db ;
parameters['table_e'] = "cleaned_ways";
parameters['table_v'] = "cleaned_ways_vertices_pgr";

conn = psycopg2.connect(database=db, user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
cur = conn.cursor()
parameters['conn'] = conn


for level in [10, 20, 30, 40, 50]:

	parameters['query'] = "SELECT source, target, cost, reverse_cost FROM cleaned_ways where promoted_level_"+ str(level) + " > 1 ;";
	parameters['directed'] = True;

	G = edge_table_to_graph(parameters);
	cc =  sorted(nx.strongly_connected_components(G), key = len, reverse = True);
	#Selecting the largest cc and removing others
	comp_id = 1;
	for s in cc: 
		
		vertex_list = list(s);

		e_query = "UPDATE "+cleaned_table_e+"\
		SET comp_id_"+ str(level) + "=" + str(comp_id) + "\
		WHERE source = ANY(%s) AND target = ANY(%s) AND promoted_level_" + str(level) +" > 1 ;"

		v_query = "UPDATE "+cleaned_table_v+"\
		SET comp_id_"+ str(level) + "=" + str(comp_id) + "\
		WHERE id = ANY(%s) AND promoted_level_" + str(level) +" >1 ;"

		cur.execute(e_query, (vertex_list, vertex_list));
		cur.execute(v_query, (vertex_list,));
		conn.commit();
		comp_id += 1;

	e_query = "UPDATE "+ cleaned_table_e + "  SET comp_id_"+str(level)+"=0 WHERE promoted_level_" + str(level)+" <= 1;" ;
	v_query = "UPDATE "+ cleaned_table_v + "  SET comp_id_"+str(level)+"=0 WHERE promoted_level_" + str(level)+" <= 1;" ;
	cur.execute(e_query);
	cur.execute(v_query);
	conn.commit();

conn.close();