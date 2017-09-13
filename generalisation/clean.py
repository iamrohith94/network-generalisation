#Cleaning the OSM data by removing small disconnectivities in the network  
import psycopg2
import networkx as nx
from common.graph_functions import edge_table_to_graph
from common.db_functions import get_count
import sys

def clean_data(parameters):
    """
    Cleans the original data, by removing small disconnectivities
    Keeps only the most strongly connected component
    Considers the graph as undirected even if it is directed
    Stores the cleaned data into cleaned_ways and cleaned_ways_vertices_pgr tables
    """ 
    db = parameters['db']
    conn = parameters['conn']
    cur = conn.cursor();
    cleaned_table_e = "cleaned_"+parameters['table_e'];
    cleaned_table_v = "cleaned_"+parameters['table_v'];
    parameters['query'] = "SELECT source, target, cost, reverse_cost FROM "+parameters['table_e'];
    
    #Converting edge table to networkX graph
    parameters['directed'] = False;
    G = edge_table_to_graph(parameters);

    #print "Original Graph"
    #print G.edges()

    #Finding connected components in the graph
    cc =  sorted(nx.connected_components(G), key = len, reverse = True)
    
    #Selecting the largest cc and removing others
    removal = set([]);
    for s in cc[1:]:
        removal = removal.union(s)
    removal = list(removal);

    #print "Removed Vertices"
    #print removal
    
    #Inserting largest cc into cleaned_ways table
    e_query = "INSERT INTO "+cleaned_table_e+"(\
    id,source,target,x1,y1,x2,y2,cost,reverse_cost,the_geom) \
    SELECT gid AS id, source,\
    target, x1, y1, x2, y2, cost, reverse_cost, the_geom \
    FROM " +parameters['table_e']+" WHERE \
    source <> ALL(%s) AND \
    target <> ALL(%s) ;"

    #Inserting largest cc into cleaned_ways_vertices table
    v_query = "INSERT INTO "+cleaned_table_v+"(\
    id,lon,lat,the_geom) \
    SELECT id,lon,lat, the_geom\
     FROM "+parameters['table_v']+" WHERE id <> ALL(%s);"
    
    cur.execute(e_query, (removal, removal));
    cur.execute(v_query, (removal,));
    conn.commit()

if __name__ == '__main__':

    d = {}
    database = sys.argv[1];
    d['db'] = database;
    d['table_e'] = "ways";
    d['table_v'] = "ways_vertices_pgr";
    conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")
    d['conn'] = conn


    d['table'] = 'ways'
    print "Initial edge count: " + str(get_count(d));

    d['table'] = 'ways_vertices_pgr'
    print "Initial vertex count: " + str(get_count(d));

    print "Cleaning data......."
    clean_data(d);

    d['table'] = 'cleaned_ways'
    print "Edge count after Cleaning: " + str(get_count(d));

    d['table'] = 'cleaned_ways_vertices_pgr'
    print "Vertex count after Cleaning: " + str(get_count(d));

    conn.close();