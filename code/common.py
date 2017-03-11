    
import psycopg2
import networkx as nx
import sys
import random
import math
from graphs import *
from psycopg2.extensions import AsIs
"""
Gives count of the number of rows of a table in a database
"""
def get_count(parameters):
    db=parameters['db']
    table = parameters['table']
    conn = parameters['conn']    
    cur = conn.cursor()
    cur.execute("SELECT count(*) from "+table)
    rows = cur.fetchall()
    for row in rows:
        count = row[0]
    return count

def run_query(parameters):
    """Executes the given query"""
    conn = parameters['conn']    
    cur = conn.cursor()
    cur.execute(parameters['query'])
    conn.commit();

def get_column_values(parameters):
    table=parameters['table']
    column = parameters['column']
    ret = []
    conn = parameters['conn']    
    cur = conn.cursor()
    cur.execute("SELECT "+column+" FROM "+table)
    rows = cur.fetchall()
    for row in rows:
        ret.append(row[0]);
    return ret

def get_selected_columns(parameters):
    """Returns the result of the given query as a list of lists"""
    query = parameters['query']
    ret = []
    conn = parameters['conn']    
    cur = conn.cursor()
    cur.execute(query)
    rows=cur.fetchall()
    for row in rows:
        ret.append(row);
    return ret

def update_column(parameters):
    """
    Updates the column given the column values
    """
    count = parameters['count']
    conn = parameters['conn']    
    cur = conn.cursor()
    query = "UPDATE %s SET %s = %s WHERE id = %s";
    for eid in count.keys():
        cur.execute(query, (AsIs(parameters['table']), AsIs(parameters['column']), count[eid], eid));
    conn.commit()
    
def generate_vertex_count(parameters):
    f = parameters['fraction'];
    db = parameters['db']
    conn = parameters['conn']    
    cur = conn.cursor()
    print "Number of vertex pairs: "+str(parameters['num_pairs']);
    count={}
    vtab={}
    vtab["db"] = db;
    vtab["table"] = parameters['table_v'];
    vtab["column"] = parameters['column'];
    vtab['conn'] = conn;
    vertices = get_column_values(vtab);
    for x in vertices:
        count[int(x)] = 0;

    inner = 'SELECT id, source, target, cost, reverse_cost \
    FROM '+parameters['table_e']+' WHERE is_contracted = FALSE';
    dijkstra_query = "SELECT node from pgr_dijkstra(%s,%s,%s)";
    random_query = "SELECT t1.id, t2.id \
    FROM %s as t1, %s AS t2 \
    ORDER BY random() LIMIT %s;"
    cur.execute(random_query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), parameters['num_pairs'],));
    pairs = cur.fetchall();
    ind = 1;
    for pair in pairs:
        #print ind
        ind = ind+1
        s=pair[0]
        t=pair[1]
        cur.execute(dijkstra_query, (inner, s, t ));
        rows = cur.fetchall()
        for row in rows:
            vid=int(row[0])
            #print eid  
            if vid != -1:
                count[vid]=count[vid]+1
    #print count
    return count

def generate_edge_count(parameters):
    """
    Runs shortest path on random (source, target) pairs and returns
    a dictionary of edges and their count(betweenness)
    """ 
    f = parameters['fraction'];
    db = parameters['db']
    conn = parameters['conn']    
    cur = conn.cursor()
    print "Number of vertex pairs: "+str(parameters['num_pairs']);
    count = {}

    #Fetching edge ids from the edge table
    etab = {}
    etab["db"] = db;
    etab["table"] = parameters['table_e'];
    etab["column"] = parameters['column'];
    etab['conn'] = conn;
    edges = get_column_values(etab);

    #Initialising their counts to 0
    for x in edges:
        count[int(x)] = 0;

    #Inner query for dijkstra -> contracted graph is taken 
    inner = 'SELECT id, source, target, cost, reverse_cost \
    FROM '+parameters['table_e']+' WHERE is_contracted = FALSE';

    
    dijkstra_query = "SELECT edge from pgr_dijkstra(%s, %s, %s)";
    
    #Fetches vertex pairs at random
    random_query = "SELECT t1.id, t2.id \
    FROM %s as t1, %s AS t2 \
    ORDER BY random() LIMIT %s;"
    
    cur.execute(random_query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), parameters['num_pairs'],));
    pairs = cur.fetchall();
    ind = 1;
    
    for pair in pairs:
        #print ind
        ind = ind+1
        s = pair[0]
        t = pair[1]
        cur.execute(dijkstra_query, (inner, s, t ));
        rows = cur.fetchall()
        for row in rows:
            eid = int(row[0])
            #print eid  
            if eid != -1:
                count[eid] = count[eid]+1
    #print count
    return count

def generate_random_pairs(parameters):
    db = parameters['db']
    conn = parameters['conn']    
    cur = conn.cursor()
    random_query = "SELECT t1.id, t2.id \
    FROM %s as t1, %s AS t2 \
    ORDER BY random() LIMIT %s;"
    cur.execute(random_query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), parameters['num_pairs'],));
    rows = cur.fetchall();
    pairs = [];
    for row in rows:
        pairs.append((row[0], row[1]));
    return pairs

def generate_random_level_pairs(parameters):
    db = parameters['db']
    conn = parameters['conn']
    level_column = parameters['level_column'];
    level = parameters['level']    
    cur = conn.cursor()
    random_query = "SELECT t1.id, t2.id \
    FROM %s as t1, %s AS t2 \
    WHERE t1.%s <= %s AND t2.%s <= %s \
    ORDER BY random() LIMIT %s;"
    cur.execute(random_query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), 
        AsIs(parameters['level_column']), parameters['level'], 
        AsIs(parameters['level_column']), parameters['level'],
        parameters['num_pairs']));
    rows = cur.fetchall();
    pairs = [];
    for row in rows:
        pairs.append((row[0], row[1]));
    return pairs



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

    print "Original Graph"
    print G.edges()

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

    

def update_promoted_level(parameters):
    n1 = parameters['source'];
    n2 = parameters['target'];
    table_e = parameters['table_e'];
    table_v = parameters['table_v'];
    original_edge_table = parameters['original_edge_table']
    level = parameters['level'];
    db = parameters['db'];
    conn = parameters['conn']
    cur = conn.cursor()
    inner = 'SELECT id,source,target,cost, reverse_cost FROM '+original_edge_table;
    cur.execute("SELECT node, edge from pgr_dijkstra(%s,%s,%s)", (inner, n1, n2 ));
    nodes = []
    edges = []
    rows = cur.fetchall()
    for row in rows:
        nodes.append(str(row[0]))
        edges.append(str(row[1]))
    e_query = "UPDATE "+table_e+"\
    SET promoted_level = "+str(level)+"\
    WHERE id = ANY(ARRAY"+str(edges)+"::BIGINT[]);"

    v_query = "UPDATE "+table_v+"\
    SET promoted_level = "+str(level)+"\
    WHERE id = ANY(ARRAY"+str(nodes)+"::BIGINT[]);"

    cur.execute(e_query);
    cur.execute(v_query);
    conn.commit();



def get_promoted_edges(parameters):
    n1 = parameters['source'];
    n2 = parameters['target'];
    table_e = parameters['table_e'];
    table_v = parameters['table_v'];
    original_edge_table = parameters['original_edge_table']
    level = parameters['level'];
    db = parameters['db'];
    conn = parameters['conn']
    cur = conn.cursor()
    inner = 'SELECT id,source,target,cost, reverse_cost FROM '+original_edge_table;
    cur.execute("SELECT node, edge from pgr_dijkstra(%s,%s,%s)", (inner, n1, n2 ));
    nodes = set()
    edges = set()
    rows = cur.fetchall()
    for row in rows:
        nodes.add(str(row[0]))
        edges.add(str(row[1]))
    return (nodes, edges);

def get_nearest_node(parameters):
    poi = parameters['poi'];
    node_set = list(parameters['nodes']);
    table = parameters['table_v'];
    db = parameters['db'];
    conn = parameters['conn']
    cur = conn.cursor()
    poi_query = "SELECT the_geom FROM "+table+" WHERE id = "+str(poi);
    cur.execute(poi_query);
    rows = cur.fetchall()
    for row in rows:
        poi_geom = row[0]
    query = "SELECT id, ST_Distance(the_geom, %s) FROM "+table+" \
    WHERE ST_DWithin(the_geom, %s, 100000)\
    AND id = ANY(%s)\
    ORDER BY ST_Distance(the_geom, %s)\
    LIMIT 1;"
    cur.execute(query, (poi_geom, poi_geom, node_set, poi_geom));
    rows = cur.fetchall()
    for row in rows:
        nearest_node = row[0]
        nearest_dist = row[1]
    return nearest_node, nearest_dist;

def get_nearest_node_in_table(parameters):
    poi = parameters['poi'];
    table = parameters['table'];
    db = parameters['db'];
    conn = parameters['conn']
    cur = conn.cursor()
    parameters['column'] = 'id';
    node_set = get_column_values(parameters);
    poi_query = "SELECT the_geom FROM "+table+" WHERE id = "+str(poi);
    cur.execute(poi_query);
    rows = cur.fetchall()
    for row in rows:
        poi_geom = row[0]
    query = "SELECT id, ST_Distance(the_geom, %s) FROM "+table+" \
    WHERE ST_DWithin(the_geom, %s, 100000)\
    AND id = ANY(%s)\
    ORDER BY ST_Distance(the_geom, %s)\
    LIMIT 1;"
    cur.execute(query, (poi_geom, poi_geom, node_set, poi_geom));
    rows = cur.fetchall()
    for row in rows:
        nearest_node = row[0]
        nearest_dist = row[1]
    return nearest_node, nearest_dist;


def populate_edge_levels(parameters):
    """
    Updates the level column in the edge table of a given set of edges
    """
    conn = parameters['conn']
    cur = conn.cursor()
    update_query = "UPDATE %s SET %s \
        = %s, %s = %s WHERE id = ANY(%s)"
    cur.execute(update_query, 
        (AsIs(parameters['table_e']), AsIs(parameters['level_column']), parameters['level'], 
             AsIs(parameters['promoted_level_column']), parameters['level'],
            parameters['level_edges'], ))
    conn.commit()

def populate_vertex_levels(parameters):
    """
    Updates the level column in the vertex table.
    Assigns vertex the minimum level of the edges joining it
    """
    conn = parameters['conn']
    cur = conn.cursor()
    update_query = "UPDATE %s SET %s = \
    (SELECT MIN(e.%s) FROM %s AS e WHERE e.source = %s.id OR e.target = %s.id) \
    WHERE %s > (SELECT MIN(e.%s) FROM %s AS e WHERE e.source = %s.id OR e.target = %s.id)";
    cur.execute(update_query, 
        (AsIs(parameters['table_v']), AsIs(parameters['level_column']), 
        AsIs(parameters['level_column']), AsIs(parameters['table_e']),
        AsIs(parameters['table_v']), AsIs(parameters['table_v']), 
        AsIs(parameters['level_column']),
        AsIs(parameters['level_column']), AsIs(parameters['table_e']),
        AsIs(parameters['table_v']), AsIs(parameters['table_v']), ));
    conn.commit();

def populate_levels(parameters):
    conn = parameters['conn']
    cur = conn.cursor();
    update_query = "UPDATE %s SET %s \
        = %s, %s = %s WHERE id = ANY(%s)";
    cur.execute(update_query, 
        (AsIs(parameters['table_v']), AsIs(parameters['level_column']), parameters['level'], 
             AsIs(parameters['promoted_level_column']), parameters['level'],
            parameters['level_vertices'], ));
    update_query = "UPDATE %s SET %s = %s, %s = %s \
        WHERE source = ANY(%s) AND target = ANY(%s)";
    cur.execute(update_query, 
        (AsIs(parameters['table_e']), AsIs(parameters['level_column']), parameters['level'], 
            AsIs(parameters['promoted_level_column']), parameters['level'],
            parameters['level_vertices'], parameters['level_vertices'], ));
    conn.commit();


def get_matching_node_count(parameters):
    conn = parameters['conn']
    cur = conn.cursor();
    inner = 'SELECT id,source,target,cost, reverse_cost FROM '+parameters['table_e'];
    cur.execute("SELECT node, cost from pgr_dijkstra(%s, %s, %s)", (inner, parameters['source'], parameters['target']));
    rows = cur.fetchall();
    nodes = [];
    cost = 0.00000;
    fraction = 0.000;
    count = 0;
    matched = 0;
    for row in rows:
        nodes.append(row[0]);
        cost = cost + row[1];
        count = count + 1
    query = "SELECT count(*) FROM %s WHERE %s = %s AND id = ANY(%s)";
    cur.execute(query, (AsIs(parameters['table_v']), AsIs(parameters['level_column']), parameters['level'], nodes));
    rows = cur.fetchall();
    for row in rows:
        matched = row[0]
    return {"cost": cost, "total": count, "matched": matched};
    