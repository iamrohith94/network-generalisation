    
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

def generate_random_pairs_from_table(parameters):
    db = parameters['db']
    conn = parameters['conn']    
    cur = conn.cursor()
    random_query = "SELECT t1.id, t2.id "\
    "FROM %s as t1, %s AS t2 \
    ORDER BY random() LIMIT %s;"
    cur.execute(random_query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), parameters['num_pairs'],));
    rows = cur.fetchall();
    pairs = [];
    for row in rows:
        pairs.append((row[0], row[1]));
    return pairs

def store_random_pairs_into_table(parameters):
    conn = parameters['conn']    
    cur = conn.cursor()
    random_query = "SELECT t1.id as source, t2.id as target INTO random_pairs "\
    "FROM %s as t1, %s AS t2 \
    ORDER BY random() LIMIT %s;"
    cur.execute(random_query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), parameters['num_pairs'],));
    conn.commit()


def store_random_pairs_from_components(parameters):
    db = parameters['db']
    conn = parameters['conn']    
    cur = conn.cursor()
    comp_query = "SELECT DISTINCT comp_id_%s FROM %s;"
    vertex_query = "SELECT id FROM %s WHERE comp_id_%s = %s LIMIT 1"
    insert_query = "INSERT INTO comp_pairs(source, target, level) VALUES(%s, %s, %s)"
    cur.execute(comp_query, (parameters['level'], AsIs(parameters['table_v']),));
    rows = cur.fetchall();
    comp_ids = []
    vertices = []
    for row in rows:
        comp_ids.append(row[0]);
    for comp_id in comp_ids:
        cur.execute(vertex_query, (AsIs(parameters['table_v']), parameters['level'], comp_id, ))
        rows = cur.fetchall()
        for row in rows:
            vertices.append(row[0])
    for i in vertices:
        for j in vertices:
            if i != j:
                cur.execute(insert_query, (i, j, parameters['level'], ))
    conn.commit()

def generate_random_pairs(parameters):
    db = parameters['db']
    conn = parameters['conn']    
    cur = conn.cursor()
    random_query = "SELECT id FROM %s ;"
    cur.execute(random_query, (AsIs(parameters['table_v']), ));
    rows = cur.fetchall()
    vertices = []
    for row in rows:
        vertices.append(row[0])
    i = 0
    pairs = []
    while i < parameters['num_pairs']:
        i1 = random.randint(0, parameters['num_pairs'])
        i2 = random.randint(0, parameters['num_pairs'])
        if i1 == i2:
            continue
        i += 1
        pairs.append((vertices[i1], vertices[i2]))
    return pairs

def store_random_pairs(parameters):
    db = parameters['db']
    conn = parameters['conn']    
    cur = conn.cursor()
    random_query = "SELECT id FROM %s ;"
    parameters['table'] = parameters['table_v']
    
    insert_query = "INSERT INTO random_pairs(source, target) VALUES(%s, %s)"
    cur.execute(random_query, (AsIs(parameters['table_v']), ));
    rows = cur.fetchall()
    vertices = []
    for row in rows:
        vertices.append(row[0])
    i = 0
    pairs = []
    count = len(vertices)
    while i < parameters['num_pairs']:
        i1 = random.randint(0, count-1)
        i2 = random.randint(0, count-1)
        print i1, i2
        if i1 == i2:
            continue
        i += 1
        cur.execute(insert_query, (vertices[i1], vertices[i2], ))
        #pairs.append((vertices[i1], vertices[i2]))
        #return pairs
    conn.commit()

def generate_random_source_target(parameters):
    conn = parameters['conn']    
    cur = conn.cursor()
    random_query = "SELECT id FROM %s ;"
    cur.execute(random_query, (AsIs(parameters['contracted_table_v']), ));
    rows = cur.fetchall()
    vertices = []
    sources = []
    targets = []
    for row in rows:
        vertices.append(row[0])
    i = 0
    print "num of vertices: ", len(vertices)
    
    while i < parameters['num_pairs']:
        i1 = random.randint(0, len(vertices)-1)
        i2 = random.randint(0, len(vertices)-1)
        #print i1, i2
        if i1 == i2:
            continue
        try:
            sources.append(vertices[i1])
            targets.append(vertices[i2])
        except IndexError:
            print i1, i2
        i += 1

    return (sources, targets)

def generate_random_pairs_dist(parameters):
    conn = parameters['conn']   
    cur = conn.cursor()
    start, end = parameters['range']
    random_query = "SELECT t1.id , t2.id "\
    "FROM %s as t1, %s AS t2 \
    WHERE ST_Distance(t1.the_geom, t2.the_geom)*111 >= %s \
    AND ST_Distance(t1.the_geom, t2.the_geom)*111 <= %s\
    AND t1.id != t2.id\
    ORDER BY random() LIMIT %s;"
    cur.execute(random_query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), 
        start, end, parameters['num_pairs'],));
    rows = cur.fetchall()
    pairs = [];
    for row in rows:
        pairs.append((row[0], row[1]))
    return pairs

def get_distance(parameters):
    """Returns distance in kms"""
    conn = parameters['conn']
    cur = conn.cursor()
    if parameters['is_max']:
        query = "SELECT ST_Distance(t1.the_geom, t2.the_geom)*111 "\
        "FROM %s as t1, %s AS t2 \
        WHERE t1.id != t2.id \
        ORDER BY ST_Distance(t1.the_geom, t2.the_geom) DESC LIMIT 1;"
    else:
        query = "SELECT ST_Distance(t1.the_geom, t2.the_geom)*111 "\
        "FROM %s as t1, %s AS t2 \
        WHERE t1.id != t2.id \
        ORDER BY ST_Distance(t1.the_geom, t2.the_geom) LIMIT 1;"
    cur.execute(query, (AsIs(parameters['table_v']), AsIs(parameters['table_v']), ));
    rows = cur.fetchall()
    dist = 0
    for row in rows:
        dist = row[0]
    return dist

def get_actual_distance(parameters):
    """Returns distance in kms"""
    conn = parameters['conn']
    cur = conn.cursor()
    if parameters['is_max']:
        query = "SELECT MAX(actual_distance/1000) FROM paths;"
    else:
        query = "SELECT MIN(actual_distance/1000) FROM paths;"
    cur.execute(query);
    rows = cur.fetchall()
    dist = 0
    for row in rows:
        dist = row[0]
    return dist

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

    inner = 'SELECT id, source, target, cost, reverse_cost ' \
    ' FROM '+parameters['table_e']+' WHERE is_contracted = FALSE';
    dijkstra_query = "SELECT node from pgr_dijkstra(%s,%s,%s)";
    random_query = "SELECT t1.id, t2.id "\
    "FROM %s as t1, %s AS t2 \
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

    #Fetching edge ids from the contracted edge table
    etab = {}
    etab["db"] = db;
    etab["table"] = parameters['contracted_table_e'];
    etab["column"] = parameters['column'];
    etab['conn'] = conn;
    edges = get_column_values(etab);

    #Initialising their counts to 0
    for x in edges:
        count[int(x)] = 0;

    #Inner query for dijkstra -> contracted graph is taken 
    inner = 'SELECT id, source, target, cost, reverse_cost '\
    'FROM '+parameters['contracted_table_e'];

    
    dijkstra_query = "SELECT edge from pgr_dijkstra(%s, %s, %s)";
    
    #Fetches vertex pairs at random
    random_query = 'SELECT t1.id, t2.id ' \
    'FROM %s as t1, %s AS t2 \
    ORDER BY random() LIMIT %s;'
    
    cur.execute(random_query, (AsIs(parameters['contracted_table_v']), AsIs(parameters['contracted_table_v']), parameters['num_pairs'],));
    pairs = cur.fetchall();
    ind = 1;
    
    for pair in pairs:
        #print ind
        ind = ind+1
        s = pair[0]
        t = pair[1]
        #print "source: "+str(s)+", target: "+str(t)
        cur.execute(dijkstra_query, (inner, s, t ));
        rows = cur.fetchall()
        for row in rows:
            eid = int(row[0])
            #print eid  
            if eid != -1:
                count[eid] = count[eid]+1
    #print count
    return count


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


def populate_edge_levels(parameters):
    """
    Updates the level column in the edge table of a given set of edges
    """
    conn = parameters['conn']
    cur = conn.cursor()
    update_query = "UPDATE %s SET %s "\
      "  = %s, %s = %s WHERE id = ANY(%s)"
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
    update_query = "UPDATE %s SET %s = "\
    "(SELECT MIN(e.%s) FROM %s AS e WHERE e.source = %s.id OR e.target = %s.id) \
    , %s = (SELECT MIN(e.%s) FROM %s AS e WHERE e.source = %s.id OR e.target = %s.id) \
    WHERE %s > (SELECT MIN(e.%s) FROM %s AS e WHERE e.source = %s.id OR e.target = %s.id)";
    cur.execute(update_query, 
        (AsIs(parameters['table_v']), AsIs(parameters['level_column']), 
        AsIs(parameters['level_column']), AsIs(parameters['table_e']),
        AsIs(parameters['table_v']), AsIs(parameters['table_v']), 
        AsIs(parameters['promoted_level_column']),
        AsIs(parameters['level_column']), AsIs(parameters['table_e']),
        AsIs(parameters['table_v']), AsIs(parameters['table_v']),
        AsIs(parameters['level_column']),
        AsIs(parameters['level_column']), AsIs(parameters['table_e']),
        AsIs(parameters['table_v']), AsIs(parameters['table_v']), ));
    conn.commit();


def get_graph_size(parameters):
    conn = parameters['conn']
    cur = conn.cursor()
    e_count = 0
    v_count = 0
    query = "SELECT count(*) from %s";
    cur.execute(query, (AsIs(parameters['table_e']), ))
    rows = cur.fetchall()
    for row in rows:
        e_count = row[0]
    cur.execute(query, (AsIs(parameters['table_v']), ))
    rows = cur.fetchall()
    for row in rows:
        v_count = row[0]
    return (e_count, v_count)

def get_comp_size(parameters):
    conn = parameters['conn']
    comp_size_e = 0
    comp_size_v = 0
    comp_id = -1
    cur = conn.cursor()
    query = "SELECT comp_id_%s from %s WHERE id = %s";
    cur.execute(query, (parameters['level'], AsIs(parameters['table_v']), parameters['vertex']))
    rows = cur.fetchall()
    for row in rows:
        comp_id = row[0]
    query = "SELECT count(*) from %s WHERE comp_id_%s = %s AND promoted_level_%s != 1";
    cur.execute(query, (AsIs(parameters['table_e']), parameters['level'], comp_id, parameters['level']))
    rows = cur.fetchall()
    for row in rows:
        comp_size_e = row[0]
    cur.execute(query, (AsIs(parameters['table_v']), parameters['level'], comp_id, parameters['level']))
    rows = cur.fetchall()
    for row in rows:
        comp_size_v = row[0]
    return (comp_size_e, comp_size_v, comp_id)


def get_skeleton_size_by_level(parameters):
    conn = parameters['conn']
    cur = conn.cursor()
    e_count = 0
    v_count = 0
    query = "SELECT count(*) from %s WHERE promoted_level_%s <= %s";
    cur.execute(query, (AsIs(parameters['table_e']), parameters['level'], 1))
    rows = cur.fetchall()
    for row in rows:
        e_count = row[0]
    cur.execute(query, (AsIs(parameters['table_v']), parameters['level'], 1))
    rows = cur.fetchall()
    for row in rows:
        v_count = row[0]
    return (e_count, v_count)


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
    return {"cost": cost, "total": count, "matched": matched}

def generate_edge_count_m_m(parameters):
    conn = parameters['conn']
    cur = conn.cursor()
    print "Number of vertex pairs: "+str(len(parameters['sources']))
    count = {}

    #Inner query for dijkstra -> contracted graph is taken 
    inner = 'SELECT id, source, target, cost, reverse_cost '\
    'FROM '+parameters['contracted_table_e'];

    dijkstra_query = "SELECT edge from pgr_dijkstra(%s, %s, %s)"
    
    cur.execute(dijkstra_query, (inner, parameters['sources'], parameters['targets']))

    rows = cur.fetchall()
    for row in rows:
        try:
            count[row[0]] += 1
        except KeyError:
            count[row[0]] = 1

    return count

def is_table_present(parameters):
    """
    Checks whether a table with a particular name in the db exists or not
    """
    conn = parameters['conn']
    cur = conn.cursor()
    query = " SELECT table_name "\
            "FROM information_schema.tables \
            WHERE table_name = %s"
    cur.execute(query, (parameters['table'], ));
    rows = cur.fetchall();
    for row in rows:
        if row[0] == parameters['table']:
            return True
        else:
            return False
    return False



def get_skeleton(parameters):
    db = parameters['db'];
    conn = parameters['conn']
    cur = conn.cursor();
    query = "SELECT id, within_nodes FROM connected_components\
             ORDER BY ST_area(st_envelope(the_geom)) DESC \
             LIMIT 1";
    cur.execute(query);
    rows = cur.fetchall();
    for row in rows:
        skeleton_id = row[0];
        within_nodes = row[1];
    return skeleton_id, within_nodes;


def update_level_skeleton(parameters):
    conn = parameters['conn']
    cur = conn.cursor()
    paths_to_add = parameters['path_additions'];
    update_query = "UPDATE %s SET %s = %s WHERE id = ANY(%s)";
    edges_to_add = []
    nodes_to_add = []
    dijkstra_query = "SELECT node, edge from pgr_dijkstra(%s,%s,%s)"; 
    inner = 'SELECT id, source, target, cost, reverse_cost \
    FROM '+parameters['table_e'];
    for edge in paths_to_add:
        src = edge[0]
        target = edge[1]
        #Adding the source and target into a table
        cur.execute(dijkstra_query, (inner, src, target, ));
        rows = cur.fetchall()
        for row in rows:
            nodes_to_add.append(row[0]);
            edges_to_add.append(row[1]);
    #print "Edge Additions: ", edges_to_add
    cur.execute(update_query, (AsIs(parameters['table_e']), AsIs(parameters['promoted_level_column']), parameters['level'], edges_to_add, ));
    cur.execute(update_query, (AsIs(parameters['table_v']), AsIs(parameters['promoted_level_column']), parameters['level'], nodes_to_add, ));

    conn.commit();

    
    