    
import psycopg2
import networkx as nx
import sys
def get_count(parameters):
    db=parameters['db']
    table=parameters['table']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("SELECT count(*) from "+table)
    rows=cur.fetchall()
    for row in rows:
        count=row[0]
    return count
    
def get_vertex_centroid(parameters):
    db=parameters['db']
    table=parameters['table']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("SELECT ST_X((ST_Centroid(ST_Multi(ST_Union(the_geom))))),ST_Y((ST_Centroid(ST_Multi(ST_Union(the_geom))))) from "+table)
    rows = cur.fetchall()
    for row in rows:
        mean_x=row[0]
        mean_y=row[1]
    return (mean_x,mean_y)
    
def change_edges_geometry(parameters):
    db=parameters['db']
    table = parameters['table']
    column=parameters['column']
    srid=parameters['srid']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("ALTER TABLE "+ table+" \
    ALTER COLUMN "+column+" TYPE geometry(Linestring,"+str(srid)+") \
    USING ST_Transform(ST_SetSRID(the_geom, 4326),"+str(srid)+");")
    conn.commit()
    conn.close()


def change_vertices_geometry(parameters):
    db=parameters['db']
    column=parameters['column']
    srid=parameters['srid']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("ALTER TABLE ways_vertices_pgr \
    ALTER COLUMN "+column+" TYPE geometry(Point,"+str(srid)+") \
    USING ST_Transform(ST_SetSRID(the_geom, 4326),"+str(srid)+");")
    conn.commit()
    conn.close()
    
def update_geom_column(parameters):
    db = parameters['db'];
    table = parameters['table'];
    column = parameters['column'] 
    parameters["srid"]=32643
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    query = "UPDATE "+table+" SET "+column+" = \
    ST_SetSRID(ST_MakeLine(ST_MakePoint(x1, y1), ST_MakePoint(x2, y2)), 4326);";
    print query
    try:     
        cur.execute(query);
    except:
        e = sys.exc_info()[0]
        print e
    #change_edges_geometry(parameters);
    conn.commit()
    conn.close()    
    
def graph_to_edge_table(parameters):
    G = parameters['graph'];
    db = parameters['db'];
    table = parameters['table'];
    xy_dict = nx.drawing.nx_agraph.graphviz_layout(G);
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")
    #print "connected"
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS "+table);
    cur.execute("CREATE TABLE "+table+" \
       (id BIGINT PRIMARY KEY     NOT NULL, \
       source INT     NOT NULL, \
       target INT     NOT NULL, \
       cost DOUBLE PRECISION DEFAULT 1.0000 NOT NULL, \
       x1 DOUBLE PRECISION NOT NULL, \
       y1 DOUBLE PRECISION NOT NULL, \
       x2 DOUBLE PRECISION NOT NULL, \
       y2 DOUBLE PRECISION NOT NULL, \
       the_geom geometry(LINESTRING, 4326) \
       )");
    print "created "+table
    count = 0
    for edge in G.edges():
        id = count;
        count = count+1
        src = edge[0]
        target = edge[1]
        x1 = xy_dict[src][0]
        y1 = xy_dict[src][1]
        x2 = xy_dict[target][0]
        y2 = xy_dict[target][1]
        cur.execute("SELECT ST_SetSRID(ST_MakeLine(ST_MakePoint("+str(x1)+", "+str(y1)+"), \
        ST_MakePoint("+str(x2)+", "+str(y2)+")), 4326)");
        rows=cur.fetchall()
        for row in rows:
            the_geom=row[0]
        #print the_geom
        #print str(id) +","+str(src)+","+str(target)+","+str(x1)+","+str(y1)+","+str(x2)+","+str(y2);        
        cur.execute("INSERT INTO "+table+" \
        (id, source, target, x1, y1, x2, y2, the_geom) \
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", (str(id), str(src), str(target), str(x1), str(y1), str(x2), str(y2), the_geom));
    
    cur.execute("SELECT  pgr_createVerticesTable('"+table+"',source:='source',target:='target',the_geom:='the_geom');");
    parameters['column'] = "the_geom"
    #update_geom_column(parameters)
    conn.commit()
    conn.close()


    

def tuple_to_id(parameters):
    n = parameters['n']
    G = parameters['graph']
    G1 = nx.Graph()
    for edge in G.edges():
        src = n*edge[0][0] + edge[0][1]
        target = n*edge[1][0] + edge[1][1]
        G1.add_edge(src, target)
    return G1


def edge_table_to_graph(parameters):
    db = parameters['db']
    table = parameters['table']
    G = nx.Graph()
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    cur.execute("SELECT source, target FROM "+table);
    rows = cur.fetchall()
    for row in rows:
        G.add_edge(int(row[0]), int(row[1]))
    return G

        