from common import *;
import heapq;
import networkx as nx;
    
#database = "gachibowli_1"
database = "sample_strokes"
par1 = {}
par1['db'] = database;
#par1['table'] = "ways_vertices_pgr"
par1['table'] = "edge_table"
#vertex_count = get_count(par1);
#par1['table'] = "ways"
edge_count = get_count(par1);
print "edge count = ", edge_count,"\n"
#print "vertex count = ", vertex_count,"\n"

max_deflection = 60;

"""
This class stores edge properties and is used to write a compare function,
to choose edges for stroke generation

"""
class Edge(object):
    def __init__(self, id, source, target, stroke_ids, geom):
        self.id = id
        self.stroke_ids = stroke_ids
        self.source = source
        self.target = target
        self.geom = geom
        return
    def __cmp__(self, other):
        return cmp(self.id, other.id) 


"""

In this function we are adding an extra column to the edge table which tells us
to which stroke does the edge belongs to, the default being 0

"""
def add_stroke_class_column(parameters):
    db=parameters['db']
    table = parameters['table']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("ALTER TABLE "+ table+" ADD COLUMN stroke_ids integer ARRAY DEFAULT '{}' ")
    conn.commit()
    conn.close()

"""

In this function we are generating strokes using the edges of the edge table.
The edge from which the stroke generation starts depends on a priority queue 
of edges. At present the priority queue would return me the edge with the
least id and didnt participate in stroke generation
 
"""

def create_graph(parameters):
    db = parameters['db']
    table = parameters['table']
    rvalue = {}
    heap = []
    G = nx.Graph(name = db);
    conn = psycopg2.connect(database = db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor();
    cur.execute("SELECT id, source, target, cost, stroke_ids, the_geom FROM "+table);
    rows = cur.fetchall()
    for row in rows:
        heapq.heappush(heap, Edge(row[0], row[1], row[2], row[4], row[5]))
        G.add_edge(int(row[1]), int(row[2]),{'id' : int(row[0]) ,'stroke_ids' :row[4] , 'geom':row[5]} ,weight = float(row[3]));     
    conn.close()
    rvalue['graph'] = G;
    rvalue['heap'] = heap;
    return rvalue

def print_table(parameters):
    db = parameters['db']
    table = parameters['table'];
    conn = psycopg2.connect(database = db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor();
    cur.execute("SELECT id,source,target,the_geom,stroke_ids from "+table);
    rows=cur.fetchall()
    flag = 0;
    for row in rows:
        if flag == 0:
            print "type of stroke_ids is ",type(row[4])
            flag = 1
        print "id: ",row[0],",src: ",row[1],",trgt: ",row[2],",the_geom: ",row[3],",stroke_ids: ",row[4];
        

def get_min_angle_edge(parameters):
    cur_edge = parameters['edge'];
    db = parameters['db']
    temp = cur_edge;
    max_angle = parameters['max_angle']
    angle = parameters['max_angle'];
    initial_angle = parameters['initial_angle'];
    ref_geom = cur_edge.geom;
    conn = psycopg2.connect(database = db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor();
    G = parameters['graph'];
    target = cur_edge.target 
    out_edges = G.edges(target);
    print out_edges
    #while(out_edges) !=0:
    # print "Out edge of ",temp
    for out_edge in out_edges:
        s = int(out_edge[0]);
        t = int(out_edge[1]);
        cur_geom = G[s][t]['geom'];
        print "cur geom: ",cur_geom;
        #print "typeof: geom ",type(cur_geom);
        cur.execute("select ST_Azimuth(ST_StartPoint(%s), ST_EndPoint(%s))/(2*pi())*360;",(ref_geom,ref_geom) );
        #cur.execute("select ST_StartPoint(%s) as start,ST_EndPoint(%s) as end;",(ref_geom,ref_geom));
        rows = cur.fetchall();
        for row in rows:
            ref_angle=row[0];
        #print row[0], ", ", row[1]
        cur.execute("select ST_Azimuth(ST_StartPoint(%s), ST_EndPoint(%s))/(2*pi())*360;",(cur_geom,cur_geom) );
        #cur.execute("select ST_StartPoint(%s) as start,ST_EndPoint(%s) as end;",(cur_geom,cur_geom));
        rows = cur.fetchall();
        for row in rows:
            cur_angle=row[0];
            #print row[0], ", ", row[1];
        diff1 = abs(float(cur_angle)- float(ref_angle));
        diff2 = abs(float(cur_angle)- float(initial_angle));
        print out_edge," deviation1: ",diff1
        print out_edge," deviation2: ",diff2                
        if(diff1 < angle and diff2 < max_angle and diff1 > 0.0):
            angle = diff1;
            temp = out_edge
    source = int(temp[0])
    target = int(temp[1])    
    id = G[source][target]['id']
    stroke_ids = G[source][target]['stroke_ids']
    geom = G[source][target]['geom']
    r = Edge(id, source, target, stroke_ids, geom);
    return r;


def generate_strokes(parameters):
    print "Generating strokes...."
    db = parameters['db']
    G = parameters['graph']
    table = parameters['table'];
    heap = parameters['heap']
    max_angle = parameters['max_angle']
    conn = psycopg2.connect(database = db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor();
    cur_stroke_id=1;
    #print G.edges(2);
    for edge in heap:
        src = edge.source
        target = edge.target  
        #print "Current edge (",src,", ",target,")";
        #print "Current edge ",edge.__dict__;
        if size(edge.stroke_ids) > 0:
            continue;     
        temp = target;
        out_edges = G.edges(target);
        ref_geom = edge.geom;
        #print "ref geom: ",ref_geom;
        cur.execute("select ST_Azimuth(ST_StartPoint(%s), ST_EndPoint(%s))/(2*pi())*360;",(ref_geom,ref_geom) );
        rows = cur.fetchall();
        for row in rows:
            initial_angle=row[0];
        parameters['initial_angle'] = initial_angle;
        edge.stroke_ids.append(cur_stroke_id);
        while size(G.edges(target))!=0:    
            parameters['edge'] = edge;
            min_edge = get_min_angle_edge(parameters);
            if min_edge == edge:
                break;
            target = min_edge.target;
            min_edge.stroke_ids.append(cur_stroke_id);
        cur_stroke_id = cur_stroke_id+1;
    print "Heap"
    for edge in heap:
        print edge.__dict__;
    conn.close()
    
problem = {}
problem['db'] = database
problem['table'] = "edge_table"
problem["column"]="the_geom"
problem["srid"]=32643
#change_edges_geometry(problem);
#add_stroke_class_column(parameters);
#generate_strokes(problem)
"""G = nx.Graph();
G.add_edge(1,2,{'class':0});
G.add_edge(1,3,{'class':1});
print G[1][2]
out = create_graph(problem)
out['db'] = database;
out['graph'] = out['graph'].to_undirected();

#generate_strokes(out)"""
#print_table(problem);
out = create_graph(problem);
problem['graph'] = out['graph'].to_undirected();
problem['heap'] = out['heap'];
problem['max_angle'] = 60;
generate_strokes(problem);