DROP FUNCTION pgr_conn_compquery(text,text,bigint,bigint,integer,boolean); 
create or replace function pgr_conn_compQuery(
    edge_table TEXT,
    vertex_table TEXT,
    source BIGINT,
    target BIGINT,
    level INTEGER,
    directed BOOLEAN DEFAULT true,
    only_cost BOOLEAN DEFAULT false,

    OUT seq integer,
    OUT node BIGINT,
    OUT edge BIGINT,
    OUT cost float,
    OUT agg_cost float)
RETURNS SETOF RECORD AS
$body$
DECLARE
  cc_source integer;
  cc_target integer;
  source_sql TEXT;
  target_sql TEXT;
  final_sql TEXT;
BEGIN
    source_sql = 'SELECT comp_id_'|| level ||' FROM ' 
    || vertex_table 
    || ' WHERE id = ' || source;
    target_sql = 'SELECT comp_id_'|| level ||' FROM ' 
    || vertex_table 
    || ' WHERE id = ' || target;
    EXECUTE source_sql INTO cc_source;
    EXECUTE target_sql INTO cc_target;
    final_sql = 'SELECT id, source, target, cost, reverse_cost FROM '
    || edge_table || ' WHERE promoted_level_'|| level ||' = 1 OR comp_id_'|| level ||' = ' 
    || cc_source || ' OR comp_id_'|| level ||' = ' || cc_target;

    RETURN QUERY SELECT a.seq, a.node, a.edge, a.cost, a.agg_cost
    FROM pgr_dijkstra(final_sql, source, target, directed, only_cost) AS a;
END
$body$ language plpgsql volatile;