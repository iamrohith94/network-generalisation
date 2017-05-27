DROP FUNCTION pgr_conn_compquery_time(text,text,bigint,bigint,integer,boolean); 
create or replace function pgr_conn_compQuery_time(
    edge_table TEXT,
    vertex_table TEXT,
    start_vids ANYARRAY,
    end_vids ANYARRAY,
    level INTEGER,
    num_iterations INTEGER DEFAULT 1,
    directed BOOLEAN DEFAULT true,

    OUT seq INTEGER,
    OUT source BIGINT,
    OUT target BIGINT,
    OUT build_time FLOAT,
    OUT avg_execution_time FLOAT)
RETURNS SETOF RECORD AS
$body$
DECLARE
  cc_source integer;
  cc_target integer;
  source_sql TEXT;
  target_sql TEXT;
  final_sql TEXT;
BEGIN
    FOR i in 1 .. array_upper(start_vids, 1)
    LOOP
        source := start_vids[i];
        target := end_vids[i];
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

        RETURN QUERY SELECT a.seq, a.source, a.target, a.build_time, a.avg_execution_time
        FROM pgr_timeAnalysis(final_sql, 'pgr_dijkstra', ARRAY[source], ARRAY[target], num_iterations, directed) AS a;
    END LOOP;
END
$body$ language plpgsql volatile;