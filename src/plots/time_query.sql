create or replace function timeQuery(fn text, query_sql text, start_vids ANYARRAY, end_vids ANYARRAY)
    returns TABLE (
        seq    INTEGER,
        source INTEGER,
        target INTEGER,
        time_taken   float,
        err text) AS

    $body$
    DECLARE
    time1 time;
    time2 time;
    deltaTime time;
    sql TEXT;
    i integer;
    info record;
    BEGIN
        seq := 1;
        FOR i IN array_lower(start_vids, 1) .. array_upper(start_vids, 1)
        LOOP
            source := start_vids[i];
            target := end_vids[i];
            sql := '
                SELECT count(*) as cnt from ' || fn || '( ' || quote_literal(query_sql) || 
                ', ' || cast(source as TEXT) || ', ' || cast(target as TEXT) ||
                ')';
            -- raise notice '%', sql; 
            time1 := clock_timestamp();
            BEGIN
                execute sql into info;
                EXCEPTION WHEN OTHERS THEN
                    err = SQLERRM;
            END;
            time2 := clock_timestamp();
            deltaTime = time2 - time1;
            time_taken := extract(epoch from deltaTime);
            -- raise notice '% % % %', seq, source, target, time_taken;
            return next;
            seq := seq + 1;
        END LOOP;
    END
    $body$ language plpgsql volatile strict cost 100 rows 100;