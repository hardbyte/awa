-- Runs after every migration step that refreshes the queue-storage helpers
-- (the steps that include v023), so the functions that step created or
-- replaced, in awa and in every discovered queue-storage schema, list pg_temp
-- last in their search_path and leave public out. See
-- awa.pin_function_search_paths in v023.
DO $search_paths$
DECLARE
    v_schema TEXT;
BEGIN
    PERFORM awa.pin_function_search_paths('awa');
    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        IF v_schema <> 'awa' THEN
            PERFORM awa.pin_function_search_paths(v_schema);
        END IF;
    END LOOP;
END;
$search_paths$;
