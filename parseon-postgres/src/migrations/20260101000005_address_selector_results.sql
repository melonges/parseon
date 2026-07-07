DO $$
DECLARE
    legacy_table RECORD;
BEGIN
    FOR legacy_table IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = current_schema()
          AND tablename ~ '^params_[0-9]+$'
    LOOP
        EXECUTE format('DROP TABLE %I CASCADE', legacy_table.tablename);
    END LOOP;
END
$$;

TRUNCATE TABLE transactions, monitors RESTART IDENTITY CASCADE;
