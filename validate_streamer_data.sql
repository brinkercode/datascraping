-- Example: Show all tables for streamers
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public' AND tablename LIKE 'streamer_%';

-- Example: Show all data for a specific streamer table (replace 'streamer_shroud' with the actual table name)
SELECT * FROM streamer_XXXXXX;
