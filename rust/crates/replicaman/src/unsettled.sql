-- Every local write still relevant, accepted ones included until a round shows them.
-- Each row is decoded individually by the adapter.
SELECT CAST(payload AS BLOB) FROM intents;
