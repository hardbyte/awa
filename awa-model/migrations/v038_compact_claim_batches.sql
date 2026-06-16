-- v038: install the compact receipt claim batch ledger in the default
-- queue-storage substrate. Custom queue-storage schemas receive the same object
-- through QueueStorage::prepare_schema(), which calls this helper directly.

SELECT awa.install_queue_storage_substrate('awa', 16, 8, 8, TRUE);

INSERT INTO awa.schema_version (version, description)
VALUES (38, 'Add compact receipt claim batch ledger for queue storage')
ON CONFLICT (version) DO NOTHING;
