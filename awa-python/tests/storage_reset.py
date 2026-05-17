import awa


RESET_STORAGE_SQL = """
UPDATE awa.storage_transition_state
SET current_engine = 'canonical',
    prepared_engine = NULL,
    state = 'canonical',
    transition_epoch = transition_epoch + 1,
    details = '{}'::jsonb,
    updated_at = now(),
    finalized_at = NULL
WHERE singleton
"""


async def reset_async(client: awa.AsyncClient) -> None:
    tx = await client.transaction()
    try:
        await tx.execute(RESET_STORAGE_SQL)
        await tx.execute(
            "DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'"
        )
        await tx.execute("DELETE FROM awa.runtime_instances")
        await tx.commit()
    except Exception:
        await tx.rollback()
        raise


def reset_sync(client: awa.Client) -> None:
    tx = client.transaction()
    try:
        tx.execute(RESET_STORAGE_SQL)
        tx.execute(
            "DELETE FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'"
        )
        tx.execute("DELETE FROM awa.runtime_instances")
        tx.commit()
    except Exception:
        tx.rollback()
        raise
