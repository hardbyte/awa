"""Packet-only serialized psycopg hook; no hard client/network deadline."""
from pathlib import Path
import time

CATALOG = (Path(__file__).parent / 'catalog.sql').read_text()
RINGS = {'queue': 'queue', 'lease': 'lease', 'claim': 'claim'}
MEMBERS = {'lease_claim_batches': 'claimed_count',
           'lease_claim_closure_batches': 'closed_count',
           'ready_claim_attempt_batches': 'claimed_count',
           'receipt_completion_batches': 'completed_count'}


def quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def fetch(connection, sql, parameters=()):
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters) if parameters else cursor.execute(sql)
        names = [column.name for column in cursor.description]
        return [dict(zip(names, row)) if not isinstance(row, dict) else row
                for row in cursor.fetchall()]


def observe(connection, phase, *, exact_rows=False, budget_seconds=2.0):
    """Use an idle autocommit connection exclusively until this call settles.

    Caller schedules metadata every 60s and exact counts at checkpoints. A 100ms
    server timeout bounds statements, not client/network wall time. Transport
    failures propagate: caller must close/reset rather than reuse the connection.
    """
    if not connection.autocommit or connection.info.transaction_status.name != 'IDLE':
        raise ValueError('observer requires an idle autocommit connection')
    start = time.monotonic()
    output = {'phase': phase, 'client_start_ns': time.time_ns(),
              'status': 'unknown', 'slots': [], 'exact_rows_requested': exact_rows,
              'server_statement_timeout_ms': 100, 'server_lock_timeout_ms': 100,
              'admission_budget_seconds': budget_seconds,
              'hard_client_deadline': False}
    original = fetch(connection, "SELECT current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout")[0]
    transport_failed = False
    try:
        fetch(connection, "SELECT set_config('statement_timeout','100ms',false), set_config('lock_timeout','100ms',false)")
        rows = fetch(connection, CATALOG)
        for slot in rows:
            slot['row_count_status'] = 'not_requested'
            output['slots'].append(slot)
            if not exact_rows or not slot['sealed']:
                continue
            if time.monotonic()-start >= budget_seconds:
                slot['row_count_status'] = 'budget_exhausted'
                continue
            family = RINGS[slot['family']]
            table = quote(slot['schema_name']) + '.' + quote(slot['child_name'])
            column = MEMBERS.get(slot['parent_name'])
            member = 'coalesce(sum(' + quote(column) + '),0)::bigint' if column else 'NULL::bigint'
            query = f'''SELECT clock_timestamp() AS counted_at, count(*)::bigint AS physical_rows,
                {member} AS counted_members,
                (SELECT generation FROM awa.{family}_ring_slots WHERE slot=%s) AS slot_generation,
                (SELECT current_slot FROM awa.{family}_ring_state WHERE singleton) AS current_slot,
                pg_relation_filenode(%s::oid)::bigint AS filenode FROM ONLY {table}'''
            try:
                counted = fetch(connection, query, (slot['slot'], slot['child_oid']))[0]
                slot.update(count=counted, member_column=column,
                    row_count_status=('counted' if counted['slot_generation'] == slot['slot_generation']
                        and counted['current_slot'] != slot['slot'] and counted['filenode'] == slot['filenode']
                        else 'stale_generation_or_rewrite'))
            except Exception as error:
                if not getattr(error, 'sqlstate', None):
                    raise
                slot.update(row_count_status='unknown', error=type(error).__name__, sqlstate=error.sqlstate)
        output['status'] = 'observed_metadata'
    except Exception as error:
        if not getattr(error, 'sqlstate', None):
            transport_failed = True
            raise
        output.update(error=type(error).__name__, sqlstate=error.sqlstate)
    finally:
        if not transport_failed:
            fetch(connection, "SELECT set_config('statement_timeout',%s,false), set_config('lock_timeout',%s,false)",
                  (original['statement_timeout'], original['lock_timeout']))
    output.update(client_end_ns=time.time_ns(), elapsed_seconds=time.monotonic()-start)
    return output
