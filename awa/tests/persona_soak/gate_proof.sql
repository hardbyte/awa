BEGIN;
CREATE TABLE public.awa_soak_handler_returns (
    event_id bigserial PRIMARY KEY,
    job_id bigint NOT NULL,
    run_lease bigint NOT NULL,
    queue text NOT NULL,
    persona text NOT NULL,
    outcome text NOT NULL,
    returned_at timestamptz NOT NULL DEFAULT clock_timestamp()
) USING heap WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false);
CREATE INDEX awa_soak_handler_returns_pair ON public.awa_soak_handler_returns(job_id,run_lease);
CREATE TABLE public.awa_soak_completed_inserts (
    event_id bigserial PRIMARY KEY,
    job_id bigint NOT NULL,
    run_lease bigint NOT NULL,
    attempt smallint NOT NULL,
    queue text NOT NULL,
    writer_shape text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
) USING heap WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false);
CREATE INDEX awa_soak_completed_inserts_pair ON public.awa_soak_completed_inserts(job_id,run_lease);
CREATE FUNCTION public.awa_soak_audit_done_insert()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
BEGIN
    INSERT INTO public.awa_soak_completed_inserts(job_id,run_lease,attempt,queue,writer_shape)
    VALUES(NEW.job_id,NEW.run_lease,NEW.attempt,NEW.queue,'done');
    RETURN NEW;
END;
$body$;
CREATE FUNCTION public.awa_soak_audit_receipt_insert()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
BEGIN
    INSERT INTO public.awa_soak_completed_inserts(job_id,run_lease,attempt,queue,writer_shape)
    SELECT item.job_id,item.run_lease,item.attempt,NEW.queue,'receipt_batch'
    FROM unnest(NEW.job_ids,NEW.run_leases,NEW.attempts) AS item(job_id,run_lease,attempt);
    RETURN NEW;
END;
$body$;
CREATE TRIGGER awa_soak_audit_done_insert
AFTER INSERT ON __SCHEMA__.done_entries FOR EACH ROW
WHEN (NEW.state='completed'::awa.job_state)
EXECUTE FUNCTION public.awa_soak_audit_done_insert();
CREATE TRIGGER awa_soak_audit_receipt_insert
AFTER INSERT ON __SCHEMA__.receipt_completion_batches FOR EACH ROW
EXECUTE FUNCTION public.awa_soak_audit_receipt_insert();
COMMIT;
