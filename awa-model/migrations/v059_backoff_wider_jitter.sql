-- v059: spread automatic retry backoff over [0.5, 1.5] x 2^attempt seconds.
-- Tracking: https://github.com/hardbyte/awa/issues/733
-- awa.backoff_duration added 0-25% jitter to 2^attempt seconds, so jobs that
-- failed together retried within a quarter of their base delay of each other
-- and reached a recovering dependency together. The delay is now drawn
-- uniformly from [0.5, 1.5] x 2^attempt seconds, still capped at 24 hours: the
-- same mean as the base delay, with retries from one failure spread over a
-- window as wide as the base delay itself.
--
-- The signature, volatility, owner and grants are unchanged. N-1 binaries call
-- awa.backoff_duration from the same statements and get the new delays as soon
-- as this migration commits; they need no change and can run before or after
-- it. Rows already scheduled keep their run_at.
--
-- Live load: one function replacement. It takes no lock on any awa or
-- queue-storage table.

CREATE OR REPLACE FUNCTION awa.backoff_duration(attempt SMALLINT, max_attempts SMALLINT)
RETURNS interval AS $$
    SELECT LEAST(
        make_interval(
            secs => power(2::double precision, attempt::double precision) * (0.5 + random())
        ),
        interval '24 hours'
    );
$$ LANGUAGE sql VOLATILE;

INSERT INTO awa.schema_version (version, description)
VALUES (59, 'Spread retry backoff over half to one and a half times the base delay')
ON CONFLICT (version) DO NOTHING;
