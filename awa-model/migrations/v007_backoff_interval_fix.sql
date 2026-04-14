CREATE OR REPLACE FUNCTION awa.backoff_duration(attempt SMALLINT, max_attempts SMALLINT)
RETURNS interval AS $$
    SELECT LEAST(
        make_interval(secs => power(2::double precision, attempt::double precision))
            + make_interval(
                secs => random() * power(2::double precision, attempt::double precision) * 0.25
            ),
        interval '24 hours'
    );
$$ LANGUAGE sql VOLATILE;

INSERT INTO awa.schema_version (version, description)
VALUES (7, 'Backoff interval creation avoids scientific-notation parse failures')
ON CONFLICT (version) DO NOTHING;
