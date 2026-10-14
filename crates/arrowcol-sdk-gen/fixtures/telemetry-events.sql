CREATE TABLE telemetry.prod.sdk_events (
    event_id BIGINT UNSIGNED NOT NULL,
    observed_at TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    trace_id FIXED_SIZE_BINARY(16) NOT NULL,
    payload BYTEA,
    amount DECIMAL(20,4),
    samples ARRAY<STRUCT<name VARCHAR, value DOUBLE>>,
    attributes MAP(VARCHAR, VARCHAR),
    event_date DATE NOT NULL
)
PARTITIONED BY (tenant VARCHAR NOT NULL)
