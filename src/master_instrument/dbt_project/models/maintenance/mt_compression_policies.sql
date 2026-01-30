{{ config(
    materialized = "view",
    schema = "maintenance",
    meta = {
        "dagster": {
            "asset_key": ["maintenance", "mt_compression_policies"],
            "group": "maintenance",
            "description": "TimescaleDB compression policies status and schedules."
        }
    }
) }}

-- TimescaleDB compression policies monitoring
-- Shows schedule, next run, and configuration for each hypertable

SELECT 
    job_id,
    hypertable_schema || '.' || hypertable_name AS table_name,
    schedule_interval,
    next_start,
    initial_start,
    scheduled,
    fixed_schedule,
    config,
    max_runtime,
    max_retries,
    retry_period,
    owner
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_compression'
ORDER BY table_name
