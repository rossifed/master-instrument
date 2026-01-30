"""Sensors for chaining jobs sequentially.

Architecture:
- Each domain (reference, market, fundamental) has a pipeline: raw → staging → intermediate → master
- Sensors watch for job completion and trigger the next job in the chain
- This prevents concurrent execution and resource contention
- Sensors ONLY chain runs that have the "scheduled_pipeline: true" tag
- Manual runs without this tag will NOT trigger the chain

Flow:
1. Schedule triggers *_raw_job with tag "scheduled_pipeline: true"
2. Sensor detects raw job success (with tag) → triggers *_staging_job (with tag)
3. Sensor detects staging job success (with tag) → triggers *_intermediate_job (with tag)
4. Sensor detects intermediate job success (with tag) → triggers *_master_job (with tag)
"""

import json
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    DagsterRunStatus,
    RunsFilter,
    DefaultSensorStatus,
)

from master_instrument.etl.jobs.daily import (
    # Reference jobs
    reference_staging_job,
    reference_intermediate_job,
    reference_master_job,
    # Market jobs
    market_raw_job,
    market_staging_job,
    market_intermediate_job,
    market_master_job,
    # Fundamental jobs
    fundamental_raw_job,
    fundamental_staging_job,
    fundamental_intermediate_job,
    fundamental_master_job,
)

# Tag that identifies scheduled pipeline runs (only these will chain)
SCHEDULED_TAG = "scheduled_pipeline"


def _create_job_chain_sensor(
    name: str,
    upstream_job_name: str,
    downstream_job,
    description: str,
):
    """Factory function to create a sensor that chains jobs sequentially.
    
    IMPORTANT: Only chains runs that have the "scheduled_pipeline: true" tag.
    Manual runs without this tag will NOT trigger downstream jobs.
    
    Args:
        name: Sensor name
        upstream_job_name: Name of the job to watch for completion
        downstream_job: Job to trigger when upstream completes
        description: Human-readable description
    """
    @sensor(
        name=name,
        job=downstream_job,
        minimum_interval_seconds=60,  # Check every minute
        default_status=DefaultSensorStatus.STOPPED,
        description=description,
    )
    def job_chain_sensor(context: SensorEvaluationContext):
        # Get the set of already processed run IDs from cursor
        cursor = context.cursor
        processed_run_ids = set(json.loads(cursor)) if cursor else set()
        
        # Find completed runs of the upstream job
        runs = context.instance.get_runs(
            filters=RunsFilter(
                job_name=upstream_job_name,
                statuses=[DagsterRunStatus.SUCCESS],
            ),
            limit=50,
        )
        
        # Filter to only runs we haven't processed yet
        new_runs = [r for r in runs if r.run_id not in processed_run_ids]
        
        if not new_runs:
            return  # No new completions
        
        # Reverse to process oldest first (get_runs returns newest first)
        new_runs = list(reversed(new_runs))
        
        # Trigger downstream job ONLY for scheduled pipeline runs (with tag)
        for run in new_runs:
            # Check if this run is part of a scheduled pipeline
            if run.tags.get(SCHEDULED_TAG) != "true":
                context.log.info(
                    f"Upstream job '{upstream_job_name}' completed (run_id: {run.run_id}) "
                    f"but is NOT a scheduled pipeline run (missing '{SCHEDULED_TAG}' tag). Skipping chain."
                )
                continue
            
            context.log.info(
                f"Upstream job '{upstream_job_name}' completed (run_id: {run.run_id}). "
                f"Triggering downstream job '{downstream_job.name}'."
            )
            yield RunRequest(
                run_key=f"{downstream_job.name}_{run.run_id}",
                tags={
                    SCHEDULED_TAG: "true",  # Propagate the tag to keep the chain going
                    "triggered_by": upstream_job_name,
                    "upstream_run_id": run.run_id,
                },
            )
        
        # Update cursor with all processed run IDs
        processed_run_ids.update(r.run_id for r in new_runs)
        # Keep only the last 100 run IDs to prevent unbounded growth
        if len(processed_run_ids) > 100:
            processed_run_ids = set(list(processed_run_ids)[-100:])
        context.update_cursor(json.dumps(list(processed_run_ids)))
    
    return job_chain_sensor


# =============================================================================
# REFERENCE DOMAIN SENSORS (raw → staging → intermediate → master)
# =============================================================================

reference_raw_to_staging_sensor = _create_job_chain_sensor(
    name="reference_raw_to_staging_sensor",
    upstream_job_name="reference_raw_job",
    downstream_job=reference_staging_job,
    description="Triggers reference_staging_job when reference_raw_job completes successfully",
)

reference_staging_to_intermediate_sensor = _create_job_chain_sensor(
    name="reference_staging_to_intermediate_sensor",
    upstream_job_name="reference_staging_job",
    downstream_job=reference_intermediate_job,
    description="Triggers reference_intermediate_job when reference_staging_job completes successfully",
)

reference_intermediate_to_master_sensor = _create_job_chain_sensor(
    name="reference_intermediate_to_master_sensor",
    upstream_job_name="reference_intermediate_job",
    downstream_job=reference_master_job,
    description="Triggers reference_master_job when reference_intermediate_job completes successfully",
)

# =============================================================================
# MARKET DOMAIN SENSORS (raw → staging → intermediate → master)
# =============================================================================

market_raw_to_staging_sensor = _create_job_chain_sensor(
    name="market_raw_to_staging_sensor",
    upstream_job_name="market_raw_job",
    downstream_job=market_staging_job,
    description="Triggers market_staging_job when market_raw_job completes successfully",
)

market_staging_to_intermediate_sensor = _create_job_chain_sensor(
    name="market_staging_to_intermediate_sensor",
    upstream_job_name="market_staging_job",
    downstream_job=market_intermediate_job,
    description="Triggers market_intermediate_job when market_staging_job completes successfully",
)

market_intermediate_to_master_sensor = _create_job_chain_sensor(
    name="market_intermediate_to_master_sensor",
    upstream_job_name="market_intermediate_job",
    downstream_job=market_master_job,
    description="Triggers market_master_job when market_intermediate_job completes successfully",
)

# =============================================================================
# FUNDAMENTAL DOMAIN SENSORS (raw → staging → intermediate → master)
# =============================================================================

fundamental_raw_to_staging_sensor = _create_job_chain_sensor(
    name="fundamental_raw_to_staging_sensor",
    upstream_job_name="fundamental_raw_job",
    downstream_job=fundamental_staging_job,
    description="Triggers fundamental_staging_job when fundamental_raw_job completes successfully",
)

fundamental_staging_to_intermediate_sensor = _create_job_chain_sensor(
    name="fundamental_staging_to_intermediate_sensor",
    upstream_job_name="fundamental_staging_job",
    downstream_job=fundamental_intermediate_job,
    description="Triggers fundamental_intermediate_job when fundamental_staging_job completes successfully",
)

fundamental_intermediate_to_master_sensor = _create_job_chain_sensor(
    name="fundamental_intermediate_to_master_sensor",
    upstream_job_name="fundamental_intermediate_job",
    downstream_job=fundamental_master_job,
    description="Triggers fundamental_master_job when fundamental_intermediate_job completes successfully",
)

# =============================================================================
# CROSS-DOMAIN SENSORS (reference → market → fundamental)
# =============================================================================

reference_to_market_sensor = _create_job_chain_sensor(
    name="reference_to_market_sensor",
    upstream_job_name="reference_master_job",
    downstream_job=market_raw_job,
    description="Triggers market_raw_job when reference_master_job completes successfully (cross-domain chaining)",
)

market_to_fundamental_sensor = _create_job_chain_sensor(
    name="market_to_fundamental_sensor",
    upstream_job_name="market_master_job",
    downstream_job=fundamental_raw_job,
    description="Triggers fundamental_raw_job when market_master_job completes successfully (cross-domain chaining)",
)

# =============================================================================
# Export all sensors
# =============================================================================

all_job_chaining_sensors = [
    # Reference chain: raw → staging → intermediate → master
    reference_raw_to_staging_sensor,
    reference_staging_to_intermediate_sensor,
    reference_intermediate_to_master_sensor,
    # Cross-domain: reference_master → market_raw
    reference_to_market_sensor,
    # Market chain: raw → staging → intermediate → master
    market_raw_to_staging_sensor,
    market_staging_to_intermediate_sensor,
    market_intermediate_to_master_sensor,
    # Cross-domain: market_master → fundamental_raw
    market_to_fundamental_sensor,
    # Fundamental chain: raw → staging → intermediate → master
    fundamental_raw_to_staging_sensor,
    fundamental_staging_to_intermediate_sensor,
    fundamental_intermediate_to_master_sensor,
]
