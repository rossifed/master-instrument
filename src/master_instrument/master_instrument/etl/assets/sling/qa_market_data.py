import os
from dagster import file_relative_path
from dagster_sling import SlingResource, sling_assets


replication_config = file_relative_path(
    __file__, "../../../../sling/replications/qa/market_replications.yaml"
)

@sling_assets(replication_config=replication_config)
def qa_market_data(context, sling: SlingResource):
    # Inject environment variable for Sling - generic timeseries start date
    timeseries_start = os.getenv('TIMESERIES_START_DATE', '2025-01-01')
    os.environ['TIMESERIES_START_DATE'] = timeseries_start
    
    # Execute Sling replication
    yield from sling.replicate(context=context)
