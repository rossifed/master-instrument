from dagster import file_relative_path
from dagster_sling import SlingResource, sling_assets


replication_config = file_relative_path(
    __file__, "../../../../sling/replications/qa/estimates_replications.yaml"
)

@sling_assets(replication_config=replication_config)
def qa_estimates_data(context, sling: SlingResource):
    """Load estimates data from QA (IBES actuals, TRE products)."""
    yield from sling.replicate(context=context)
