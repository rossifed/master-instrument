import os
from dagster import file_relative_path
from dagster_sling import SlingResource, sling_assets


replication_config = file_relative_path(
    __file__, "../../../../sling/replications/qa/fundamental_replications.yaml"
)

@sling_assets(replication_config=replication_config)
def qa_fundamental_data(context, sling: SlingResource):

    yield from sling.replicate(context=context)
