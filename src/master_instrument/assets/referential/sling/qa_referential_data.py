from dagster import file_relative_path
from dagster_sling import SlingResource, sling_assets
from master_instrument.resources.sling.translators import SlingTranslator

replication_config = file_relative_path(
    __file__, "../../../../config/sling/referential/qa_replications.yaml"
)

@sling_assets(replication_config=replication_config)
def qa_referential_data(context, sling: SlingResource):
    yield from sling.replicate(context=context)
