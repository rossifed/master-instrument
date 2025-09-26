from dagster import file_relative_path
from dagster_sling import sling_assets, SlingResource
from ...resources.sling.translators import SlingTranslator   # import local

replication_config=file_relative_path(__file__, "../../../config/sling/referential/refinitive_replications.yaml")

@sling_assets(replication_config=replication_config, dagster_sling_translator=SlingTranslator())
def qa_referential_data(context, sling: SlingResource):
    yield from sling.replicate(context=context)