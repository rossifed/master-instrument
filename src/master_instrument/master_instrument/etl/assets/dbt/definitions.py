import dagster as dg
from dagster._utils import file_relative_path
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets


dbt_project = DbtProject(project_dir=file_relative_path(__file__, "../../../../dbt_project"))
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
