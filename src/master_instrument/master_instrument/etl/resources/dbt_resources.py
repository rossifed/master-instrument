from dagster import file_relative_path
from dagster_dbt import DbtCliResource, DbtProject


dbt_project = DbtProject(project_dir=file_relative_path(__file__, "../../../dbt_project"))
dbt_resource = DbtCliResource(project_dir=dbt_project)