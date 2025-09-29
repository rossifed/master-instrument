# pyright: reportCallIssue=none
from dagster_sling import SlingConnectionResource, SlingResource

from dagster import EnvVar
sling_resources = SlingResource(
    connections=[
        SlingConnectionResource(
            name="QA_MSSQL",
            type="sqlserver",
            connection_string=EnvVar("QA_MSSQL_CONN"),
        ), # type: ignore
        SlingConnectionResource(
            name="REFERENTIAL_POSTGRES",
            type="postgres",
            connection_string=EnvVar("REFERENTIAL_POSTGRES_CONN"),
        ), # type: ignore
    
]
)