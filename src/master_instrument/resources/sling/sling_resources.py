# pyright: reportCallIssue=none
from dagster_sling import SlingConnectionResource, SlingResource

from dagster import EnvVar
sling_resources = SlingResource(
    connections=[
        SlingConnectionResource(
            name="QA_MSSQL",
            type="sqlserver",
            host=EnvVar("QA_HOST"),
            user=EnvVar("QA_USER"),
            password=EnvVar("QA_PSW"),
            database=EnvVar("QA_DB"),
        ), # type: ignore
        SlingConnectionResource(
            name="REFERENTIAL_POSTGRES",
            type="POSTGRES",
            host=EnvVar("PG_HOST"),
            user=EnvVar("PG_USER"),
            port=EnvVar("PG_PORT"),
            database=EnvVar("PG_DB"),
        ), # type: ignore
    
]
)