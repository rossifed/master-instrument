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
        ),
        SlingConnectionResource(
            name="S3_FUNDY_DEV",
            type="s3",
            bucket="fundy-dev",
            region_name=EnvVar("S3_REGION"),    
            access_key_id=EnvVar("S3_ACCESS_KEY_ID"),
            secret_access_key=EnvVar("S3_SECRET_ACCESS_KEY"),
        ), # type: ignore
        SlingConnectionResource(
            name="SLING_POSTGRES",
            type="postgres",
            connection_string=EnvVar("REFERENTIAL_POSTGRES_CONN"),
        ),
    
]
)