# pyright: reportCallIssue=none
from dagster_sling import SlingConnectionResource, SlingResource

from dagster import EnvVar
sling_resources = SlingResource(
    connections=[
        SlingConnectionResource(
            name="QA_MSSQL",
            type="sqlserver",
            connection_string="sqlserver://sa:%3Ds3F%7BJ2%3CaU%275%2Bx9eaq@192.168.123.20:1433?database=qai",
        ), # type: ignore
        SlingConnectionResource(
            name="REFERENTIAL_POSTGRES",
            type="postgres",
            connection_string="postgres://postgres:postgres123@host.docker.internal:5434/master-instrument?sslmode=disable",
        ), # type: ignore
    
]
)