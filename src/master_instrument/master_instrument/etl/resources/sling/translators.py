from typing import Any, Mapping
from dagster import AssetSpec
from dagster_sling import DagsterSlingTranslator


class SlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition: Mapping[str, Any]) -> AssetSpec:
        default_spec = super().get_asset_spec(stream_definition)

        try:
            description = stream_definition["config"]["meta"]["description"]
        except KeyError:
            description = ""

        return default_spec.replace_attributes(
            deps=[],
            description=description,
        )
