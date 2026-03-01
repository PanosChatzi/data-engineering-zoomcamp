"""NYC taxi data REST API pipeline."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


@dlt.source
def nyc_taxi_rest_api_source():
    """Define dlt resources from NYC taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "params": {
                        "limit": 1000,
                    },
                    "data_selector": "$",
                    "paginator": PageNumberPaginator(
                        page=1,
                        page_param="page",
                        total_path=None,
                        stop_after_empty_page=True,
                    ),
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)  # noqa: T201
