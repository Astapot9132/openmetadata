import time
from pprint import pprint

import requests

from cfg import OPENMETADATA_HOST_PORT, CLICKHOUSE_SERVICE_NAME, OPENMETADATA_API_TOKEN, CLICKHOUSE_USE_FQN_FILTERS, \
    CLICKHOUSE_MARK_DELETED_TABLES, SCHEDULE_INTERVAL, CLICKHOUSE_SCHEMA_INCLUDES, CLICKHOUSE_MARK_DELETED_SCHEMAS, \
    CLICKHOUSE_OVERRIDE_METADATA
from create_filters import create_filters


def delete_pipeline_if_exists(pipeline_name: str) -> None:
    """Ensure we start from a clean slate before creating an ingestion pipeline."""
    auth_headers = {"Authorization": f"Bearer {OPENMETADATA_API_TOKEN}"}
    lookup = requests.get(
        f"{OPENMETADATA_HOST_PORT}/api/v1/services/ingestionPipelines/name/{pipeline_name}",
        headers=auth_headers,
        params={"fields": "id"},
        timeout=30,
    )

    if lookup.status_code == 404:
        print(f"Ingestion '{pipeline_name}' not found, nothing to delete.")
        return

    lookup.raise_for_status()
    pipeline_id = lookup.json()["id"]
    delete = requests.delete(
        f"{OPENMETADATA_HOST_PORT}/api/v1/services/ingestionPipelines/{pipeline_id}",
        headers=auth_headers,
        params={"hardDelete": "true"},
        timeout=30,
    )
    delete.raise_for_status()
    print(f"Removed existing ingestion '{pipeline_name}' ({pipeline_id}).")

def main():
    resp = requests.get(
        f"{OPENMETADATA_HOST_PORT}/api/v1/services/databaseServices/name/{CLICKHOUSE_SERVICE_NAME}",
        headers={"Authorization": f"Bearer {OPENMETADATA_API_TOKEN}"},
        params={"fields": "id"},
        timeout=30,
    )
    service_id = resp.json()["id"]
    print(service_id)
    print(resp.json())
    payload = {
        "name": "clickhouse_metadata_hourly",
        "pipelineType": "metadata",
        "service": {"id": service_id, "type": "databaseService"},
        "sourceConfig": {
            "config": {
                "type": "DatabaseMetadata",
                "useFqnForFiltering": CLICKHOUSE_USE_FQN_FILTERS,
                "overrideMetadata": CLICKHOUSE_OVERRIDE_METADATA,
                "markDeletedTables": CLICKHOUSE_MARK_DELETED_TABLES,
                "markDeletedSchemas": CLICKHOUSE_MARK_DELETED_SCHEMAS,
            }
        },
        "airflowConfig": {"scheduleInterval": SCHEDULE_INTERVAL, "pipelineTimezone": "UTC"},
        "raiseOnError": True
    }
    filters = create_filters()
    if filters:
        payload["sourceConfig"]["config"].update(filters)
    resp = requests.post(
        f"{OPENMETADATA_HOST_PORT}/api/v1/services/ingestionPipelines",
        headers={"Authorization": f"Bearer {OPENMETADATA_API_TOKEN}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    pipeline = resp.json()
    print(pipeline)
    
    pipeline_id = pipeline["id"]
    for action in ("deploy", "trigger"):
        print(action)
        resp = requests.post(
            f"{OPENMETADATA_HOST_PORT}/api/v1/services/ingestionPipelines/{action}/{pipeline_id}",
            headers={"Authorization": f"Bearer {OPENMETADATA_API_TOKEN}"},
            timeout=30,
        )
        resp.raise_for_status()
        if action == 'deploy':
            time.sleep(2)



if __name__ == '__main__':
    delete_pipeline_if_exists(f'{CLICKHOUSE_SERVICE_NAME}.clickhouse_metadata_hourly')
    main()