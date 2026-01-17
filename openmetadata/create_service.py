#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from pprint import pprint
import requests
from typing import Dict, List, Optional
from urllib import error, request
from cfg import CLICKHOUSE_HOST_PORT, CLICKHOUSE_USERNAME, CLICKHOUSE_DB_NAME, CLICKHOUSE_PASSWORD, \
    CLICKHOUSE_DATABASE_SCHEMA, CLICKHOUSE_SERVICE_DESCRIPTION, CLICKHOUSE_SERVICE_NAME, OPENMETADATA_HOST_PORT, \
    OPENMETADATA_API_TOKEN, CLICKHOUSE_DB_INCLUDES, CLICKHOUSE_DB_EXCLUDES, CLICKHOUSE_SCHEMA_INCLUDES, \
    CLICKHOUSE_SCHEMA_EXCLUDES, CLICKHOUSE_TABLE_INCLUDES, CLICKHOUSE_TABLE_EXCLUDES, CLICKHOUSE_USE_FQN_FILTERS
from create_filters import create_filters


def build_payload() -> Dict[str, object]:

    connection_config: Dict[str, object] = {
        "type": "CustomDatabase",
        "sourcePythonClass": "custom_ingestors.clickhouse.ClickhouseCustomIngestor",
        "hostPort": CLICKHOUSE_HOST_PORT,
        "username": CLICKHOUSE_USERNAME,
        "password": CLICKHOUSE_PASSWORD,
        "databaseName": CLICKHOUSE_DB_NAME

    }
    
    if CLICKHOUSE_DATABASE_SCHEMA:
        connection_config.update({"databaseSchema": CLICKHOUSE_DATABASE_SCHEMA})

    filters = create_filters()
    if filters:
        connection_config.update(filters)

    payload: Dict[str, object] = {
        "name": CLICKHOUSE_SERVICE_NAME,
        "serviceType": "CustomDatabase",
        "connection": {"config": connection_config},
    }
    if CLICKHOUSE_SERVICE_DESCRIPTION:
        payload["description"] = CLICKHOUSE_SERVICE_DESCRIPTION
    return payload


def create_or_update_service(
    base_url: str, token: str, service_name: str, payload: Dict[str, object], test_run: bool = True
) -> None:
    url = f"{base_url.rstrip('/')}/api/v1/services/databaseServices"
    if test_run:
        print("Dry-run mode: would send payload to", url)
        print(json.dumps(payload, indent=2))
        return

    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "openmetadata-clickhouse-helper/1.0",
        "Authorization": f"Bearer {token}"
    }
    if not token:
        raise Exception('Добавьте токен авторизации')
    print(payload)
    try:
        resp = requests.put(url=url, data=data, headers=headers)
        print(f"Service '{service_name}' created/updated successfully.")
        if resp.json():
            print(resp.json())
    except error.HTTPError as http_error:
        message = http_error.read().decode("utf-8")
        raise RuntimeError(
            f"OpenMetadata API returned {http_error.code}: {message}"
        ) from http_error
    except error.URLError as url_error:
        raise RuntimeError(f"Failed to reach OpenMetadata API: {url_error}") from url_error


def main() -> None:
    payload = build_payload()
    pprint(payload)
    create_or_update_service(
        base_url=OPENMETADATA_HOST_PORT,  # pyright: ignore [reportArgumentType]
        token=OPENMETADATA_API_TOKEN,
        service_name=CLICKHOUSE_SERVICE_NAME,  # pyright: ignore [reportArgumentType]
        payload=payload,
        test_run=False
    )


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as runtime_error:
        raise SystemExit(f"[runtime error] {runtime_error}") from runtime_error