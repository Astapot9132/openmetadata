FROM docker.getcollate.io/openmetadata/ingestion:1.11.4

COPY --chown=airflow:0 custom_ingestors/dist/custom_ingestors-0.1.0-py3-none-any.whl /tmp/

RUN pip install --no-cache-dir /tmp/custom_ingestors-0.1.0-py3-none-any.whl && rm -f /tmp/custom_ingestors-0.1.0-py3-none-any.whl