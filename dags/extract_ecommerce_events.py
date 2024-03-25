import datetime
import logging
import os
import csv
import json
import re
import subprocess
import tempfile
import zipfile

import google
from airflow import DAG
from airflow.utils.dates import datetime, timedelta
from google.cloud import storage, bigquery
from pathlib import Path


SOURCE_KAGGLE_DATASET = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
DESTINATION_BIGQUERY_DATASET = "landing_ecommerce_events"

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = Path(CURRENT_PATH) / "schema.json"


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 1),
    # "end_date": datetime(2020, 3, 1) - timedelta(days=1),
    "email": "email@example.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "owner": "Data Engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

from airflow.decorators import task


task_logger = logging.getLogger(__name__)


def store_file_in_gcs(local_path: Path, bucket_name: str = "ecommerce-events", destination_filename: str = "ecommerce-events") -> str:
    task_logger.info("Storing file %s in GCS under %s", local_path.name, destination_filename)
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_filename)
    blob.upload_from_filename(local_path)
    gcs_path = f'gs://{bucket_name}/{destination_filename}'
    task_logger.info("File %s stored in GCS as %s", local_path.name, gcs_path)
    return gcs_path


def load_bq_schema_from_json(path: Path = SCHEMA_PATH) -> list[bigquery.SchemaField]:
    with open(path, 'r') as f:
        schema_json = json.load(f)
    return [bigquery.SchemaField(**field) for field in schema_json]


with DAG(
    "extract_ecommerce_events",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=8),
    description="Extracting ecommerce events",
    schedule_interval='@monthly',
    catchup=False,
) as dag:

    @task
    def check_gcp():
        """
        Checks if GCP credentials are available.
        """
        try:
            client = storage.Client()
            client.list_buckets()
            task_logger.info("GCP credentials are available")
        except Exception as e:
            task_logger.error("GCP credentials are not available: %s", e)
            raise e

    @task
    def list_dataset_files(dataset: str) -> list[str]:
        """
        Kaggle credentials are read from KAGGLE_USERNAME and KAGGLE_KEY env variables.
        """
        command = f"kaggle datasets files {dataset} --csv"
        try:
            run_result = subprocess.run(command, check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            text_output = run_result.stdout.decode().splitlines()
            if text_output[0].startswith('Warning: Looks like you\'re using an outdated API Version'):
                task_logger.warning('Kaggle API version warning: %s', text_output[0])
                text_output = text_output[1:]
            reader = csv.DictReader(text_output)
            rows = [row for row in reader]
            task_logger.info('Found files:\n%s', '\n'.join(json.dumps(row) for row in rows))
            return [row.get('name') for row in rows]
        except subprocess.CalledProcessError as e:
            task_logger.error('Failed to list files in dataset %s error %s, %s', dataset, e.stdout, e.stderr)
            raise e


    @task
    def store_dataset_file(dataset: str, file: str) -> str:
        task_logger.info("Processing file %s from dataset %s", file, dataset)
        command = f"kaggle datasets download {dataset} -f {file} --quiet"
        with tempfile.TemporaryDirectory() as tmpdirname:
            subprocess.run(command, check=True, cwd=tmpdirname, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            # BigQuery load doesn't support loading from zipped files, so we need to unzip if before upload
            archive_name, = os.listdir(tmpdirname)
            with zipfile.ZipFile(Path(tmpdirname, archive_name), 'r') as zip_ref:
                zip_ref.extractall(tmpdirname)
            events_date = datetime.strptime(file.split('.')[0], '%Y-%b').date()
            task_logger.info('Downloaded file %s with date %s', file, events_date)
            destination_filename = re.sub(r'^[^.]*', events_date.strftime('%Y-%m'), file)
            return store_file_in_gcs(local_path=Path(tmpdirname) / file, destination_filename=destination_filename)

    @task
    def ingest_file(gcp_path: str, dataset: str):
        task_logger.info("Ingesting file %s to BiqQuery dataset %s", gcp_path, dataset)
        schema = load_bq_schema_from_json()

        client = bigquery.Client()
        table = os.path.basename(gcp_path).split('.')[0]
        table_id = f"{dataset}.{table}"

        try:
            client.get_dataset(dataset)
        except google.api_core.exceptions.NotFound as e:
            task_logger.error("Destination dataset %s not found: %s", dataset, e)
            raise e

        try:
            destination_table = client.get_table(table_id)
            task_logger.info("Destination table %s already exists, deleting it", destination_table)
            client.delete_table(table_id)
        except google.api_core.exceptions.NotFound:
            task_logger.info("Destination table %s not found, creating a table from scratch", table_id)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        load_job = client.load_table_from_uri(gcp_path, table_id, job_config=job_config)
        load_job.result()
        task_logger.info("Loaded {:,} rows into {}.".format(load_job.output_rows, table_id))
        destination_table = client.get_table(table_id)
        destination_table.description = f"Loaded from %s with %s" % (SOURCE_KAGGLE_DATASET, __file__)
        client.update_table(destination_table, ["description"])


    source_dataset_files = list_dataset_files(SOURCE_KAGGLE_DATASET)
    stored_files = check_gcp() >> store_dataset_file.partial(dataset=SOURCE_KAGGLE_DATASET).expand(
        file=source_dataset_files)
    stored_files >> ingest_file.partial(dataset=DESTINATION_BIGQUERY_DATASET).expand(gcp_path=stored_files)


if __name__ == "__main__":
    dag.test()