import datetime
import logging
import os
import csv
import json
import re
import subprocess
import tempfile


from airflow import DAG
from airflow.utils.dates import datetime, timedelta
from google.cloud import storage, bigquery
from pathlib import Path


DATASET = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
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
    task_logger.info("Storing file %s in GCS", local_path.name)
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
            reader = csv.DictReader(run_result.stdout.decode().splitlines())
            rows = [row for row in reader]
            task_logger.info('Found files:\n%s', '\n'.join(json.dumps(row) for row in rows))
            # return [row.get('name') for row in rows]
            return [rows[0].get('name')]
        except subprocess.CalledProcessError as e:
            task_logger.error('Failed to list files in dataset %s error %s, %s', dataset, e.stdout, e.stderr)
            raise e


    @task
    def store_dataset_file(dataset: str, file: str) -> str:
        task_logger.info("Processing file %s from dataset %s", file, dataset)
        command = f"kaggle datasets download {dataset} -f {file} --quiet"
        with tempfile.TemporaryDirectory() as tmpdirname:
            subprocess.run(command, check=True, cwd=tmpdirname, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            archive_name, = os.listdir(tmpdirname)
            events_date = datetime.strptime(archive_name.split('.')[0], '%Y-%b').date()
            task_logger.info('Downloaded file %s with date %s', archive_name, events_date)
            destination_filename = re.sub(r'^[^.]*', events_date.strftime('%Y-%m'), archive_name)
            return store_file_in_gcs(local_path=Path(tmpdirname) / archive_name, destination_filename=destination_filename)

    @task
    def ingest_file(gcp_path: str, dataset: str, table: str):
        task_logger.info("Ingesting file %s to BiqQuery dataset %s", gcp_path, dataset)
        schema = load_bq_schema_from_json()

        client = bigquery.Client()

        table_id = f"{dataset}.{table}"

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"
        load_job = client.load_table_from_uri(
            gcp_path, table_id, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        print(
            "Loaded {} rows into {}.".format(
                load_job.output_rows, table_id
            )
        )
        destination_table = client.get_table(table_id)
        destination_table.description = "Updated description"
        client.update_table(destination_table, ["description"])


    # dataset_files = list_dataset_files(DATASET)
    # check_gcp() >> store_dataset_file.partial(dataset=DATASET).expand(file=dataset_files)

    ingest_file("gs://ecommerce-events/2019-Oct.csv", "ecommerce_events", "source_events_load")


if __name__ == "__main__":
    dag.test()