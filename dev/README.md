# Development Environment

The development environment is based on Docker Compose to streamline the use on any host OS.

## Prerequisites

1. Install Docker

    * Follow the instructions in [Docs](https://docs.docker.com/get-docker/) to install Docker.
    * Start Docker Desktop.

2. Install Google Cloud CLI

    * Follow the instructions in [Docs](https://cloud.google.com/sdk/docs/install) to install the Google Cloud CLI.
    * Initialize and authenticate the gcloud CLI.

      Select your target GCP project when prompted.
      ```
      gcloud init
      gcloud auth application-default login
      ```
      
3. Generate a Kaggle key and configure the environment

    * Follow the instructions on [Kaggle website](https://www.kaggle.com/docs/api#authentication) to generate an API key.
    * Create a `.env` file based on `.env.template`. Edit the KAGGLE_USERNAME and KAGGLE_KEY values.

## Running individual dags

Run a single DAG, for example the `extract_ecommerce_events`

    docker compose -f dev/docker-compose.yaml  run airflow-worker airflow dags test extract_ecommerce_events

Running any Airflow command, for example `info`

    docker compose -f dev/docker-compose.yaml  run airflow-worker airflow info

## Running the Airflow UI

RUN the Airflow UI:

    docker compose -f dev/docker-compose.yaml up

The Airflow UI is available at [http://localhost:8080](http://localhost:8080) with user `airflow` and password `airflow`.

To clear the dev env state (remove stopped containers, clear the Airflow database), run:

    docker compose -f dev/docker-compose.yaml down --volumes --remove-orphans

To drop outdated, dangling images after editing the Dockerfile:

    docker image prune

To drop all images:

    docker --rmi all


