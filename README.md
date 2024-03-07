# A sample ELT pipeline for a public ecommerce dataset

The project uses [Kaggle ecommerce dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop) to demonstrate a simple ELT pipeline using Apache Airflow and Google Cloud Platform.

## Running the project

Follow the instructions in [Development Environment](dev/README.md) to set up the development environment.

## Source data

The source data structure is documented in [dataset.md](docs/dataset.md)

## IDE setup

To set up a local virtualenv for the IDE suggestions, use the following commands:

    python3.10 -m venv venv/
    pip install --upgrade pip
    pip install 'apache-airflow==2.8.1' \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
    pip install -r dev/requirements.txt
