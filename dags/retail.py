# retail.py
import os
from dotenv import load_dotenv
import requests
import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

load_dotenv()
URL = "https://countryapi.io/api/all?apikey=" + os.getenv("API_KEY")

@task
def get_country():
    if not os.path.exists('include/dataset/country.csv'):
        response = requests.get(URL) #fetch API
        #process Dataframe
        df = pd.DataFrame(response.json()).T.reset_index().drop(columns='index')[['name', 'official_name', 'alpha3Code', 'capital']]
        df.to_csv('include/dataset/country.csv',index=False)

@dag (
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=['retail'],
)

def retail():

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src=['include/dataset/online_retail.csv','include/dataset/country.csv'],
        dst='raw/',
        bucket='siriwut-online-retail',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )

    @task_group(group_id = 'load_to_bq')
    def load_to_bq():
        gcs_to_bq_invoice = aql.load_file(
            task_id='gcs_to_bq_invoice',
            input_file=File(
                'gs://siriwut-online-retail/raw/online_retail.csv',
                conn_id='gcp',
                filetype=FileType.CSV,
            ),
            output_table=Table(
                name='raw_invoices',
                conn_id='gcp',
                metadata=Metadata(schema='retail')
            ),
            use_native_support=False,
            if_exists="replace"
        )

        gcs_to_bq_country = GCSToBigQueryOperator(
            task_id='gcs_to_bq_country',
            bucket= "siriwut-online-retail",
            source_objects = ["raw/country.csv"],
            destination_project_dataset_table='retail.country',
            schema_fields=[
                {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'official_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'alpha3Code', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'capital', 'type': 'STRING', 'mode': 'NULLABLE'},
            ],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
       )
        [gcs_to_bq_invoice, gcs_to_bq_country]

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    report = DbtTaskGroup(
        group_id='create_view',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    chain(
        get_country(),
        upload_csv_to_gcs,
        create_retail_dataset,
        load_to_bq(),
        transform,
        report
    )

retail()