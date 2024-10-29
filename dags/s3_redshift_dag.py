import ccxt

from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator




default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}


def _create_small_csv():
    """Creates a small CSV and saves it to `/tmp/my_csv_file.csv`.
    """
    pd.DataFrame({
        "first_name": ["Dark", "Dark"],
        "last_name": ["Vador", "Maul"]
    }).to_csv("/tmp/my_csv_file.csv", header=None, index=False)


def _upload_to_s3(ti):
    """Uploads our `/tmp/my_csv_file.csv` to s3 using hooks.
    """
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_file(
        filename="/tmp/my_csv_file.csv",
        key="my_csv_file.csv",
        bucket_name=Variable.get("S3BucketName"),
        replace=True
    )


with DAG(dag_id="s3_redshift_dag", default_args=default_args, catchup=False) as dag:
    create_small_csv = PythonOperator(task_id="create_small_csv", python_callable=_create_small_csv)

    upload_to_s3 = PythonOperator(task_id="upload_to_s3", python_callable=_upload_to_s3)
    
    create_redshift_table = RedshiftSQLOperator(
        task_id="create_redshift_table",
        sql="""
        CREATE TABLE IF NOT EXISTS my_table (
            first_name VARCHAR,
            last_name VARCHAR
        )
        """,
        redshift_conn_id="redshift_default" )
    
    s3_to_redshift = S3ToRedshiftOperator(
        task_id="s3_to_redshift",
        schema="PUBLIC",
        table="my_table",
        s3_bucket="{{ var.value.S3BucketName }}",
        s3_key="my_csv_file.csv",
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
        copy_options=["csv"]
    )

    create_small_csv >> upload_to_s3 >> create_redshift_table >> s3_to_redshift