from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from postgres_operator import MyPostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}


with DAG(dag_id="your_own_operator_dag", default_args=default_args, catchup=False) as dag:
    start_dag = BashOperator(task_id="start_dag", bash_command="echo 'Start!'")

    create_table = PostgresOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS starwars (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL);
        """,
        postgres_conn_id="postgres_default",
    )

    insert_in_table = MyPostgresOperator(
        task_id="insert_in_table",
        postgres_conn_id="postgres_default",
        sql="INSERT INTO starwars (name) VALUES ('Darth JarJar')",
    )

    end_dag = BashOperator(task_id="end_dag", bash_command="echo 'End!'")

    start_dag >> create_table >> insert_in_table >> end_dag