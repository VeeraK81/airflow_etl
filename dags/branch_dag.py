import random
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}

with DAG(dag_id="parallel_tasks", default_args=default_args, catchup=False) as dag:
    start_dag = BashOperator(task_id="start_dag", bash_command="echo 'Start!'")

    first_branch = BashOperator(task_id="first_branch", bash_command="echo 'First branch!'; sleep 1")

    second_branch = BashOperator(task_id="second_branch", bash_command="echo 'Second branch!'; sleep 2")

    third_branch = BashOperator(task_id="third_branch", bash_command="echo 'Third branch!'; sleep 3")

    join_all = BashOperator(task_id="join_all", bash_command="echo 'Join all!'")

    (
        start_dag
        >> [first_branch, second_branch, third_branch]
        >> join_all
    )