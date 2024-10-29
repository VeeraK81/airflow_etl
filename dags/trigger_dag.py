"""
Example of TriggerRule DAG.
"""
import random
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}


def _condition_python_function():
    random_value = random.choice([1, 2, 3])
    if random_value == 1:
        return "first_branch"
    elif random_value == 2:
        return "second_branch"
    else:
        return "third_branch"


with DAG(dag_id="trigger_dag", default_args=default_args, catchup=False) as dag:

    condition_python_function = BranchPythonOperator(
        task_id="condition_python_function",
        python_callable=_condition_python_function,
    )

    first_branch = BashOperator(task_id="first_branch", bash_command="echo 'First branch!'; sleep 1")

    second_branch = BashOperator(task_id="second_branch", bash_command="echo 'Second branch!'; sleep 1")

    third_branch = BashOperator(task_id="third_branch", bash_command="echo 'Third branch!'; sleep 1")

    join_all = BashOperator(task_id="join_all", bash_command="echo 'Join all!'", trigger_rule="one_success")

    condition_python_function >> [first_branch, second_branch, third_branch] >> join_all
