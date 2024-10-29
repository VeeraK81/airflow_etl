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
        return "first_branch_again"
    elif random_value == 2:
        return "second_branch_again"
    else:
        return "third_branch_again"


with DAG(dag_id="conditions_dag", default_args=default_args, catchup=False) as dag:
    start_dag = BashOperator(task_id="start_dag", bash_command="echo 'Start!'")

    first_branch = BashOperator(task_id="first_branch", bash_command="echo 'First branch!'; sleep 1")

    second_branch = BashOperator(task_id="second_branch", bash_command="echo 'Second branch!'; sleep 2")

    third_branch = BashOperator(task_id="third_branch", bash_command="echo 'Third branch!'; sleep 3")

    condition_python_function = BranchPythonOperator(
        task_id="condition_python_function",
        python_callable=_condition_python_function,
    )

    first_branch_again = BashOperator(task_id="first_branch_again", bash_command="echo 'First branch again!'; sleep 1")

    second_branch_again = BashOperator(
        task_id="second_branch_again",
        bash_command="echo 'Second branch again!'; sleep 2",
    )

    third_branch_again = BashOperator(task_id="third_branch_again", bash_command="echo 'Third branch again!'; sleep 3")

    join_all = BashOperator(task_id="join_all", bash_command="echo 'Join all!'")

    (
        start_dag
        >> [first_branch, second_branch, third_branch]
        >> condition_python_function
        >> [first_branch_again, second_branch_again, third_branch_again]
        >> join_all
    )