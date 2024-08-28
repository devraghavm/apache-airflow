from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2022, 11, 16)
}

with DAG(dag_id="pools", default_args=default_args, schedule_interval=timedelta(1)) as dag:
    t1 = BashOperator(task_id="task-1", bash_command="sleep 5", pool="pool_1")
    t1 = BashOperator(task_id="task-2", bash_command="sleep 5", pool="pool_1")
    t1 = BashOperator(task_id="task-3", bash_command="sleep 5",
                      pool="pool_2", priority_weight=2)
    t1 = BashOperator(task_id="task-4", bash_command="sleep 5", pool="pool_2")
