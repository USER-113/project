from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import sys

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 31),
}

dag = DAG(
    dag_id="ods_dag",
    default_args=default_args,
    schedule_interval=None,  # отключен запуск по расписанию
    catchup=False,
)

dag_directory = os.path.dirname(os.path.abspath(__file__))
script_path = os.path.join(dag_directory, "logics_files", "drone_parsing.py")

run_drone_parsing = BashOperator(
    task_id="run_drone_parsing",
    bash_command=f"{sys.executable} {script_path}",
    dag=dag,
)
trigger_dds_unparsed = TriggerDagRunOperator(
    task_id="trigger_dds_unparsed_messages",
    trigger_dag_id="dds_unparsed_messages_dag",
    wait_for_completion=False,  # можно True, если нужно ждать
    dag=dag,
)
run_drone_parsing >> trigger_dds_unparsed