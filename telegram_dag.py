from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import sys

# --------------------|        Аргументы DAG        |--------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 17),  # дата запуска DAG
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
# --------------------|             DAG            |--------------------
dag = DAG(
    dag_id="telegram_dag",
    default_args=default_args,
    description="Dag загрузки сообщений из Telegram в PostgreSQL",
    schedule_interval="59 23 * * *",  # поставить None для ручного запуска
    catchup=False,
    max_active_runs=1,                                                  # Исключаем наложение запусков
    concurrency=1,
    tags=["telegram", "elt"],
)
"""
*    *    *    *    *
│    │    │    │    │
│    │    │    │    └─ день недели (0–7, где 0 и 7 — воскресенье)
│    │    │    └───── месяц (1–12)
│    │    └────────── день месяца (1–31)
│    └─────────────── час (0–23)
└─────────────────── минута (0–59)
* * * * *	Каждую минуту
0 * * * *	Каждый час в 0 минут
0 6 * * *	Каждый день в 06:00
0 0 * * 0	Каждое воскресенье в 00:00
0 */6 * * *	Каждые 6 часов в 0 минут
30 9 * * 1-5	В будние дни (Mon–Fri) в 09:30
*/<значение> - означает каждое указанное значение
"""
# --------------------|       Пути к файлам        |--------------------
dag_directory = os.path.dirname(os.path.abspath(__file__))
script_path = os.path.join(dag_directory, "logics_files", "telegram_load.py")
# --------------------|            Задача          |--------------------
run_parser = BashOperator(
    task_id="run_telegram_load",
    bash_command=f"{sys.executable} {script_path}",
    execution_timeout=timedelta(minutes=30),                            # Убиваем зависшие процессы через 30 минут
    dag=dag
 ) 
# -----|Условия запуска второго dag в случае успешного завершения|------
trigger_ods = TriggerDagRunOperator(
    task_id="trigger_ods_dag",
    trigger_dag_id="ods_dag",  # dag_id второго DAG
    wait_for_completion=False,  # True если нужно ждать завершения
    dag=dag,
)
# ----------|Запуск второго dag в случае успешного завершения|----------
run_parser >> trigger_ods