from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from logics_files import con_data
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 17),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    dag_id="dds_unparsed_messages_dag",
    default_args=default_args,
    description="Sync DDS.unparsed_messages from ODS",
    schedule_interval= None, #"*/10 * * * *",  # каждые 10 минут
    catchup=False,
    max_active_runs=1,
    tags=["dds", "elt"],
)

# --- SQL ---
INSERT_SQL = """
INSERT INTO "DDS".unparsed_messages (
    msg_id, row_num, count_uav, entity_code,
    from_dt, to_dt, msg_dt, load_dt, msg_text, msg_sample
)
SELECT 
    msg_id, row_num, count_uav, entity_code,
    from_dt, to_dt, msg_dt, NOW(), msg_text, msg_sample
FROM "ODS".parsed_messages
WHERE msg_id IN (
    SELECT DISTINCT ON (msg_id) msg_id
    FROM "ODS".parsed_messages
    WHERE row_num IS NULL
       OR entity_code IS NULL
       OR count_uav IS NULL
       OR count_uav = 0
);
"""
# -------------------- Функция --------------------
def run_sync():
    conn = psycopg2.connect(
        host=con_data.host,
        port=con_data.port,
        dbname=con_data.dbname,
        user=con_data.user,
        password=con_data.password,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(INSERT_SQL)    # INSERT
    cur.close()
    conn.close()
# -------------------- Task --------------------
sync_task = PythonOperator(
    task_id="sync_dds_unparsed_messages",
    python_callable=run_sync,
    dag=dag,
)
trigger_dds_messages = TriggerDagRunOperator(
    task_id="trigger_dds_messages",
    trigger_dag_id="dds_messages_dag",
    wait_for_completion=False,
    dag=dag,
)
sync_task >> trigger_dds_messages
