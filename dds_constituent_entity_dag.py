from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from logics_files import con_data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 17),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    dag_id="dds_constituent_entity_dag",
    default_args=default_args,
    description="Sync DDS.constituent_entity from ODS",
    schedule_interval="*/10 * * * *",  # каждые 10 минут
    catchup=False,
    max_active_runs=1,
    tags=["dds", "elt"],
)
# -------------------- SQL --------------------
UPDATE_SQL = """
UPDATE "DDS".constituent_entity d
SET
    entity_name = o.entity_name,
    entity_code = o.entity_code,
    entity_type = o.entity_type,
    entity_active = o.entity_active,
    entity_shot_name = o.entity_shot_name,
    update_dt = now()
FROM "ODS".constituent_entity o
WHERE d.entity_code = o.entity_code;
"""

INSERT_SQL = """
INSERT INTO "DDS".constituent_entity
(
    entity_name,
    entity_code,
    entity_type,
    entity_active,
    entity_shot_name,
    update_dt,
    load_dt
)
SELECT
    o.entity_name,
    o.entity_code,
    o.entity_type,
    o.entity_active,
    o.entity_shot_name,
    now(),
    now()
FROM "ODS".constituent_entity o
WHERE NOT EXISTS (
    SELECT 1
    FROM "DDS".constituent_entity d
    WHERE d.entity_code = o.entity_code
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
    cur.execute(UPDATE_SQL)    # UPDATE
    cur.execute(INSERT_SQL)    # INSERT
    cur.close()
    conn.close()
# -------------------- Task --------------------
sync_task = PythonOperator(
    task_id="sync_dds_constituent_entity",
    python_callable=run_sync,
    dag=dag,
)