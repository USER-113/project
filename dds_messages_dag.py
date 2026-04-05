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
    dag_id="dds_messages_dag",
    default_args=default_args,
    description="Sync DDS.messages from ODS",
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
    tags=["dds", "elt"],
)
# -------------------- SQL --------------------
UPDATE_SQL = """
update "DDS".messages
	set 
	msg_id=updt.msg_id, 
	row_num=updt.row_num, 
	count_uav=updt.count_uav, 
	entity_code=updt.entity_code, 
	from_dt=updt.from_dt, 
	to_dt=updt.to_dt, 
	msg_dt=updt.msg_dt,
	load_dt=now(),
	msg_text=updt.msg_text, 
	msg_sample=updt.msg_sample
from
(select distinct on (msg_id, row_num, count_uav, entity_code)
msg_id, row_num, count_uav, entity_code, from_dt, to_dt, msg_dt, msg_text, msg_sample
from "ODS".parsed_messages
where "ODS".parsed_messages.msg_id not in 
(select 
distinct on (msg_id)
"ODS".parsed_messages.msg_id
from "ODS".parsed_messages 
where "ODS".parsed_messages.row_num is null or "ODS".parsed_messages.entity_code is null 
or "ODS".parsed_messages.count_uav is null or "ODS".parsed_messages.count_uav  = 0
)) as updt where
	"DDS".messages.msg_id = updt.msg_id 
	and "DDS".messages.entity_code = updt.entity_code;
"""

INSERT_SQL = """
insert into "DDS".messages (msg_id, row_num, count_uav, entity_code, from_dt, to_dt, msg_dt, load_dt, msg_text, msg_sample)
select
insr.msg_id, 
insr.row_num, 
insr.count_uav, 
insr.entity_code, 
insr.from_dt, 
insr.to_dt, 
insr.msg_dt, 
now(), 
insr.msg_text, 
insr.msg_sample
from
(select distinct on (msg_id, row_num, count_uav, entity_code)
msg_id, row_num, count_uav, entity_code, from_dt, to_dt, msg_dt, msg_text, msg_sample
from "ODS".parsed_messages
where "ODS".parsed_messages.msg_id not in 
(select 
distinct on (msg_id)
"ODS".parsed_messages.msg_id
from "ODS".parsed_messages 
where "ODS".parsed_messages.row_num is null or "ODS".parsed_messages.entity_code is null 
or "ODS".parsed_messages.count_uav is null or "ODS".parsed_messages.count_uav  = 0
)) as insr where 
insr.msg_id||insr.entity_code not in (select "DDS".messages.msg_id||"DDS".messages.entity_code from "DDS".messages);
truncate table "ODS".parsed_messages;
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
    task_id="sync_dds_messages",
    python_callable=run_sync,
    dag=dag,
)