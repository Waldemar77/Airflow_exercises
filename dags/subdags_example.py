from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from datetime import datetime
from subdags.subdag_downloads import subdag_downloads
from subdags.subdag_transforms import subdag_trasforms

with DAG(
    "group_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["udemy_course"],
) as dag:
    args = {
        "start_date": dag.start_date,
        "schedule_interval": dag.schedule_interval,
        "catchup": dag.catchup,
        "tags": dag.tags[0],
    }

    downloads = SubDagOperator(
        task_id="downloads", subdag=subdag_downloads(dag.dag_id, "downloads", args)
    )

    check_files = BashOperator(task_id="check_files", bash_command="sleep 10")

    transforms = SubDagOperator(
        task_id="transforms", subdag=subdag_trasforms(dag.dag_id, "transforms", args)
    )

    (downloads >> check_files >> transforms)
