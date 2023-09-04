from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime
from group_task.download_tasks import downloads_tasks
from group_task.transform_tasks import trasforms_tasks

with DAG(
    "group_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["udemy_course"],
) as dag:
    downloads = downloads_tasks()

    check_files = BashOperator(task_id="check_files", bash_command="sleep 10")

    transforms = trasforms_tasks()

    (downloads >> check_files >> transforms)
