from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

# setting object type Dataset
my_file = Dataset("/tmp/my_file.txt")

# in order to set two datasets, we must add another uri
my_file_2 = Dataset("/tmp/my_file_2.txt")

# creating DAG
with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["udemy_course"],
):
    # first dataset function into this task
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update")

    # second dataset function into this task
    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f:
            f.write("producer update 2")

    update_dataset() >> update_dataset_2()
