from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

# this URI must be equal to the URI into the producer.py file
my_file = Dataset("/tmp/my_file.txt")

# called the same second uri from producer.py file
my_file_2 = Dataset("/tmp/my_file_2.txt")

with DAG(
    dag_id="consumer",
    schedule=[my_file, my_file_2],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["udemy_course"],
):

    @task
    def read_database():
        with open(my_file.uri, "r") as f:
            print(f.read())

    read_database()
