from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from datetime import datetime
import json
from pandas import json_normalize


# function nested in the process_user PythonOperator
def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user["results"][0]
    processed_user = json_normalize(
        {
            "firstname": user["name"]["first"],
            "lastname": user["name"]["last"],
            "country": user["location"]["country"],
            "username": user["login"]["username"],
            # "username": user["login"]["password"],
            "email": user["email"],
        }
    )
    processed_user.to_csv("/tmp/processed_user.csv", index=None, header=False)


# function to store user data into postgres table
def _store_user():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY users2 FROM stdin WITH DELIMITER as ','",
        filename="/tmp/processed_user.csv",
    )


with DAG(
    "user_data",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["udemy_course"],
) as dag:
    # first task for my DAG
    # Creating a new table on postgresql

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users2(
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            email TEXT NOT NULL
        );""",
    )

    # sensor operator example for check if the api is available

    is_api_available = HttpSensor(
        task_id="is_api_available", http_conn_id="user_api", endpoint="api/"
    )

    # task to get data from api throughout api connection

    extract_user = SimpleHttpOperator(
        task_id="extract_user",
        http_conn_id="user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    # task to call a python function to transform api data in json format and
    # save it with csv format.

    process_user = PythonOperator(task_id="process_user", python_callable=_process_user)

    # task to call a python function to  store data transformed into the postgres
    # table called "users"

    store_user = PythonOperator(task_id="store_user", python_callable=_store_user)

    # setting the flow of our tasks
    create_table >> is_api_available >> extract_user >> process_user >> store_user
