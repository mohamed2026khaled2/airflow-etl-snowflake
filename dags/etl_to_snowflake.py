from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def extract_transform():
    data = {
        "id": [1, 2, 3],
        "name": ["Ahmed", "Sara", "Omar"],
        "email": ["ahmed@example.com", "sara@example.com", "omar@example.com"]
    }
    df = pd.DataFrame(data)
    df.to_csv("/tmp/cleaned_data.csv", index=False)
    print("Data written to /tmp/cleaned_data.csv")

default_args = {
    "start_date": datetime(2023, 1, 1)
}

with DAG("etl_to_snowflake",
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:

    task = PythonOperator(
        task_id="extract_transform_csv",
        python_callable=extract_transform
    )
