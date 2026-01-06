import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id="hello_dag",
    schedule=None,  
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
)
def hello_dag_v3():
    @task.bash  #
    def hello() -> str:
        return 'echo "Hello Airflow 3 (Task SDK)!"'

    hello()

hello_dag_v3()
