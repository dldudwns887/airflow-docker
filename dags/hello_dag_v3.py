from pathlib import Path

import pendulum
from airflow.sdk import dag, task

IMG_DIR = Path("/home/papalio/airflow-docker/img")
# IMG_DIR = Path("/home/papalio/airflow-docker/dags")
OUTFILE = IMG_DIR / "hello.txt"

@dag(
    dag_id="hello_dag_v3",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo"],
)
def hello_dag_v3():
    @task
    def append_hello():
        IMG_DIR.mkdir(parents=True, exist_ok=True)
        with OUTFILE.open("a", encoding="utf-8") as f:
            f.write("hello!\n")
        return {"path": str(OUTFILE), "bytes": OUTFILE.stat().st_size}

    append_hello()

hello_dag_v3()
