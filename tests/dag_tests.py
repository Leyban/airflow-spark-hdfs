import datetime

import pytest
from airflow import DAG

@pytest.fixture
def test_dag():
    return DAG(
        dag_id="test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2018, 1, 1)},
        schedule_interval=datetime.timedelta(days=1),
    )

pytest_plugins = ["helpers_namespace"]

@pytest.helpers.register
def run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )

import pytest
from airflow.operators.bash import BashOperator

def test_dummy(test_dag, tmpdir):
    tmpfile = tmpdir.join("hello.txt")

    task = BashOperator(task_id="test", bash_command=f"echo 'hello' > {tmpfile}", dag=test_dag)
    pytest.helpers.run_task(task=task, dag=test_dag)

    assert len(tmpdir.listdir()) == 1
    assert tmpfile.read().replace("n", "") == "hello"