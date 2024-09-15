from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.dag import DAG
from airflow.models import DagRun
from airflow.utils.state import State
from datetime import datetime
from utils.movie_analysis import *
from airflow.exceptions import AirflowSkipException

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 14)
}


def check_sensor_status(**context):
    # If the external task failed, raise a SkipException to skip the current DAG
    if context['task_instance'].xcom_pull(task_ids='check_pipeline_1_status') is None:
        raise AirflowSkipException('External task did not succeed. Skipping the rest of the DAG.')


dag = DAG(dag_id='pipeline_2',
          default_args=default_args,
          schedule_interval='0 20 * * 1-5',
          catchup=False
          )

check_pipeline_1_status = ExternalTaskSensor(
    task_id='check_pipeline_1_status',
    external_dag_id='pipeline_1',
    allowed_states=[State.SUCCESS],
    failed_states=[State.FAILED],
    timeout=10, dag=dag, mode='poke')

check_sensor_status_task = PythonOperator(
    task_id='check_sensor_status_task',
    python_callable=check_sensor_status,
    provide_context=True, dag=dag
)

start = DummyOperator(task_id='start', dag=dag)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task1,
    provide_context=True, dag=dag  # Provide context variables like execution date
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task2,
    provide_context=True, dag=dag  # Provide context variables like execution date
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task3,
    provide_context=True, dag=dag  # Provide context variables like execution date
)

task_4 = PythonOperator(
    task_id='task_4',
    python_callable=task4,
    provide_context=True, dag=dag  # Provide context variables like execution date
)

end = DummyOperator(task_id='end', dag=dag)

check_pipeline_1_status >> check_sensor_status_task >> start >> task_1 >> task_2 >> task_3 >> task_4 >> end
