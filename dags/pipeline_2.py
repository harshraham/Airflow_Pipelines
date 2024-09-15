from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from datetime import datetime
from utils.movie_analysis import *

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 14)
}

dag = DAG(dag_id='pipeline_2',
          default_args=default_args,
          schedule_interval='0 20 * * 1-5',
          catchup=False
          )

start = DummyOperator(task_id='start', dag=dag)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task1,
    provide_context=True,  # Provide context variables like execution date
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task2,
    provide_context=True,  # Provide context variables like execution date
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task3,
    provide_context=True,  # Provide context variables like execution date
)

task_4 = PythonOperator(
    task_id='task_4',
    python_callable=task4,
    provide_context=True,  # Provide context variables like execution date
)

end = DummyOperator(task_id='end', dag=dag)

start >> task_1 >> task_2 >> task_3 >> task_4 >> end
