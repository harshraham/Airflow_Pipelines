from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from datetime import datetime
from utils.get_ticker_data import *

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 14)
}

dag = DAG(dag_id='pipeline_1',
          default_args=default_args,
          schedule_interval='0 19 * * 1-5',
          catchup=False
          )

start = DummyOperator(task_id='start', dag=dag)

get_hdfc = PythonOperator(
    task_id='get_hdfc',
    python_callable=get_top_5_stories_hdfc,
    provide_context=True, dag=dag
)

get_tata_motors = PythonOperator(
    task_id='get_tata_motors',
    python_callable=get_top_5_stories_tata_motors,
    provide_context=True, dag=dag
)
end = DummyOperator(task_id='end', dag=dag)

start >> get_hdfc >> get_tata_motors >> end
