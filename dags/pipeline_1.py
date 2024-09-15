from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from datetime import datetime
from utils.get_ticker_data import *

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2023, 9, 14)
}

dag = DAG(dag_id='pipeline_1',
          default_args=default_args,
          schedule_interval='0 19 * * 1-5',
          catchup=False
          )

start = DummyOperator(task_id= 'start', dag = dag)

get_hdfc=PythonOperator(
     task_id='get_hdfc',
     python_callable=get_top_5_stories('HDFC'),
     provide_context=True,  # Provide context variables like execution date
 )

get_tata_motors=PythonOperator(
    task_id='get_tata_motors',
    python_callable=get_top_5_stories('Tata Motors'),
    provide_context=True,  # Provide context variables like execution date
)
end = DummyOperator(task_id= 'end', dag = dag)

start>>get_hdfc>>get_tata_motors>>end