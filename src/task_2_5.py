from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# Simple DAG Intialization
dag = DAG(
    'first_dag',
    description='First  DAG',
    catchup=False,
    schedule_interval='0 0 * * *',
    start_date=datetime(2021,9,20),
)

# The DummyOperator used to create required tasks
task1 = DummyOperator(
    task_id='task1',
    dag=dag
)

task2 = DummyOperator(
    task_id='task2',
    dag=dag,
    trigger_rule='all_done'
)

task3 = DummyOperator(
    task_id='task3',
    dag=dag,
    trigger_rule='all_done'
)

task4 = DummyOperator(
    task_id='task4',
    dag=dag,
    trigger_rule='all_success',
)

task5 = DummyOperator(
    task_id='task5',
    dag=dag,
    trigger_rule='all_success',
)

task6 = DummyOperator(
    task_id='task6',
    dag=dag,
    trigger_rule='all_success',
)

# Defining the DAG flow along with dependencies
task1 >> task2
task1 >> task3
task2 >> task4
task3 >> task4
task2 >> task5
task3 >> task5
task2 >> task6
task3 >> task6