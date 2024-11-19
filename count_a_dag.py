from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import random
import string
import os
from concurrent.futures import ThreadPoolExecutor

default_args = {
    'owner': 'user',
    'start_date': days_ago(1),
}

dag = DAG(
    'count_a_dag',
    default_args=default_args,
    schedule_interval=None,
)

DATA_DIR = '/opt/airflow/data'

def generate_files():
    os.makedirs(DATA_DIR, exist_ok=True)
    for i in range(1, 101):
        random_letters = ''.join(random.choices(string.ascii_lowercase, k=1000))
        with open(f'{DATA_DIR}/{i}.txt', 'w') as f:
            f.write(random_letters)

def count_a_in_file(file_number):
    with open(f'{DATA_DIR}/{file_number}.txt', 'r') as f:
        content = f.read()
    count = content.count('a')
    with open(f'{DATA_DIR}/{file_number}.res', 'w') as f:
        f.write(str(count))

def count_a_parallel():
    with ThreadPoolExecutor() as executor:
        executor.map(count_a_in_file, range(1, 101))

def sum_results():
    total_count = 0
    for i in range(1, 101):
        with open(f'{DATA_DIR}/{i}.res', 'r') as f:
            count = int(f.read())
            total_count += count
    with open(f'{DATA_DIR}/total_count.txt', 'w') as f:
        f.write(f"Total count of 'a': {total_count}")
    print(f"Total count of 'a': {total_count}")

generate_files_task = PythonOperator(
    task_id='generate_files',
    python_callable=generate_files,
    dag=dag,
)

count_a_task = PythonOperator(
    task_id='count_a_parallel',
    python_callable=count_a_parallel,
    dag=dag,
)

sum_results_task = PythonOperator(
    task_id='sum_results',
    python_callable=sum_results,
    dag=dag,
)

generate_files_task >> count_a_task >> sum_results_task
