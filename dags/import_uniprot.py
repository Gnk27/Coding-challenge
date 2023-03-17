from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from uniprot_reader import read_xml_file

args = {
    'owner': 'Giancarlo Marquez',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='import-uniprot-dag',
    default_args=args,
    schedule_interval='@daily'
)

with dag:
    read_uniprot_xml = PythonOperator(
        task_id='read_xml_file',
        python_callable=read_xml_file,
        # provide_context=True
    )