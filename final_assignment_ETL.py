#Importar as bibliotecas
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from numpy import extract


#Tarefa 1.1 - Definir argumentos do DAG
default_args = {
    'owner':'Nayara Bernardo',
    'start_date':days_ago(0),
    'email':['nayyarabernardo@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes = 5)
}

#Tarefa 1.2 - Definir o DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

#Tarefa 1.3 - Crie uma tarefa para descompactar dados
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzf /home/nayara/Downloads/IBM/ETL_DataPipelines_Airflow_Kafka/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag    
)

#Tarefa 1.4 - Crie uma tarefa para extrair dados do arquivo csv
extract_data_from_csv = BashOperator (
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag = dag,
)

#Tarefa 1.5 - Crie uma tarefa para extrair dados do arquivo tsv
extract_data_from_tsv = BashOperator (
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 tollplaza-data.tsv > tsv_data.csv',
    dag = dag,
)

#Tarefa 1.6 - Crie uma tarefa para extrair dados de um arquivo de largura fixa
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'awk 'NF{print $(NF-1),$NF}'  OFS="\t"  payment-data.txt > fixed_width_data.csv',
    dag = dag,
)

#Tarefa 1.7 - Crie uma tarefa para consolidar dados extraídos de tarefas anteriores
consolidate_data = BashOperator(
    task_id = 'consolidate_date',
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag = dag,
)

#Tarefa 1.8 - Transforme e carregue os dados
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'awk '$5 = toupper($5)' < extracted_data.csv > transformed_data.csv',
    dag = dag,
)

#Tarefa 1.9 - Defina o pipeline de tarefas
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
