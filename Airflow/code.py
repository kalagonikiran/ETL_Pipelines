from datetime import timedelta 
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator 
#dag_arguments
dag_arguments={
    'owner':'kiran',
    'start_date':days_ago(0),
    'email':['kalagonikiran@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'rery_delay':timedelta(minutes=5),
}
#dag_defination
dag=DAG(
    dag_id='ETL_toll_data',
    default_args=dag_arguments,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)
#tasks
unzip_data=BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)
extract_data_from_csv=BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)
extract_data_from_tsv=BashOperator(
    task_id='extract_data_from_tsv',
    bash_command = 'cut -f5-7 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)
extract_data_from_fixed_width=BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='awk "NF{print $(NF-1),$NF}" OFS="\t" payment-data.txt > fixed_width_data.csv',
    dag=dag,
)
consolidate_data=BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)
transform_data=BashOperator(
    task_id='transform_data',
    bash_command='awk "$5 = toupper($5)" < extracted_data.csv > transformed_data.csv',
    dag=dag,
)
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
