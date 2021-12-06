from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
# for file operations
from airflow.sensors.filesystem import FileSensor
#for bash commands
from airflow.operators.bash import BashOperator

#chain
from airflow.models.baseoperator import chain, cross_downstream



#dictionary for default vars
default_args={
    'retry':5,
    'retry_delay':timedelta(minutes=5),
    #'email_on_failure':True,
    #'email_on_retry':True,
    #'email': 'someemail@address',
    #'smtpserver'
}

#def _downloading_data(**kwargs):
#    print(kwargs)

def _downloading_data(**kwargs):
    with open('/tmp/my_file.txt','w') as f:
        f.write('my_data')
    return 42

def _checking_data(ti):
    my_xcom=ti.xcom_pull(key='return_value', task_ids=['downloading_data'])
    print(my_xcom)

def _failure(context):
    print("On callback failure")
    print(context)

with DAG(dag_id='simple_dag', schedule_interval="@daily",default_args=default_args, start_date=days_ago(3), catchup=False, max_active_runs=3) as dag: # don't use dynamic datetime .now()
    ##start_date can be
    #"*/10 * * * *"
    #@daily
    #@weekly
    #timedelta(days=3)
    #catchup=True -default
    #task_1 =  DummyOperator(task_id='task_1')#start_date=datetime(2022,1,2) can be done but never do this

    #task_2 =  DummyOperator(task_id='task_2')

    #task_3 =  DummyOperator(task_id='task_3')

    downloading_data=PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data #python functions above
    )

    checking_data=PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data #python functions above
    )

    waiting_for_data=FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt'
        #poke_intervlal=15
        #python_callable=_downloading_data
    )

    processing_data=BashOperator(
        task_id='processing_data',
        bash_command='exit 0',
        on_failure_callback=_failure
    )

    #downloading_data.set_downstream(waiting_for_data)
    #waiting_for_data.set_downstream(processing_data)
    #processing_data.set_upstream(waiting_for_data)
    #waiting_for_data.set_upstream(downloading_data)

    #commonly used to define path!
    #downloading_data >> [waiting_for_data, processing_data]
    
    # the same process as above string
    chain(downloading_data,checking_data,  processing_data,waiting_for_data)

    #cross_downstream([downloading_data, checking_data], [waiting_for_data, processing_data])
