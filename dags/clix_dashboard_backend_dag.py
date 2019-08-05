import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks import SSHHook


from datetime import date, timedelta, datetime

import scripts.sync_school_data as sync_school_data
#import scripts.load_school_tables as load_school_tables
import config.clix_config as clix_config

# --------------------------------------------------------------------------------
# set default arguments
# --------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    #'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'clix_dashboard_backend_dag', default_args=default_args,
    schedule_interval=timedelta(hours=5))

# --------------------------------------------------------------------------------
# Each state is synced independently. We have four states and syncthing data folders
# corresponding to those states are synced through sync_school_data
# --------------------------------------------------------------------------------
sshHook = SSHHook(conn_id=<YOUR CONNECTION ID FROM THE UI>)

for each_state in clix_config.states:

    src = clix_config.remote_src + each_state
    dst = clix_config.local_dst + each_state

    sync_state_data = SSHExecuteOperator( task_id="task1",
    bash_command= rsync -avzhe ssh {0}@{1}:{2} {3}".format(user, ip, src, dst),
    ssh_hook=sshHook,
    dag=dag)

    sync_state_data = PythonOperator(
        task_id='sync_state_data_' + each_state,
        python_callable=sync_school_data.rsync_data_ssh,
        op_kwargs={'state': each_state, 'src': src, 'dst': dst},
        dag=dag)

    # For parallel processing of files in the list of schools updated
    # we use three parallel tasks each taking the portion of the list
    # of files. This is done instead of generating tasks dynamically
    # refer: https://stackoverflow.com/questions/55672724/airflow-creating-dynamic-tasks-from-xcom
    '''
    load_state_tables1 = PythonOperator(
        task_id='load_state_tables1' + each_state,
        python_callable= load_school_tables.process_school_tables,
        op_kwargs={'state': each_state, 'prop_schools': 30, 'chunk': 1},
        dag=dag)

    load_state_tables2 = PythonOperator(
        task_id='load_state_tables1' + each_state,
        python_callable= load_school_tables.process_school_tables,
        op_kwargs={'state': each_state, 'prop_schools': 30, 'chunk': 2},
        dag=dag)

    load_state_tables3 = PythonOperator(
        task_id='load_state_tables1' + each_state,
        python_callable= load_school_tables.process_school_tables,
        op_kwargs={'state': each_state, 'prop_schools': 40, 'chunk': 3},
        dag=dag)

    sync_state_data.set_downstream(load_school_tables1)
    sync_state_data.set_downstream(load_school_tables2)
    sync_state_data.set_downstream(load_school_tables3)
    '''
