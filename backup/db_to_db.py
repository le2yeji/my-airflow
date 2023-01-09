from datetime import timedelta

import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Seoul")

from pendulum.tz.timezone import Timezone
from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun

 

import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa')
import pattern_Common

dag_id = 'pattern2_D2D'
owner = 'Admin'

 

task_args = {
    'owner': owner,
    'retries': 1,
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'execution_timeout': timedelta(hours=1)
}

 

dag = DAG(
    dag_id=dag_id,
    description='kubernetes pod operator',
    default_args=task_args,
    start_date=datetime(2022, 11, 10, 16, 0, 0, 0, tzinfo=local_tz),
    schedule_interval='*/5 * * * *',
    max_active_runs=1,
    catchup=True
)

 
def get_most_recent_dag_run(dt):
    dag_runs=DagRun.find(dag_id="batchTest_Common")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date

sensors_Start = ExternalTaskSensor(
    task_id="sensors_Start",
    external_dag_id='batchTest_Common',
    external_task_id='task_start',
    execution_date_fn=get_most_recent_dag_run,
    mode='reschedule',
    timeout=3600
)
 
def authResponse_func(ti):
    result = pattern_Common.get_auth_check("true")
    print("===== authResponse_func ===== result =", result)
    isTrue = True if result == 'true' else False
    if isTrue:
        task_id = 'runJob1'
    else:
        task_id = 'sensors_Complete'
    return task_id

authResponse = BranchPythonOperator(
    task_id="authResponse",
    python_callable=authResponse_func,
    dag=dag
)

 
runJob1 = KubernetesPodOperator(
    task_id="runJob1",
    namespace='sa-dedi',
    image='ghcr.io/shclub/edu15:v1',
    #image_pull_secrets=[k8s.V1LocalObjectReference('dspace-nexus')],
    name="pod_D2D",
#     arguments=["--job.names=sampJob"],
    arguments=["--job.names=sampJob", "--arg.test=" + str(datetime.now().date())],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)
 
# runJob2 = KubernetesPodOperator(
#     task_id="runJob2",
#     namespace='sa-dedi',
#     image='nexus.dspace.kt.co.kr/icis/icis-samp-batch-db-to-db-daemon:20230105_1',
#     image_pull_secrets=[k8s.V1LocalObjectReference('dspace-nexus')],
#     name="pod_D2D",
#     arguments=["--job.names=batchSampTaskletJob"],
#     is_delete_operator_pod=True,
#     get_logs=True,
#     dag=dag
# )

 

sensors_Complete = ExternalTaskSensor(
    task_id="sensors_Complete",
    external_dag_id='batchTest_Common',
    external_task_id='task_complete',
    execution_date_fn=get_most_recent_dag_run,
    trigger_rule='all_done',
    mode='reschedule',
    timeout=3600
)

sensors_Start >> runJob1 >> sensors_Complete

sensors_Start >> sensors_Complete

#sensors_Start >> authResponse

#authResponse >> runJob1 >> sensors_Complete
#authResponse >> sensors_Complete
