from datetime import datetime, timedelta

from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.secret import Secret
#from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

dag_id = 'kubernetes-dag'

task_default_args = {
    'owner': 'airflow',
    #'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
  #  'email': ['shclub@gmail.com'],
  #  'email_on_retry': False,
  #  'email_on_failure': True,
    'execution_timeout': timedelta(hours=1)
}


dag = DAG(
    dag_id=dag_id,
    description='kubernetes pod operator',
    default_args=task_default_args,
    #schedule_interval='5 16 * * *',
    schedule_interval= None,
    max_active_runs=1
)

#args = {
#    'owner': 'airflow'
#}

#with DAG(
#    dag_id='kubernetes-simple-dag',
#    default_args=args,
#    schedule_interval=None,
#    start_date=days_ago(2)
#) as dag:
    
#env = Secret(
#    'env',
#    'TEST',
#    'test_env',
#    'TEST',
#)

# Use k8s_client.V1ResourceRequirements to define resource limits
k8s_resource_requirements = k8s.V1ResourceRequirements(
    requests={"cpu": "0.2","memory": "100Mi"}, limits={"cpu": "0.5","memory": "512Mi"}
)
        
#configmaps = [
#    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='secret')),
#]

start = DummyOperator(task_id="start", dag=dag)

run = KubernetesPodOperator(
    task_id="kubernetespodoperator",
    namespace='edu31',
    image='nginx',
    #cmds=["sleep", "360d"],
    #secrets=[
    #    env
    #],
    #image_pull_secrets=[k8s.V1LocalObjectReference('image_credential')],
    name="nginx3",
    is_delete_operator_pod=True,
    get_logs=True,
    #resources=pod_resources,
    resources = k8s_resource_requirements,
    #env_from=configmaps,
    dag=dag,
)

start >> run
