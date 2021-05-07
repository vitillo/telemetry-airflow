from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

docs = """
### Clean GKE Pods

Built from cloudops-infra repo, projects/airflow/pod-clean

#### Purpose

This DAG executes a GKEPodOperator to clean out old completed pods
on the shared derived-datasets gke cluster. We need to do this periodically
because GCP has a 1500 object limit quota.

#### Owner

hwoo@mozilla.com
"""


default_args = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 26),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG("clean-gke-pods", default_args=default_args, schedule_interval="@daily", doc_md = docs)

docker_image='gcr.io/moz-fx-data-airflow-prod-88e0/gke-pod-clean:1.3'
gke_cluster_name='bq-load-gke-1'
gke_location='us-central1-a'

docker_args = [
    '--project', 'moz-fx-data-derived-datasets',
    '--gke-cluster', gke_cluster_name,
    '--region', gke_location,
    '--retention-days', '4'
]

clean_gke_pods = GKEPodOperator(
    task_id="clean-gke-pods",
    name='clean-gke-pods',
    image=docker_image,
    arguments=docker_args,
    gcp_conn_id='google_cloud_gke_sandbox',
    project_id='moz-fx-data-gke-sandbox',
    location='us-west1',
    cluster_name='hwoo-gke-sandbox',
    dag=dag)
