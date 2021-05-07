from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.kubernetes.pod import Resources
from airflow.operators.sensors import ExternalTaskSensor

default_args = {
    'owner': 'wlachance@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 30),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG('missioncontrol',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    gke_location="us-central1-a"
    gke_cluster_name="bq-load-gke-1"

    # Built from repo https://github.com/mozilla/missioncontrol-v2
    docker_image='gcr.io/wlach-test-258214/missioncontrol-etl:latest'

    # Cluster autoscaling works on pod resource requests, instead of usage
    resources = Resources(request_memory='32G', request_cpu='8',
                          limit_memory='48G', limit_cpu='16')

    missioncontrol_etl = GKEPodOperator(
        task_id="missioncontrol_etl",
        gcp_conn_id=gcp_conn_id,
        project_id=connection.project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        name='missioncontrol-etl',
        namespace='default',
        # Needed to scale the highcpu pool from 0 -> 1
        resources=resources,
        # This job requires a node with a large number of cores to complete in a reasonable amount of time, thus the highcpu node pool
        # node_selectors={"nodepool" : "highcpu"},
        # Due to the nature of the container run, we set get_logs to False,
        # To avoid urllib3.exceptions.ProtocolError: 'Connection broken: IncompleteRead(0 bytes read)' errors
        # Where the pod continues to run, but airflow loses its connection and sets the status to Failed
        get_logs=False,
        # Give additional time since we will likely always scale up when running this job
        startup_timeout_seconds=360,
        image=docker_image,
        is_delete_operator_pod=True,
        arguments=[
            '/mc-etl/complete.runner.sh'
        ],
        email=['wlachance@mozilla.com', 'hwoo@mozilla.com'],
        env_vars={
            'GCP_PROJECT_ID': connection.project_id,
            'GCS_OUTPUT_PREFIX': 'gs://missioncontrol-v2',
            'RAW_OUTPUT_TABLE': 'missioncontrol_v2_raw_data_test',
            'MODEL_OUTPUT_TABLE': 'missioncontrol_v2_model_output_test',
            'SIMPLE': '0'
        },
        dag=dag)

    # this task depends on clients daily (and telemetry.crash, but we
    # figure that should be in place by the time this runs)
    #wait_for_clients_daily = ExternalTaskSensor(
    #    task_id="wait_for_clients_daily",
    #    project_id="moz-fx-data-shared-prod",
    #    external_dag_id="main_summary",
    #    external_task_id="clients_daily",
    #    check_existence=True,
    #    dag=dag,
    #)

    # wait_for_clients_daily >> missioncontrol_etl
