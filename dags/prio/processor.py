"""Airflow configuration for coordinating the Prio processing pipeline.

The prio-processor docs details the internals of the containerized application.
It contains storage and processing scripts that are coordinated by Airflow.

The graph has three phases: partitioning, publishing, and processing.
Partitioning reads batched prio pings and range-partitions across date,
batch-id, and share-id. The partitioned data sets are published to all
subscribed servers. Each of the servers runs a Kubernetes cluster running from
the `mozilla/prio-processor:latest` image. A container runs the processing
pipeline, starting with input at `{private-bucket}/raw/` and resulting in output
at `{private-bucket}/processed/`.

There are two sets of servers that run in Google Kubernetes Engine (GKE) under
Data Operations. The servers are assigned service accounts with access to a
private storage bucket associated with its project, and a pair of mutually
shared buckets. An administrative account oversees interop with ingestion,
including a process and transfer job from Firefox Telemetry into a receiving
bucket located in each server's project.

The SubDag operator simplifies the definition of each phase. The clusters are
ephemeral -- there are situations where clusters can become orphaned. These are
usually resolved by rerunning the individual SubDag, which should clean-up
properly.

The following Airflow variables should be set:

    prio_private_key_hex_internal
    prio_public_key_hex_internal
    prio_private_key_hex_external
    prio_public_key_hex_external

These variables are encrypted and passed into the prio-processor container via
the environment.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.operators.subdag_operator import SubDagOperator
from prio import partitioning, processing

DEFAULT_ARGS = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 24),
    "email": [
        "amiyaguchi@mozilla.com",
        "hwoo@mozilla.com",
        "dataops+alerts@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "schedule_interval": "@weekly",
    "dagrun_timeout": timedelta(hours=2),
}


dag = DAG(dag_id="prio_processor", default_args=DEFAULT_ARGS)


# Copy the dependencies necessary for running the `prio-processor staging` job
# into the relevant GCS locations. This includes the runner and the egg
# containing the bundled dependencies.

prio_staging_bootstrap = SubDagOperator(
    subdag=processing.prio_processor_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="bootstrap",
        default_args=DEFAULT_ARGS,
        server_id="admin",
        gcp_conn_id="google_cloud_prio_admin",
        service_account="prio-admin-runner@moz-fx-prio-admin.iam.gserviceaccount.com",
        arguments=[
            "bash",
            "-c",
            "cd processor; prio-processor bootstrap --output gs://moz-fx-data-prio-bootstrap",
        ],
        env_vars={},
    ),
    task_id="bootstrap",
    dag=dag,
)

# Run the PySpark job for range-partioning Prio pings.
prio_staging = SubDagOperator(
    subdag=partitioning.prio_processor_staging_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="staging",
        default_args=DEFAULT_ARGS,
        gcp_conn_id="google_cloud_prio_admin",
        service_account="prio-admin-runner@moz-fx-prio-admin.iam.gserviceaccount.com",
        num_preemptible_workers=2,
    ),
    task_id="staging",
    default_args=DEFAULT_ARGS,
    dag=dag,
)

# Copy the partitioned data from the staging bucket into the corresponding
# receiving buckets in each processor. The job then submits a `_SUCCESS` file
# which indicates the data is ready for processing.
#
# See: https://github.com/mozilla/prio-processor/blob/3cdc368707f8dc0f917d7b3d537c31645f4260f7/processor/tests/test_staging.py#L190-L205

load_processor_a = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="load_processor_a",
    source_bucket="moz-fx-data-prio-data",
    source_object="staging/submission_date={{ ds }}/server_id=a/*",
    destination_bucket="project-a-private",
    destination_object="raw/submission_date={{ ds }}/",
    google_cloud_storage_conn_id="google_cloud_prio_admin",
    dag=dag,
)

load_processor_b = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id="load_processor_b",
    source_bucket="moz-fx-data-prio-data",
    source_object="staging/submission_date={{ ds }}/server_id=b/*",
    destination_bucket="project-b-private",
    destination_object="raw/submission_date={{ ds }}/",
    google_cloud_storage_conn_id="google_cloud_prio_admin",
    dag=dag,
)

# Initiate the processors by spinning up GKE clusters and running the main
# processing loop. These SubDags are coordinated by a higher level graph, but
# they can be decoupled and instead run on the same schedule. This structure is
# done for convenience, since both processors are running on the same
# infrastructure (but partitioned along project boundaries).

processor_a = SubDagOperator(
    subdag=processing.prio_processor_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="processor_a",
        default_args=DEFAULT_ARGS,
        server_id="a",
        gcp_conn_id="google_cloud_prio_a",
        service_account="prio-runner-a@moz-fx-priotest-project-a.iam.gserviceaccount.com",
        arguments=["processor/bin/process"],
        env_vars={
            "DATA_CONFIG": "/app/processor/config",
            "SERVER_ID": "A",
            "SHARED_SECRET": "m/AqDal/ZSA9597GwMM+VA==",
            # TODO: this is the built-in testing key
            "PRIVATE_KEY_HEX": "{{ var.value.prio_private_key_hex_internal }}",
            "PUBLIC_KEY_HEX_INTERNAL": "{{ var.value.prio_public_key_hex_internal }}",
            "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_external }}",
            "BUCKET_INTERNAL_PRIVATE": "project-a-private",
            "BUCKET_INTERNAL_SHARED": "project-a-shared",
            "BUCKET_EXTERNAL_SHARED": "project-b-shared",
            "RETRY_LIMIT": "5",
            "RETRY_DELAY": "3",
            "RETRY_BACKOFF_EXPONENT": "2",
        },
    ),
    # TODO: refactor to avoid hardcoding name here
    task_id="processor_a",
    dag=dag,
)

processor_b = SubDagOperator(
    subdag=processing.prio_processor_subdag(
        parent_dag_name=dag.dag_id,
        child_dag_name="processor_b",
        default_args=DEFAULT_ARGS,
        server_id="b",
        gcp_conn_id="google_cloud_prio_b",
        service_account="prio-runner-b@moz-fx-priotest-project-b.iam.gserviceaccount.com",
        arguments=["processor/bin/process"],
        env_vars={
            "DATA_CONFIG": "/app/processor/config",
            "SERVER_ID": "B",
            "SHARED_SECRET": "m/AqDal/ZSA9597GwMM+VA==",
            "PRIVATE_KEY_HEX": "{{ var.value.prio_private_key_hex_external }}",
            "PUBLIC_KEY_HEX_INTERNAL": "{{ var.value.prio_public_key_hex_external }}",
            "PUBLIC_KEY_HEX_EXTERNAL": "{{ var.value.prio_public_key_hex_internal }}",
            "BUCKET_INTERNAL_PRIVATE": "project-b-private",
            "BUCKET_INTERNAL_SHARED": "project-b-shared",
            "BUCKET_EXTERNAL_SHARED": "project-a-shared",
            "RETRY_LIMIT": "5",
            "RETRY_DELAY": "3",
            "RETRY_BACKOFF_EXPONENT": "2",
        },
    ),
    task_id="processor_b",
    dag=dag,
)

# Stitch the operators together into a directed acyclic graph.
prio_staging_bootstrap >> prio_staging
prio_staging >> load_processor_a >> processor_a
prio_staging >> load_processor_b >> processor_b
