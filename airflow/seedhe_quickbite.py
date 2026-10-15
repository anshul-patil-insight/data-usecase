"""
seedhe_quickbite
-----------------
Incremental Drive -> GCS -> BigQuery raw -> Dataform -> marts pipeline for the
QuickBite capstone.

Design summary
~~~~~~~~~~~~~~
* One Airflow "logical date" (ds) == one QuickBite export day. We rely on
  catchup to walk 2026-10-10 .. 2026-10-13 in order, and the DAG keeps working
  unmodified for any later day dropped into Drive.
* Every value that is project/bucket/dataset specific comes from Airflow
  Variables prefixed with ``seedhe_`` - nothing is hard-coded.
* Idempotent raw load: each file is copied into a date-stamped GCS prefix,
  loaded into a throwaway BigQuery staging table, then merged into the raw
  table via DELETE WHERE source_date = ds + INSERT ... SELECT. Re-running a
  day therefore can never duplicate raw rows.
* Dataform does all the business-rule heavy lifting (latest-wins, status
  standardisation, invalid order/item filtering, incremental fct_orders,
  marts). The DAG only triggers a compilation + workflow invocation scoped to
  this run's date via a Dataform compilation variable (``run_date``).
* A lightweight BigQuery data-quality check (no duplicate order_id in
  fct_orders, no NULLs in key columns) gates the "success" notification.
* Processed Drive copies are moved from the GCS landing prefix to the GCS
  archive prefix once everything downstream has succeeded.
"""
from __future__ import annotations

import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import cross_downstream
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor

# --------------------------------------------------------------------------
# Config - everything pulled from Airflow Variables, prefixed "seedhe_".
# Create these once via: airflow variables import seedhe_variables.json
# (see README for the exact command / values).
# --------------------------------------------------------------------------
PREFIX = "seedhe"

PROJECT_ID = Variable.get(f"{PREFIX}_project_id")
GCS_BUCKET = Variable.get(f"{PREFIX}_gcs_bucket")                 # dedicated capstone bucket (no gs:// prefix)
DRIVE_FOLDER_ID = Variable.get(f"{PREFIX}_drive_folder_id")       # "QuickBite Exports" folder id
RAW_DATASET = Variable.get(f"{PREFIX}_raw_dataset", default_var=f"{PREFIX}_raw")
CORE_DATASET = Variable.get(f"{PREFIX}_core_dataset", default_var=f"{PREFIX}_core")
MARTS_DATASET = Variable.get(f"{PREFIX}_marts_dataset", default_var=f"{PREFIX}_marts")
BQ_LOCATION = Variable.get(f"{PREFIX}_bq_location", default_var="US")

DATAFORM_REGION = Variable.get(f"{PREFIX}_dataform_region", default_var="us-central1")
DATAFORM_REPOSITORY = Variable.get(f"{PREFIX}_dataform_repository", default_var=f"{PREFIX}-quickbite")
DATAFORM_WORKSPACE = Variable.get(f"{PREFIX}_dataform_workspace", default_var="main")

SENSOR_TIMEOUT_SECONDS = int(Variable.get(f"{PREFIX}_file_wait_timeout_seconds", default_var="21600"))  # 6h
SENSOR_POKE_SECONDS = int(Variable.get(f"{PREFIX}_file_wait_poke_seconds", default_var="600"))          # 10m

TABLES = ["restaurants", "customers", "orders", "order_items"]

# Each raw table is the CSV columns (all STRING, "values arrive as text") plus
# lineage columns we stamp on during the raw merge step.
RAW_SCHEMA_FIELDS = {
    "restaurants": [
        "restaurant_id", "name", "city", "cuisine", "is_active", "onboarded_date",
    ],
    "customers": [
        "customer_id", "name", "email", "phone", "city", "signup_date", "updated_at",
    ],
    "orders": [
        "order_id", "customer_id", "restaurant_id", "order_ts", "status",
        "payment_method", "updated_at",
    ],
    "order_items": [
        "order_id", "line_no", "item_name", "quantity", "unit_price",
    ],
}


def _bq_schema(cols: list[str]) -> list[dict]:
    return [{"name": c, "type": "STRING", "mode": "NULLABLE"} for c in cols]


default_args = {
    "owner": "seedhe",
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id=f"{PREFIX}_quickbite",
    description="QuickBite capstone: Drive -> GCS -> BigQuery raw -> Dataform -> marts",
    start_date=datetime.datetime(2026, 10, 10),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,          # process days strictly in order
    default_args=default_args,
    tags=["seedhe", "quickbite", "capstone"],
) as dag:

    ds = "{{ ds }}"  # the export date this run is responsible for (logical date)

    def _landing_prefix(context=None):
        return f"capstone/{PREFIX}/landing/{{{{ ds }}}}"

    load_raw_groups = []

    for table in TABLES:
        file_name = f"{table}_{{{{ ds }}}}.csv"
        landing_object = f"capstone/{PREFIX}/landing/{{{{ ds }}}}/{table}_{{{{ ds }}}}.csv"
        archive_object = f"capstone/{PREFIX}/archive/{{{{ ds }}}}/{table}_{{{{ ds }}}}.csv"

        with TaskGroup(group_id=f"load_{table}") as tg:

            wait_for_file = GoogleDriveFileExistenceSensor(
                task_id=f"wait_for_{table}_file",
                folder_id=DRIVE_FOLDER_ID,
                file_name=file_name,
                mode="reschedule",                 # don't hog a worker slot while waiting
                poke_interval=SENSOR_POKE_SECONDS,
                timeout=SENSOR_TIMEOUT_SECONDS,
                soft_fail=False,
            )

            copy_to_landing = GoogleDriveToGCSOperator(
                task_id=f"copy_{table}_to_landing",
                folder_id=DRIVE_FOLDER_ID,
                file_name=file_name,
                bucket_name=GCS_BUCKET,
                object_name=landing_object,
            )

            load_to_staging = GCSToBigQueryOperator(
                task_id=f"load_{table}_to_staging",
                bucket=GCS_BUCKET,
                source_objects=[landing_object],
                destination_project_dataset_table=f"{PROJECT_ID}.{RAW_DATASET}.__stg_{table}",
                source_format="CSV",
                field_delimiter=",",
                skip_leading_rows=1,
                schema_fields=_bq_schema(RAW_SCHEMA_FIELDS[table]),
                write_disposition="WRITE_TRUNCATE",   # staging table is scratch space, always overwritten
                create_disposition="CREATE_IF_NEEDED",
                autodetect=False,
            )

            # Idempotent merge into the real raw table: wipe this source_date's
            # rows then re-insert, stamping lineage (_source_file/_source_date/
            # _loaded_at) so raw rows are traceable to the exact file/day they
            # came from. Re-running the same `ds` is therefore a no-op on the
            # final row count.
            cols_csv = ", ".join(RAW_SCHEMA_FIELDS[table])
            merge_sql = f"""
            DECLARE src_date DATE DEFAULT DATE('{ds}');

            CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{RAW_DATASET}.{table}` (
              {", ".join(f"{c} STRING" for c in RAW_SCHEMA_FIELDS[table])},
              _source_file STRING,
              _source_date DATE,
              _loaded_at TIMESTAMP
            )
            PARTITION BY _source_date;

            DELETE FROM `{PROJECT_ID}.{RAW_DATASET}.{table}`
            WHERE _source_date = src_date;

            INSERT INTO `{PROJECT_ID}.{RAW_DATASET}.{table}`
            SELECT {cols_csv},
                   '{file_name}' AS _source_file,
                   src_date AS _source_date,
                   CURRENT_TIMESTAMP() AS _loaded_at
            FROM `{PROJECT_ID}.{RAW_DATASET}.__stg_{table}`;
            """

            merge_into_raw = BigQueryInsertJobOperator(
                task_id=f"merge_{table}_into_raw",
                configuration={
                    "query": {
                        "query": merge_sql,
                        "useLegacySql": False,
                    }
                },
                location=BQ_LOCATION,
            )

            wait_for_file >> copy_to_landing >> load_to_staging >> merge_into_raw

        load_raw_groups.append(tg)

    # ----------------------------------------------------------------------
    # Dataform: compile scoped to this run's date (vars.run_date), then run
    # the workflow invocation end to end (staging -> core -> marts +
    # assertions). We fail the DAG run if the invocation or any assertion
    # fails.
    # ----------------------------------------------------------------------
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="dataform_compile",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{DATAFORM_REGION}/"
                f"repositories/{DATAFORM_REPOSITORY}/workspaces/{DATAFORM_WORKSPACE}"
            ),
            "code_compilation_config": {
                "vars": {"run_date": ds},
            },
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="dataform_run",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY,
        workflow_invocation={
            "compilation_result": (
                "{{ task_instance.xcom_pull('dataform_compile')['name'] }}"
            ),
        },
        asynchronous=True,
    )

    wait_for_workflow_invocation = DataformWorkflowInvocationStateSensor(
        task_id="dataform_wait",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('dataform_run')['name'].split('/')[-1] }}"
        ),
        expected_statuses={"SUCCEEDED"},
        failure_statuses={"FAILED", "CANCELLED"},
    )

    # ----------------------------------------------------------------------
    # Data quality gate (runs after Dataform's own assertions, as a second,
    # independent check before we tell anyone the day is "done").
    # ----------------------------------------------------------------------
    dq_no_duplicate_orders = BigQueryCheckOperator(
        task_id="dq_fct_orders_no_duplicates",
        sql=f"""
            SELECT COUNT(*) = 0
            FROM (
              SELECT order_id
              FROM `{PROJECT_ID}.{CORE_DATASET}.fct_orders`
              GROUP BY order_id
              HAVING COUNT(*) > 1
            )
        """,
        use_legacy_sql=False,
        location=BQ_LOCATION,
    )

    dq_no_null_keys = BigQueryCheckOperator(
        task_id="dq_fct_orders_no_null_keys",
        sql=f"""
            SELECT COUNT(*) = 0
            FROM `{PROJECT_ID}.{CORE_DATASET}.fct_orders`
            WHERE order_id IS NULL OR restaurant_id IS NULL OR customer_id IS NULL
        """,
        use_legacy_sql=False,
        location=BQ_LOCATION,
    )

    # ----------------------------------------------------------------------
    # Archive: move each of today's 4 landing files out of landing/ once
    # everything downstream succeeded, so next run's landing prefix starts
    # clean and nothing gets reprocessed.
    # ----------------------------------------------------------------------
    archive_tasks = []
    for table in TABLES:
        landing_object = f"capstone/{PREFIX}/landing/{{{{ ds }}}}/{table}_{{{{ ds }}}}.csv"
        archive_object = f"capstone/{PREFIX}/archive/{{{{ ds }}}}/{table}_{{{{ ds }}}}.csv"
        archive_tasks.append(
            GCSToGCSOperator(
                task_id=f"archive_{table}",
                source_bucket=GCS_BUCKET,
                source_object=landing_object,
                destination_bucket=GCS_BUCKET,
                destination_object=archive_object,
                move_object=True,
            )
        )

    def _notify(ds_date, **context):
        dag_run = context["dag_run"]
        failed = [t.task_id for t in dag_run.get_task_instances() if t.state == "failed"]
        if failed:
            message = f"[seedhe_quickbite] FAILED for {ds_date} - failed tasks: {failed}"
            print(message)
            # Replace this with a Slack/Email hook call as needed; failing the
            # task here surfaces the failure in the Airflow UI/alerts.
            raise Exception(message)
        message = (
            f"[seedhe_quickbite] SUCCESS for {ds_date}: raw loaded, Dataform run "
            f"completed, DQ checks passed, files archived."
        )
        print(message)

    # Resolve the templated ds at render time via Jinja.
    notify = PythonOperator(
        task_id="notify_run_result",
        python_callable=_notify,
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={"ds_date": "{{ ds }}"},
    )

    for tg in load_raw_groups:
        tg >> create_compilation_result

    (
        create_compilation_result
        >> create_workflow_invocation
        >> wait_for_workflow_invocation
        >> [dq_no_duplicate_orders, dq_no_null_keys]
    )
    cross_downstream([dq_no_duplicate_orders, dq_no_null_keys], archive_tasks)
    archive_tasks >> notify
