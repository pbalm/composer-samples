# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime

from airflow import models
from airflow.decorators import task
import uuid
import time

from airflow.providers.google.cloud.operators.dataplex import DataplexCreateTaskOperator
from airflow.exceptions import AirflowFailException

from google.cloud import dataplex_v1
from google.cloud.dataplex_v1 import DataplexServiceClient


PROJECT_ID = "pbalm-dataplex"
REGION = "us-central1"
LAKE_ID = "main-lake"
SERVICE_ACC = "28982355077-compute@developer.gserviceaccount.com"
TRIGGER_SPEC_TYPE = "ON_DEMAND"

EXAMPLE_TASK_BODY = {
    "trigger_spec": {"type_": TRIGGER_SPEC_TYPE},
    "execution_spec": {"service_account": SERVICE_ACC,
                       "args": {"TASK_ARGS": """clouddq-executable.zip, ALL, gs://dataplex-clouddq-config/config_dataplex.zip, --gcp_project_id="pbalm-dataplex", --gcp_region_id="US", --gcp_bq_dataset_id="clouddq_project", --target_bigquery_summary_table="pbalm-dataplex.clouddq_project.composer-summary"
    """}
                       },
    "spark": {"python_script_file": f"gs://dataplex-clouddq-artifacts-us-central1/clouddq_pyspark_driver.py",
              "file_uris": ["gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip",
                            "gs://dataplex-clouddq-artifacts-us-central1/clouddq-executable.zip.hashsum",
                            "gs://dataplex-clouddq-config/config_dataplex.zip"],
              #    "infrastructure_spec": {"vpc_network": {"sub_network": "projects/dataplex-tasks-external-test/regions/us-central1/subnetworks/x-project-shared-subnet-1"}}
              },
}

# for best practices
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

with models.DAG(
        'data_quality',
        catchup=False,
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:

    @task(task_id='gen_dataplex_task_id')
    def gen_task_id():
        task_id = "task-clouddq-" + str(uuid.uuid4())
        print(f'Generated task ID {task_id}')
        return task_id

    create_dataplex_dq_task = DataplexCreateTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id='{{ ti.xcom_pull(task_ids=\'gen_dataplex_task_id\') }}',
        task_id="dataplex_dq_task",
    )

    @task(task_id='check_task_completion')
    def check_task_completion(**kwargs):
        client = DataplexServiceClient()

        ti = kwargs['ti']
        task_id = ti.xcom_pull(task_ids='gen_dataplex_task_id')
        request = dataplex_v1.ListJobsRequest(
            parent=f'projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/tasks/{task_id}',
        )

        s = 0
        cycle = 0
        while s != 4 and cycle < 100:
            cycle = cycle + 1
            time.sleep(10) # seconds
            page_result = client.list_jobs(request=request)
            s = list(page_result)[0].state
            print(f'Task ID {task_id} job is in state {s}')

        # State 4 is SUCCESS
        if s != 4:
            raise AirflowFailException(f'Task ID {task_id} job is in state {s}')

    gen_task_id() >> create_dataplex_dq_task >> check_task_completion()
