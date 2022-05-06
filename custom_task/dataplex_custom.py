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

from airflow.sensors.base import BaseSensorOperator

from airflow.exceptions import AirflowFailException
from google.cloud import dataplex_v1
from airflow.providers.google.cloud.hooks.dataplex import DataplexHook

class DataplexJobCompletionSensor(BaseSensorOperator):

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        task_id_generator: str,
        api_version: str = "v1",
        *args,
        ** kwargs
    ) -> None:

        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.task_id_generator = task_id_generator
        self.api_version = api_version


    def poke(self, context) -> bool:

        task_id = context['ti'].xcom_pull(task_ids=self.task_id_generator)

        self.log.info(f"Waiting for job of task {task_id} to complete")
        hook = DataplexHook(
            gcp_conn_id="google_cloud_default",
            delegate_to=None,
            api_version=self.api_version,
            impersonation_chain=None,
        )

        client = hook.get_dataplex_client()
        request = dataplex_v1.ListJobsRequest(
            parent=f'projects/{self.project_id}/locations/{self.region}/lakes/{self.lake_id}/tasks/{task_id}',
        )

        page_result = client.list_jobs(request=request)
        s = list(page_result)[0].state
        print(f'Task ID {task_id} job is in state {s}')

        if s == 5:
            raise AirflowFailException(f"Job for task ID {task_id} failed")

        return s == 4
