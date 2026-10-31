#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import pytest
from sqlalchemy import select

from airflow.models.callback import DagProcessorCallback
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_callbacks

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"
STATE_PATH = "/task-instances/{task_instance_id}/state"


@pytest.mark.parametrize("version", ["2026-06-30", "2026-10-30"])
def test_old_api_omits_request_callbacks_from_schema(client, version):
    """The request_callbacks query parameter did not exist before the 2027-02-28 API version."""
    response = client.get(f"/execution/openapi.json?version={version}")
    assert response.status_code == 200
    parameters = response.json()["paths"][STATE_PATH]["patch"]["parameters"]
    assert "request_callbacks" not in {parameter["name"] for parameter in parameters}


def test_head_api_has_request_callbacks_in_schema(client):
    response = client.get("/execution/openapi.json?version=2027-02-28")
    assert response.status_code == 200
    parameters = response.json()["paths"][STATE_PATH]["patch"]["parameters"]
    assert "request_callbacks" in {parameter["name"] for parameter in parameters}


@pytest.mark.parametrize("state", ["success", "failed", "up_for_retry"])
def test_old_api_ignores_request_callbacks(client, session, create_task_instance, state):
    """An old worker never sends the parameter; a stray one changes nothing in an old API version."""
    clear_db_callbacks()
    ti = create_task_instance(task_id="legacy_final_state", state=State.RUNNING)
    session.commit()
    client.headers["Airflow-API-Version"] = "2026-10-30"

    response = client.patch(
        f"/execution{STATE_PATH.format(task_instance_id=ti.id)}",
        params={"request_callbacks": "true"},
        json={"state": state, "end_date": TIMESTAMP_STR},
    )

    assert response.status_code == 204
    assert session.scalars(select(DagProcessorCallback)).all() == []
