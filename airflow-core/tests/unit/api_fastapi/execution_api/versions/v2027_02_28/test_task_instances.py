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

from airflow.utils.state import State

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"

FINAL_STATE_PAYLOADS = ["TISuccessStatePayload", "TITerminalStatePayload", "TIRetryStatePayload"]


@pytest.mark.parametrize("version", ["2026-06-30", "2026-10-30"])
def test_old_api_omits_callbacks_ran_from_schema(client, version):
    """The callbacks_ran field did not exist before the 2027-02-28 API version."""
    response = client.get(f"/execution/openapi.json?version={version}")
    assert response.status_code == 200
    schemas = response.json()["components"]["schemas"]
    for payload in FINAL_STATE_PAYLOADS:
        assert "callbacks_ran" not in schemas[payload]["properties"], payload


@pytest.mark.parametrize("state", ["success", "failed", "up_for_retry"])
def test_old_api_rejects_callbacks_ran(client, session, create_task_instance, state):
    ti = create_task_instance(task_id="legacy_final_state", state=State.RUNNING)
    session.commit()
    client.headers["Airflow-API-Version"] = "2026-10-30"

    response = client.patch(
        f"/execution/task-instances/{ti.id}/state",
        json={"state": state, "end_date": TIMESTAMP_STR, "callbacks_ran": False},
    )

    assert response.status_code == 422
    session.refresh(ti)
    assert ti.state == State.RUNNING
