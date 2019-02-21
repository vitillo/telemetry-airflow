# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from plugins.moz_databricks import MozDatabricksSubmitRunOperator

# The environment variables required by the MozDatabricks operator must be available
# at module import because the `os.environ` is accessed in the class scope. These
# variables are used by Airflow to template variables. Monkeypatch occurs after import,
# so the variables are defined in `tox.ini` instead.


def test_missing_tbv_or_mozetl_env(mocker):
    mock_hook = mocker.patch("plugins.databricks.databricks_operator.DatabricksHook")
    with pytest.raises(ValueError):
        MozDatabricksSubmitRunOperator(
            job_name="test_databricks", env={}, instance_count=1
        )


def test_mozetl_success(mocker):
    mock_hook = mocker.patch("plugins.databricks.databricks_operator.DatabricksHook")
    mock_hook_instance = mock_hook.return_value
    mock_hook_instance.submit_run.return_value = 1

    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"MOZETL_COMMAND": "test"},
        instance_count=1,
    )
    operator.execute(None)
    mock_hook_instance.submit_run.assert_called_once()

    json = mock_hook_instance.submit_run.call_args[0][0]
    assert json.get("spark_python_task") is not None


def test_tbv_success(mocker):
    mock_hook = mocker.patch("plugins.databricks.databricks_operator.DatabricksHook")
    mock_hook_instance = mock_hook.return_value
    mock_hook_instance.submit_run.return_value = 1

    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"TBV_CLASS": "test", "ARTIFACT_URL": "https://test.amazonaws.com/test"},
        instance_count=1,
    )
    operator.execute(None)
    mock_hook_instance.submit_run.assert_called_once()

    json = mock_hook_instance.submit_run.call_args[0][0]
    assert json.get("spark_jar_task") is not None
