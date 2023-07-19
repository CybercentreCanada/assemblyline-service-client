import os

from assemblyline_service_client import task_handler

from assemblyline.odm.models.service import Service

SERVICE_CONFIG_NAME = "service_manifest.yml"
TEMP_SERVICE_CONFIG_PATH = os.path.join("/tmp", SERVICE_CONFIG_NAME)


def setup_module():
    if not os.path.exists(TEMP_SERVICE_CONFIG_PATH):
        open_manifest = open(TEMP_SERVICE_CONFIG_PATH, "w")
        open_manifest.write("\n".join([
            "name: Sample",
            "version: sample",
            "docker_config:",
            "    image: sample",
            "heuristics:",
            "  - heur_id: 17",
            "    name: blah",
            "    description: blah",
            "    filetype: '*'",
            "    score: 250",
        ]))
        open_manifest.close()


def teardown_module():
    if os.path.exists(TEMP_SERVICE_CONFIG_PATH):
        os.remove(TEMP_SERVICE_CONFIG_PATH)


def test_init():
    # Defaults
    default_th = task_handler.TaskHandler()
    assert default_th._shutdown_timeout == task_handler.SHUTDOWN_SECONDS_LIMIT
    assert default_th.service_manifest_yml == "/tmp/service_manifest.yml"
    assert default_th.status == None
    assert default_th.register_only is False
    assert default_th.container_mode is False
    assert default_th.wait_start is None
    assert default_th.task_fifo_path == "/tmp/service_task.fifo"
    assert default_th.done_fifo_path == "/tmp/service_done.fifo"
    assert default_th.task_fifo is None
    assert default_th.done_fifo is None
    assert default_th.tasks_processed == 0

    assert default_th.service is None
    assert default_th.service_manifest_data is None
    assert default_th.service_heuristics == []
    assert default_th.service_tool_version is None
    assert default_th.file_required is None
    assert default_th.service_api_host == 'http://localhost:5003'
    assert default_th.service_api_key == task_handler.DEFAULT_API_KEY
    assert default_th.container_id == 'dev-service'
    assert default_th.session is None
    assert default_th.headers is None
    assert default_th.task is None
    assert default_th.tasking_dir == "/tmp"

    # Custom
    custom_th = task_handler.TaskHandler(
        shutdown_timeout=1,
        api_host="blah",
        api_key="blah",
        container_id="blah",
        register_only=True,
        container_mode=True
    )
    assert custom_th._shutdown_timeout == 1
    assert custom_th.service_manifest_yml == "/tmp/service_manifest.yml"
    assert custom_th.status == None
    assert custom_th.register_only is True
    assert custom_th.container_mode is True
    assert custom_th.wait_start is None
    assert custom_th.task_fifo_path == "/tmp/service_task.fifo"
    assert custom_th.done_fifo_path == "/tmp/service_done.fifo"
    assert custom_th.task_fifo is None
    assert custom_th.done_fifo is None
    assert custom_th.tasks_processed == 0

    assert custom_th.service is None
    assert custom_th.service_manifest_data is None
    assert custom_th.service_heuristics == []
    assert custom_th.service_tool_version is None
    assert custom_th.file_required is None
    assert custom_th.service_api_host == 'blah'
    assert custom_th.service_api_key == 'blah'
    assert custom_th.container_id == 'blah'
    assert custom_th.session is None
    assert custom_th.headers is None
    assert custom_th.task is None
    assert custom_th.tasking_dir == "/tmp"


def test_path():
    # Default
    default_th = task_handler.TaskHandler()
    assert default_th._path("blah") == "http://localhost:5003/api/v1/blah/"

    # With args
    assert default_th._path("blah", "blah1", "blah2") == "http://localhost:5003/api/v1/blah/blah1/blah2/"


def test_load_service_manifest():
    default_th = task_handler.TaskHandler()
    default_th.load_service_manifest()
    service_object = Service(
        {
            "name": "Sample",
            "version": "sample",
            "docker_config": {
                "image": "sample"
            },
        }
    )
    assert default_th.service.__dict__ == service_object.__dict__
