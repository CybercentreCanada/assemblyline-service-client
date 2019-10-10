import copy
import json
import os
import select
import shutil
import signal
import tempfile
import time
from typing import Optional

import requests
import yaml

from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.str_utils import StringTable
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.service import Service
from assemblyline_core.server_base import ServerBase

STATUSES = StringTable('STATUSES', [
    ('INITIALIZING', 0),
    ('WAITING_FOR_TASK', 1),
    ('DOWNLOADING_FILE', 2),
    ('DOWNLOADING_FILE_COMPLETED', 3),
    ('PROCESSING', 4),
    ('RESULT_FOUND', 5),
    ('ERROR_FOUND', 6),
    ('STOPPING', 7),
])

SHUTDOWN_SECONDS_LIMIT = 10
DEFAULT_API_KEY = 'ThisIsARandomAuthKey...ChangeMe!'
SUPPORTED_API = 'v1'
TASK_REQUEST_TIMEOUT = 30
TASK_FIFO_PATH = "/tmp/task.fifo"
DONE_FIFO_PATH = "/tmp/done.fifo"

# The number of tasks a service will complete before stopping, letting the environment start a new container.
# By default there is no limit, but this lets the orchestration environment set one
TASK_COMPLETE_LIMIT = os.environ.get('AL_SERVICE_TASK_LIMIT', float('inf'))


class ServiceServerException(Exception):
    pass


class TaskHandler(ServerBase):
    def __init__(self, shutdown_timeout=SHUTDOWN_SECONDS_LIMIT, api_host=None, api_key=None, container_id=None):
        super().__init__('assemblyline.service.task_handler', shutdown_timeout=shutdown_timeout)

        self.service_manifest_yml = os.path.join(tempfile.gettempdir(), 'service_manifest.yml')

        self.status = None
        self.wait_start = None
        self.task_fifo = None
        self.done_fifo = None
        self.tasks_processed = 0

        self.service = None
        self.service_manifest_data = None
        self.service_heuristics = []
        self.service_tool_version = None
        self.file_required = None
        self.service_api_host = api_host or os.environ.get('SERVICE_API_HOST', 'http://localhost:5003')
        self.service_api_key = api_key or os.environ.get('SERVICE_API_KEY', DEFAULT_API_KEY)
        self.container_id = container_id or os.environ.get('HOSTNAME', 'dev-service')
        self.session = None
        self.headers = None

        self.task = None

    def _path(self, prefix, *args):
        """
        Calculate the API path using the prefix as shown:
            /api/v1/<prefix>/[arg1/[arg2/[...]]][?k1=v1[...]]
        """
        return os.path.join(self.service_api_host, 'api', SUPPORTED_API, prefix, *args) + '/'

    def start(self):
        self.log.info("Loading service manifest...")
        if self.service_api_key == DEFAULT_API_KEY:
            key = '**default key** - You should consider setting SERVICE_API_KEY in your service containers'
        else:
            key = '**custom key**'
        self.log.info("---- TaskHandler config ----")
        self.log.info(f"SERVICE_API_HOST: {self.service_api_host}")
        self.log.info(f"SERVICE_APIKEY: {key}")
        self.log.info(f"CONTAINER-ID: {self.container_id}")
        self.load_service_manifest()
        self.log.info("----------------------------")

        self.headers = dict(
            X_APIKEY=self.service_api_key,
            container_id=self.container_id,
            service_name=self.service.name,
            service_version=self.service.version,
            service_tool_version=self.service_tool_version,
        )

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        super().start()
        signal.signal(signal.SIGUSR1, self.handle_service_crash)

    # noinspection PyUnusedLocal
    def handle_service_crash(self, signum, frame):
        """USER1 is raised when the service has crashed, this represents an unknown error."""
        self.handle_task_error("", self.task)
        self.stop()

    def load_service_manifest(self):
        # Load from the service manifest yaml
        while not self.service:
            if os.path.exists(self.service_manifest_yml):
                with open(self.service_manifest_yml, 'r') as yml_fh:
                    self.service_manifest_data = yaml.safe_load(yml_fh)
                    self.service_tool_version = self.service_manifest_data.get('tool_version')
                    self.file_required = self.service_manifest_data.get('file_required', True)

                    # Save the heuristics from service manifest
                    for heuristic in self.service_manifest_data.get('heuristics', []):
                        self.service_heuristics.append(heuristic)

                    # Pop the 'extra' data from the service manifest to build the service object
                    service = copy.deepcopy(self.service_manifest_data)
                    for x in ['file_required', 'tool_version', 'heuristics']:
                        service.pop(x, None)
                    self.service = Service(service)

            if not self.service:
                time.sleep(5)
            else:
                self.log.info(f"SERVICE: {self.service.name}")
                self.log.info(f"HEURISTICS_COUNT: {len(self.service_heuristics)}")

    def update_service_manifest(self, data):
        for x in ['version']:
            data.pop(x, None)

        if os.path.exists(self.service_manifest_yml):
            with open(self.service_manifest_yml, 'r') as yml_fh:
                self.service_manifest_data = yaml.safe_load(yml_fh)

                self.service_manifest_data.update(data)

            with open(self.service_manifest_yml, 'w') as yml_fh:
                yaml.safe_dump(self.service_manifest_data, yml_fh)

    # noinspection PyBroadException
    @staticmethod
    def cleanup_working_directory(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if file_path != TASK_FIFO_PATH or file_path != DONE_FIFO_PATH:
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception:
                    pass

    def request_with_retries(self, method: str, url: str, **kwargs):
        if 'headers' in kwargs:
            self.session.headers.update(kwargs['headers'])
            kwargs.pop('headers')

        back_off_time = 1

        while True:
            try:
                func = getattr(self.session, method)
                resp = func(url, **kwargs)

                if resp.status_code == 400 and resp.json():
                    self.log.exception(resp.json()['api_error_message'])
                    raise ServiceServerException(resp.json()['api_error_message'])
                else:
                    resp.raise_for_status()

                return resp.json()['api_response']
            except requests.ConnectionError:
                time.sleep(back_off_time)
                self.log.exception(f"ConnectionError. Retrying after {back_off_time}s.")
            except requests.Timeout:  # Handles ConnectTimeout and ReadTimeout
                time.sleep(back_off_time)
            except requests.HTTPError as e:
                self.log.exception(str(e))
                raise
            except requests.exceptions.RequestException as e:  # All other types of exceptions
                self.log.exception(str(e))
                raise

            back_off_time = min(back_off_time*2, 30)

    def try_run(self):
        self.initialize_service()

        while self.running and self.tasks_processed < TASK_COMPLETE_LIMIT:
            self.task = self.get_task()
            if not self.task:
                continue

            # Download file if required by service
            if self.file_required:
                file_path = self.download_file(self.task.fileinfo.sha256)

                # Check if file_path was returned, meaning the file was downloaded successfully
                if file_path is None:
                    continue

            # Save task as JSON, so that run_service can start processing task
            task_json_path = os.path.join(tempfile.gettempdir(),
                                          f'{self.task.sid}_{self.task.fileinfo.sha256}_task.json')
            with open(task_json_path, 'w') as f:
                json.dump(self.task.as_primitives(), f)
            self.log.info(f"Saved task to: {task_json_path}")

            self.status = STATUSES.PROCESSING

            # Send tasking message to run_service
            self.task_fifo.write(f"{task_json_path}\n")
            self.task_fifo.flush()

            while True:
                try:
                    read_ready, _, _ = select.select([self.done_fifo], [], [], 1)
                    if read_ready:
                        break
                except ValueError:
                    self.log.info('Done fifo is closed. Cleaning up...')
                    return

            json_path, status = json.loads(self.done_fifo.readline().strip())
            self.status = status

            self.log.info(f"Task completed (SID: {self.task.sid})")
            self.tasks_processed += 1
            if self.status == STATUSES.RESULT_FOUND:
                self.handle_task_result(json_path, self.task)
            elif self.status == STATUSES.ERROR_FOUND:
                self.handle_task_error(json_path, self.task)

            # Cleanup contents of tempdir which contains task json, result json, and working directory of service
            self.cleanup_working_directory(tempfile.gettempdir())

    def initialize_service(self):
        self.status = STATUSES.INITIALIZING
        r = self.request_with_retries('put', self._path('service', 'register'), json=self.service_manifest_data)
        if not r['keep_alive']:
            self.log.info(f"Service registered with {len(r['new_heuristics'])} heuristics. Now stopping...")
            self.status = STATUSES.STOPPING
            self.stop()
            return

        # Update service manifest with data received from service server
        self.update_service_manifest(r['service_config'])

        # Start task receiving fifo
        self.log.info('Waiting for receive task named pipe to be ready...')
        while not os.path.exists(TASK_FIFO_PATH):
            time.sleep(1)
        self.task_fifo = open(TASK_FIFO_PATH, "w")

        # Start task completing fifo
        self.log.info('Waiting for complete task named pipe to be ready...')
        while not os.path.exists(DONE_FIFO_PATH):
            time.sleep(1)
        self.done_fifo = open(DONE_FIFO_PATH, "r")

    def get_task(self) -> ServiceTask:
        self.status = STATUSES.WAITING_FOR_TASK
        task = None
        headers = dict(timeout=str(TASK_REQUEST_TIMEOUT))
        self.log.info(f"Requesting a task with {TASK_REQUEST_TIMEOUT}s timeout...")
        r = self.request_with_retries('get', self._path('task'), headers=headers, timeout=TASK_REQUEST_TIMEOUT*2)
        if r['task'] is False:  # No task received
            self.log.info(f"No task received")
        else:  # Task received
            try:
                task = ServiceTask(r['task'])
                self.log.info(f"Received task (SID: {task.sid})")
            except ValueError as e:
                self.log.error(f"Invalid task received: {str(e)}")
                # TODO: return error to service server

        return task

    def download_file(self, sha256) -> Optional[str]:
        self.status = STATUSES.DOWNLOADING_FILE
        received_file_sha256 = ''
        retry = 0
        file_path = None
        self.log.info(f"Downloading file (SHA256: {sha256})")
        while received_file_sha256 != sha256 and retry < 3:
            r = self.session.get(self._path('file', sha256), headers=self.headers)
            retry += 1
            # self.log.info(str(r.ok))
            if r.status_code == 404:
                self.log.error(f"Requested file not found in the system ({sha256})")
                return None
            else:
                file_path = os.path.join(tempfile.gettempdir(), sha256)
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)

                received_file_sha256 = get_sha256_for_file(file_path)

        if received_file_sha256 != sha256:
            self.log.error(f"File (SHA256: {sha256}) could not be downloaded after 3 tries. "
                           "Reporting task error to service server.")
            # TODO: report error to service server
            return None

        self.status = STATUSES.DOWNLOADING_FILE_COMPLETED
        return file_path

    def handle_task_result(self, result_json_path: str, task: ServiceTask):
        with open(result_json_path, 'r') as f:
            result = json.load(f)

        # Map of file info by SHA256
        result_files = {}
        for file in result['response']['extracted'] + result['response']['supplementary']:
            result_files[file['sha256']] = copy.deepcopy(file)
            file.pop('path', None)

        data = dict(task=task.as_primitives(), result=result)
        r = self.request_with_retries('post', self._path('task'), json=data)
        if not r['success'] and r['missing_files']:
            while not r['success'] and r['missing_files']:
                for f_sha256 in r['missing_files']:
                    file_info = result_files[f_sha256]
                    headers = dict(
                        sha256=file_info['sha256'],
                        classification=file_info['classification'],
                        ttl=str(task.ttl),
                    )

                    files = dict(file=open(file_info['path'], 'rb'))

                    # Upload the file requested by service server
                    self.log.info(f"Uploading file (Path: {file_info['path']}, SHA256: {file_info['sha256']})")
                    self.request_with_retries('put', self._path('file'), files=files, headers=headers)

                r = self.request_with_retries('post', self._path('task'), json=data)

    def handle_task_error(self, error_json_path: str, task: ServiceTask):
        if not error_json_path:
            error = Error(dict(
                created='NOW',
                expiry_ts=now_as_iso(task.ttl * 24 * 60 * 60),
                response=dict(
                    message="The service instance processing this task has terminated unexpectedly.",
                    service_name=task.service_name,
                    service_version=' ',
                    status='FAIL_NONRECOVERABLE',
                ),
                sha256=task.fileinfo.sha256,
                type='UNKNOWN',
            ))
        else:
            with open(error_json_path, 'r') as f:
                error = Error(json.load(f))

        data = dict(task=task.as_primitives(), error=error.as_primitives())
        self.request_with_retries('post', self._path('task'), json=data)

    def stop(self):
        self.log.info("Closing named pipes...")
        if self.done_fifo is not None:
            self.done_fifo.close()
        if self.task_fifo is not None:
            self.task_fifo.close()

        if self.status == STATUSES.WAITING_FOR_TASK:
            # A task request was sent and a task might be received, so shutdown after giving service time to process it
            self._shutdown_timeout = TASK_REQUEST_TIMEOUT + self.service.timeout
        elif self.status != STATUSES.INITIALIZING:
            # A task is currently running, so wait until service timeout before doing a hard stop
            self._shutdown_timeout = self.service.timeout
        else:
            # Already the default
            self._shutdown_timeout = SHUTDOWN_SECONDS_LIMIT

        super().stop()


if __name__ == '__main__':
    TaskHandler().serve_forever()
