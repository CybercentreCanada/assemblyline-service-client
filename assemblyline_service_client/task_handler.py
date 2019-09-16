import copy
import json
import os
import shutil
import signal
import tempfile
import time
from queue import Empty
from typing import BinaryIO
import requests

import yaml
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import EventQueue

from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.str_utils import StringTable
from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.service import Service
from assemblyline_core.server_base import ServerBase
from assemblyline.odm.messages.task import Task as ServiceTask

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


class FileEventHandler(PatternMatchingEventHandler):
    def __init__(self, queue, patterns):
        PatternMatchingEventHandler.__init__(self, patterns=patterns)
        self.queue = queue

    def process(self, event):
        if event.src_path.endswith('result.json'):
            self.queue.put((event.src_path, STATUSES.RESULT_FOUND))
        elif event.src_path.endswith('error.json'):
            self.queue.put((event.src_path, STATUSES.ERROR_FOUND))

    def on_created(self, event):
        self.process(event)


class FileWatcher:
    def __init__(self, queue, watch_path):
        self.watch_path = watch_path
        self.observer = None
        self.queue = queue

    def start(self):
        patt = ['*.json']
        event_handler = FileEventHandler(self.queue, patterns=patt)
        self.observer = Observer()
        self.observer.schedule(event_handler, path=self.watch_path)
        self.observer.start()

    def stop(self):
        self.observer.stop()


class TaskHandler(ServerBase):
    def __init__(self, shutdown_timeout=SHUTDOWN_SECONDS_LIMIT, api_host=None, api_key=None, container_id=None):
        super().__init__('assemblyline.service.task_handler', shutdown_timeout=shutdown_timeout)

        self.service_manifest_yml = os.path.join(tempfile.gettempdir(), 'service_manifest.yml')

        self.status = None

        self.wait_start = None
        self.queue = EventQueue()
        self.file_watcher = None

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

        self.file_upload_count = 0
        self.processing_upload = False

        self.received_dir = None
        self.completed_dir = None

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
            service_timeout=str(self.service.timeout),
        )

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        super().start()
        signal.signal(signal.SIGUSR1, self.handle_service_crash)

    def handle_service_crash(self, signum, frame):
        """USER1 is raised when the service has crashed, this represents an unknown error."""
        self.queue.put((None, STATUSES.ERROR_FOUND))

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
                self.log.info(f"HEURISTICS COUNT: {len(self.service_heuristics)}")

    # noinspection PyBroadException
    @staticmethod
    def cleanup_working_directory(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
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

        retries = 0

        while True:
            try:
                func = getattr(self.session, method)
                return func(url, **kwargs).json()['api_response']
            except requests.ConnectionError:
                pass
            except requests.Timeout:  # Handles ConnectTimeout and ReadTimeout
                pass

    def try_run(self):
        self.initialize_service()

        while self.running:
            task = self.get_task()
            if not task:
                continue

            # Download file if required by service
            if self.file_required:
                self.download_file(task.fileinfo.sha256)

            # Save task as JSON, so that run_service can start processing task
            task_json_path = os.path.join(self.received_dir, f'{task.fileinfo.sha256}_task.json')
            with open(task_json_path, 'w') as f:
                json.dump(task.as_primitives(), f)
            self.log.info(f"Saved task to: {task_json_path}")

            self.status = STATUSES.PROCESSING

            # Wait until the task completes with a result or error produced
            json_path = ''
            while self.status != STATUSES.RESULT_FOUND or self.status != STATUSES.ERROR_FOUND:
                try:
                    json_path, status = self.queue.get(timeout=1)
                    self.status = status
                except Empty:
                    pass

            self.log.info(f"Task completed (SID: {task.sid})")
            if self.status == STATUSES.RESULT_FOUND:
                self.handle_task_result(json_path, task)
            elif self.status == STATUSES.ERROR_FOUND:
                self.handle_task_error(json_path, task)

    def initialize_service(self):
        self.status = STATUSES.INITIALIZING
        r = self.request_with_retries('put', self._path('service', 'register'), json=self.service_manifest_data)
        if r['keep_alive']:
            self.status = STATUSES.WAITING_FOR_TASK
        else:
            self.log.info(f"Service registered with {len(r['new_heuristics'])} heuristics. Now stopping...")
            self.status = STATUSES.STOPPING
            self.stop()
            return

        # Set and create the directory for the output of completed tasks
        self.received_dir = os.path.join(tempfile.gettempdir(), self.service.name.lower(), 'received')
        if not os.path.isdir(self.received_dir):
            os.makedirs(self.received_dir)

        # Set and create the directory for the output of completed tasks
        self.completed_dir = os.path.join(tempfile.gettempdir(), self.service.name.lower(), 'completed')
        if not os.path.isdir(self.completed_dir):
            os.makedirs(self.completed_dir)

        # Start the file watcher for the completed folder
        self.file_watcher = FileWatcher(self.queue, self.completed_dir)
        self.file_watcher.start()
        self.log.info(f"Started watching folder for result/error: {self.completed_dir}")

    def get_task(self) -> ServiceTask:
        task = None
        headers = dict(timeout=str(TASK_REQUEST_TIMEOUT))
        self.log.info(f"Requesting a task with {TASK_REQUEST_TIMEOUT}s timeout...")
        r = self.request_with_retries('get', self._path('task'), headers=headers, timeout=TASK_REQUEST_TIMEOUT*2)
        if r['task'] is False:  # No task received
            self.log.info(f"No task received")
        else:  # Task received
            try:
                task = ServiceTask(task)
            except ValueError as e:
                self.log.error(f"Invalid task received: {str(e)}")
                # TODO: return error to service server

        return task

    def download_file(self, sha256) -> None:
        received_file_sha256 = ''
        retry = 0
        while received_file_sha256 != sha256 and retry < 3:
            r = self.session.get(self._path('file', sha256), headers=self.headers, stream=True)
            file_path = os.path.join(self.received_dir, sha256)
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()

            received_file_sha256 = get_sha256_for_file(file_path)
            retry += 1

        if received_file_sha256 != sha256:
            self.log.error(f"File (SHA256: {sha256}) could not be downloaded after 3 tries. "
                           "Reporting task error to service service server.")
            # TODO: report error to service server

    def handle_task_result(self, result_json_path: str, task: ServiceTask):
        with open(result_json_path, 'r') as f:
            result = json.load(f)

        data = dict(task=task.as_primitives(), result=result)
        r = self.request_with_retries('post', self._path('task'), json=data)
        if r['requested_files']:
            # Map of file info by SHA256
            result_files = {file['sha256']: file for file in
                            result['response']['extracted'] + result['response']['supplementary']}

            files = dict()
            for f_sha256 in r['requested_files']:
                file_info = result_files[f_sha256]
                files[f_sha256] = (f_sha256, open(os.path.join(self.completed_dir, file_info['name']), 'rb'),
                                   'application/octet-stream',
                                   dict(classification=file_info['classification'], ttl=task.ttl))

            # Upload the files requested by service server
            r = self.request_with_retries('put', self._path('file'), files=files)

    def handle_task_error(self, error_json_path: str, task: ServiceTask):
        if error_json_path is None:
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

        data = dict(task=task.as_primitives(), error=error)
        r = self.request_with_retries('post', self._path('task'), json=data)

    def stop(self):
        if self.file_watcher:
            self.file_watcher.stop()
            self.log.info(f"Stopped watching folder for result/error: {self.completed_dir}")

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
