import json
import os
import shutil
import signal
import tempfile
import time
from queue import Empty
from typing import BinaryIO
import requests

import socketio
import socketio.exceptions
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
        self.session = requests.Session()

        self.file_upload_count = 0
        self.processing_upload = False

        self.received_dir = None
        self.completed_dir = None

    def _path(self, prefix, *args, **kw):
        """
        Calculate the API path using the prefix as shown:
            /api/v1/<prefix>/[arg1/[arg2/[...]]][?k1=v1[...]]
        """
        # path = '/'.join(['api', SUPPORTED_API, prefix] + list(args) + [''])
        path = os.path.join(self.service_api_host, 'api', SUPPORTED_API, prefix, *args) + '/'
        return path

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

        super().start()
        signal.signal(signal.SIGUSR1, self.handle_service_crash)

    def handle_service_crash(self, signum, frame):
        """USER1 is raised when the service has crashed, this represents a unknown error."""
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
                    service = self.service_manifest_data
                    for x in ['file_required', 'tool_version', 'heuristics']:
                        service.pop(x, None)
                    self.service = Service(service)

            if not self.service:
                time.sleep(5)
            else:
                self.log.info(f'SERVICE: {self.service.name}')
                self.log.info(f"HEURISTICS_COUNT: {len(self.service_heuristics)}")

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

    def request_with_retries(self, method: str, *args, **kwargs):
        retries = 0

        while True:
            try:
                func = getattr(self.session, method)
                return func(*args, **kwargs).json()['api_response']
            except requests.ConnectionError:
                pass
            except requests.Timeout:  # Handles ConnectTimeout and ReadTimeout
                pass

    def try_run(self):
        self.status = STATUSES.INITIALIZING

        self.log.info(f"Connecting to service server: {self.service_api_host}")

        headers = dict(X_APIKEY=self.service_api_key)
        # headers = {'X-APIKEY': self.service_api_key}
        data = self.service_manifest_data
        r = self.request_with_retries('put', self._path('service', 'register'), headers=headers, json=data)
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

        # Request a task
        headers = dict(

            container_id=self.container_id,
            service_name=self.service.name,
            service_version=self.service.version,
            service_timeout=str(self.service.timeout),
            timeout=str(30)
        )
        r = self.request_with_retries('get', self._path('task'), headers=headers, timeout=6000)
        print('here')

    def stop(self):
        # TODO: wait until task has finished processing before stopping observer
        if self.file_watcher:
            self.file_watcher.stop()
            self.log.info(f"Stopped watching folder for result/error: {self.completed_dir}")

        super().stop()


if __name__ == '__main__':
    TaskHandler().serve_forever()
