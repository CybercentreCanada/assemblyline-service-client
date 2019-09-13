import json
import os
import shutil
import signal
import tempfile
import time
from queue import Empty
from typing import BinaryIO

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
        self.service_heuristics = []
        self.service_tool_version = None
        self.file_required = None
        self.service_api_host = api_host or os.environ.get('SERVICE_API_HOST', 'http://localhost:5003')
        self.service_api_auth_key = api_key or os.environ.get('SERVICE_API_AUTH_KEY', DEFAULT_API_KEY)
        self.container_id = container_id or os.environ.get('HOSTNAME', 'dev-service')

        self.file_upload_count = 0
        self.processing_upload = False

        self.received_folder_path = None
        self.completed_folder_path = None
        self.sio = self.build_sio_client()

    def start(self):
        self.log.info("Loading service manifest...")
        if self.service_api_auth_key == DEFAULT_API_KEY:
            key = '**default key** - You should consider setting SERVICE_API_AUTH_KEY in your service containers'
        else:
            key = '**custom key**'
        self.log.info("---- TaskHandler config ----")
        self.log.info(f"SERVICE_API_HOST: {self.service_api_host}")
        self.log.info(f"SERVICE_API_AUTH_KEY: {key}")
        self.log.info(f"CONTAINER-ID: {self.container_id}")
        self.load_service_manifest()
        self.log.info("----------------------------")

        super().start()
        signal.signal(signal.SIGUSR1, self.handle_service_crash)

    def handle_service_crash(self, signum, frame):
        """USER1 is raised when the service has crashed, this represents a unknown error."""
        self.queue.put((None, STATUSES.ERROR_FOUND))

    def build_sio_client(self):
        sio = socketio.Client(logger=self.log, engineio_logger=self.log)
        # Register sio event handlers
        sio.on('connect', handler=self.on_connect, namespace='/tasking')
        sio.on('disconnect', handler=self.on_disconnect, namespace='/tasking')
        sio.on('got_task', handler=self.on_got_task, namespace='/tasking')
        sio.on('write_file_chunk', handler=self.write_file_chunk, namespace='/helper')
        sio.on('quit', handler=self.on_quit, namespace='/helper')
        sio.on('upload_success', handler=self.on_upload_success, namespace='/helper')
        sio.on('chunk_upload_success', handler=self.on_chunk_upload_success, namespace='/helper')
        return sio

    def on_chunk_upload_success(self, success):
        if success:
            self.processing_upload = False

    def callback_file_exists(self, sha256, file_path, classification, ttl):
        # File doesn't exist on the service server, start upload
        if sha256:
            file_size = os.path.getsize(file_path)
            offset = 0
            chunk_size = 64 * 1024
            with open(file_path, 'rb') as f:
                last_chunk = False
                for chunk in read_in_chunks(f, chunk_size):
                    if (file_size < chunk_size) or ((offset + chunk_size) >= file_size):
                        last_chunk = True

                    while self.processing_upload:
                        time.sleep(0.1)

                    self.processing_upload = True
                    self.sio.emit('upload_file_chunk', (offset, chunk, last_chunk, classification, sha256, ttl),
                                  namespace='/helper')

                    offset += chunk_size
        else:
            self.file_upload_count += 1

    def callback_register_service(self, keep_alive):
        if keep_alive:
            self.status = STATUSES.WAITING_FOR_TASK
        else:
            self.log.info("New service registered. Stopping...")
            self.status = STATUSES.STOPPING

    def callback_save_heuristic(self, new):
        if new:
            self.log.info("Heuristic saved successfully.")

    def callback_wait_for_task(self):
        self.log.info(f"Server aware we are waiting for task for service: {self.service.name}_{self.service.version}")

        self.status = STATUSES.WAITING_FOR_TASK

        self.wait_start = time.time()

    def load_service_manifest(self):
        # Load from the service manifest yaml
        while not self.service:
            if os.path.exists(self.service_manifest_yml):
                with open(self.service_manifest_yml, 'r') as yml_fh:
                    service_manifest_data = yaml.safe_load(yml_fh)
                    self.service_tool_version = service_manifest_data.get('tool_version')
                    self.file_required = service_manifest_data.get('file_required', True)

                    # Save the heuristics from service manifest
                    for heuristic in service_manifest_data.get('heuristics', []):
                        self.service_heuristics.append(heuristic)

                    # Pop the 'extra' data from the service manifest
                    for x in ['file_required', 'tool_version', 'heuristics']:
                        service_manifest_data.pop(x, None)

                    self.service = Service(service_manifest_data)

            if not self.service:
                time.sleep(5)
            else:
                self.log.info(f'SERVICE: {self.service.name}')
                self.log.info(f"HEURISTICS_COUNT: {len(self.service_heuristics)}")

    def on_connect(self):
        self.log.info("Connected to tasking SocketIO server")
        self.sio.emit('wait_for_task', namespace='/tasking', callback=self.callback_wait_for_task)

    def on_disconnect(self):
        self.log.info("Disconnected from SocketIO server")

    def on_got_task(self, task):
        task = Task(task)

        try:
            start_time = time.time()
            idle_time = int((start_time - self.wait_start) * 1000)

            if not os.path.isdir(self.received_folder_path):
                os.makedirs(self.received_folder_path)

            # Get file if required by service
            file_path = os.path.join(self.received_folder_path, task.fileinfo.sha256)
            if self.file_required:
                # Create empty file to prepare for downloading the file in chunks
                with open(file_path, 'wb'):
                    pass

                self.sio.emit('start_download', (task.fileinfo.sha256, file_path), namespace='/helper')
                self.status = STATUSES.DOWNLOADING_FILE

                retry = 0
                max_retries = 3
                while retry < max_retries:
                    wait = 0
                    max_wait = 60
                    while not (self.status == STATUSES.DOWNLOADING_FILE_COMPLETED):
                        # Wait until the file is completely downloaded or until 60 seconds max
                        if wait > max_wait:
                            break
                        time.sleep(1)
                        wait += 1

                    # We might be finished downloading the file, check that we have
                    received_sha256 = get_sha256_for_file(file_path)
                    if task.fileinfo.sha256 != received_sha256:
                        retry += 1
                        self.log.warning(
                            f"An error occurred while downloading file. "
                            f"SHA256 mismatch between requested and downloaded file. "
                            f"{task.fileinfo.sha256} != {received_sha256}")
                    else:
                        # The file matches the expected hash, so we can break out of the retry loop
                        break

                if retry >= max_retries:
                    raise ValueError("Couldn't download the file from the server.")

            # Save task.json
            task_json_path = os.path.join(self.received_folder_path, f'{task.fileinfo.sha256}_task.json')
            with open(task_json_path, 'w') as f:
                json.dump(task.as_primitives(), f)
            self.log.info(f"Saving task to: {task_json_path}")

            self.sio.emit('got_task', idle_time, namespace='/tasking')

            self.status = STATUSES.PROCESSING

            while True:
                try:
                    json_path, status = self.queue.get(timeout=1)
                except Empty:
                    continue

                self.log.info(f"{task.service_name} service task completed, SID: {task.sid}")
                self.status = status

                exec_time = int((time.time() - start_time) * 1000)

                if self.status == STATUSES.RESULT_FOUND:
                    result_json_path = json_path
                    with open(result_json_path, 'r') as f:
                        result = json.load(f)

                    new_files = result['response']['extracted'] + result['response']['supplementary']
                    if new_files:
                        new_file_count = 0
                        log_time = 0
                        self.file_upload_count = 0

                        for file in new_files:
                            new_file_count += 1
                            file_path = os.path.join(self.completed_folder_path, file['name'])
                            self.sio.emit('file_exists', namespace='/helper', callback=self.callback_file_exists,
                                          data=(file['sha256'], file_path, result['classification'], task.ttl))

                            while self.file_upload_count != new_file_count:
                                time.sleep(0.1)

                                # Every 10 seconds print a log message about how the updates are going
                                if time.time() - log_time > 10:
                                    log_time = time.time()
                                    self.log.info(f"Waiting for files to be uploaded: "
                                                  f"{self.file_upload_count}/{new_file_count}")

                    self.sio.emit('done_task', (exec_time, task.as_primitives(), result), namespace='/tasking')

                elif self.status == STATUSES.ERROR_FOUND:
                    if json_path is None:
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
                        error_json_path = json_path
                        with open(error_json_path, 'r') as f:
                            error = Error(json.load(f))

                    self.sio.emit('done_task', (exec_time, task.as_primitives(), error.as_primitives()),
                                  namespace='/tasking')

                break

        except Exception:
            self.log.exception("An exception occurred processing a task")
            raise
        finally:
            # Cleanup contents of 'received' and 'completed' directory
            self.cleanup_working_directory(self.received_folder_path)
            self.cleanup_working_directory(self.completed_folder_path)

        self.sio.emit('wait_for_task', namespace='/tasking', callback=self.callback_wait_for_task)

    def on_quit(self, *args):
        self.stop()

    def on_upload_success(self, success):
        if success:
            self.file_upload_count += 1
        else:
            self.sio.emit('file_exists', namespace='/helper', callback=self.callback_file_exists,
                          data=(file['sha256'], file_path, result['classification'], task.ttl))

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

    def try_run(self):
        self.status = STATUSES.INITIALIZING

        headers = {
            'Container-Id': self.container_id,
            'Service-API-Auth-Key': self.service_api_auth_key,
            'Service-Name': self.service.name,
            'Service-Version': self.service.version,
            'Service-Timeout': str(self.service.timeout),
        }

        if self.service_tool_version:
            headers['Service-Tool-Version'] = self.service_tool_version

        self.log.info(f"Connecting to SocketIO service server: {self.service_api_host}")
        retry_count = 0
        while True:
            try:
                self.sio.connect(self.service_api_host, headers=headers, namespaces=['/helper'])
                break
            except socketio.exceptions.ConnectionError:
                if retry_count > 3:
                    self.log.warning(f"Can't connect to SocketIO service server: {self.service_api_host}")
                time.sleep(5)
                retry_count += 1

        # Register service
        self.sio.emit('register_service', self.service.as_primitives(), namespace='/helper',
                      callback=self.callback_register_service)

        # Save any new service Heuristic(s)
        if self.service_heuristics:
            self.sio.emit('save_heuristics', self.service_heuristics, namespace='/helper',
                          callback=self.callback_save_heuristic)

        # Wait until service initialization is complete
        while self.status == STATUSES.INITIALIZING:
            time.sleep(1)

        if self.status == STATUSES.WAITING_FOR_TASK:
            self.sio.disconnect()
        elif self.status == STATUSES.STOPPING:
            self.stop()
            return

        self.sio = self.build_sio_client()
        self.sio.connect(self.service_api_host, headers=headers, namespaces=['/helper', '/tasking'])

        self.received_folder_path = os.path.join(tempfile.gettempdir(), self.service.name.lower(), 'received')
        if not os.path.isdir(self.received_folder_path):
            os.makedirs(self.received_folder_path)

        self.completed_folder_path = os.path.join(tempfile.gettempdir(), self.service.name.lower(), 'completed')
        if not os.path.isdir(self.completed_folder_path):
            os.makedirs(self.completed_folder_path)

        # Start the file watcher
        self.file_watcher = FileWatcher(self.queue, self.completed_folder_path)
        self.file_watcher.start()
        self.log.info(f"Started watching folder for result/error: {self.completed_folder_path}")

        while self.running:
            time.sleep(1)

        # while self.status != STATUSES.WAITING_FOR_TASK:
        #     time.sleep(1)
        #     self.log.info('waiting for task to finish before stopping')

    def write_file_chunk(self, file_path, offset, data, last_chunk):
        try:
            with open(file_path, 'r+b') as f:
                f.seek(offset)
                f.write(data)
        except IOError:
            self.log.error(f"An error occurred while downloading file to: {file_path}")
        finally:
            if last_chunk:
                self.status = STATUSES.DOWNLOADING_FILE_COMPLETED

    def stop(self):
        # TODO: wait until task has finished processing before stopping observer
        if self.file_watcher:
            self.file_watcher.stop()
            self.log.info(f"Stopped watching folder for result/error: {self.completed_folder_path}")

        if self.sio:
            self.sio.disconnect()

        super().stop()


def read_in_chunks(file_object: BinaryIO, chunk_size: int):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


if __name__ == '__main__':
    TaskHandler().serve_forever()
