#!/usr/bin/env python

import json
import os
import shutil
import subprocess
import tempfile
import time
from queue import Empty

import socketio
import yaml
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import EventQueue

from al_core.server_base import ServerBase
from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.str_utils import StringTable
from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service

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


class FileEventHandler(PatternMatchingEventHandler):
    def __init__(self, queue, patterns):
        PatternMatchingEventHandler.__init__(self, patterns=patterns)
        self.queue = queue

    def process(self, event):
        if event.src_path.endswith('result.json'):
            self.queue.put((event.src_path, STATUSES.RESULT_FOUND))
        elif event.pathname.endswith('error.json'):
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
        self.observer.daemon = True
        self.observer.start()

    def stop(self):
        # TODO: wait until task has finished processing before stopping observer
        # self.observer.stop()
        pass


class TaskHandler(ServerBase):
    def __init__(self, shutdown_timeout=SHUTDOWN_SECONDS_LIMIT):
        super().__init__('assemblyline.service.task_handler', shutdown_timeout=shutdown_timeout)

        self.classification_yml = '/etc/assemblyline/classification.yml'
        self.service_manifest_yml = '/etc/assemblyline/service_manifest.yml'
        self.constants_json = '/etc/assemblyline/constants.json'

        self.sio = socketio.Client()
        self.status = None

        self.wait_start = None
        self.queue = EventQueue()
        self.file_watcher = None

        self.service = None
        self.service_heuristics = []
        self.service_tool_version = None
        self.file_required = None
        self.service_api_host = os.environ['SERVICE_API_HOST']
        self.service_api_auth_key = os.environ['SERVICE_API_AUTH_KEY']

        self.container_id = os.environ['HOSTNAME']

        self.received_folder_path = None
        self.completed_folder_path = None

        # Register sio event handlers
        self.sio.on('connect', handler=self.on_connect, namespace='/tasking')
        self.sio.on('disconnect', handler=self.on_disconnect, namespace='/tasking')
        self.sio.on('got_task', handler=self.on_got_task, namespace='/tasking')
        self.sio.on('write_file_chunk', handler=self.write_file_chunk, namespace='/helper')
        self.sio.on('quit', handler=self.on_quit, namespace='/helper')

    def callback_get_classification_definition(self, classification_definition):
        self.log.info(f"Received classification definition. Saving it to: {self.classification_yml}")

        with open(self.classification_yml, 'w') as fh:
            yaml.safe_dump(classification_definition, fh)

    def callback_get_system_constants(self, system_constants):
        self.log.info(f"Received system constants. Saving them to: {self.constants_json}")

        with open(self.constants_json, 'w') as fh:
            json.dump(system_constants, fh)

    def callback_register_service(self, keep_alive):
        if keep_alive:
            self.status = STATUSES.WAITING_FOR_TASK
        else:
            self.status = STATUSES.STOPPING

        self.sio.disconnect()

    def callback_save_heuristic(self, new):
        if new:
            self.log.info("Heuristic saved successfully.")

    def callback_wait_for_task(self):
        self.log.info(f"Server aware we are waiting for task for service: {self.service.name}_{self.service.version}")

        self.status = STATUSES.WAITING_FOR_TASK

        self.wait_start = time.time()

    def get_classification(self):
        self.log.info("Requesting classification definition...")

        # Get classification definition and save it
        self.sio.emit('get_classification_definition', namespace='/helper',
                      callback=self.callback_get_classification_definition)

    def load_service_manifest(self):
        # Load from the config yaml
        while True:
            self.log.info("Trying to load service config YAML...")
            if os.path.exists(self.service_manifest_yml):
                with open(self.service_manifest_yml, 'r') as yml_fh:
                    service_manifest_data = yaml.safe_load(yml_fh)
                    self.service_tool_version = service_manifest_data.get('tool_version')
                    self.file_required = service_manifest_data.get('file_required', True)

                    self.service = Service(dict(
                        accepts=service_manifest_data.get('accepts', None),
                        rejects=service_manifest_data.get('rejects', None),
                        category=service_manifest_data.get('category', None),
                        config=service_manifest_data.get('config', None),
                        cpu_cores=service_manifest_data.get('cpu_cores', None),
                        description=service_manifest_data.get('description', None),
                        enabled=service_manifest_data.get('enabled', None),
                        install_by_default=service_manifest_data.get('install_by_default', None),
                        is_external=service_manifest_data.get('is_external', None),
                        licence_count=service_manifest_data.get('licence', None),
                        name=service_manifest_data.get('name'),
                        version=service_manifest_data.get('version'),
                        ram_mb=service_manifest_data.get('ram_mb', None),
                        disable_cache=service_manifest_data.get('diasble_cache', None),
                        stage=service_manifest_data.get('stage', None),
                        submission_params=service_manifest_data.get('submission_params', None),
                        supported_platforms=service_manifest_data.get('supported_platforms', None),
                        timeout=service_manifest_data.get('timeout', None),
                        docker_config=service_manifest_data.get('docker_config', None),
                        update_config=service_manifest_data.get('update_config', None),
                    ))

                    for heuristic in service_manifest_data.get('heuristics', []):
                        self.service_heuristics.append(Heuristic(dict(
                            attack_id=heuristic.get('attack_id', None),
                            classification=heuristic.get('classification', None),
                            description=heuristic.get('description'),
                            filetype=heuristic.get('filetype'),
                            heur_id=heuristic.get('heur_id'),
                            name=heuristic.get('name'),
                            namespace=heuristic.get('namespace', None),
                            score=heuristic.get('score'),
                        )))

                    break
            else:
                time.sleep(5)

    def get_systems_constants(self):
        self.log.info("Requesting system constants...")

        self.sio.emit('get_system_constants', namespace='/helper', callback=self.callback_get_system_constants)

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

                    received_sha256 = get_sha256_for_file(file_path)
                    if task.fileinfo.sha256 != received_sha256:
                        retry += 1
                        self.log.info(
                            f"An error occurred while downloading file. "
                            f"SHA256 mismatch between requested and downloaded file. "
                            f"{task.fileinfo.sha256} != {received_sha256}")
                    else:
                        break

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
                        result = Result(json.load(f))

                    new_files = result.response.extracted + result.response.supplementary
                    if new_files:
                        for file in new_files:
                            file_path = os.path.join(self.completed_folder_path, file.name)
                            with open(file_path, 'rb') as f:
                                self.sio.emit('upload_file',
                                              (f.read(), result.classification.value, file.sha256, task.ttl),
                                              namespace='/helper')

                    self.sio.emit('done_task', (exec_time, task.as_primitives(), result.as_primitives()),
                                  namespace='/tasking')

                elif self.status == STATUSES.ERROR_FOUND:
                    error_json_path = json_path
                    with open(error_json_path, 'r') as f:
                        error = Error(json.load(f))

                    self.sio.emit('done_task', (exec_time, task.as_primitives(), error.as_primitives()),
                                  namespace='/tasking')

                break

        except Exception as e:
            self.log.info(str(e))
        finally:
            # Cleanup contents of 'received' and 'completed' directory
            self.cleanup_working_directory(self.received_folder_path)
            self.cleanup_working_directory(self.completed_folder_path)

            self.sio.emit('wait_for_task', namespace='/tasking', callback=self.callback_wait_for_task)

    def on_quit(self, *args):
        self.stop()

    @staticmethod
    def cleanup_working_directory(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except:
                pass

    def try_run(self):
        self.status = STATUSES.INITIALIZING
        self.load_service_manifest()

        headers = {
            'Container-Id': self.container_id,
            'Service-API-Auth-Key': self.service_api_auth_key,
            'Service-Name': self.service.name,
            'Service-Version': self.service.version,
            'Service-Timeout': str(self.service.timeout),
        }

        if self.service_tool_version:
            headers['Service-Tool-Version'] = self.service_tool_version

        self.sio.connect(self.service_api_host, headers=headers, namespaces=['/helper'])

        self.get_classification()
        # self.get_systems_constants()

        # Register service
        self.sio.emit('register_service', self.service.as_primitives(), namespace='/helper',
                      callback=self.callback_register_service)

        # Save any new service Heuristic(s)
        heuristics = [heuristic.as_primitives() for heuristic in self.service_heuristics]
        self.sio.emit('save_heuristics', heuristics, namespace='/helper', callback=self.callback_save_heuristic)

        while True:
            if self.status == STATUSES.WAITING_FOR_TASK:
                break
            elif self.status == STATUSES.STOPPING:
                self.stop()
                return
            else:
                time.sleep(1)

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
        subprocess.call(["supervisorctl", "signal", "SIGKILL", "run_service"])

        if self.file_watcher:
            self.file_watcher.stop()
            self.log.info(f"Stopped watching folder for result/error: {self.completed_folder_path}")

        if self.sio:
            self.sio.disconnect()

        super().stop()


if __name__ == '__main__':
    TaskHandler().serve_forever()
