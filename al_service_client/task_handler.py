#!/usr/bin/env python

# Run a standalone AL service

import json
import logging
import os
import shutil
import signal
import tempfile
import time

import pyinotify
import socketio
import yaml

from assemblyline.common import log
from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.str_utils import StringTable
from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result

log.init_logging('assemblyline.task_handler')
log = logging.getLogger('assemblyline.task_handler')

svc_api_host = os.environ['SERVICE_API_HOST']
svc_api_auth_key = os.environ['SERVICE_API_AUTH_KEY']

sio = socketio.Client()

wm = pyinotify.WatchManager()  # Watch Manager

wait_start = None

chunk_size = 64*1024

STATUSES = StringTable('STATUSES', [
    ('WAITING', 0),
    ('DOWNLOADING_FILE', 1),
    ('DOWNLOADING_FILE_COMPLETED', 2),
    ('PROCESSING', 3),
    ('RESULT_FOUND', 4),
    ('ERROR_FOUND', 5),
])

status = None


def disconnect():
    log.info('emitting disconnect signal to sio')
    sio.emit('disconnect', namespace='/tasking')
    sio.disconnect()


signal.signal(signal.SIGINT, disconnect)
signal.signal(signal.SIGTERM, disconnect)


class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        global status

        if event.pathname.endswith('result.json'):
            status = STATUSES.RESULT_FOUND
        elif event.pathname.endswith('error.json'):
            status = STATUSES.ERROR_FOUND


def callback_get_classification_definition(classification_definition, yml_file):
    log.info("Received classification definition")

    with open(yml_file, 'w') as fh:
        yaml.safe_dump(classification_definition, fh)


def callback_get_system_constants(system_constants, json_file):
    log.info("Received system constants")

    with open(json_file, 'w') as fh:
        json.dump(system_constants, fh)


def callback_wait_for_task():
    global wait_start, status

    log.info(f"Server aware we are waiting for task for service: {service_name}_{service_version}")

    status = STATUSES.WAITING

    wait_start = time.time()


@sio.on('connect', namespace='/tasking')
def on_connect():
    log.info("Connected to tasking SocketIO server")
    sio.emit('wait_for_task', namespace='/tasking', callback=callback_wait_for_task)


@sio.on('disconnect', namespace='/tasking')
def on_disconnect():
    log.info("Disconnected from SocketIO server")


@sio.on('got_task', namespace='/tasking')
def on_got_task(task):
    global status, wait_start

    task = Task(task)

    received_folder_path = os.path.join(tempfile.gettempdir(), task.service_name.lower(), 'received')
    completed_folder_path = os.path.join(tempfile.gettempdir(), task.service_name.lower(), 'completed')

    try:
        start_time = time.time()
        idle_time = int((start_time-wait_start)*1000)

        if not os.path.isdir(received_folder_path):
            os.makedirs(received_folder_path)

        # Get file if required by service
        file_path = os.path.join(received_folder_path, task.fileinfo.sha256)
        if file_required:
            # Create empty file to prepare for downloading the file in chunks
            with open(file_path, 'wb') as f:
                pass

            sio.emit('start_download', (task.fileinfo.sha256, file_path), namespace='/helper')
            status = STATUSES.DOWNLOADING_FILE

            retry = 0
            max_retries = 3
            while retry < max_retries:
                wait = 0
                max_wait = 60
                while not (status == STATUSES.DOWNLOADING_FILE_COMPLETED):
                    # Wait until the file is completely downloaded or until 60 seconds max
                    if wait > max_wait:
                        break
                    time.sleep(1)
                    wait += 1

                received_sha256 = get_sha256_for_file(file_path)
                if task.fileinfo.sha256 != received_sha256:
                    retry += 1
                    log.info(f"An error occurred while downloading file. SHA256 mismatch between requested and downloaded file. {task.fileinfo.sha256} != {received_sha256}")
                else:
                    break

        # Save task.json
        task_json_path = os.path.join(received_folder_path, f'{task.fileinfo.sha256}_task.json')
        with open(task_json_path, 'w') as f:
            json.dump(task.as_primitives(), f)
        log.info(f"Saving task to: {task_json_path}")

        sio.emit('got_task', idle_time, namespace='/tasking')

        if not os.path.isdir(completed_folder_path):
            os.makedirs(completed_folder_path)

        # Check if 'completed' directory already contains a result.json or error.json
        existing_files = os.listdir(completed_folder_path)
        if 'result.json' in existing_files:
            status = STATUSES.RESULT_FOUND
        elif 'error.json' in existing_files:
            status = STATUSES.ERROR_FOUND
        else:
            wdd = wm.add_watch(completed_folder_path, pyinotify.IN_CREATE, rec=True)
            status = STATUSES.PROCESSING

        while not ((status == STATUSES.RESULT_FOUND) or (status == STATUSES.ERROR_FOUND)):
            time.sleep(1)

        log.info(f"{task.service_name} task completed, SID: {task.sid}")

        if status == STATUSES.PROCESSING:
            try:
                wm.rm_watch(list(wdd.values()))
            except:
                pass

        exec_time = int((time.time() - start_time) * 1000)

        if status == STATUSES.RESULT_FOUND:
            result_json_path = os.path.join(completed_folder_path, 'result.json')
            with open(result_json_path, 'r') as f:
                result = Result(json.load(f))

            new_files = result.response.extracted + result.response.supplementary
            if new_files:
                for file in new_files:
                    file_path = os.path.join(completed_folder_path, file.name)
                    with open(file_path, 'rb') as f:
                        sio.emit('upload_file', (f.read(), result.classification.value, file.sha256, task.ttl), namespace='/helper')

            sio.emit('done_task', (exec_time, task.as_primitives(), result.as_primitives()), namespace='/tasking')

        elif status == STATUSES.ERROR_FOUND:
            error_json_path = os.path.join(completed_folder_path, 'error.json')
            with open(error_json_path, 'r') as f:
                error = Error(json.load(f))

            sio.emit('done_task', (exec_time, task.as_primitives(), error.as_primitives()), namespace='/tasking')
    finally:
        # Cleanup contents of 'received' and 'completed' directory
        cleanup_working_directory(received_folder_path)
        cleanup_working_directory(completed_folder_path)

        sio.emit('wait_for_task', namespace='/tasking', callback=callback_wait_for_task)


def get_classification(yml_classification=None):
    log.info("Getting classification definition...")

    if yml_classification is None:
        yml_classification = '/etc/assemblyline/classification.yml'

    # Get classification definition and save it
    sio.emit('get_classification_definition', yml_classification, namespace='/helper', callback=callback_get_classification_definition)


def get_service_config(yml_config=None):
    if yml_config is None:
        yml_config = '/etc/assemblyline/service_config.yml'

    # Load from the yaml config
    while True:
        log.info("Trying to load service config YAML...")
        if os.path.exists(yml_config):
            with open(yml_config, 'r') as yml_fh:
                yaml_config = yaml.safe_load(yml_fh)
            return yaml_config
        else:
            time.sleep(5)


def get_systems_constants(json_constants=None):
    log.info('Getting system constants...')

    if json_constants is None:
        json_constants = '/etc/assemblyline/constants.json'

        # Get system constants and save it
        sio.emit('get_system_constants', json_constants, namespace='/helper', callback=callback_get_system_constants)


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


@sio.on('write_file_chunk', namespace='/helper')
def write_file_chunk(file_path, offset, data, last_chunk):
    global status

    try:
        with open(file_path, 'r+b') as f:
            f.seek(offset)
            f.write(data)
    except IOError:
        log.error(f"An error occurred while downloading file to: {file_path}")
    finally:
        if last_chunk:
            status = STATUSES.DOWNLOADING_FILE_COMPLETED


def task_handler():
    notifier = pyinotify.ThreadedNotifier(wm, EventHandler())

    try:
        notifier.start()
        # sio.wait()
        while True:
            time.sleep(1)
    finally:
        notifier.stop()


if __name__ == '__main__':
    service_config = get_service_config()
    while True:
        try:
            service_name = service_config['SERVICE_NAME']
            service_version = service_config['SERVICE_VERSION']
            service_tool_version = service_config['SERVICE_TOOL_VERSION']
            service_category = service_config['SERVICE_CATEGORY']
            service_stage = service_config['SERVICE_STAGE']
            file_required = service_config['SERVICE_FILE_REQUIRED']
            break
        except TypeError:
            continue

    headers = {
        'Service-API-Auth-Key': svc_api_auth_key,
        'Service-Name': service_name,
        'Service-Version': service_version,
        'Service-Tool-Version': service_tool_version,
    }

    sio.connect(svc_api_host, headers=headers, namespaces=['/helper', '/tasking'])
    get_classification()
    get_systems_constants()

    # Register service
    service_data = {
        'name': service_name,
        'enabled': True,
        'category': service_category,
        'stage': service_stage,
        'version': service_version
    }

    sio.emit('register_service', service_data, namespace='/helper')

    task_handler()
