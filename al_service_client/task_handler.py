#!/usr/bin/env python

# Run a standalone AL service

import hashlib
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

sio = socketio.Client()

wm = pyinotify.WatchManager()  # Watch Manager

download_status = 'not_started'
wait_start = None

chunk_size = 64*1024

STATUSES = StringTable('STATUSES', [
    ('WAITING', 0),
    ('PROCESSING', 1),
    ('RESULT_FOUND', 2),
    ('ERROR_FOUND', 3),
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


def callback_service_client_connect():
    log.info('Connected to tasking socketIO server')
    sio.emit('wait_for_task', (service_name, service_version, service_tool_version), namespace='/tasking')


@sio.on('disconnect', namespace='/tasking')
def on_disconnect():
    log.info('Disconnected from the socketIO server')


@sio.on('write_file_chunk', namespace='/helper')
def write_chunk(file_path, offset, data):
    try:
        with open(file_path, 'r+b') as f:
            f.seek(offset)
            f.write(data)
    except IOError:
        log.error(f"An error occurred while downloading file to: {file_path}")
    return True


def callback_download_file(data, file_path):
    global download_status

    download_status = 'started'
    log.info(f"Saving received file to: {file_path}")
    with open(file_path, 'wb') as f:
        f.write(data)
        f.close()

    sha256 = os.path.basename(file_path)
    received_sha256 = get_sha256_for_file(file_path)
    if received_sha256 == sha256:
        download_status = 'success'
    else:
        download_status = 'fail'


def callback_get_classification_definition(classification_definition, yml_file):
    log.info("Received classification definition")

    with open(yml_file, 'w') as fh:
        yaml.safe_dump(classification_definition, fh)


def callback_get_system_constants(system_constants, json_file):
    log.info("Received system constants")

    with open(json_file, 'w') as fh:
        json.dump(system_constants, fh)


@sio.on('wait_for_task', namespace='/tasking')
def on_wait_for_task(client_id):
    global wait_start, status

    log.info(f"Server aware we are waiting for task for service: {service_name}[{service_version}]  {client_id}")

    status = STATUSES.WAITING

    wait_start = time.time()


@sio.on('got_task', namespace='/tasking')
def on_got_task(task):
    global download_status, status, wait_start

    task = Task(task)

    start_time = time.time()
    idle_time = int((start_time-wait_start)*1000)

    sio.emit('got_task', (service_name, idle_time), namespace='/tasking')

    try:
        task_hash = hashlib.md5(str(task.sid + task.fileinfo.sha256).encode('utf-8')).hexdigest()

        folder_path = os.path.join(tempfile.gettempdir(), task.service_name.lower(), 'received')
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)

        # Get file if required by service
        file_path = os.path.join(folder_path, task.fileinfo.sha256)
        if file_required:
            with open(file_path, 'wb') as f:
                pass

            sio.emit('start_download', (task.fileinfo.sha256, file_path), namespace='/helper')
            received_sha256 = None
            while task.fileinfo.sha256 != received_sha256:
                time.sleep(1)
                try:
                    received_sha256 = get_sha256_for_file(file_path)
                except:
                    pass

        # Save task.json
        task_json_path = os.path.join(folder_path, f'{task.fileinfo.sha256}_task.json')
        with open(task_json_path, 'w') as f:
            json.dump(task.as_primitives(), f)
        log.info(f"Saving task to: {task_json_path}")

        folder_path = os.path.join(tempfile.gettempdir(), task.service_name.lower(), 'completed', task_hash)
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)

        # Check if 'completed' directory already contains a result.json or error.json
        existing_files = os.listdir(folder_path)
        if 'result.json' in existing_files:
            status = STATUSES.RESULT_FOUND
        elif 'error.json' in existing_files:
            status = STATUSES.ERROR_FOUND
        else:
            wdd = wm.add_watch(folder_path, pyinotify.IN_CREATE, rec=True)
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
            result_json_path = os.path.join(folder_path, 'result.json')
            with open(result_json_path, 'r') as f:
                result = Result(json.load(f))

            new_files = result.response.extracted + result.response.supplementary
            if new_files:
                for file in new_files:
                    file_path = os.path.join(folder_path, file.name)
                    with open(file_path, 'rb') as f:
                        sio.emit('upload_file', (f.read(), result.classification.value, service_name, file.sha256, task.ttl), namespace='/helper')

            sio.emit('done_task', (service_name, exec_time, task.as_primitives(), result.as_primitives()), namespace='/tasking')

        elif status == STATUSES.ERROR_FOUND:
            error_json_path = os.path.join(folder_path, 'error.json')
            with open(error_json_path, 'r') as f:
                error = Error(json.load(f))

            sio.emit('done_task', (service_name, exec_time, task.as_primitives(), error.as_primitives()), namespace='/tasking')
            folder_path = os.path.join(tempfile.gettempdir(), task.service_name.lower())
            cleanup_working_directory(folder_path)

        sio.emit('wait_for_task', (service_name, service_version, service_tool_version), namespace='/tasking')
    except Exception as e:
        log.info(f"ERROR::{str(e)}")


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
    shutil.rmtree(folder_path)


def task_handler():
    notifier = pyinotify.ThreadedNotifier(wm, EventHandler())

    try:
        notifier.start()
        sio.emit('service_client_connect', namespace='/tasking', callback=callback_service_client_connect)
        sio.wait()
    finally:
        notifier.stop()


if __name__ == '__main__':
    sio.connect(svc_api_host, namespaces=['/helper', '/tasking'])
    get_classification()
    get_systems_constants()

    service_config = get_service_config()
    while True:
        try:
            service_name = service_config['SERVICE_NAME']
            service_version = service_config['SERVICE_VERSION']
            service_tool_version = service_config['SERVICE_TOOL_VERSION']
            file_required = service_config['SERVICE_FILE_REQUIRED']
            break
        except TypeError:
            continue

    task_handler()
