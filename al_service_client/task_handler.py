#!/usr/bin/env python

# Run a standalone AL service

import hashlib
import json
import logging
import os
import shutil
import socketio
import tempfile
import time

import pyinotify
import yaml

from al_service_client import Client
from assemblyline.common import log

log.init_logging('assemblyline.task_handler', log_level=logging.INFO)
log = logging.getLogger('assemblyline.task_handler')

svc_api_host = os.environ['SERVICE_API_HOST']
# name = os.environ['SERVICE_PATH']

# svc_name = name.split(".")[-1].lower()

svc_client = Client(svc_api_host)
sio = socketio.Client()

wm = pyinotify.WatchManager()  # Watch Manager

result_found = False
wait_start = None


@sio.on('connect', namespace='/tasking')
def on_connect():
    log.info('Connected to tasking socketIO server')
    sio.emit('wait_for_task', (service_name, service_version, service_tool_version), namespace='/tasking', callback=callback_wait_for_task)


@sio.on('disconnect', namespace='/tasking')
def on_disconnect():
    log.info('Disconnected from the socketIO server')


def callback_wait_for_task():
    global wait_start

    log.info(f"Server aware we are waiting for task for service: {service_name}[{service_version}]")

    wait_start = time.time()


@sio.on('got_task', namespace='/tasking')
def on_got_task(task):
    global result_found, wait_start

    start_time = time.time()
    idle_time = int((start_time-wait_start)*1000)

    sio.emit('got_task', (service_name, idle_time), namespace='/tasking')

    task_hash = hashlib.md5(str(task['sid'] + task['fileinfo']['sha256']).encode('utf-8')).hexdigest()

    folder_path = os.path.join(tempfile.gettempdir(), task['service_name'].lower(), 'received', task_hash)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

    # Get file if required by service
    file_path = os.path.join(folder_path, task['fileinfo']['sha256'])
    if file_required:
        svc_client.task.get_file(task['fileinfo']['sha256'], file_path)

    # Save task.json
    task_json_path = os.path.join(folder_path, 'task.json')
    with open(task_json_path, 'w') as f:
        json.dump(task, f)

    folder_path = os.path.join(tempfile.gettempdir(), task['service_name'].lower(), 'completed', task_hash)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

    wdd = wm.add_watch(folder_path, pyinotify.IN_CREATE, rec=False)

    while not result_found:
        time.sleep(0.1)

    result_found = False
    wm.rm_watch(list(wdd.values()))

    result_json_path = os.path.join(folder_path, 'result.json')
    with open(result_json_path, 'r') as f:
        result = json.load(f)
    done_task(task, result, task_hash)

    exec_time = int((wait_start-start_time)*1000)
    sio.emit('done_task', (service_name, exec_time), namespace='/tasking')

    sio.emit('wait_for_task', (service_name, service_version, service_tool_version), namespace='/tasking', callback=callback_wait_for_task)


class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        global result_found

        if 'result.json' in event.pathname:
            result_found = True


def done_task(task, result, task_hash):
    folder_path = os.path.join(tempfile.gettempdir(), task['service_name'].lower())
    try:
        msg = svc_client.task.done_task(task=task, result=result)
        log.info('RESULT OF DONE_TASK:: '+msg)
    finally:
        if os.path.isdir(folder_path):
            shutil.rmtree(folder_path)


def get_classification(yml_classification=None):
    log.info('Getting classification definition...')

    if yml_classification is None:
        yml_classification = "/etc/assemblyline/classification.yml"

    # Get classification definition and save it
    classification = svc_client.help.get_classification_definition()
    with open(yml_classification, 'w') as fh:
        yaml.safe_dump(classification, fh)


def get_systems_constants(json_constants=None):
    log.info('Getting system constants...')

    if json_constants is None:
        json_constants = "/etc/assemblyline/constants.json"

        # Get system constants and save it
        constants = svc_client.help.get_systems_constants()
        with open(json_constants, 'w') as fh:
            json.dump(constants, fh)


def get_service_config(yml_config=None):
    if yml_config is None:
        yml_config = "/etc/assemblyline/service_config.yml"

    # Load from the yaml config
    while True:
        log.info('Trying to load service config YAML...')
        if os.path.exists(yml_config):
            with open(yml_config, 'r') as yml_fh:
                yaml_config = yaml.safe_load(yml_fh)
            return yaml_config
        else:
            time.sleep(5)


def task_handler():
    notifier = pyinotify.ThreadedNotifier(wm, EventHandler())

    try:
        notifier.start()

        while True:
            sio.connect(svc_api_host, namespaces=['/tasking'])
            sio.wait()
    finally:
        notifier.stop()


if __name__ == '__main__':
    get_classification()
    get_systems_constants()

    service_config = get_service_config()
    service_name = service_config['SERVICE_NAME']
    service_version = service_config['SERVICE_VERSION']
    service_tool_version = service_config['TOOL_VERSION']
    file_required = service_config['SERVICE_FILE_REQUIRED']

    task_handler()
