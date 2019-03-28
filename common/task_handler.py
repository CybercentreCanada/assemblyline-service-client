#!/usr/bin/env python

# Run a standalone AL service

import json
import logging
import os
import time
import yaml
import tempfile

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from assemblyline.common import log

from svc_client import Client

log.init_logging('assemblyline.task_handler', log_level=logging.INFO)
log = logging.getLogger('assemblyline.task_handler')

svc_api_host = os.environ['SERVICE_API_HOST']
# name = os.environ['SERVICE_PATH']

# svc_name = name.split(".")[-1].lower()

svc_client = Client(svc_api_host)

os.system('python /opt/alv4/alv3_service/common/run_service.py')


def done_task(task, result):
    svc_client.task.done_task(task=task,
                              result=result)


def get_classification(yml_classification=None):
    log.info('Getting classification definition...')

    if yml_classification is None:
        yml_classification = "/etc/assemblyline/classification.yml"

    # Get classification definition and save it
    classification = svc_client.help.get_classification_definition()
    with open(yml_classification, 'w') as fh:
        yaml.dump(classification, fh)


def get_systems_constants(json_constants=None):
    log.info('Getting system constants...')

    if json_constants is None:
        json_constants = "/etc/assemblyline/constants.json"

        # Get system constants and save it
        cosntants = svc_client.help.get_systems_constants()
        with open(json_constants, 'w') as fh:
            json.dump(cosntants, fh)


def get_task(service_config):
    task = svc_client.task.get_task(service_name=service_config['SERVICE_NAME'],
                             service_version=service_config['SERVICE_VERSION'],
                             service_tool_version=service_config['TOOL_VERSION'],
                             file_required=service_config['SERVICE_FILE_REQUIRED'])
    return task


def get_service_config(yml_config=None):
    if yml_config is None:
        # service_path = os.environ['SERVICE_PATH']

        yml_config = "/etc/assemblyline/service_config.yml"

    # Load from the yaml config
    while True:
        log.info('Trying to load service config YAML...')
        if os.path.exists(yml_config):
            with open(yml_config) as yml_fh:
                service_config = yaml.safe_load(yml_fh.read())
            return service_config
        else:
            time.sleep(5)


def task_handler(service_config):
    def on_created(event):
        # This function is called when a file is created
        log.info(f"on_created, {event.src_path} has been created!")
        file_path = os.path.join(folder_path, 'results.json')

        if event.src_path == file_path:
            log.info('result.json found!')
            my_observer.stop()
            my_observer.join()

    def on_deleted(event):
        # This function is called when a file is deleted
        log.info(f"on_deleted, {event.src_path}!")

    def on_modified(event):
        # This function is called when a file is modified
        log.info(f"on_modified, {event.src_path} has been modified")

    def on_moved(event):
        # This function is called when a file is moved
        log.info(f"on_moved, {event.src_path} to {event.dest_path}")

    patterns = "*"
    ignore_patterns = ""
    ignore_directories = False
    case_sensitive = True

    while True:
        task = get_task(service_config)

        my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

        my_event_handler.on_created = on_created
        my_event_handler.on_deleted = on_deleted
        my_event_handler.on_modified = on_modified
        my_event_handler.on_moved = on_moved

        # Create an observer
        folder_path = os.path.join(tempfile.gettempdir(), task['service_name'].lower(), task['sid'])
        go_recursively = True

        my_observer = Observer()
        my_observer.schedule(my_event_handler, folder_path, recursive=go_recursively)

        # Start the observer
        my_observer.start()
        log.info(f'Waiting for result.json in: {folder_path}')

        while True:
            time.sleep(0.5)

        log.info('calling done_task')
        result_json_path = os.path.join(folder_path, 'result.json')
        result = json.load(result_json_path)
        done_task(task, result)


if __name__ == '__main__':
    get_classification()
    get_systems_constants()
    service = get_service_config()

    task_handler(service)
