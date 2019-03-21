#!/usr/bin/env python

# Run a standalone AL service

import logging
import os

from assemblyline.common.importing import load_module_by_path
from common import log as al_log
from common.task import Task

from svc_client import Client
from common.mock_modules import modules1, modules2
modules1()
modules2()


def run_service(svc_client, svc_class):

    service = svc_class(cfg=None)
    service.start_service()

    try:
        while True:
            task = Task(svc_client.task.get_task(service_name=service.service_name(),
                                                 service_version=service.get_service_version(),
                                                 service_tool_version=service.get_tool_version(),
                                                 file_required=service.SERVICE_FILE_REQUIRED,
                                                 path=service.working_directory))
            service._handle_task(task)
    finally:
        service.stop_service()


def main():
    svc_api_host = os.environ['SERVICE_API_HOST']
    name = os.environ['SERVICE_PATH']

    svc_name = name.split(".")[-1].lower()
    al_log.init_logging(log_level=logging.INFO)
    log = logging.getLogger('assemblyline.svc.%s' % svc_name)

    svc_client = Client(svc_api_host)

    try:
        svc_class = load_module_by_path(name)
    except:
        log.error('Could not find service in path.')
        raise

    run_service(svc_client, svc_class)


if __name__ == '__main__':
    main()
