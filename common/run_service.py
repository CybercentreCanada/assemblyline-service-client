#!/usr/bin/env python

# Run a standalone AL service

import logging
import sys
import os

from assemblyline.common.importing import load_module_by_path
from common.task import Task

from svc_client import Client
from common.mock_modules import modules1, modules2
modules1()
modules2()

logger = logging.getLogger('assemblyline.run_service')
logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')


def run_service(svc_client, svc_class):
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    service = svc_class(cfg=None)
    service.start_service()

    try:
        while True:
            # Get task
            task = Task(svc_client.task.get_task(service_name=service.service_name(),
                                                 service_version=service.get_service_version(),
                                                 service_tool_version=service.get_tool_version(),
                                                 file_required=service.SERVICE_FILE_REQUIRED,
                                                 path=service.working_directory))
            service._handle_task(task)
    except KeyboardInterrupt:
        logging.info("run service log")
        print("Exiting.")
    finally:
        service.stop_service()


def main():
    svc_api_host = os.environ['SERVICE_API_HOST']
    name = os.environ['SERVICE_PATH']

    svc_name = name.split(".")[-1].lower()
    logger = logging.getLogger('assemblyline.svc.%s' % svc_name)
    logger.setLevel(logging.DEBUG)

    svc_client = Client(svc_api_host)

    try:
        svc_class = load_module_by_path(name)
    except:
        raise

    run_service(svc_client, svc_class)


if __name__ == '__main__':
    main()
