#!/usr/bin/env python

# A Quick and Dirty test harness for services.
#
# Usage: python ./debug_service ./input_file

import json
import logging
import os
import platform
import pprint
import sys
import time

from itertools import chain


from assemblyline.common.importing import load_module_by_path
# from assemblyline.common.str_utils translate_str
from common.task import Task
from svc_client import Client

from common.mock_modules import modules1, modules2
modules1()
modules2()


svc_api_host = os.environ['SERVICE_API_HOST']
svc_client = Client(svc_api_host)


def scan_file(svc_class):
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    # Don't use srl normalization for filenames (i.e. 1/2/3/4/1234mysha256)

    # We use mocks for dispatcher, restore store etc that will inject the results into
    # these lists.

    # dispatch_result_collector = mocks.MockDispatchCollector()
    result_store_good = {}
    result_store_bad = {}
    children = []
    supplementary = []

    # cfg = task.get('params', {})

    service = svc_class(cfg=None)

    # Create Task object
    task = Task(svc_client.task.get_task(path=service.working_directory, service_version=service.get_service_version()))

    # cfg = task.get('service', {})

    # import functools
    # forge.get_filestore = functools.partial(mocks.get_local_transport, '.')
    # forge.get_submit_client = functools.partial(mocks.get_mock_submit_client, children, supplementary)
    # forge.get_dispatch_queue = lambda: dispatch_result_collector
    # forge.get_datastore = functools.partial(
    #         mocks.get_mock_result_store,
    #         result_store_good,
    #         result_store_bad)


    service.start_service()

    # Run all inputs through the service. Children will end up in the children list,
    # results will end up in the results list. Actual fleshed out service results
    # will be in riak.
    
    start = time.time()

    if service.BATCH_SERVICE:
        service._handle_task_batch([task, ])
    else:
        service._handle_task(task)
    end = time.time()
    duration = end - start
    print('Duration: %s' % duration)
    pprint.pprint(task.as_service_result())
    # (serviced_ok,
    #  serviced_fail_recover,
    #  serviced_fail_nonrecover) = dispatch_result_collector.get_serviced_results()

    # for response in chain(serviced_ok, serviced_fail_recover, serviced_fail_nonrecover):
    #     # TODO: we should be able to find it by key in our result_store_good
    #     if 'response' in response and 'cache_key' in response['response']:
    #         if response['response']['cache_key'] not in result_store_good:
    #             print("Appear to be missing result in result store")
    #     pprint.pprint(response)

    # for (_key, full_result) in result_store_good.items():
    #     if full_result and 'result' in full_result:
    #         pprint.pprint(full_result)
    #         json.dumps(full_result, ensure_ascii=True).encode('utf-8')

    service.stop_service()


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('service_name')
    parser.add_argument('sample')
    args = parser.parse_args()

    name = args.service_name
    svc_class = load_module_by_path(name)
    
    filename = args.sample
    if not os.path.isfile(filename):
        print('Invalid input file: %s' % filename)
        exit(3)

    fi = svc_client.identify.get_fileinfo(filename)

    sha256 = fi['sha256']
    # The transport expects the filename to be the sha256.
    # Create a symlink if required.
    created_link = False
    if filename != sha256:
        try:
            if platform.system() == 'Windows':
                import shutil
                shutil.copyfile(filename, sha256)
            else:
                os.symlink(filename, sha256)
        except Exception as ex:
            print('exception trying to link file: %s' % str(ex))
        created_link = True

    scan_file(svc_class)

    if created_link:
        os.unlink(sha256)


if __name__ == '__main__':
    main()
