import os

from svc_client import Client

svc_api_host = os.environ['SERVICE_API_HOST']
svc_client = Client(svc_api_host)


def get_classification():
    from assemblyline.common.classification import Classification
    return Classification(svc_client.help.get_classification_definition())


def get_constants():
    return svc_client.help.get_systems_constants()
