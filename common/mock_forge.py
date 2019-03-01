from svc_client import Client

svc_client = Client("http://0.0.0.0:5000")


def get_classification():
    from assemblyline.common.classification import Classification
    return Classification(svc_client.help.get_classification_definition())


def get_constants():
    return svc_client.help.get_systems_constants()
