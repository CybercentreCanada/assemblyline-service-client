import os

from assemblyline.common.importing import load_module_by_path
from assemblyline.common.classification import Classification
from svc_client import Client

svc_api_host = os.environ['SERVICE_API_HOST']
svc_client = Client(svc_api_host)

Classification = Classification(svc_client.help.get_classification_definition())


class InvalidClassificationException(Exception):
    pass


class Heuristic(object):
    def __init__(self, hid, name, filetype, description, classification=Classification.UNRESTRICTED):
        self.id = hid
        self.name = name
        self.filetype = filetype
        self.description = description
        self.classification = classification
        if not Classification.is_valid(classification):
            raise InvalidClassificationException()

    def __repr__(self):
        return "Heuristic('{id}', '{name}', '{filetype}', " \
               "'{description}', '{classification}')".format(id=self.id, name=self.name,
                                                             filetype=self.filetype, description=self.description,
                                                             classification=self.classification)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "filetype": self.filetype,
            "description": self.description.strip(),
            "classification": self.classification
        }


def get_heuristics_form_class(cls):
    out = []
    try:
        for c_cls in list(cls.__mro__)[:-1][::-1]:
            out.extend([v for v in c_cls.__dict__.itervalues() if isinstance(v, Heuristic) and v not in out])
    except AttributeError:
        pass

    return sorted(out, key=lambda k: k.id)


def list_all_heuristics(srv_list):
    out = []
    for srv in srv_list:
        cls_path = srv.get('classpath', None)
        if cls_path:
            try:
                cls = load_module_by_path(cls_path)
            except ImportError:
                continue
            out.extend(cls.list_heuristics())
    return out, {x['id']: x for x in out}
