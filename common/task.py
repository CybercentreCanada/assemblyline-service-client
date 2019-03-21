#!/usr/bin/env python

from easydict import EasyDict

class Task(object):
    """Task objects are an abstraction layer over a task (dict) received from the dispatcher"""

    def __init__(self, task):
        self.original_task = task
        self.classification = None
        self.deep_scan = False
        self.drop_file = False
        self.extracted = []
        self.max_extracted = 100  # TODO: get from task
        self.max_supplementary = 100  # TODO: get from task
        self.milestones = {}
        self.response = {}
        self.result = {}
        self.score = 0
        self.service_config = task['service_config']
        self.service_name = None
        self.service_version = None
        self.service_tool_version = None
        self.sha1 = task['fileinfo']['sha1']
        self.sha256 = task['fileinfo']['sha256']
        self.sid = task['sid']
        self.size = task['fileinfo']['size']
        self.supplementary = []
        self.tag = task['fileinfo']['type']

    def add_extracted(self, path, description, name, classification, sha256, normalize=lambda x: x):
        if None in (path, sha256):
            return False
        if not name:
            name = normalize(path)
        if self.extracted is None:
            self.clear_extracted()
        limit = self.max_extracted
        if limit and len(self.extracted) >= int(limit):
            return False
        if not classification:
            classification = self.classification
        self.extracted.append(self.add_child(name, sha256, description, classification, path))
        return True

    def add_supplementary(self, path, description, name, classification, sha256, normalize=lambda x: x):
        if None in (path, sha256):
            return False
        if not name:
            name = normalize(path)
        if self.supplementary is None:
            self.clear_supplementary()
        limit = self.max_supplementary
        if limit and len(self.supplementary) >= int(limit):
            return False
        if not classification:
            classification = self.classification
        self.supplementary.append(self.add_child(name, sha256, description, classification, path))
        return True

    def add_child(self, name, sha256, description, classification, path):
        return EasyDict({'display_name': name,
                         'sha256': sha256,
                         'description': description,
                         'classification': classification,
                         'path': path
                         })

    def as_service_result(self):
        if not self.extracted:
            self.extracted = []
        if not self.supplementary:
            self.supplementary = []
        if not self.result:
            self.result = []
        if not self.response:
            self.get_response()

        result = {'classification': self.classification,
                  'drop_file': self.drop_file,
                  'response': self.response,
                  'result': self.result,
                  'sha256': self.sha256
                  }
        return result

    def get_response(self):
        self.response = {'extracted': self.extracted,
                         'milestones': self.milestones,
                         'service_name': self.service_name,
                         'service_version': self.service_version,
                         'service_tool_version': self.service_tool_version or 'empty',
                         'supplementary': self.supplementary
                         }
        return self.response

    def clear_extracted(self):
        self.extracted = []

    def clear_supplementary(self):
        self.supplementary = []

    def drop(self):
        self.drop_file = True

    def set_milestone(self, name, value):
        if not self.milestones:
            self.milestones = {}
        self.milestones[name] = value

    def success(self):
        # Assign aggregate classification for the result based on max classification of tags and result sections
        self.classification = self.result['classification']
        del self.result['classification']

        for item in range(len(self.extracted)):
            self.extracted[item]['name'] = self.extracted[item].pop('display_name')

        # TODO: self.score not used for anything right now
        self.score = 0
        if self.result:
            try:
                self.score = int(self.result.get('score', 0))
            except:
                self.score = 0

    def watermark(self, service_name, service_version, service_tool_version):
        self.service_name = service_name
        self.service_version = service_version
        self.service_tool_version = service_tool_version

    def get_service_params(self, service_name):
        if not service_name or not self.service_config:
            return {}

        return self.service_config.get(service_name, {})
