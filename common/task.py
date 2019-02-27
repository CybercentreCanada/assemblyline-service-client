#!/usr/bin/env python


class Task(object):
    """Task objects are an abstraction layer over a task (dict) received from the dispatcher"""

    def __init__(self, task):
        self.original_task = task
        self.classification = 'unrestricted'
        self.extracted = []
        self.milestones = {}
        self.response = {}
        self.result = {}
        self.score = 0
        self.service_name = ''
        self.service_version = ''
        self.sha1 = task['fileinfo']['sha1']
        self.sha256 = task['fileinfo']['sha256']
        self.sid = task['sid']
        self.supplementary = []
        self.tag = task['fileinfo']['tag']

    def as_service_result(self):
        if not self.extracted:
            self.extracted = []
        if not self.supplementary:
            self.supplementary = []
        # if not t.message:
        #     t.message = ''
        if not self.result:
            self.result = []
        if not self.response:
            self.get_response()

        result = {'classification': self.classification,
                  'response': self.response,
                  'result': self.result,
                  'sha256': self.sha256
                  }
        return result

    def get_response(self):
        self.response = {'milestones': self.milestones,
                         'service_name': self.service_name,
                         'service_version': self.service_version
                         }
        return self.response

    def set_milestone(self, name, value):
        if not self.milestones:
            self.milestones = {}
        self.milestones[name] = value

    def success(self):
        # TODO: self.score not used for anything right now
        self.score = 0
        if self.result:
            try:
                self.score = int(self.result.get('score', 0))
            except:
                self.score = 0

    def watermark(self, service_name, service_version):
        self.service_name = service_name
        self.service_version = service_version
