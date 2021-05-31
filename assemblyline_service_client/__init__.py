import hashlib
import logging
import os
import pickle
import sys
import tempfile
import time
from base64 import b64decode
from json import dumps
from urllib.parse import quote

import requests

from assemblyline.common import log

__all__ = ['Client', 'ClientError']
__version__ = "4.0.0.dev0"
_package_version_path = os.path.join(os.path.dirname(__file__), 'VERSION')
if os.path.exists(_package_version_path):
    with open(_package_version_path) as _package_version_file:
        __version__ = _package_version_file.read().strip()

MAX_RETRY_BACKOFF = 10
SUPPORTED_API = 'v1'

log.init_logging('assemblyline.service_client')


def as_python_object(dct):
    if '_python_object' in dct:
        return pickle.loads(b64decode(dct['_python_object'].encode('utf-8')))
    return dct


def _bool_to_param_string(b):
    if not isinstance(b, bool):
        return b
    return {True: 'true', False: 'false'}[b]


def _convert(response):
    return response.json()['api_response']


def _decode_multipart(response):
    from requests_toolbelt.multipart import decoder
    multipart_data = decoder.MultipartDecoder.from_response(response)
    return multipart_data


def _encode_multipart(fields):
    from requests_toolbelt.multipart.encoder import MultipartEncoder
    multipart_data = MultipartEncoder(fields=fields)
    return multipart_data


def _join_param(k, v):
    return '='.join((k, quote(str(v))))


def _join_kw(kw):
    return '&'.join([
        _join_param(k, v) for k, v in kw.items() if v is not None
    ])


def _join_params(q, params):
    return '&'.join([quote(q)] + [_join_param(*e) for e in params if _param_ok(e)])


def _kw(*ex):
    local_frames = sys._getframe().f_back.f_locals
    return {
        k: _bool_to_param_string(v) for k, v in local_frames.items() if k not in ex
    }


# Calculate the API path using the class and method names as shown below:
#
#     /api/v1/<class_name>/<method_name>/[arg1/[arg2/[...]]][?k1=v1[...]]
#
def _magic_path(obj, *args, **kw):
    c = obj.__class__.__name__.lower()
    m = sys._getframe().f_back.f_code.co_name

    return _path('/'.join((c, m)), *args, **kw)


def _param_ok(k):
    return k not in ('q', 'df', 'wt')


# Calculate the API path using the prefix as shown:
#
#     /api/v1/<prefix>/[arg1/[arg2/[...]]][?k1=v1[...]]
#
def _path(prefix, *args, **kw):
    path = '/'.join(['api', SUPPORTED_API, prefix] + list(args) + [''])

    params = _join_kw(kw)
    if not params:
        return path

    return '?'.join((path, params))


def _raw(response):
    return response.content


def _stream(output):
    def _do_stream(response):
        f = output
        if isinstance(output, str):
            f = open(output, 'wb')
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
        if f != output:
            f.close()
    return _do_stream


class Client(object):
    def __init__(
        self, server, debug=lambda x: None,
        headers=None, retry_backoff=MAX_RETRY_BACKOFF
    ):
        self._connection = Connection(
            server, debug, headers, retry_backoff
        )

        self.file = File(self._connection)
        self.help = Help(self._connection)


class ClientError(Exception):
    def __init__(self, message, status_code):
        super(ClientError, self).__init__(message)
        self.status_code = status_code


class Connection(object):
    def __init__(
        self, server, debug, headers, retry_backoff
    ):
        self.debug = debug
        self.retry_backoff = retry_backoff
        self.server = server
        self.log = logging.getLogger('assemblyline.service_client')

        session = requests.Session()
        session.headers.update({'content-type': 'application/json'})

        if headers:
            session.headers.update(headers)

        self.session = session

        r = self.request(self.session.get, 'api/', _convert)
        s = {SUPPORTED_API}
        if not isinstance(r, list) or not set(r).intersection(s):
            raise ClientError(f"Supported API ({s}) not available", 0)

    def delete(self, path, **kw):
        return self.request(self.session.delete, path, _convert, **kw)

    def download(self, path, process, **kw):
        return self.request(self.session.get, path, process, **kw)

    def download_multipart(self, path, **kw):
        return self.request(self.session.get, path, _decode_multipart, **kw)

    def get(self, path, **kw):
        return self.request(self.session.get, path, _convert, **kw)

    def post(self, path, **kw):
        return self.request(self.session.post, path, _convert, **kw)

    def post_multipart(self, path, **kw):
        return self.request(self.session.post, path, _convert, **kw)

    def request(self, func, path, process, **kw):
        self.debug(path)
        retries = 0
        while True:
            try:
                response = func('/'.join((self.server, path)), **kw)
                if response.ok:
                    return process(response)
            except requests.RequestException:
                self.log.warning("No connection to service server, retrying...")
                if retries < self.retry_backoff:
                    time.sleep(retries)
                else:
                    time.sleep(self.retry_backoff)
                retries += 1


class File(object):
    def __init__(self, connection):
        self._connection = connection
        self.log = logging.getLogger('assemblyline.service_client')

    def download_file(self, sha256, file_path):
        path = _path('file/download', sha256)
        multipart_data = self._connection.download_multipart(path)

        with open(file_path, 'wb') as f:
            f.write(multipart_data.parts[0].content)
            f.close()

    def save_file(self, task, result):
        task_hash = hashlib.md5((str(task['sid'] + task['fileinfo']['sha256']).encode('utf-8'))).hexdigest()
        folder_path = os.path.join(tempfile.gettempdir(), task['service_name'].lower(), 'completed', task_hash,)

        fields = {}

        # Add the extracted and supplementary files to the response
        for file in result['response']['extracted'] + result['response']['supplementary']:
            file_path = os.path.join(folder_path, file['name'])
            with open(file_path, 'rb') as f:
                fields[file['sha256']] = (file['sha256'], f.read(), 'application/octet-stream')

        # Add the task and result JSON to the response
        fields['task_json'] = ('task.json', dumps(task), 'application/json')
        fields['result_json'] = ('result.json', dumps(result), 'application/json')

        from requests_toolbelt.multipart.encoder import MultipartEncoder
        data = MultipartEncoder(fields=fields)
        headers = {'content-type': data.content_type}

        url = '/'.join((self._connection.server, _path('file/save')))
        r = requests.post(url, data=data, headers=headers)

        return r.json()['api_response']


class Help(object):
    def __init__(self, connection):
        self._connection = connection

    def get_classification_definition(self):
        return self._connection.get(_path('help/classification_definition'))

    def get_systems_constants(self):
        ret = self._connection.get(_path('help/constants'))

        # Decode nested list into list of tuples for StringTable
        temp = {}
        stringtables = ["FILE_SUMMARY", "STANDARD_TAG_CONTEXTS", "STANDARD_TAG_TYPES"]
        for x in ret:
            if x in stringtables:
                temp_list = []
                for y in ret[x]:
                    temp_list.append((str(y[0]), int(y[1])))
                temp[x] = temp_list

        temp['RECOGNIZED_TYPES'] = ret['RECOGNIZED_TYPES']
        constants = {'RECOGNIZED_TYPES': ret['RECOGNIZED_TYPES'],
                     'RULE_PATH': ret['RULE_PATH'],
                     'STANDARD_TAG_TYPES': temp['STANDARD_TAG_TYPES'],
                     'FILE_SUMMARY': temp['FILE_SUMMARY'],
                     'STANDARD_TAG_CONTEXTS': temp['STANDARD_TAG_CONTEXTS']
                     }

        return constants

    def get_system_configuration(self, static=False):
        request = {
            'static': static
        }
        return self._connection.get(_path('help/configuration'), data=dumps(request))
