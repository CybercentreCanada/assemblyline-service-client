
import re
import requests
import sys
import time
import pickle
import cgi
import os
import logging
import tempfile

from json import dumps, loads
from base64 import b64decode
from urllib.parse import quote
from easydict import EasyDict

from assemblyline.common import log

__all__ = ['Client', 'ClientError']

# INVALID_STREAM_SEARCH_PARAMS = ('cursorMark', 'rows', 'sort')
MAX_RETRY_BACKOFF = 10
# SEARCHABLE = ('alert', 'file', 'result', 'signature', 'submission')
SUPPORTED_API = 'v1'

log.init_logging(log_level=logging.INFO)


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


def _join_params(q, l):
    return '&'.join([quote(q)] + [_join_param(*e) for e in l if _param_ok(e)])


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


def _walk(obj, path, paths):
    if isinstance(obj, int):
        return
    for m in dir(obj):
        mobj = getattr(obj, m)
        if m == '__call__':
            doc = str(mobj.__doc__)
            if doc in (
                'x.__call__(...) <==> x(...)',
                'Call self as a function.'
            ):
                doc = str(obj.__doc__)
            doc = doc.split("\n\n", 1)[0]
            doc = re.sub(r'\s+', ' ', doc.strip())
            if doc != 'For internal use.':
                paths.append(['.'.join(path), doc])
            continue
        elif m.startswith('_') or m.startswith('im_'):
            continue

        _walk(mobj, path + [m], paths)


class Client(object):
    def __init__(
        self, server, debug=lambda x: None,
        headers=None, retry_backoff=MAX_RETRY_BACKOFF
    ):
        self._connection = Connection(
            server, debug, headers, retry_backoff
        )

        self.help = Help(self._connection)
        self.identify = Identify(self._connection)
        self.task = Task(self._connection)

        paths = []
        _walk(self, [''], paths)

        self.__doc__ = 'Client provides the following methods:\n\n' + \
            '\n'.join(['\n'.join(p + ['']) for p in paths])


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

        temp['RECOGNIZED_TAGS'] = ret['RECOGNIZED_TAGS']
        constants = {'RECOGNIZED_TAGS': ret['RECOGNIZED_TAGS'],
                     'RULE_PATH': ret['RULE_PATH'],
                     'STANDARD_TAG_TYPES': temp['STANDARD_TAG_TYPES'],
                     'FILE_SUMMARY': temp['FILE_SUMMARY'],
                     'STANDARD_TAG_CONTEXTS': temp['STANDARD_TAG_CONTEXTS']
                     }

        return EasyDict(constants)

    def get_system_configuration(self, static=False):
        request = {
            'static': static
        }
        return self._connection.get(_path('help/configuration'), data=dumps(request))


class Identify(object):
    def __init__(self, connection):
        self._connection = connection

    def get_fileinfo(self, path):
        request = {
            'path': str(path)
        }
        return self._connection.post(_path('identify/fileinfo'), data=dumps(request))


class Task(object):
    def __init__(self, connection):
        self._connection = connection
        self.log = logging.getLogger('assemblyline.service_client')

    def get_task(self, service_name, service_version, service_tool_version, file_required):
        self.log.info(f'Getting task for: {service_name}')

        request = {
            'service_name': service_name,
            'service_version': service_version,
            'service_tool_version': service_tool_version,
            'file_required': file_required
        }

        multipart_data = self._connection.download_multipart(_path('task/get'), data=dumps(request))

        # Load the task.json
        task = None
        for part in multipart_data.parts:
            value, params = cgi.parse_header(part.headers['Content-Disposition'])
            if params['name'] == 'task_json':
                task = loads(part.content)

        if task:
            folder_path = os.path.join(tempfile.gettempdir(), service_name.lower(), task['sid'])
            self.log.info(f"Task received for: {task['service_name']}, saving task to: {folder_path}")

            # Download the task.json and the file to be processed (if required)
            for part in multipart_data.parts:
                value, params = cgi.parse_header(part.headers['Content-Disposition'])
                file_path = os.path.join(folder_path, params['filename'])
                with open(file_path, 'wb') as file:
                    file.write(part.content)
                    file.close()

    def done_task(self, task, result):
        self.log.info(f"Task completed by: {task['service_name']}, SID: {task['sid']}")
        folder_path = os.path.join(tempfile.gettempdir(), task['service_name'].lower(), task['sid'])

        fields = {}

        # Add the extracted and supplementary files to the response
        for file in result['response']['extracted'] + result['response']['supplementary']:
            file_path = os.path.join(folder_path, file['name'])
            fields[file['sha256']] = (file['sha256'], open(file_path), 'plain/txt')

        # Add the task and result JSON to the response
        fields['task_json'] = ('task.json', dumps(task), 'application/json')
        fields['result_json'] = ('result.json', dumps(result), 'application/json')

        from requests_toolbelt.multipart.encoder import MultipartEncoder
        data = MultipartEncoder(fields=fields)
        headers = {'content-type': data.content_type}

        url = '/'.join((self._connection.server, _path('task/done')))
        r = requests.post(url, data=data, headers=headers)

        return r.json()['api_response']

    def get_file(self, sha256, output=None):
        path = _path('task/file', sha256)

        if output:
            return self._connection.download(path, _stream(output))
        return self._connection.download(path, _raw)
