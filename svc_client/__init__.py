
import re
import requests
import sys
import time
import pickle
import cgi
import os
import logging

from json import dumps, loads
from base64 import b64decode

from common import log

__all__ = ['Client', 'ClientError']

try:
    # noinspection PyUnresolvedReferences,PyUnboundLocalVariable
    basestring
except NameError:
    # noinspection PyShadowingBuiltins
    basestring = str  # pylint: disable=W0622

try:
    from urllib2 import quote
except ImportError:
    # noinspection PyUnresolvedReferences
    from urllib.parse import quote  # pylint: disable=E0611,F0401

INVALID_STREAM_SEARCH_PARAMS = ('cursorMark', 'rows', 'sort')
RETRIES = 10
SEARCHABLE = ('alert', 'file', 'result', 'signature', 'submission')
SUPPORTED_API = 'v1'


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


# noinspection PyProtectedMember
def _kw(*ex):
    local_frames = sys._getframe().f_back.f_locals  # pylint: disable=W0212
    return {
        k: _bool_to_param_string(v) for k, v in local_frames.items() if k not in ex
    }


# Calculate the API path using the class and method names as shown below:
#
#     /api/v1/<class_name>/<method_name>/[arg1/[arg2/[...]]][?k1=v1[...]]
#
# noinspection PyProtectedMember
def _magic_path(obj, *args, **kw):
    c = obj.__class__.__name__.lower()
    m = sys._getframe().f_back.f_code.co_name  # pylint:disable=W0212

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
        if isinstance(output, basestring):
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
    def __init__(  # pylint: disable=R0913
        self, server, debug=lambda x: None,
        headers=None, retries=RETRIES
    ):
        self._connection = Connection(
            server, debug, headers, retries
        )

        self.help = Help(self._connection)
        self.identify = Identify(self._connection)
        self.log = Log(self._connection)
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
        self, server, debug, headers, retries
    ):
        log.init_logging(log_level=logging.INFO)
        self.debug = debug
        self.max_retries = retries
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
            raise ClientError("Supported API (%s) not available" % s, 0)

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
                self.log.warning("No connection to service server, retying...")
                if retries < self.max_retries:
                    time.sleep(retries)
                else:
                    time.sleep(self.max_retries)
                retries += 1


class Help(object):
    def __init__(self, connection):
        self._connection = connection

    def get_classification_definition(self):
        return self._connection.get(_path('help/classification_definition'))

    def get_systems_constants(self):
        ret = self._connection.get(_path('help/constants'))

        # Decode nested list into list of tuples for StringTable
        new_dict = {}
        stringtables = ["FILE_SUMMARY", "STANDARD_TAG_CONTEXTS", "STANDARD_TAG_TYPES"]
        for x in ret:
            if x in stringtables:
                temp_list = []
                for y in ret[x]:
                    temp_list.append((str(y[0]), int(y[1])))
                new_dict[x] = temp_list

        new_dict['RECOGNIZED_TAGS'] = ret['RECOGNIZED_TAGS']
        self.RECOGNIZED_TAGS = ret['RECOGNIZED_TAGS']
        self.RULE_PATH = ret['RULE_PATH']
        self.STANDARD_TAG_TYPES = new_dict['STANDARD_TAG_TYPES']
        self.FILE_SUMMARY = new_dict['FILE_SUMMARY']
        self.STANDARD_TAG_CONTEXTS = new_dict['STANDARD_TAG_CONTEXTS']

        return self

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


class Log(object):
    def __init__(self, connection):
        self._connection = connection

    def __call__(self, log=None):
        if log is None:
            raise ClientError('You need to provide the name of the logger.', 400)

        self.log = log
        return self

    def debug(self, msg):
        request = {
            'log': self.log,
            'msg': msg
        }
        return self._connection.post(_path('log/debug'), data=dumps(request))

    def error(self, msg):  # TODO: log with stacktrace and also do another api for terminal error
        request = {
            'log': self.log,
            'msg': msg
        }
        return self._connection.post(_path('log/error'), data=dumps(request))

    def info(self, msg, params):
        msg = msg.replace("%s", params)
        request = {
            'log': self.log,
            'msg': msg
        }

        return self._connection.post(_path('log/info'), data=dumps(request))

    def warning(self, msg):
        request = {
            'log': self.log,
            'msg': msg
        }
        return self._connection.post(_path('log/warning'), data=dumps(request))


class Task(object):
    def __init__(self, connection):
        self._connection = connection

    def get_task(self, service_name, service_version, service_tool_version, file_required, path=None):
        request = {
            'service_name': service_name,
            'service_version': service_version,
            'service_tool_version': service_tool_version,
            'file_required': file_required
        }

        multipart_data = self._connection.download_multipart(_path('task/get'), data=dumps(request))
        task_json_data = None
        for part in multipart_data.parts:
            value, params = cgi.parse_header(part.headers['Content-Disposition'])
            if params['name'] == 'task_json':
                task_json_data = part.content
            else:
                if file_required:
                    file_path = os.path.join(path, params['filename'])
                    with open(file_path, 'wb') as file:
                        file.write(part.content)
                        file.close()

        if task_json_data:
            return loads(task_json_data)

    def done_task(self, task, result):
        fields = {}

        # Add the extracted files
        for file in result['response']['extracted']:
            fields[file['sha256']] = (file['sha256'], open(file['path']), 'plain/txt')
            del file['path']

        # Add the supplementary files
        for file in result['response']['supplementary']:
            fields[file['sha256']] = (file['sha256'], open(file['path']), 'plain/txt')
            del file['path']

        # Add the task JSON
        task_json = dumps(task)
        fields['task_json'] = ('task.json', task_json, 'application/json')

        # Add the results JSON
        result_json = dumps(result)
        fields['result_json'] = ('result.json', result_json, 'application/json')

        from requests_toolbelt.multipart.encoder import MultipartEncoder
        data = MultipartEncoder(fields=fields)
        headers = {'content-type': data.content_type}

        path = _path('task/done')
        url = '/'.join((self._connection.server, path))
        r = requests.post(url, data=data, headers=headers)

        return r.json()['api_response']

    def get_file(self, sha256, output=None):
        path = _path('task/file', sha256)

        if output:
            return self._connection.download(path, _stream(output))
        return self._connection.download(path, _raw)
