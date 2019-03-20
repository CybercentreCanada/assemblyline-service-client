import inspect
import os
import sys
import tempfile
import time
import shutil
import json

from assemblyline.common import net
from assemblyline.common.classification import Classification
from assemblyline.common import digests
from assemblyline.common import version
from common.result import Result
# from assemblyline.remote.datatypes.hash import ExpiringHash
# from assemblyline.remote.datatypes.counters import Counters
from svc_client import Client

svc_api_host = os.environ['SERVICE_API_HOST']
svc_client = Client(svc_api_host)

# config = forge.get_config()
# config = svc_client.help.get_system_configuration()

Classification = Classification(svc_client.help.get_classification_definition())


class UpdaterFrequency(object):
    MINUTE = 60
    QUARTER_HOUR = MINUTE * 15
    HALF_HOUR = MINUTE * 30
    HOUR = MINUTE * 60
    QUAD_HOUR = HOUR * 4
    QUARTER_DAY = HOUR * 6
    HALF_DAY = HOUR * 12
    DAY = HOUR * 24

    @staticmethod
    def is_valid(freq):
        try:
            int(freq)
        except ValueError:
            return False

        return freq >= UpdaterFrequency.MINUTE


class UpdaterType(object):
    BOX = 'box'
    CLUSTER = 'cluster'
    PROCESS = 'process'
    NON_BLOCKING = 'non_blocking'

    @staticmethod
    def is_valid(utype):
        return utype in [UpdaterType.BOX, UpdaterType.CLUSTER, UpdaterType.PROCESS, UpdaterType.NON_BLOCKING]

    @staticmethod
    def blocking_types():
        return [
            UpdaterType.BOX,
            UpdaterType.CLUSTER,
            UpdaterType.PROCESS
        ]

    @staticmethod
    def unique_updater_types():
        return [
            UpdaterType.BOX,
            UpdaterType.CLUSTER
        ]


class ServiceBase(object):

    # If a service indicates it is a BATCH_SERVICE, the driver will attempt to
    # spool multiple requests before invoking the services execute() method.
    BATCH_SERVICE = False

    SERVICE_ACCEPTS = '.*'
    SERVICE_REJECTS = 'empty|metadata/.*'

    SERVICE_CATEGORY = 'Uncategorized'
    SERVICE_CLASSIFICATION = Classification.UNRESTRICTED
    # The default cfg used when an instance of this service is created.
    # Override this in your subclass with sane defaults for your service.
    # Default service config is a key/value where the value can be str, bool, int or list. Nothing else
    SERVICE_DEFAULT_CONFIG = {}
    # The default submission parameters that will be made available to the users when they submit files to your service.
    # Override this in your subclass with sane defaults for your service.
    # Default submission params list of dictionary. Dictionaries must have 4 keys (default, name, type, value) where
    # default is the default value, name is the name of the variable, type is the type of data (str, bool, int or list)
    #  and value should be set to the same as default.
    SERVICE_DEFAULT_SUBMISSION_PARAMS = []
    SERVICE_DESCRIPTION = "N/A"
    SERVICE_DISABLE_CACHE = False
    SERVICE_ENABLED = False
    SERVICE_FILE_REQUIRED = True
    SERVICE_LICENCE_COUNT = 0
    SERVICE_REVISION = '0'
    SERVICE_SAVE_RESULT = True
    SERVICE_SAFE_START = False
    SERVICE_STAGE = 'CORE'
    SERVICE_SUPPORTED_PLATFORMS = ['Linux']
    SERVICE_TIMEOUT = 10  # TODO: get value from Task
    SERVICE_VERSION = '0'
    SERVICE_IS_EXTERNAL = False

    SERVICE_CPU_CORES = 1
    SERVICE_RAM_MB = 1024

    def __init__(self, cfg=None):
        # Start with default config and override that with anything provided.
        self.cfg = self.SERVICE_DEFAULT_CONFIG.copy()
        print(self.cfg)
        if cfg:
            self.cfg.update(cfg)

        # Initialize non trivial members in start_service rather than __init__.
        self.log = svc_client.log(log='assemblyline.svc.%s' % self.service_name().lower())
        self.counters = None
        self.dispatch_queue = None
        self.result_store = None
        self.submit_client = None
        self.transport = None
        self.worker = None
        self._working_directory = None
        self._ip = '127.0.0.1'
        self.mac = net.get_mac_for_ip(net.get_hostip())
        self._updater = None
        self._updater_id = None
        self.submission_tags = {}

    def start(self):
        """
        Called at worker start.

        :return:
        """
        pass

    def stop(self):
        pass

    @classmethod
    def service_name(cls):
        return cls.__name__

    def get_tool_version(self):
        return ''

    @classmethod
    def get_service_version(cls):
        t = (
            version.SYSTEM_VERSION,
            version.FRAMEWORK_VERSION,
            cls.SERVICE_VERSION,
            cls.SERVICE_REVISION,
        )
        return '.'.join([str(v) for v in t])

    @staticmethod
    def parse_revision(revision):
        try:
            abs_path = os.path.abspath((inspect.stack()[1])[1])
            directory_of_1py = os.path.dirname(abs_path)
            return git_repo_revision(directory_of_1py)[:7]
        except Exception:
            print("Determining registration by fallback ")
            pass

        try:
            return revision.strip('$').split(':')[1].strip()[:7]
        except:
            return '0'

    def start_service(self):
        # Start this service. Common service start is performed and then
        # the derived services start() is invoked.
        # Services should perform any pre-fork (once per celery app) init
        # in the constructor. Any init/config that is not fork-safe or is
        # otherwise subprocess specific should be done here.

        try:
            self._ip = net.get_hostip()
        except:
            pass
        # self.counters = Counters()
        # self.counters['name'] = self.service_name()
        # self.counters['type'] = "service"
        # self.counters['host'] = self._ip
        # self.transport = forge.get_filestore()
        # self.result_store = forge.get_datastore()
        # self.submit_client = forge.get_submit_client(self.result_store)
        # self.dispatch_queue = forge.get_dispatch_queue()
        self.log.info('Service Starting: %s', self.service_name())

        # Tell the service to do its service specific imports.
        # We pop the CWD from the module search path to avoid
        # namespace collisions.
        cwd_save = sys.path.pop(0)
        self.import_service_deps()
        sys.path.insert(0, cwd_save)

        self.start()
        if self.SERVICE_SAFE_START:
            NamedQueue('safe-start-%s' % self.mac).push('up')

    def import_service_deps(self):
        """
        Do non-standard service specific imports.

        :return:
        """
        pass

    def _handle_task(self, task):
        # self._block_for_updater()
        # self.counters[EXECUTE_START] += 1
        # task_age = self.get_task_age(task)
        # self.log.info('Start: %s/%s (%s)[p%s] AGE:%s', task.sid, task.srl, task.tag, task.priority, task_age)
        task.watermark(self.service_name(), self.get_service_version(), self.get_tool_version())
        task.save_result_flag = self.SERVICE_SAVE_RESULT

        # if task.profile:
        #     task.set_debug_info("serviced_on:%s" % self._ip)
        # self._send_dispatcher_ack(task)

        try:
            start_time = time.time()
            # First try to fetch from cache. If that misses,
            # run the service execute to get a fresh result.
            # if not self._lookup_result_in_cache(task):

            # task.clear_extracted()
            # task.clear_supplementary()
            # Pass it to the service for processing. Wrap it in ServiceRequest
            # facade so service writers don't see a request interface with 80 members.
            request = ServiceRequest(self, task)
            # Collect submission_tags
            # if task.is_initial():
            #     self.submission_tags = {}
            # else:
                # self.submission_tags = ExpiringHash(task.get_submission_tags_name()).items()
            # task.save_result_flag = False
            # TODO: remove this line from here, just for testing

            old_result = self.execute(request)
            if old_result:
                self.log.warning("Service %s is using old convention "
                                 "returning result instead of setting "
                                 "it in request", self.service_name())
                task.result = old_result
            elif task.save_result_flag and not task.result:
                self.log.info("Service %s supplied NO result at all. Creating empty result for the service...",
                              self.service_name())
                task.result = Result()

            task.milestones = {'service_started': start_time, 'service_completed': time.time()}
            if task.save_result_flag:
                task.result.finalize()
            self._success(task)
            # self._log_completion_record(task, (time.time() - start_time))
        except Exception as ex:
            print(ex)
            self._handle_execute_failure(task, ex, exceptions.get_stacktrace_info(ex))
            if not isinstance(ex, exceptions.RecoverableError):
                self.log.exception("While processing task: %s/%s", task.sid, task.srl)
                raise
            else:
                self.log.info("While processing task: %s/%s", task.sid, task.srl)
        finally:
            # self._send_dispatcher_response(task)
            self._cleanup_working_directory()
            # self.counters[EXECUTE_DONE] += 1

    def _save_result(self, task):
        # if task.from_cache:
        #     return task.from_cache

        if not task.save_result_flag:
            return None

        res = svc_client.task.done_task(task=task.original_task, result=task.as_service_result())
        print("saving result="+res)

    def _cleanup_working_directory(self):
        try:
            if self._working_directory:
                shutil.rmtree(self._working_directory)
        except:
            svc_client.log.warning('Could not remove working directory: %s', self._working_directory)
            pass
        self._working_directory = None

    def stop_service(self):
        # Perform common stop routines and then invoke the child's stop().
        self.log.info('Service Stopping: %s', self.service_name())
        self.stop()
        # self._cleanup_working_directory()

    @staticmethod
    def get_task_age(task):
        received = 0
        try:
            received = task.request.get('sent')
        except:
            pass
        if not received:
            return 0
        return time.time() - received

    def _success(self, task):
        if task.result:
            tags = task.result.get('tags', None) or []
            # if tags:
            #     ExpiringSet(task.get_tag_set_name()).add(*tags)

        # self._ensure_size_constraints(task)

        task.success()
        cache_key = self._save_result(task)

        task.cache_key = cache_key

    def execute(self, request):
        # type: (ServiceRequest) -> None
        raise NotImplementedError('execute() not implemented.')

    @property
    def working_directory(self):
        pid = os.getpid()
        al_temp_dir = os.path.join(tempfile.gettempdir(), 'al', 'received', self.service_name(), str(pid))
        if not os.path.isdir(al_temp_dir):
            os.makedirs(al_temp_dir)
        if self._working_directory is None:
            self._working_directory = tempfile.mkdtemp(dir=al_temp_dir)
        return self._working_directory


class ServiceRequest(object):
    def __init__(self, service, task):
        # type: (ServiceBase, Task) -> None
        # self.srl = task.srl
        # self.sid = task.sid
        # self.config = task.config
        self.tag = task.tag
        # self.md5 = task.md5
        # self.sha1 = task.sha1
        self.sha256 = task.sha256
        # self.priority = task.priority
        # self.ignore_filtering = task.ignore_filtering
        self.task = task
        # self.local_path = ''
        # self.successful = True
        # self.error_is_recoverable = True
        # self.error_text = None
        # self.current_score = task.max_score
        self.deep_scan = task.deep_scan
        self.extracted = task.extracted
        self.max_extracted = task.max_extracted
        self.path = task.sha256  # TODO: self.path = task.path or task.sha256

        self._svc = service

    @property
    def result(self):
        # type: () -> Result
        return self.task.result

    @result.setter
    def result(self, value):
        self.task.result = value

    def add_extracted(self, name, text, display_name=None, classification=None, submission_tag=None, sha256=None):
        """
        Add an extracted file for additional processing.

        :param name: Path to file to attach
        :param text: Descriptive text about the file
        :param display_name: Optional display text
        :param classification: The classification of this extracted file. Defaults to current classification.
        :param submission_tag:
        :param sha256: Hash of the file
        :return: None
        """
        if not sha256:
            sha256 = digests.get_sha256_for_file(name)

        return self.task.add_extracted(
            name, text, display_name, classification or self._svc.SERVICE_CLASSIFICATION, sha256
        )

    def add_supplementary(self, name, text, display_name=None, classification=None, sha256=None):
        if not sha256:
            sha256 = digests.get_sha256_for_file(name)

        return self.task.add_supplementary(
            name, text, display_name, classification or self._svc.SERVICE_CLASSIFICATION, sha256
        )

    def download(self):
        sha256 = self.sha256
        file_path = os.path.join(self._svc.working_directory, sha256)
        if not os.path.exists(file_path):
            print('File not found locally, downloading again')
            svc_client.task.get_file(sha256=sha256, output=sha256)
        if not os.path.exists(file_path):
            raise Exception('Download failed. Not found on local filesystem')

        received_sha256 = digests.get_sha256_for_file(file_path)
        if received_sha256 != sha256:
            raise Exception('SHA256 mismatch between requested and downloaded file. %s != %s' % (sha256, received_sha256))
        return file_path

    def tempfile(self, sha256):
        return os.path.join(self._svc.working_directory, sha256)
