
from common.properties import classproperty
from assemblyline.common import net
from svc_client import Client

# TODO: Client server & login details
svc_client = Client("http://assemblyline-internal")

# config = forge.get_config()
config = svc_client.help.get_system_configuration()

# Classification = forge.get_classification()
Classification = svc_client.help.get_classification_definition()


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
    SERVICE_LICENCE_COUNT = 0
    SERVICE_REVISION = '0'
    SERVICE_SAVE_RESULT = True
    SERVICE_SAFE_START = False
    SERVICE_STAGE = 'CORE'
    SERVICE_SUPPORTED_PLATFORMS = ['Linux']
    SERVICE_TIMEOUT = config.services.timeouts.default
    SERVICE_VERSION = '0'
    SERVICE_IS_EXTERNAL = False

    SERVICE_CPU_CORES = 1
    SERVICE_RAM_MB = 1024

    def __init__(self, cfg=None):
        # Start with default config and override that with anything provided.
        self.cfg = self.SERVICE_DEFAULT_CONFIG.copy()
        if cfg:
            self.cfg.update(cfg)

        # Initialize non trivial members in start_service rather than __init__.
        # self.log = logging.getLogger('assemblyline.svc.%s' % self.SERVICE_NAME.lower())
        self.log = svc_client.log(log='assemblyline.svc.%s' % self.SERVICE_NAME.lower())
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

    @classproperty
    @classmethod
    def SERVICE_NAME(cls):
        return cls.__name__

