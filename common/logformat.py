
hostname = 'unknownhost'

try:
    from assemblyline.common.net import get_hostname
    hostname = get_hostname()
except:
    pass

ip = 'x.x.x.x'

try:
    from assemblyline.common.net import get_hostip
    ip = get_hostip()
except Exception:
    pass

AL_LOG_FORMAT = '%(asctime)-16s %(levelname)8s ' + hostname + ' %(process)d %(name)30s | %(message)s'
