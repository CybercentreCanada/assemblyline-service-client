import socket
import threading
import time
import signal


class TimeoutException(Exception):
    pass


class alarm_clock:
    """A context manager that causes an exception to be raised after a timeout."""

    def __init__(self, timeout):
        self.timeout = timeout
        self.alarm_default = signal.getsignal(signal.SIGALRM)

    def __enter__(self):
        def handler(signum, frame):
            raise TimeoutException("Timeout")

        signal.signal(signal.SIGALRM, handler)

        signal.alarm(self.timeout)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self.alarm_default)


def timeout(func, args=(), kwargs=None, timeout_duration=10, default=None):
    """Run a function in a thread with a timeout."""
    class InterruptableThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.result = default
            self.error = None

        def run(self):
            while True:
                try:
                    self.result = func(*args, **kwargs)
                    break
                except socket.error as e:
                    if e.errno == 111:
                        time.sleep(0.1)
                    else:
                        self.error = e
                        break
                except Exception as e:
                    self.error = e
                    break

    if kwargs is None:
        kwargs = {}

    it = InterruptableThread()
    it.start()
    it.join(timeout_duration)
    if it.isAlive():
        it._Thread__stop()
        raise TimeoutException()
    else:
        if it.error:
            raise BaseException(it.error)
        return it.result


# noinspection PyBroadException
class SubprocessTimer(object):
    """Create a timer that kills processes running more than timeout_value seconds."""

    def __init__(self, timeout_value, raise_on_timeout=True):
        self.timeout = timeout_value
        self.timed_out = False
        self.stime = 0
        self.proc = None
        self.stop = False
        self.raise_on_timeout = raise_on_timeout
        self.timeout_t = self._init_thread()

    def __enter__(self):
        self.timeout_t.start()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        if self.timed_out and self.raise_on_timeout:
            raise TimeoutException("%s seconds timeout reached" % self.timeout)

    def _init_thread(self):
        t = threading.Thread(target=self._check_timeout,
                             name="PROCESS_TIMEOUT_THREAD_%s_SEC" % str(self.timeout))
        t.daemon = True
        return t

    def _check_timeout(self):
        while True:
            if self.stop:
                break

            if self.proc is not None:
                if time.time() - self.stime > self.timeout:
                    self.timed_out = True
                    try:
                        self.proc.kill()
                    except:
                        pass
                    self.proc = None
                    self.stime = 0
            time.sleep(0.1)

    def close(self):
        self.stop = True

    def has_timed_out(self):
        return self.timed_out

    def run(self, running_process):
        if self.timeout_t and not self.timeout_t.isAlive():
            self.timeout_t = self._init_thread()
            self.timeout_t.start()

        self.stop = False
        self.timed_out = False
        self.stime = time.time()
        self.proc = running_process

        return running_process
