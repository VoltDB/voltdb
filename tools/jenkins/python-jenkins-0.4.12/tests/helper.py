import functools
from multiprocessing import Process
from multiprocessing import Queue
import traceback

from six.moves import socketserver


class TestsTimeoutException(Exception):
    pass


def time_limit(seconds, fp, func, *args, **kwargs):

    if fp:
        if not hasattr(fp, 'write'):
            raise TypeError("Expected 'file-like' object, got '%s'" % fp)
        else:
            def record(msg):
                fp.write(msg)
    else:
        def record(msg):
            return

    def capture_results(msg_queue, func, *args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            msg_queue.put(
                "Running function '%s' resulted in exception '%s' with "
                "message: '%s'\n" % (func.__name__, e.__class__.__name__, e))
            # no point re-raising an exception from the subprocess, instead
            # return False
            return False
        else:
            msg_queue.put(
                "Running function '%s' finished with result '%s', and"
                "stack:\n%s\n" % (func.__name__, result,
                                  traceback.format_stack()))
            return result

    messages = Queue()
    # although creating a separate process is expensive it's the only way to
    # ensure cross platform that we can cleanly terminate after timeout
    p = Process(target=functools.partial(capture_results, messages, func),
                args=args, kwargs=kwargs)
    p.start()
    p.join(seconds)
    if p.is_alive():
        p.terminate()
        while not messages.empty():
            record(messages.get())
        record("Running function '%s' did not finish\n" % func.__name__)

        raise TestsTimeoutException
    else:
        while not messages.empty():
            record(messages.get())
        record("Running function '%s' finished with exit code '%s'\n"
               % (func.__name__, p.exitcode))


class NullServer(socketserver.TCPServer):

    request_queue_size = 1

    def __init__(self, server_address, *args, **kwargs):
        # TCPServer is old style in python 2.x so cannot use
        # super() correctly, explicitly call __init__.

        # simply init'ing is sufficient to open the port, which
        # with the server not started creates a black hole server
        socketserver.TCPServer.__init__(
            self, server_address, socketserver.BaseRequestHandler,
            *args, **kwargs)
