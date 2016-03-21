import HTTPListener
import os


def get_most_recent_log():
    logdir = HTTPListener.Global.CONFIG_PATH

    logfiles = sorted([ f for f in os.listdir(logdir) if f.startswith('voltserver.output')])

    return logfiles[-1]


def get_error_log_details():
    outfilename = os.path.join(HTTPListener.Global.CONFIG_PATH, get_most_recent_log())
    try:
        rfile = open(outfilename, 'r')
        error = rfile.read()
        return error
    except:
        return "error"
