import HTTPListener
import os

def get_error_log():
    # if G.LOG_FILE_NAME is not '':
    #     outfilename = os.path.join(HTTPListener.Global.PATH,
    #         ('voltserver.output.%s.%u') % (G.OUTFILE_TIME, G.OUTFILE_COUNTER))

    outfilename = os.path.join(HTTPListener.Global.PATH, get_most_recent_log())
    try:
        rfile = open(outfilename, 'r')
        read_it = rfile.read()
        myLine = ""
        for line in read_it.splitlines():
            if "ERROR:" in line:
                myLine = line
                break

        return myLine
    except:
        return "error"
    pass


def get_most_recent_log():
    logdir = HTTPListener.Global.PATH # path to your log directory

    logfiles = sorted([ f for f in os.listdir(logdir) if f.startswith('voltserver.output')])

    return logfiles[-1]