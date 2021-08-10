#!/usr/bin/env python3

# if the testoutput directory isn't clear, archive it and clear it

import sys,os,re,logging,time,shutil,datetime
from optparse import OptionParser

def cmd(cmd):
    fd = os.popen(cmd)
    retval = fd.read()
    fd.close()
    return retval

if __name__ == "__main__":

    # parse two command line options
    parser = OptionParser()
    parser.add_option("-o", "--outputpath",
                      action="store", type="string", dest="outputpath",
                      help="path to JUnit results")
    parser.add_option("-a", "--archivepath",
                      action="store", type="string", dest="archivepath",
                      help="path to folder holding junit archive")
    (options, args) = parser.parse_args()

    outdir = os.path.abspath(options.outputpath)
    now = datetime.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
    filename = "archive-%s.tgz" % (now)
    archive = os.path.join(options.archivepath, filename)
    if len(os.listdir(outdir)) > 0:
        # archive the contents of testout
        cmd("tar -czf %s %s" % (archive, outdir))
        shutil.rmtree(outdir)
        os.makedirs(outdir)
    if len(os.listdir(outdir)) > 0:
        sys.exit(-1)
