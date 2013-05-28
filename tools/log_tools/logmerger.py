#!/usr/bin/env python

#TODO: the exclude file is hardwired - take as an input and use checked in
#      version as default
#TODO: get apprunner output from a URL
#TODO: other file formats (default is server log)
#           - messages
#TODO: Find out where the CRLFs went to - some (but not all) are missing



import copy
from datetime import datetime
import heapq
from optparse import OptionParser
import os.path
import re
import sys
import tarfile
import time

#Known timestamp formats
# apprunner           04-18 07:14:35
# dragent             2013-03-14 00:00:33
# servers             2013-03-14 00:00:33,877

def epochtimemillis_keyfunc(ts):
    """Return ms since epoch for timestamp of format yyyy-mm-dd hh:mm:ss[,xxx]
    """
    split_ts = ts.split(',')
    seconds = time.strptime(split_ts[0],"%Y-%m-%d %H:%M:%S")
    if len(split_ts) > 1:
        millis = int(split_ts[1])
    else:
        millis = 0

    return int(time.mktime(seconds) * 1000) + int(millis)

def decorated_log_split(f, offset, keyfunc, fnamedict = {}):
    """ Generator that splits on timestamps. This returns
    (millis-since-epoch, {"datetime":<original timestamp> ,
                          "newdatetime":<offset timestamp>,
                          "message": <log message>
                          "filename":<filename>})
    """
    ts_format = '\d{0,4}-?\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},?\d{0,3}'

    log_re = re.compile(r'''(?P<datetime>%s)
                             \s+
                             (?P<message>.*)
                             ''' % ts_format, re.VERBOSE)
    if hasattr(f, 'name'):
        fname = os.path.basename(f.name)
    else:
        fname = ''
    currentdict = {}
    for line in f:
        m = log_re.match(line)
        if m:
            #When we match, spit back the accumulated currentdict
            if currentdict:
                yield (keyfunc(currentdict["newdatetime"]), currentdict)
            #Then re-initialize currentdict with the new match
            currentdict = copy.deepcopy(m.groupdict())
            currentdict["filename"] = fname
            #Add a year if none exists (like in apprunner.log)
            #Note to self: make apprunner log the year.
            if currentdict["datetime"][4] != '-':
                currentdict["datetime"] = str(datetime.now().year) + \
                    '-' + currentdict["datetime"]

            new_epoch_ms = keyfunc(currentdict["datetime"]) + offset
            currentdict["newdatetime"] = time_str = "%s,%03d" % \
                (time.strftime("%Y-%m-%d %H:%M:%S",
                               time.localtime((new_epoch_ms / 1000))),
                 (new_epoch_ms % 1000))

        else:
            if "message" in currentdict:
                currentdict["message"] += line
    yield (new_epoch_ms, currentdict)

def tz_offset_callback(option, opt_str, value, parser):
    """ check to see the tz_offset looks like [+-]nn:nn and then store it
    as the delta milliseconds"""

    offset_re = re.compile('(\+?-?\d{1,2}):(\d{2})')
    m = offset_re.match(value)
    if m:
        offset_ms = int(m.groups()[0]) * 60 * 60 * 1000 + int(m.groups()[1]) * 60 * 1000
        setattr(parser.values, option.dest, str(offset_ms))
    else:
        raise OptionValueError(value + " is not a valid timezone offset")


def merge_logs(files, offsets):
    #print options.tzoffset
    for (epoch_ms, entry) in heapq.merge(*[decorated_log_split(f, offsets[f.name], epochtimemillis_keyfunc) for f in files]):
        yield (epoch_ms, entry)


class ApprunnerTarFile():
    def __init__(self, f):
        self.tar = tarfile.open(f)
        self.prefix = os.path.commonprefix(self.tar.getnames())

        #validate this file is really apprunner
        if not re.match('tmp/\S+/apprunner/\S+',
                        os.path.commonprefix(self.tar.getnames())):
            raise IOError("This isn't a valid apprunner file")

    def show_files(self):
        return self.tar.getnames()

    def get_serverlogs(self):
        pat = '/serverlogs/.*-log\.txt'
        return [f for f in self.tar.getnames() if re.search(pat,f)]

    def get_otherlogs(self):
        patterns = ['apprunner.log',
                    '\.Benchmark\.',
                    'VoltDBReplicationAgent'
                    ]
        pat = '|'.join(patterns)
        return [f for f in self.tar.getnames() if re.search(pat,f)]

    def get_server_tzoffset(self):
        """open a server file and apprunner.py - compute time delta"""
        return str(-4 * 60 * 60 * 1000)

if __name__ == "__main__":

    parser = OptionParser(usage = "usage: %prog [options] FILE...")
    parser.add_option("-o", "--output", dest="outputfile",
                      help="write to ")
    parser.add_option("-t", "--tzoffset", type="string", default="0",
                      nargs=1, metavar="TZ_OFFSET",
                      action="callback", callback=tz_offset_callback,
                      help="Change the timestamps by [-|+]hh:mm to timezones. "
                      "EST is -05:00, PDT is -08:00. Don't forget daylight savings.")
    (options, args) = parser.parse_args()
    offsets = {}

    #If we have 1 arg, see if it is a tarfile
    if len(args) == 1 and tarfile.is_tarfile(args[0]):
        try:
            tar = ApprunnerTarFile(args[0])
        except IOError as e:
            sys.exit(e)

        serverlogs = tar.get_serverlogs()
        for f in serverlogs:
           offsets[f] = int(tar.get_server_tzoffset())
        otherlogs = tar.get_otherlogs()
        for f in otherlogs:
            offsets[f] = 0
        files = map(tar.tar.extractfile, serverlogs + otherlogs)

    else:
        try:
            files = map(open, args)
        except IOError as e:
            sys.exit("Cannot open input file: " + str(e))
        for f in files:
            offsets[f.name] = int(options.tzoffset)

    if options.outputfile:
        try:
            sys.stdout = open(options.outputfile, 'w')
        except IOError as e:
            sys.exit("Cannot write output file %s: %s " %
                     (options.outputfile, str(e)))

    excludes = []
    exclude_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'exclude.txt')
    with open(exclude_file) as exf:
        excludes = [l.rstrip() for l in exf if l[0] != '#']

    name_dict = {
        '(volt\w*)-.*.txt': '  ',
        'org.voltdb.(dr).VoltDBReplicationAgent': '%%',
        '(apprunner).log': '&&',
        '(.*)\.Benchmark\.': '--',
        }

    for  (time_str, entry) in merge_logs(files, offsets):
        #This is really only good for apprunnerish stuff
        if tar:
            for key in name_dict:
                fmatch = re.match(key, entry["filename"])
                if fmatch:
                    entry["filename"] = "%s %s %s" % (
                        name_dict[key], fmatch.group(1), name_dict[key])


        ematch = None
        for e in excludes:
            if e:
                ematch = re.search(e, entry["message"])
                if ematch:
                    break
        if ematch:
            continue
        new_logline = "%s\t%s\t%s" % (entry["newdatetime"],
                                      entry["filename"],
                                      entry["message"])
        try:
            print new_logline
        except IOError as e:
            import errno
            if e.errno == errno.EPIPE:
                sys.exit(0)
            else:
                sys.exit("Error writing output: " + str(e))





