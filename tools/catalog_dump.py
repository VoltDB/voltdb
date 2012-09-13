#!/usr/bin/env python

# Dump catalog.txt from inside a catalog jar and decode hex strings without
# needing to unzip jar files.

import sys
import os
import re
from zipfile import ZipFile

reHexPat = '^(\s*set\s+.*\s+)(schema|explainplan|plannodetree)(\s+")([0-9a-f]+)(".*)$'
reHex = re.compile(reHexPat, re.IGNORECASE)

def error(*msgs):
    for msg in msgs:
        print 'ERROR: %s' % str(msg)

def abort(*msgs):
    error(*msgs)
    sys.exit(1)

def dump(path):
    print ('\n========== %s ==========\n' % path)
    try:
        zipf = ZipFile(path)
        try:
            for line in zipf.read('catalog.txt').split('\n'):
                m = reHex.search(line)
                if m:
                    sys.stdout.write(''.join((m.group(1),
                                              m.group(2),
                                              m.group(3),
                                              m.group(4).decode('hex'),
                                              m.group(5))))
                else:
                    print (line)
        finally:
            zipf.close()
    except (IOError, OSError), e:
        abort('Unable to open catalog "%s".' % path, e)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if os.path.exists(arg):
                dump(os.path.realpath(arg))
            else:
                abort('Catalog file "%s" does not exist.' % arg)
    else:
        print 'Usage: %s CATALOG_JAR ...' % os.path.basename(sys.argv[0])
