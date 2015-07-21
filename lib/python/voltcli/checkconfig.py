# This file is part of VoltDB.

# Copyright (C) 2008-2015 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import glob

def check_thp_config():
    thp_filenames = glob.glob("/sys/kernel/mm/*transparent_hugepage/enabled")
    thp_filenames += glob.glob("/sys/kernel/mm/*transparent_hugepage/defrag")
    for filename in thp_filenames:
        with file(filename) as f:
            if '[always]' in f.read():
                return "The kernel is configured to always use transparent " \
                    "huge pages. This is not a supported configuration for " \
                    "running VoltDB. To disable this feature, please run the " \
                    "following:\n" \
                    "for f in /sys/kernel/mm/*transparent_hugepage/enabled; do\n" \
                    "    if test -f $f; then echo never > $f; fi\n" \
                    "done\n" \
                    "for f in /sys/kernel/mm/*transparent_hugepage/defrag; do\n" \
                    "    if test -f $f; then echo never > $f; fi\n" \
                    "done\n" \
                    "As root, you may add these options to your " \
                    "/etc/rc.local file. It is acceptable to replace 'never' " \
                    "with 'madvise' in the above commands to allow applications " \
                    "to opt into using transparent huge pages."

def check_config():
    return check_thp_config()