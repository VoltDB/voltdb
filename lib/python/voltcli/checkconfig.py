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

import os.path

def check_thp_file(file_pattern, file_prefix):
    filename = file_pattern.format(file_prefix)
    if not os.path.isfile(filename):
        return None
    with file(filename) as f:
        if '[always]' in f.read():
            return True
    return False

def check_thp_files(file_prefix):
    res = check_thp_file("/sys/kernel/mm/{0}transparent_hugepage/enabled", file_prefix)
    if res is not False:
        return res
    return bool(check_thp_file("/sys/kernel/mm/{0}transparent_hugepage/defrag", file_prefix))

def check_thp_config():
    file_prefix = "redhat_"
    has_error = check_thp_files(file_prefix)
    if has_error is None:
        file_prefix = ""
        has_error = bool(check_thp_files(file_prefix))
    if has_error:
        return "The kernel is configured to use transparent huge pages (THP). " \
            "This is not supported when running VoltDB. Use the following commands " \
            "to disable this feature for the current session:\n\n" \
            "bash -c \"echo never > /sys/kernel/mm/{0}transparent_hugepage/enabled\"\n" \
            "bash -c \"echo never > /sys/kernel/mm/{0}transparent_hugepage/defrag\"\n\n" \
            "To disable THP on reboot, add the preceding commands to the /etc/rc.local file.".format(file_prefix)

def check_config():
    return check_thp_config()