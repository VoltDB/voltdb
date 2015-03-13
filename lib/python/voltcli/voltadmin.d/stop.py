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

# Stop a node. Written to easily support multiple, but configured for
# a single host for now.


class Host(dict):

    def __init__(self, id):
        self.id = id

    # Provide pseudo-attributes for dictionary items with error checking
    def __getattr__(self, name):
        try:
            return self[name]
        except IndexError:
            runner.abort('Attribute "%s" not present for host.' % name)


class Hosts(object):

    def __init__(self):
        self.hosts_by_id = {}

    def update(self, host_id_raw, prop_name_raw, value):
        host_id = int(host_id_raw)
        prop_name = prop_name_raw.lower()
        self.hosts_by_id.setdefault(host_id, Host(host_id))[prop_name] = value

    def lookup(self, *host_names):
        """
        Lookup host name(s) and return found and unused host objects,
        plus a list of the host names that are missing. The unused host
        list is arbitrarily ordered.
        """
        found = []
        unused = []
        missing = set(host_names)
        for host in self.hosts_by_id.values():
            if host.hostname in host_names:
                found.append(host)
                missing.remove(host.hostname)
            else:
                unused.append(host)
        return found, unused, sorted(list(missing))


# To make it work with multiple hosts:
#   - set max_count to None in the argument definition
#   - change the lookup() call to hosts.lookup(*runner.opts.target_host)
#   - change message text to reflect the possibility of multiple targets.
@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Stop one host of a running VoltDB cluster.',
    arguments = (
        VOLT.StringArgument('target_host', 'the target HOST name or address',
                            min_count=1, max_count=1),
    ),
)
def stop(runner):

    # Exec @SystemInformation to find out about the cluster.
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts()
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    # Look up the hosts specified as command line arguments.
    found, unused, missing = hosts.lookup(runner.opts.target_host)
    if len(missing) > 0:
        runner.abort('Host not found in cluster: %s' % ' '.join(missing))
    if len(unused) == 0:
        runner.abort('The entire cluster is being stopped, use "shutdown" instead.')

    # Connect to an arbitrary host that isn't being stopped.
    hostname = unused[0].hostname
    port = int(unused[0].adminport)
    username = runner.opts.username
    password = runner.opts.password
    if username:
        user_info = ', user: %s' % username
    else:
        user_info = ''
    runner.info('Connecting to host: %s:%d%s' % (hostname, port, user_info))
    runner.voltdb_connect(hostname, port, username, password)

    # Stop all the requested hosts using exec @StopNode HOST_ID
    for host in found:
        runner.info('Stopping host %d: %s' % (host.id, host.hostname))
        response = runner.call_proc('@StopNode',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [host.id],
                                    check_status=False)
        print response
