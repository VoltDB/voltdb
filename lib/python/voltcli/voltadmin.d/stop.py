# This file is part of VoltDB.

# Copyright (C) 2008-2016 VoltDB Inc.
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

    def __init__(self, id, abort_func):
        self.id = id
        self.abort_func = abort_func

    # Provide pseudo-attributes for dictionary items with error checking
    def __getattr__(self, name):
        try:
            return self[name]
        except IndexError:
            self.abort_func('Attribute "%s" not present for host.' % name)


class Hosts(object):

    def __init__(self, abort_func):
        self.hosts_by_id = {}
        self.abort_func = abort_func

    def update(self, host_id_raw, prop_name_raw, value_raw):
        host_id = int(host_id_raw)
        prop_name = prop_name_raw.lower()
        value = value_raw
        if prop_name.endswith('port'):
            value = int(value)
        self.hosts_by_id.setdefault(host_id, Host(host_id, self.abort_func))[prop_name] = value

    def get_target_and_connection_host(self, host_name):
        """
        Find an arbitrary host that isn't the one being stopped.
        Returns a tuple with connection and target host objects.
        """
        connection_host = None
        target_host = None
        for host in self.hosts_by_id.values():
            if host.hostname == host_name:
                target_host = host
            elif connection_host is None:
                connection_host = host
            if not connection_host is None and not target_host is None:
                break
        return (target_host, connection_host)


@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Stop one host of a running VoltDB cluster.',
    arguments = (
        VOLT.StringArgument('target_host', 'the target HOST name or address'),
    ),
)
def stop(runner):

    # Exec @SystemInformation to find out about the cluster.
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['OVERVIEW'])

    # Convert @SystemInformation results to objects.
    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    # Connect to an arbitrary host that isn't being stopped.
    (thost, chost) = hosts.get_target_and_connection_host(runner.opts.target_host)
    if thost is None:
        runner.abort('Host not found in cluster: %s' % runner.opts.target_host)
    if chost is None:
        runner.abort('The entire cluster is being stopped, use "shutdown" instead.')

    if runner.opts.username:
        user_info = ', user: %s' % runner.opts.username
    else:
        user_info = ''
    runner.info('Connecting to host: %s:%d%s' % (chost.hostname, chost.adminport, user_info))
    runner.voltdb_connect(chost.hostname, chost.adminport,
                          runner.opts.username, runner.opts.password)

    # Stop the requested host using exec @StopNode HOST_ID
    runner.info('Stopping host %d: %s' % (thost.id, thost.hostname))
    if not runner.opts.dryrun:
        response = runner.call_proc('@StopNode',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [thost.id],
                                    check_status=False)
        print response
