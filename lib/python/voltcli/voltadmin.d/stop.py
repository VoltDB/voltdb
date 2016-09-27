# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

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

    def get_target_and_connection_host(self, target_host_name, connection_host_name):
        """
        Find host being stopped and current host connected to. 
        Returns a tuple with connection and target host objects.
        """
        connection_host = None
        target_host = None
        for host in self.hosts_by_id.values():
            if host.hostname == target_host_name:
                target_host = host
            if connection_host_name in host.values():
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
    (thost, chost) = hosts.get_target_and_connection_host(runner.opts.target_host, runner.opts.host.host)
    if thost is None:
        runner.abort('Host not found in cluster: %s' % runner.opts.target_host)
    if chost is None:
        runner.abort('The entire cluster is being stopped, use "shutdown" instead.')
    if thost == chost:
        runner.abort('You must connect to a different host than the one you want to stop')

    if runner.opts.username:
        user_info = ', user: %s' % runner.opts.username
    else:
        user_info = ''

    # Stop the requested host using exec @StopNode HOST_ID
    runner.info('Stopping host %d: %s' % (thost.id, thost.hostname))
    if not runner.opts.dryrun:
        response = runner.call_proc('@StopNode',
                                    [VOLT.FastSerializer.VOLTTYPE_INTEGER],
                                    [thost.id],
                                    check_status=False)
        print response
