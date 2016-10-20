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

    def get_admininterface(self):
        """
        Return the likely admininterface.
        Implementation note: Currently, ipaddress will be set to externalinterface
        if set by user, otherwise server selects any interface.
        """

        if 'admininterface' in self and self['admininterface']:
            return self['admininterface']
        if 'ipaddress' in self and self['ipaddress']:
            return self['ipaddress']
        return None

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

    def get_connection_host(self, host_names):
        """
        Find an arbitrary host that isn't the one being stopped.
        """
        connection_host = None
        for host in self.hosts_by_id.values():
            if host.hostname not in host_names:
                connection_host = host
                break
        return connection_host

    def get_host(self, host_name):
        for host in self.hosts_by_id.values():
            if host.hostname == host_name:
                return host
        return None


@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    options = (
        VOLT.IntegerOption('-c', '--count', 'hostcount', 'number of hosts in the cluster'),
        VOLT.StringOption('-t', '--stop', 'stoplist', 'target nodes to be stopped'),
        VOLT.IntegerOption('-k', '--kfactor', 'kfactor', 'kfactor in cluster'),
    ),
    description = 'Rebalance the cluster.',
    hideverb = True
)

def rebalance(runner):

    # find out about the cluster.
    response = runner.call_proc('@SystemInformation',[VOLT.FastSerializer.VOLTTYPE_STRING],['OVERVIEW'])

    hosts = Hosts(runner.abort)
    for tuple in response.table(0).tuples():
        hosts.update(tuple[0], tuple[1], tuple[2])

    #stop list
    target_host_ids = []
    targets = []
    if runner.opts.stoplist:
        targets = runner.opts.stoplist.split(',')
        for host in targets:
            target = hosts.get_host(host)
            if target == None:
                runner.abort('Host not found in cluster: %s' % host)
            else:
                target_host_ids.append(str(target.id))

    # Connect to an arbitrary host that isn't being stopped.
    chost = hosts.get_connection_host(targets)
    if chost is None:
        runner.abort('Could not find a host other than the hosts to be stoppted.in cluster: %s' % runner.opts.target_host)

    user_info = ''
    if runner.opts.username:
        user_info = ', user: %s' % runner.opts.username

    runner.info('Connecting to %s:%d%s (%s) to issue "rebalance" command' %
                (chost.get_admininterface(), chost.adminport, user_info, chost.hostname))

    #set up connections
    runner.voltdb_connect(chost.get_admininterface(), chost.adminport,runner.opts.username, runner.opts.password)

    json_opts = ['command:"rebalance"']
    if target_host_ids:
        stoplist = 'hosts:%s' % ("-".join(target_host_ids))
        json_opts.append(stoplist)

    if runner.opts.kfactor:
        json_opts.append('kfactor:%s' % (runner.opts.kfactor))

    if runner.opts.hostcount:
        json_opts.append('hostcount:%s' % (runner.opts.hostcount))

    if not runner.opts.dryrun:
        response = runner.call_proc('@Rebalance', [VOLT.FastSerializer.VOLTTYPE_STRING], ['{%s}' % (','.join(json_opts))])
        print response
