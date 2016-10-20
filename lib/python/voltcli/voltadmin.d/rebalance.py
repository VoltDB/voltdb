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


from voltcli.hostinfo import Host
from voltcli.hostinfo import Hosts

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    options = (
        VOLT.IntegerOption('-c', '--hostcount', 'hostcount', 'target number of hosts in the cluster'),
        VOLT.StringOption('-t', '--stop', 'stoplist', 'target nodes to be stopped'),
        VOLT.IntegerOption('-k', '--kfactor', 'kfactor', 'kfactor in cluster'),
    ),
    description = 'Rebalance the cluster: The procedure is not ready for use',
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
