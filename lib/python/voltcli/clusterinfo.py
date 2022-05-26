# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

class Cluster(object):

    def __init__(self, id, version, kfactor, hostcount, uptime):
        self.id = id
        self.version = version
        self.kfactor = kfactor
        self.hostcount = hostcount
        self.hosts_by_id = dict()
        self.remoteclusters_by_id = dict()
        self.uptime = uptime
        self.liveclients = 0
        self.elastic_status = ""
        self.percentage_moved = 100.0

    def update_live_clients(self, liveclients):
        self.liveclients = liveclients

    def add_member(self, host_id, host_name):
        self.hosts_by_id[host_id] = host_name

    def add_remote_cluster(self, cluster_id, status, role):
        self.remoteclusters_by_id[cluster_id] = RemoteCluster(cluster_id, status, role)

    def get_remote_cluster(self, cluster_id):
        return self.remoteclusters_by_id[cluster_id]

    def set_elastic_status(self, elastic_status, percentage_moved):
        self.elastic_status = elastic_status
        self.percentage_moved = percentage_moved

class RemoteCluster(object):

    def __init__(self, cluster_id, status, role):
        self.id = cluster_id
        self.status = status
        self.role = role
        self.members = set()
        self.producer_max_latency = dict()

    def add_remote_member(self, host_name):
        self.members.add(host_name)

    def update_producer_latency(self, host_name, remote_cluster_id, delay):
        key = host_name + str(remote_cluster_id)
        if key not in self.producer_max_latency:
            self.producer_max_latency[key] = delay
        elif (delay > self.producer_max_latency[key]):
            self.producer_max_latency[key] = delay
