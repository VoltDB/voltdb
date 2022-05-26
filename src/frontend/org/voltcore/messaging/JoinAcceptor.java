/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltcore.messaging;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.voltcore.utils.VersionChecker;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;

/**
 * An interface that allows non voltcore components to effect how hosts
 * can become or not mesh members
 */
public interface JoinAcceptor extends JSONString {

    /**
     * Give the acceptor the {@link JSONObject} provided at the initial connection handshake
     *
     * @param hostId host id for the new connection
     * @param jo the {@link JSONObject} provided at the initial connection handshake
     */
    public void accrue(int hostId, JSONObject jo);

    /**
     * Give the acceptor a map of host ids and {@link JSONObject}  provided at the initial
     * connection handshake
     *
     * @param jos a map of host ids and {@link JSONObject}
     */
    public void accrue(Map<Integer, JSONObject> jos);

    /**
     * Allows the acceptor to inject its fields into the connection handshake message
     * @param jo the initial handshake message
     */
    default JSONObject decorate(JSONObject jo, Optional<Boolean> paused) {
        return jo;
    }

    /**
     * Returns the list of mesh coordinators
     * @return the list of mesh coordinators
     */
    default NavigableSet<String> getCoordinators() {
        return ImmutableSortedSet.of("");
    }

    /**
     * notifies the acceptor when a host is removed from the mesh
     *
     * @param zooKeeper {@link ZooKeeper} instance to use for zookeeper operations
     * @param hostId    ID of host which was removed
     */
    public void detract(ZooKeeper zooKeeper, int hostId);

    /**
     * notifies the acceptor when a {@link Set&lt;Integer&gt;} hostIds
     * are removed from the mesh
     *
     * @param hostIds a {@link Set&lt;Integer&gt;} of removed hostIds
     */
    public void detract(Set<Integer> hostIds);

    /**
     * On accepting a connection the acceptor is queried whether or
     * not the mesh plea should be accepted or not, and if it is not
     * if it can be retried
     *
     * @param zk a {@link ZooKeeper} connection
     * @param hostId hostId
     * @param jo the {@link JSONObject} provided at the initial connection handshake
     * @return a {@link PleaDecision}
     */
    default PleaDecision considerMeshPlea(ZooKeeper zk, int hostId, JSONObject jo) {
        return new PleaDecision(null /* no error message */, true /* accepted */,  false /* retriable */);
    }

    public static class PleaDecision {
        public final String errMsg;
        public final boolean accepted;
        public final boolean mayRetry;
        public PleaDecision(String errMsg, boolean accepted, boolean mayRetry) {
            this.errMsg = errMsg;
            this.accepted = accepted;
            this.mayRetry = mayRetry;
        }
        @Override
        public String toString() {
            return "PleaDecision [errMsg=" + errMsg + ", accepted=" + accepted
                    + ", mayRetry=" + mayRetry + "]";
        }
    }

    /**
     * Memoized supplier of version information so that it reads files only once
     */
    static final public Supplier<String[]> versionSupplier =
            Suppliers.memoize(
                    new Supplier<String[]>() {
                        @Override
                        public String[] get() {
                            return org.voltdb.RealVoltDB.extractBuildInfo(null);
                        }
                    });

    static final VersionChecker DEFAULT_VERSION_CHECKER = new VersionChecker() {
        @Override
        public boolean isCompatibleVersionString(String other) {
            return org.voltdb.RealVoltDB.staticIsCompatibleVersionString(other);
        }
        @Override
        public String getVersionString() {
            return versionSupplier.get()[0];
        }
        @Override
        public String getBuildString() {
            return versionSupplier.get()[1];
        }
    };

    /**
     * @return a version checker
     */
    default VersionChecker getVersionChecker() {
        return DEFAULT_VERSION_CHECKER;
    }

    /**
     * Get the the first coordinator in lexicographical order
     * @return the first coordinator in lexicographical order
     */
    default HostAndPort getLeader() {
        return HostAndPort.fromString(getCoordinators().first())
                .withDefaultPort(org.voltcore.common.Constants.DEFAULT_INTERNAL_PORT);
    }

    @Override
    default String toJSONString() {
        return "{}";
    }
}
