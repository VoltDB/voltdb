/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.deploy;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static org.voltdb.VoltDB.DEFAULT_CLUSTER_NAME;
import static org.voltdb.VoltDB.DEFAULT_EXTERNAL_INTERFACE;
import static org.voltdb.VoltDB.DEFAULT_INTERNAL_INTERFACE;
import static org.voltdb.VoltDB.DEFAULT_INTERNAL_PORT;
import static org.voltdb.VoltDB.DEFAULT_PORT;
import static org.voltdb.VoltDB.DEFAULT_ZK_PORT;
import static org.voltdb.VoltDB.DISABLED_PORT;
import static org.voltdb.utils.MiscUtils.isValidHostSpec;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google_voltpatches.common.net.HostAndPort;

public class MemberNetConfig {
    protected final static Pattern numRE = Pattern.compile("\\d+");

    protected final String clusterName;
    protected final String internalInterface;
    protected final String externalInterface;
    protected final String publicInterface;
    protected final String zkInterface;
    protected final String drInterface;
    protected final String httpInterface;
    protected final int meshPort;
    protected final int clientPort;
    protected final int adminPort;
    protected final int drPort;
    protected final int httpPort;
    protected final boolean adminMode;

    public MemberNetConfig(
            String clusterName,
            String internalInterface,
            String externalInterface,
            String publicInterface,
            String zkInterface,
            String drInterface,
            String httpInterface,
            int meshPort,
            int clientPort,
            int adminPort,
            int drPort,
            int httpPort,
            boolean adminMode
            ) {
        checkArgument(clusterName != null && !clusterName.trim().isEmpty(),
                "cluster name is null, emtpy, or blank");
        checkArgument(internalInterface != null
                && (internalInterface.trim().isEmpty() || isValidHostSpec(internalInterface.trim(),DEFAULT_PORT)),
                "invalid internal interface specification: %s", internalInterface);
        checkArgument(externalInterface != null
                && (externalInterface.trim().isEmpty() || isValidHostSpec(externalInterface.trim(),DEFAULT_PORT)),
                "invalid external interface specification: %s", internalInterface);
        checkArgument(publicInterface != null
                && (publicInterface.trim().isEmpty() || isValidHostSpec(publicInterface.trim(),DEFAULT_PORT)),
                "invalid public interface specification: %s", publicInterface);
        checkArgument(drInterface != null
                && (drInterface.trim().isEmpty() || isValidHostSpec(drInterface.trim(),DEFAULT_PORT)),
                "invalid dr interface specification: %s", drInterface);
        checkArgument(httpInterface != null
                && (httpInterface.trim().isEmpty() || isValidHostSpec(httpInterface.trim(),DEFAULT_PORT)),
                "invalid http interface specification: %s", httpInterface);
        checkArgument(zkInterface != null && !zkInterface.trim().isEmpty(),
                "zk interface is null, empty, or blank");
        int zkPort = -1;
        if (zkInterface.indexOf(':') >= 0) {
            HostAndPort zkhp = HostAndPort.fromString(zkInterface).withDefaultPort(DEFAULT_ZK_PORT);
            checkArgument(isValidHostSpec(zkhp.getHostText(),DEFAULT_ZK_PORT),
                    "invalid host specification in zk interface: %s", zkInterface);
            zkPort = zkhp.getPort();
        } else {
            Matcher mtc = numRE.matcher(zkInterface);
            checkArgument(mtc.matches(), "invalid port specification for zk interface: %s", zkInterface);
            zkPort = Integer.parseInt(zkInterface);
            checkArgument(isValidPort(meshPort), "Zk port out of range: %s", meshPort);
        }
        checkArgument(isValidPort(meshPort), "Mesh port out of range: %s", meshPort);
        checkArgument(isValidPort(clientPort), "Client port out of range: %s", clientPort);
        checkArgument(adminPort == DISABLED_PORT || isValidPort(adminPort), "Admin port out of range: %s", adminPort);
        checkArgument(drPort == DISABLED_PORT || isValidPort(drPort), "DR port out of range: %s", drPort);
        checkArgument(httpPort == DISABLED_PORT || isValidPort(httpPort), "HTTP port out of range: %s", drPort);

        int distincts = 3;
        Set<Integer> ports = new HashSet<>(Arrays.asList(zkPort,clientPort,meshPort));
        for (int p: new int[]{adminPort,drPort,httpPort}) {
            if (p == DISABLED_PORT) continue;
            ++distincts;
            ports.add(p);
        }

        checkArgument(ports.size() == distincts, "ports must have differing values");

        this.clusterName = clusterName;
        this.internalInterface = internalInterface;
        this.externalInterface = externalInterface;
        this.publicInterface = publicInterface;
        this.drInterface = drInterface;
        this.httpInterface = httpInterface;
        this.zkInterface = zkInterface;
        this.meshPort = meshPort;
        this.clientPort = clientPort;
        this.adminPort = adminPort;
        this.drPort = drPort;
        this.httpPort = httpPort;
        this.adminMode = adminMode;
    }

    public MemberNetConfig() {
        this(DEFAULT_CLUSTER_NAME);
    }

    public MemberNetConfig(String clusterName) {
        this(clusterName,
             DEFAULT_INTERNAL_INTERFACE,
             DEFAULT_EXTERNAL_INTERFACE,
             "",
             "127.0.0.1:"+DEFAULT_ZK_PORT,
             "",
             "",
             DEFAULT_INTERNAL_PORT,
             DEFAULT_PORT,
             DISABLED_PORT,
             DISABLED_PORT,
             DISABLED_PORT,
             false);
    }

    public MemberNetConfig(MemberNetConfig o) {
        this(checkNotNull(o, "net config is null").clusterName,
             o.internalInterface,
             o.externalInterface,
             o.publicInterface,
             o.zkInterface,
             o.drInterface,
             o.httpInterface,
             o.meshPort,
             o.clientPort,
             o.adminPort,
             o.drPort,
             o.httpPort,
             o.adminMode);
    }

    protected static boolean isValidPort(int port) {
        return port >= 0 && port <= 65535;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getInternalInterface() {
        return internalInterface;
    }

    public String getExternalInterface() {
        return externalInterface;
    }

    public String getPublicInterface() {
        return publicInterface;
    }

    public String getDrInterface() {
        return drInterface;
    }

    public String getHttpInterface() {
        return httpInterface;
    }

    public String getZkInterface() {
        return zkInterface;
    }

    public int getMeshPort() {
        return meshPort;
    }

    public int getClientPort() {
        return clientPort;
    }

    public int getAdminPort() {
        return adminPort;
    }

    public int getDrPort() {
        return drPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public boolean isAdminMode() {
        return adminMode;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (adminMode ? 1231 : 1237);
        result = prime * result + adminPort;
        result = prime * result + clientPort;
        result = prime * result + drPort;
        result = prime * result + httpPort;
        result = prime * result
                + ((clusterName == null) ? 0 : clusterName.hashCode());
        result = prime * result + ((externalInterface == null) ? 0
                : externalInterface.hashCode());
        result = prime * result + ((internalInterface == null) ? 0
                : internalInterface.hashCode());
        result = prime * result + meshPort;
        result = prime * result
                + ((publicInterface == null) ? 0 : publicInterface.hashCode());
        result = prime * result
                + ((zkInterface == null) ? 0 : zkInterface.hashCode());
        result = prime * result
                + ((drInterface == null) ? 0 : drInterface.hashCode());
        result = prime * result
                + ((httpInterface == null) ? 0 : httpInterface.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MemberNetConfig other = (MemberNetConfig) obj;
        if (adminMode != other.adminMode)
            return false;
        if (adminPort != other.adminPort)
            return false;
        if (clientPort != other.clientPort)
            return false;
        if (drPort != other.drPort)
            return false;
        if (httpPort != other.httpPort)
            return false;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        if (externalInterface == null) {
            if (other.externalInterface != null)
                return false;
        } else if (!externalInterface.equals(other.externalInterface))
            return false;
        if (internalInterface == null) {
            if (other.internalInterface != null)
                return false;
        } else if (!internalInterface.equals(other.internalInterface))
            return false;
        if (meshPort != other.meshPort)
            return false;
        if (publicInterface == null) {
            if (other.publicInterface != null)
                return false;
        } else if (!publicInterface.equals(other.publicInterface))
            return false;
        if (zkInterface == null) {
            if (other.zkInterface != null)
                return false;
        } else if (!zkInterface.equals(other.zkInterface))
            return false;
        if (drInterface == null) {
            if (other.drInterface != null)
                return false;
        } else if (!drInterface.equals(other.drInterface))
            return false;
        if (httpInterface == null) {
            if (other.httpInterface != null)
                return false;
        } else if (!httpInterface.equals(other.httpInterface))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(256);
        builder.append("NetConfig [clusterName=");
        builder.append(clusterName);
        builder.append(", internalInterface=");
        builder.append(internalInterface);
        builder.append(", externalInterface=");
        builder.append(externalInterface);
        builder.append(", httpInterface=");
        builder.append(publicInterface);
        builder.append(", zkInterface=");
        builder.append(zkInterface);
        builder.append(", drInterface=");
        builder.append(drInterface);
        builder.append(", httpInterface=");
        builder.append(httpInterface);
        builder.append(", meshPort=");
        builder.append(meshPort);
        builder.append(", clientPort=");
        builder.append(clientPort);
        builder.append(", adminPort=");
        builder.append(adminPort);
        builder.append(", drPort=");
        builder.append(drPort);
        builder.append(", httpPort=");
        builder.append(httpPort);
        builder.append(", adminMode=");
        builder.append(adminMode);
        builder.append("]");
        return builder.toString();
    }
}
