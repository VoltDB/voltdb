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

package org.voltcore.messaging;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.UUID;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.StartAction;
import org.voltdb.common.NodeState;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;

public class HostCriteria {

    public final static String IS_PAUSED = "paused";
    public final static String CONFIG_HASH = "configHash";
    public final static String MESH_HASH = "meshHash";
    public final static String IS_BARE = "isBare";
    public final static String START_ACTION = "startAction";
    public final static String IS_ENTERPRISE = "isEnterprise";
    public final static String HOST_COUNT = "hostCount";
    public final static String NODE_STATE = "nodeState";
    public final static String ADD_ALLOWED = "addAllowed";

    public final static UUID UNDEFINED = new UUID(0L,0L);

    public static boolean hasCriteria(JSONObject jo) {
        try {
            return jo != null
                && jo.has(IS_PAUSED) && (jo.getBoolean(IS_PAUSED) ? true : true)
                && jo.has(IS_BARE) && (jo.getBoolean(IS_BARE) ? true : true)
                && jo.has(IS_ENTERPRISE) && (jo.getBoolean(IS_ENTERPRISE) ? true : true)
                && jo.has(ADD_ALLOWED) && (jo.getBoolean(ADD_ALLOWED) ? true : true)
                && jo.has(CONFIG_HASH) && jo.getString(CONFIG_HASH) != null && !jo.getString(CONFIG_HASH).trim().isEmpty()
                && jo.has(MESH_HASH) && jo.getString(MESH_HASH) != null && !jo.getString(MESH_HASH).trim().isEmpty()
                && jo.has(START_ACTION) && jo.getString(START_ACTION) != null && !jo.getString(START_ACTION).trim().isEmpty()
                && jo.has(NODE_STATE) && jo.getString(NODE_STATE) != null && !jo.getString(NODE_STATE).trim().isEmpty()
                && jo.has(HOST_COUNT) && jo.getInt(HOST_COUNT) > 0;
        } catch (JSONException e) {
            return false;
        }
    }

    protected final boolean paused;
    protected final UUID configHash;
    protected final UUID meshHash;
    protected final boolean enterprise;
    protected final StartAction startAction;
    protected final boolean bare;
    protected final int hostCount;
    protected final NodeState nodeState;
    protected final boolean addAllowed;

    public HostCriteria(JSONObject jo) {
        checkArgument(jo != null, "json object is null");
        paused = jo.optBoolean(IS_PAUSED, false);
        bare = jo.optBoolean(IS_BARE, false);
        enterprise = jo.optBoolean(IS_ENTERPRISE, false);
        configHash = UUID.fromString(jo.optString(CONFIG_HASH,UNDEFINED.toString()));
        meshHash = UUID.fromString(jo.optString(MESH_HASH,UNDEFINED.toString()));
        startAction = StartAction.valueOf(jo.optString(START_ACTION, StartAction.CREATE.name()));
        hostCount = jo.optInt(HOST_COUNT,1);
        nodeState = NodeState.valueOf(jo.optString(NODE_STATE, NodeState.INITIALIZING.name()));
        addAllowed = jo.optBoolean(ADD_ALLOWED, false);
    }

    public HostCriteria(boolean paused, UUID configHash, UUID meshHash,
            boolean enterprise, StartAction startAction, boolean bare,
            int hostCount, NodeState nodeState, boolean addAllowed) {
        checkArgument(configHash != null, "config hash is null");
        checkArgument(meshHash != null, "mesh hash is null");
        checkArgument(startAction != null, "start action is null");
        checkArgument(hostCount > 0, "host count %s is less then one", hostCount);

        this.paused = paused;
        this.configHash = configHash;
        this.meshHash = meshHash;
        this.enterprise = enterprise;
        this.startAction = startAction;
        this.bare = bare;
        this.hostCount = hostCount;
        this.nodeState = nodeState;
        this.addAllowed = addAllowed;
    }

    public boolean isPaused() {
        return paused;
    }

    public UUID getConfigHash() {
        return configHash;
    }

    public UUID getMeshHash() {
        return meshHash;
    }

    public boolean isEnterprise() {
        return enterprise;
    }

    public StartAction getStartAction() {
        return startAction;
    }

    public boolean isBare() {
        return bare;
    }

    public int getHostCount() {
        return hostCount;
    }

    public NodeState getNodeState() {
        return nodeState;
    }

    public boolean isAddAllowed() {
        return addAllowed;
    }

    public JSONObject appendTo(JSONObject jo) {
        checkArgument(jo != null, "json object is null");
        try {
            jo.put(IS_BARE, bare);
            jo.put(IS_ENTERPRISE, enterprise);
            jo.put(IS_PAUSED, paused);
            jo.put(CONFIG_HASH, configHash.toString());
            jo.put(MESH_HASH, meshHash.toString());
            jo.put(START_ACTION, startAction.name());
            jo.put(HOST_COUNT, hostCount);
            jo.put(NODE_STATE, nodeState.name());
            jo.put(ADD_ALLOWED, addAllowed);
        } catch (JSONException e) {
            Throwables.propagate(e);
        }
        return jo;
    }

    public boolean isUndefined() {
        return UNDEFINED.equals(configHash) && UNDEFINED.equals(meshHash);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (bare ? 1231 : 1237);
        result = prime * result
                + ((configHash == null) ? 0 : configHash.hashCode());
        result = prime * result + (enterprise ? 1231 : 1237);
        result = prime * result + (addAllowed ? 1231 : 1237);
        result = prime * result + hostCount;
        result = prime * result
                + ((meshHash == null) ? 0 : meshHash.hashCode());
        result = prime * result + (paused ? 1231 : 1237);
        result = prime * result
                + ((startAction == null) ? 0 : startAction.hashCode());
        result = prime * result
                + ((nodeState == null) ? 0 : nodeState.hashCode());
        return result;
    }

    public List<String> listIncompatibilities(HostCriteria o) {
        checkArgument(o != null, "cant check compatibility against a null host criteria");
        ImmutableList.Builder<String> ilb = ImmutableList.builder();

        if (startAction != o.startAction) {
            ilb.add(String.format("Start action %s does not match %s", o.startAction, startAction));
        }
        if (startAction != StartAction.PROBE) {
            return ilb.build();
        }
        if (enterprise != o.enterprise) {
            ilb.add("Cannot join a community edition with an eterprise one, or viceversa");
        }
        if (hostCount != o.hostCount) {
            ilb.add(String.format("Mismatched host count: %d, and %d", hostCount, o.hostCount));
        }
        if (!meshHash.equals(o.meshHash)) {
            ilb.add("Mismatched host parameters given at database startup");
        }
        if (!configHash.equals(o.configHash)) {
            ilb.add("Mismatched deployment configuration given at database startup");
        }
        return ilb.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HostCriteria other = (HostCriteria) obj;
        if (bare != other.bare)
            return false;
        if (configHash == null) {
            if (other.configHash != null)
                return false;
        } else if (!configHash.equals(other.configHash))
            return false;
        if (enterprise != other.enterprise)
            return false;
        if (addAllowed != other.addAllowed)
            return false;
        if (hostCount != other.hostCount)
            return false;
        if (meshHash == null) {
            if (other.meshHash != null)
                return false;
        } else if (!meshHash.equals(other.meshHash))
            return false;
        if (paused != other.paused)
            return false;
        if (startAction != other.startAction)
            return false;
        if (nodeState != other.nodeState)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "HostCriteria [paused=" + paused + ", configHash=" + configHash
                + ", meshHash=" + meshHash + ", enterprise=" + enterprise
                + ", startAction=" + startAction + ", bare=" + bare
                + ", hostCount=" + hostCount + ", nodeState=" + nodeState
                + ", addAllowed=" + addAllowed+ "]";
    }
}
