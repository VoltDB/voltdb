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

package org.voltdb.probe;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.UUID;

import javax.annotation.Generated;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.StartAction;
import org.voltdb.common.NodeState;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;

/**
 *  The HostCriteria is a data transfer object (DTO) that snapshots a subset of
 *  {@link MeshProber} fields, and is passed around in initial mesh requests.
 *
 *  Be aware that some fields may become stale once the database is running, for
 *  example the configHash and licenseHash. Operationally this does not matter,
 *  since we demand those fields match only on mesh creation.
 */
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
    public final static String SAFE_MODE = "safeMode";
    public final static String TERMINUS_NONCE = "terminusNonce";
    public final static String LICENSE_HASH = "licenseHash";

    public final static UUID UNDEFINED = new UUID(0L,0L);
    public final static String NO_LICENSE = ""; // the value used for community edition

    /**
     * It checks whether the given {@link JSONObject} contains the
     * fields necessary to create an instance of {@link HostCriteria}.
     * The get[type] variant of {@link JSONObject} throws a {@link JSONException}
     * when it does not parse to the expected types
     */
    public static boolean hasCriteria(JSONObject jo) {
        try {
            return jo != null
                && jo.has(IS_PAUSED) && (jo.getBoolean(IS_PAUSED) ? true : true)
                && jo.has(IS_BARE) && (jo.getBoolean(IS_BARE) ? true : true)
                && jo.has(IS_ENTERPRISE) && (jo.getBoolean(IS_ENTERPRISE) ? true : true)
                && jo.has(ADD_ALLOWED) && (jo.getBoolean(ADD_ALLOWED) ? true : true)
                && jo.has(SAFE_MODE) && (jo.getBoolean(SAFE_MODE) ? true : true)
                && jo.has(CONFIG_HASH) && jo.getString(CONFIG_HASH) != null && !jo.getString(CONFIG_HASH).trim().isEmpty()
                && jo.has(MESH_HASH) && jo.getString(MESH_HASH) != null && !jo.getString(MESH_HASH).trim().isEmpty()
                && jo.has(START_ACTION) && jo.getString(START_ACTION) != null && !jo.getString(START_ACTION).trim().isEmpty()
                && jo.has(NODE_STATE) && jo.getString(NODE_STATE) != null && !jo.getString(NODE_STATE).trim().isEmpty()
                && jo.has(HOST_COUNT) && jo.getInt(HOST_COUNT) > 0
                && (jo.optString(TERMINUS_NONCE, null) == null || !jo.optString(TERMINUS_NONCE).trim().isEmpty())
                && jo.has(LICENSE_HASH) && jo.getString(LICENSE_HASH) != null;
        } catch (JSONException e) {
            return false;
        }
    }

    protected final boolean m_paused;
    protected final UUID m_configHash;
    /** coordinators and host count digest */
    protected final UUID m_meshHash;
    protected final boolean m_enterprise;
    protected final StartAction m_startAction;
    /**
     * {@code true} if there are no recoverable artifacts (Command Logs, Snapshots)
     */
    protected final boolean m_bare;
    protected final int m_hostCount;
    protected final NodeState m_nodeState;
    protected final boolean m_addAllowed;
    protected final boolean m_safeMode;
    protected final String m_terminusNonce;
    protected final String m_licenseHash;

    public HostCriteria(JSONObject jo) {
        checkArgument(jo != null, "json object is null");
        m_paused = jo.optBoolean(IS_PAUSED, false);
        m_bare = jo.optBoolean(IS_BARE, false);
        m_enterprise = jo.optBoolean(IS_ENTERPRISE, false);
        m_configHash = UUID.fromString(jo.optString(CONFIG_HASH,UNDEFINED.toString()));
        m_meshHash = UUID.fromString(jo.optString(MESH_HASH,UNDEFINED.toString()));
        m_startAction = StartAction.valueOf(jo.optString(START_ACTION, StartAction.CREATE.name()));
        m_hostCount = jo.optInt(HOST_COUNT,1);
        m_nodeState = NodeState.valueOf(jo.optString(NODE_STATE, NodeState.INITIALIZING.name()));
        m_addAllowed = jo.optBoolean(ADD_ALLOWED, false);
        m_safeMode = jo.optBoolean(SAFE_MODE, false);
        m_terminusNonce = jo.optString(TERMINUS_NONCE, null);
        m_licenseHash = jo.optString(LICENSE_HASH, NO_LICENSE);
    }

    public HostCriteria(boolean paused, UUID configHash, UUID meshHash,
            boolean enterprise, StartAction startAction, boolean bare,
            int hostCount, NodeState nodeState, boolean addAllowed,
            boolean safeMode, String terminusNonce, String licenseHash) {
        checkArgument(configHash != null, "config hash is null");
        checkArgument(meshHash != null, "mesh hash is null");
        checkArgument(startAction != null, "start action is null");
        checkArgument(hostCount > 0, "host count %s is less then one", hostCount);
        checkArgument(terminusNonce == null || !terminusNonce.trim().isEmpty(),
                "terminus should not be blank");
        checkArgument(licenseHash != null, "license hash is null");

        m_paused = paused;
        m_configHash = configHash;
        m_meshHash = meshHash;
        m_enterprise = enterprise;
        m_startAction = startAction;
        m_bare = bare;
        m_hostCount = hostCount;
        m_nodeState = nodeState;
        m_addAllowed = addAllowed;
        m_safeMode = safeMode;
        m_terminusNonce = terminusNonce;
        m_licenseHash = licenseHash;
    }

    public boolean isPaused() {
        return m_paused;
    }

    public UUID getConfigHash() {
        return m_configHash;
    }

    public UUID getMeshHash() {
        return m_meshHash;
    }

    public boolean isEnterprise() {
        return m_enterprise;
    }

    public StartAction getStartAction() {
        return m_startAction;
    }

    /**
     * @return {@code true} if there are no recoverable artifacts (Command Logs, Snapshots)
     */
    public boolean isBare() {
        return m_bare;
    }

    public int getHostCount() {
        return m_hostCount;
    }

    public NodeState getNodeState() {
        return m_nodeState;
    }

    public boolean isAddAllowed() {
        return m_addAllowed;
    }

    public boolean isSafeMode() {
        return m_safeMode;
    }

    public String getTerminusNonce() {
        return m_terminusNonce;
    }

    public String getLicenseHash() {
        return m_licenseHash;
    }

    public JSONObject appendTo(JSONObject jo) {
        checkArgument(jo != null, "json object is null");
        try {
            jo.put(IS_BARE, m_bare);
            jo.put(IS_ENTERPRISE, m_enterprise);
            jo.put(IS_PAUSED, m_paused);
            jo.put(CONFIG_HASH, m_configHash.toString());
            jo.put(MESH_HASH, m_meshHash.toString());
            jo.put(START_ACTION, m_startAction.name());
            jo.put(HOST_COUNT, m_hostCount);
            jo.put(NODE_STATE, m_nodeState.name());
            jo.put(ADD_ALLOWED, m_addAllowed);
            jo.put(SAFE_MODE, m_safeMode);
            jo.put(TERMINUS_NONCE, m_terminusNonce);
            jo.put(LICENSE_HASH, m_licenseHash);
        } catch (JSONException e) {
            Throwables.propagate(e);
        }
        return jo;
    }

    public boolean isUndefined() {
        return UNDEFINED.equals(m_configHash) && UNDEFINED.equals(m_meshHash);
    }

    public List<String> listIncompatibilities(HostCriteria o) {
        checkArgument(o != null, "can't check compatibility against a null host criteria");
        ImmutableList.Builder<String> ilb = ImmutableList.builder();
        if (o.isUndefined()) {
            ilb.add("Joining node has incompatible version");
            return ilb.build();
        }
        if (m_startAction != o.m_startAction) {
            ilb.add(String.format("Start action %s does not match %s", o.m_startAction, m_startAction));
        }
        if (m_startAction != StartAction.PROBE) {
            return ilb.build();
        }
        if (m_enterprise != o.m_enterprise) {
            ilb.add("Cluster cannot contain both enterprise and community editions");
        }
        if (m_hostCount != o.m_hostCount) {
            ilb.add(String.format("Mismatched host count: %d, and %d", m_hostCount, o.m_hostCount));
        }
        if (!m_meshHash.equals(o.m_meshHash)) {
            ilb.add("Mismatched list of hosts given at database startup");
        }
        if (!m_configHash.equals(o.m_configHash)) {
            ilb.add("Servers are initialized with deployment options that do not match");
        }
        if (m_terminusNonce != null
            && o.m_terminusNonce != null
            && !m_terminusNonce.equals(o.m_terminusNonce)) {
            ilb.add("Servers have different startup snapshot nonces: "
                    + m_terminusNonce + " vs. " + o.m_terminusNonce);
        }
        if (!m_licenseHash.equals(o.m_licenseHash)) {
            ilb.add("Servers have different licenses");
        }
        return ilb.build();
    }

    @Override @Generated("by eclipse's equals and hashCode source generators")
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (m_bare ? 1231 : 1237);
        result = prime * result
                + ((m_configHash == null) ? 0 : m_configHash.hashCode());
        result = prime * result + (m_enterprise ? 1231 : 1237);
        result = prime * result + (m_addAllowed ? 1231 : 1237);
        result = prime * result + (m_safeMode ? 1231 : 1237);
        result = prime * result + m_hostCount;
        result = prime * result
                + ((m_meshHash == null) ? 0 : m_meshHash.hashCode());
        result = prime * result
                + ((m_terminusNonce == null) ? 0 : m_terminusNonce.hashCode());
        result = prime * result
            + ((m_licenseHash == null) ? 0 : m_licenseHash.hashCode());
        result = prime * result + (m_paused ? 1231 : 1237);
        result = prime * result
                + ((m_startAction == null) ? 0 : m_startAction.hashCode());
        result = prime * result
                + ((m_nodeState == null) ? 0 : m_nodeState.hashCode());
        return result;
    }

    @Override @Generated("by eclipse's equals and hashCode source generators")
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HostCriteria other = (HostCriteria) obj;
        if (m_bare != other.m_bare)
            return false;
        if (m_configHash == null) {
            if (other.m_configHash != null)
                return false;
        } else if (!m_configHash.equals(other.m_configHash))
            return false;
        if (m_enterprise != other.m_enterprise)
            return false;
        if (m_addAllowed != other.m_addAllowed)
            return false;
        if (m_safeMode != other.m_safeMode)
            return false;
        if (m_hostCount != other.m_hostCount)
            return false;
        if (m_meshHash == null) {
            if (other.m_meshHash != null)
                return false;
        } else if (!m_meshHash.equals(other.m_meshHash))
            return false;
        if (m_terminusNonce == null) {
            if (other.m_terminusNonce != null)
                return false;
        } else if (!m_terminusNonce.equals(other.m_terminusNonce))
            return false;
        if (m_licenseHash == null) {
            if (other.m_licenseHash != null)
                return false;
        } else if (!m_licenseHash.equals(other.m_licenseHash))
            return false;
        if (m_paused != other.m_paused)
            return false;
        if (m_startAction != other.m_startAction)
            return false;
        if (m_nodeState != other.m_nodeState)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "HostCriteria [paused=" + m_paused + ", configHash=" + m_configHash
                + ", meshHash=" + m_meshHash + ", enterprise=" + m_enterprise
                + ", startAction=" + m_startAction + ", bare=" + m_bare
                + ", hostCount=" + m_hostCount + ", nodeState=" + m_nodeState
                + ", addAllowed=" + m_addAllowed + ", safeMode=" + m_safeMode
                + ", terminusNonce=" + m_terminusNonce
                + ", licenseHash=" + (NO_LICENSE.equals(m_licenseHash) ? "none" : m_licenseHash)
                + "]";
    }
}
