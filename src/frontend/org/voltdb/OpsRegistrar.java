/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb;

import java.lang.reflect.Constructor;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltcore.messaging.HostMessenger;

/**
 * This class centralizes construction of and access to much of the OpsAgent machinery.
 */
public class OpsRegistrar {
    private Map<OpsSelector, OpsAgent> m_agents;

    /**
     * Construct an OpsRegistrar.  Will iterate through the OpsSelectors and instantiate one
     * instance of each OpsAgent for each selector.
     */
    public OpsRegistrar() {
        m_agents = new EnumMap<OpsSelector, OpsAgent>(OpsSelector.class);
        for (OpsSelector selector : OpsSelector.values()) {
            try {
                Constructor<?> constructor = selector.getAgentClass()
                        .getConstructor();
                OpsAgent newAgent = (OpsAgent) constructor.newInstance();
                newAgent.setDummyMode(true);
                m_agents.put(selector, newAgent);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(
                        "Unable to instantiate OpsAgent for selector: "
                                + selector.name(), true, e);
            }
        }
    }

    /**
     * Register the local mailboxes for each local OpsAgent.  Needs to be separated from
     * construction so that any additional operations to make the OpsAgent ready for access
     * can take place before requests get routed to it (example: registerStatsSource for StatsAgent)
     */
    public void registerMailboxes(HostMessenger messenger) {
        for (Entry<OpsSelector, OpsAgent> entry : m_agents.entrySet()) {
            entry.getValue().registerMailbox(messenger,
                    entry.getKey().getHSId(messenger.getHostId()));
        }
    }

    /**
     * Toggle dummy mode where agents give empty responses to all incoming requests
     * Allows new agents at startup to not block the cluster
     * @param enabled
     */
    public void setDummyMode(boolean enabled) {
        for (OpsAgent agent : m_agents.values()) {
            agent.setDummyMode(enabled);
        }
    }

    /**
     * Return the OpsAgent for the specified selector.
     */
    public OpsAgent getAgent(OpsSelector selector) {
        OpsAgent agent = m_agents.get(selector);
        assert (agent != null);
        return agent;
    }

    /**
     * Shutdown all the OpsAgent's executor services.  Should be possible
     * to eventually consolidate all of them into a single executor service.
     */
    public void shutdown() {
        for (Entry<OpsSelector, OpsAgent> entry : m_agents.entrySet()) {
            try {
                entry.getValue().shutdown();
            }
            catch (InterruptedException e) {}
        }
        m_agents.clear();
    }
}
