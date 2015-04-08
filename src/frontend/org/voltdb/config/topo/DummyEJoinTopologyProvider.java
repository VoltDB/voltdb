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
package org.voltdb.config.topo;

import javax.inject.Inject;

import org.json_voltpatches.JSONObject;
import org.voltdb.StartAction;
import org.voltdb.config.Configuration;

/**
 * @author black
 *
 */

public class DummyEJoinTopologyProvider implements TopologyProvider {

    @Inject
    private Configuration config;

    @Override
    public JSONObject getTopo() {
        StartAction startAction = config.m_startAction;

        if (startAction == StartAction.JOIN) {
            throw new UnsupportedOperationException("getTopology is only supported for elastic join");
        }
        return null;
    }

}
