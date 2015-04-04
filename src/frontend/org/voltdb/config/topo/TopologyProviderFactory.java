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

import java.util.List;

import org.json_voltpatches.JSONObject;
import javax.inject.Inject;

/**
 * @author black
 *
 */

public class TopologyProviderFactory {

    @Inject
    private List<TopologyProvider> topologyProvidersChain;

    /**
     * Gets topology information. If rejoining, get it directly from ZK.
     * Otherwise, try to do the write/read race to ZK on startup. NOTE: override
     */
    public JSONObject getTopo() {
        for (TopologyProvider provider : topologyProvidersChain) {
            JSONObject topo = provider.getTopo();
            if (topo != null) {
                return topo;
            }
        }
        throw new UnsupportedOperationException("Cannot find relevant topology provider");
    }

}
