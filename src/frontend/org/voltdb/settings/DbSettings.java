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
package org.voltdb.settings;

import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.base.Supplier;

public class DbSettings {

    private final PathSettings m_paths;
    private final Supplier<ClusterSettings> m_cluster;
    public DbSettings(Supplier<ClusterSettings> clusterSettings, PathSettings pathSettings) {
        m_paths = pathSettings;
        m_cluster = clusterSettings;
    }
    /**
     * For testing purposes only
     * @param dt deployment JAXB object
     */
    public DbSettings(DeploymentType dt) {
        m_paths = PathSettings.create(CatalogUtil.asPathSettingsMap(dt));
        m_cluster = ClusterSettings.create(CatalogUtil.asClusterSettingsMap(dt)).asSupplier();
    }

    public ClusterSettings getCluster() {
        return m_cluster.get();
    }

    public PathSettings getPath() {
        return m_paths;
    }

    @Override
    public String toString() {
        return "DbSettings [paths=" + m_paths + ", cluster=" + m_cluster
                + "]";
    }
}
