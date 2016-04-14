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
package org.voltdb;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import org.voltdb.common.Constants;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

/**
 *
 * @author akhanzode
 */
public class ConfigTracker implements ConfigProvider {
    private final DeploymentType m_deployment;

    private final Supplier<String> m_deploymentHashSupplier = Suppliers.memoize(new Supplier<String>() {
        @Override
        public String get() {
            try {
                String s = CatalogUtil.getDeployment(m_deployment);
                return Encoder.hexEncode(CatalogUtil.makeDeploymentHash(s.getBytes(Constants.UTF8ENCODING)));
            } catch (Exception e) {
                throw new RuntimeException("Failed to compute deployment hash.");
            }
        }
    });

    public ConfigTracker(DeploymentType deployment) {
        m_deployment = deployment;
    }

    @Override
    public int getHostCount() {
        //return host count
        return 0;
    }

    @Override
    public String getDeploymentHash() {
        return m_deploymentHashSupplier.get();
    }

    @Override
    public boolean isAdminModeStart() {
        if (m_deployment.getAdminMode() != null) {
            return m_deployment.getAdminMode().isAdminstartup();
        }
        return false;
    }

}
