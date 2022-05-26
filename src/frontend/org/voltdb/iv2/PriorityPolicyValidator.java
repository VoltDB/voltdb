/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.iv2;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogValidator;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.PriorityPolicyType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;

/**
 * A class validating the priority policy specified in deployment.
 * <p>
 * Notes:
 * <ul>
 * <li>The priority policy is IMMUTABLE, but this is enforced by {@link CatalogDiffEngine},
 *      and not by this validator.</li>
 * <li>Most of the syntax is validated by the XSD specification.</li>
 * </ul>
 */
public class PriorityPolicyValidator extends CatalogValidator {
    private static VoltLogger s_logger = new VoltLogger("COMPILER");

    @Override
    public boolean validateDeployment(Catalog catalog, DeploymentType newDep, DeploymentType curDep, CatalogChangeResult ccr) {
        PriorityPolicyType ppNew = PriorityPolicy.getPolicyFromDeployment(newDep);
        if (curDep == null) {
            // On first deployment, validated by xsd
            if (!ppNew.isEnabled()) {
                s_logger.info("No priorities used for transactions");
            }
            else {
                s_logger.info(PriorityPolicy.toString(ppNew, "Priorities used for transactions"));

                // Check if a snapshot delay factor is defined and log a warning
                SystemSettingsType sys = newDep.getSystemsettings();
                SystemSettingsType.Snapshot snap = sys.getSnapshot();
                if (snap != null && snap.getPriority() > 0) {
                    s_logger.warn("The snapshot priority under 'systemsettings' is superseded "
                            + "by the snapshot priority under 'priorities' and will be ignored");
                }
            }
        }
        return true;
    }
}
