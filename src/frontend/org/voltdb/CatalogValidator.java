/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.deploymentfile.DeploymentType;

/**
 * Interface to be implemented by components that need to be notified of catalog updates
 * and need to run validations on the catalog updates.
 * The validators need to register themselves with VoltDB instance to be notified of catalog updates
 * for validations.
 */
public interface CatalogValidator {
    /**
     * Validates the parts of the catalog relevant for this component.
     * @param newDep the updated deployment
     * @param curDep current deployment
     * @param ccr the results of validation including any errors need to be set on this result object
     * @return boolean indicating if the validation was successful or not.
     */
    public boolean validateDeploymentUpdates(DeploymentType newDep, DeploymentType curDep, CatalogChangeResult ccr);
}
