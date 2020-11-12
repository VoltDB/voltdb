/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.task;

import org.voltdb.CatalogValidator;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;

public class TaskValidator extends CatalogValidator {

    @Override
    public boolean validateConfiguration(Catalog catalog, DeploymentType deployment,
            InMemoryJarfile catalogJar, CatalogChangeResult ccr) {
        String taskErrors = TaskManager.validateTasks(CatalogUtil.getDatabase(catalog), catalogJar.getLoader());
        if (taskErrors != null) {
            ccr.errorMsg = taskErrors;
            return false;
        }
        return true;
    }
}
