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

package org.voltdb.plannerv2.guards;

import org.voltdb.VoltDB;
import org.voltdb.plannerv2.guards.CalciteCompatibilityCheck.DisapprovingCheck;

/**
 * Large queries cannot be handled by Calcite now.
 * Fail the check if the large query mode is enabled.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
@DisapprovingCheck
public class BanLargeQuery extends CalciteCompatibilityCheck {

    private static final boolean s_isLargeTempTableTarget =
            VoltDB.instance().getBackendTargetType().isLargeTempTableTarget;

    @Override protected final boolean doCheck(String sql) {
        // This is a disapproving check.
        // It does not approve any queries for Calcite processing under any circumstances.
        return false;
    }

    @Override protected boolean isFinal() {
        // If the large query mode is not enabled, this check's result will not be final.
        // The query still has chance to be approved by subsequent approving checks.
        // Otherwise, the result becomes final and no more subsequent approving checks will
        // be visited.
        return s_isLargeTempTableTarget;
    }
}
