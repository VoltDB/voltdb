/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.sqlfunctions;

import com.google.common.collect.ImmutableList;
import org.voltcore.utils.Pair;

/**
 * Implementation of calls to VoltDB functions through Calcite.
 * <p>
 * Steps to implement a function through Calcite:
 * <ul>
 * <li>Create a static class with a static {@code eval} method.
 * {@link org.apache.calcite.util.Smalls} in the Calcite project has many detailed examples.
 * <li>The {@code eval} method is used to validate the SQL query, we only care about its signature,
 * you can return whatever value you want.
 * <li>Put the function class together with its name into {@link #VOLT_SQL_FUNCTIONS}.
 *
 * @author Chao Zhou
 * @since 9.1
 */
public class VoltSqlFunctions {
    // the name, class pair of volt extend sql functions
    public static final ImmutableList<Pair<String, Class<?>>> VOLT_SQL_FUNCTIONS =
            ImmutableList.of(Pair.of("MIGRATING", Migrating.class));

    //-------------------------------------------------------------
    //                   volt extend sql functions
    //-------------------------------------------------------------

    // MIGRATING() function
    public static class Migrating {
        public static boolean eval() {
            // we only need the sql function in validate&plan phase, and the return value doesn't matter.
            // Calcite won't evaluate the function during planning
            return true;
        }
    }
}
