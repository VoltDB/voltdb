/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.List;

import org.voltdb.planner.ParameterInfo;

/**
 * Provides access to some non-public SQLStmt methods needed for ad hoc queries.
 */
public class SQLStmtAdHocHelper {
    /**
     * Factory method to construct a SQLStmt instance with a compiled plan attached.
     *
     * @param sqlText Valid VoltDB compliant SQL
     * @param aggregatorFragment Compiled aggregator fragment
     * @param collectorFragment Compiled collector fragment
     * @param isReplicatedTableDML Flag set to true if replicated
     *
     * @return SQLStmt object with plan added
     */
    public static SQLStmt createWithPlan(String sqlText,
                                         String aggregatorFragment,
                                         String collectorFragment,
                                         boolean isReplicatedTableDML,
                                         List<ParameterInfo> params) {
        return SQLStmt.createWithPlan(sqlText, aggregatorFragment, collectorFragment, isReplicatedTableDML, params);
    }
}
