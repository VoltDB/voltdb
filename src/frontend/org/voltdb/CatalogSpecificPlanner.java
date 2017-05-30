/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.sysprocs.AdHocNTBase;
import org.voltdb.sysprocs.AdHocNTBase.AdHocPlanningException;
import org.voltdb.sysprocs.AdHocNTBase.AdHocSQLMix;

/*
 * Wrapper around a planner tied to a specific catalog version. This planner
 * is specifically configured to generate plans from within a stored procedure
 * so it will give a slightly different set of config to the planner.
 */
public class CatalogSpecificPlanner {
    private final CatalogContext m_catalogContext;

    public CatalogSpecificPlanner(CatalogContext context) {
        m_catalogContext = context;
    }

    public AdHocPlannedStmtBatch plan(String sql, Object[] userParams, boolean singlePartition) throws AdHocPlanningException {

        List<String> sqlStatements = new ArrayList<>();
        AdHocSQLMix mix = AdHocNTBase.processAdHocSQLStmtTypes(sql, sqlStatements);

        switch (mix) {
        case EMPTY:
            throw new AdHocPlanningException("No valid SQL found.");
        case ALL_DDL:
        case MIXED:
            throw new AdHocPlanningException("DDL not supported in stored procedures.");
        default:
            break;
        }

        if (sqlStatements.size() != 1) {
            throw new AdHocPlanningException("Only one statement is allowed in stored procedure, but received " + sqlStatements.size());
        }

        sql = sqlStatements.get(0);

        // any object will signify SP
        Object partitionKey = singlePartition ? "1" : null;

        List<AdHocPlannedStatement> stmts = new ArrayList<>();
        AdHocPlannedStatement result = null;

        result = AdHocNTBase.compileAdHocSQL(m_catalogContext,
                                             sql,
                                             false,
                                             partitionKey,
                                             ExplainMode.NONE,
                                             false,
                                             userParams);
        stmts.add(result);


        return new AdHocPlannedStmtBatch(userParams,
                                         stmts,
                                         -1,
                                         null,
                                         null,
                                         userParams);
    }
}
