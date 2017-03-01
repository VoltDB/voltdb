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

package org.voltdb.sysprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.common.Constants;
import org.voltdb.compiler.AdHocPlannedStatement;

/**
 * Execute a user-provided read-write multi-partition SQL statement.
 * This code coordinates the execution of the plan fragments generated by the
 * embedded planner process.
 *
 * AdHocBase implements the core logic.
 * This subclass is needed for @ProcInfo.
 */
@ProcInfo(singlePartition = false)
public class AdHoc_RW_MP extends AdHocBase {

    Database m_db = null;

    /**
     * System procedure run hook.
     * Use the base class implementation.
     *
     * @param ctx  execution context
     * @param aggregatorFragments  aggregator plan fragments
     * @param collectorFragments  collector plan fragments
     * @param sqlStatements  source SQL statements
     * @param replicatedTableDMLFlags  flags set to 1 when replicated
     *
     * @return  results as VoltTable array
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte[] serializedBatchData) {
        AdHocPlannedStatement[] statements = decodeSerializedBatchData(serializedBatchData).getSecond();
        if (statements.length == 1) {
            String sqlStmt = new String(statements[0].sql, Constants.UTF8ENCODING);
            if (sqlStmt.startsWith("@SwapTables ")) {
                String[] args = sqlStmt.split(" ");
                VoltDB.instance().swapTables(args[1], args[2]);
            }
        }
        return runAdHoc(ctx, serializedBatchData);
    }

}
