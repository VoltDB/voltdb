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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.VoltType;

/**
 * This is the clean up leg of Migrating table rows off of the partition
 * through the Export path. When the rows have been acknowledged by
 * Export, the rows can be removed from the Persistent Table.
  */
public class MigrateRowsAcked_MP extends VoltSystemProcedure {

    VoltLogger exportLog = new VoltLogger("EXPORT");
    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{SysProcFragmentId.PF_migrateRows, SysProcFragmentId.PF_migrateRowsAggregate};
    }

    @Override
    public long[] getAllowableSysprocFragIdsInTaskLog() {
        return new long[]{SysProcFragmentId.PF_migrateRows};
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer,List<VoltTable>> dependencies,
        long fragmentId, ParameterSet params, SystemProcedureExecutionContext context)
    {
        try {
            if (fragmentId == SysProcFragmentId.PF_migrateRows) {
                final String tableName = (String) params.toArray()[0];
                final long deletableTxnId = (Long) params.toArray()[1];

                final TransactionState txnState = m_runner.getTxnState();
                boolean txnRemainingDeleted = context.getSiteProcedureConnection().deleteMigratedRows(
                        txnState.txnId, txnState.m_spHandle, txnState.uniqueId,
                        tableName, deletableTxnId);
                VoltTable results = new VoltTable(new ColumnInfo(MigrateRowsDeleterNT.ROWS_TO_BE_DELETED, VoltType.BIGINT));
                results.addRow(txnRemainingDeleted ? 1 : 0);
                return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_migrateRows, results);
            }
            else if (fragmentId == SysProcFragmentId.PF_migrateRowsAggregate) {
                return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_migrateRowsAggregate,
                        VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_migrateRows)));
            }
        }
        catch (Exception ex) {
            exportLog.warn(String.format("Migrating delete error: %s", ex.getMessage()));
        }
        return null;
    }

    /**
    *
    * @param ctx execution context
    * @param tableName Name of table that can have rows deleted
    * @param deletableTxnId All rows with this transaction or the first transaction before this can be deleted
    * @return
    */
    public VoltTable run(SystemProcedureExecutionContext ctx, String tableName, long deletableTxnId) {
        VoltTable result = null;

        try {
            VoltTable [] results = createAndExecuteSysProcPlan(SysProcFragmentId.PF_migrateRows,
                    SysProcFragmentId.PF_migrateRowsAggregate,
                    ParameterSet.fromArrayNoCopy(tableName, deletableTxnId));
            if (results != null && results.length != 0) {
                result = results[0];
            }
        } catch (Exception ex) {
            exportLog.warn(String.format("Migrating delete error on table %s, error: %s", tableName, ex.getMessage()));
        }
        return result;
    }
}
