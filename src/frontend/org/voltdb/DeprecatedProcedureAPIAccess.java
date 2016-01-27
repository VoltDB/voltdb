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

import org.voltdb.VoltProcedure.VoltAbortException;

/**
 * Version 6.0 removes some long deprecated functions from {@link org.voltdb.VoltProcedure VoltProcedure}.
 * This helper class allows internal bits of VoltDB and VoltDB test code to still call them, while making
 * it much harder for users to call them.
 * End users: Please don't call these methods. They may be removed without fanfare in a minor update.
 *
 */
public abstract class DeprecatedProcedureAPIAccess {

    /**
     * Returns the VoltDB 3.0 transaction ID which is a sequence number instead
     * of the time-based ID used in pre-3.0 VoltDB. It is less unique in that sequence numbers can revert
     * if you load data from one volt database into another via CSV or other external methods
     * that bypass the combination of snapshot restore/command log replay which maintains these per partition
     * sequence numbers.
     *
     * @deprecated Do not use outside of VoltDB internal code.
     * @param procedure {@link org.voltdb.VoltProcedure VoltProcedure} instance on which to access deprecated method.
     * @return VoltDB 3.0-esque transaction id.
     */
    @Deprecated
    public static long getVoltPrivateRealTransactionId(VoltProcedure procedure) {
        return procedure.m_runner.getTransactionId();
    }

    /**
     * YOU MUST BE RUNNING NTP AND START NTP WITH THE -x OPTION
     * TO GET GOOD BEHAVIOR FROM THIS METHOD - e.g. time always goes forward
     *
     * Allow VoltProcedures access to a unique ID generated for each transaction. Synonym of getUniqueID
     * that is kept around to support legacy applications
     *
     * The id consists of a time based component in the most significant bits followed
     * by a counter, and then a generator id to allow parallel unique number generation
     * @return transaction id
     * @deprecated Use the synonymous getUniqueId() instead
     * @param procedure {@link org.voltdb.VoltProcedure VoltProcedure} instance on which to access deprecated method.
     */
    @Deprecated
    public static long getTransactionId(VoltProcedure procedure) {
        return procedure.m_runner.getUniqueId();
    }

    /**
     * <p>Currently unsupported in VoltDB.</p>
     * <p>Batch load method for populating a table with a large number of records.</p>
     *
     * <p>Faster then calling {@link #voltQueueSQL(SQLStmt, Object...)} and {@link #voltExecuteSQL()} to
     * insert one row at a time.</p>
     *
     * @deprecated This method is not fully tested to be used in all contexts.
     * @param procedure {@link org.voltdb.VoltProcedure VoltProcedure} instance on which to access deprecated method.
     * @param clusterName Name of the cluster containing the database, containing the table
     *                    that the records will be loaded in.
     * @param databaseName Name of the database containing the table to be loaded.
     * @param tableName Name of the table records should be loaded in.
     * @param data {@link org.voltdb.VoltTable VoltTable} containing the records to be loaded.
     *             {@link org.voltdb.VoltTable.ColumnInfo VoltTable.ColumnInfo} schema must match the schema of the table being
     *             loaded.
     * @param returnUniqueViolations If true will not fail on unique violations, will return the violating rows.
     * @return A byte array representing constraint violations in a semi-opaque format.
     * @throws VoltAbortException on failure.
     */
    @Deprecated
    public static byte[] voltLoadTable(VoltProcedure procedure,
                                String clusterName,
                                String databaseName,
                                String tableName,
                                VoltTable data,
                                boolean returnUniqueViolations,
                                boolean shouldDRStream)
    throws VoltAbortException
    {
        return procedure.m_runner.voltLoadTable(clusterName, databaseName, tableName, data, returnUniqueViolations, shouldDRStream);
    }
}
