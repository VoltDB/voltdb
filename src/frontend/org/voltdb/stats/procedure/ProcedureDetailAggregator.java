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
package org.voltdb.stats.procedure;

import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.compiler.ProcedureCompiler;

public class ProcedureDetailAggregator {

    private final ReadOnlyProcedureInformation readOnlyProcedureInformation;

    public static ProcedureDetailAggregator create() {
        return new ProcedureDetailAggregator(new ReadOnlyProcedureInformation());
    }

    public ProcedureDetailAggregator(ReadOnlyProcedureInformation readOnlyProcedureInformation) {
        this.readOnlyProcedureInformation = readOnlyProcedureInformation;
    }

    public VoltTable[] sortProcedureDetailStats(VoltTable[] baseStats) {
        ProcedureDetailResultTable result = new ProcedureDetailResultTable(baseStats[0]);
        return result.getSortedResultTable();
    }

    /**
     * Produce PROCEDURE aggregation of PROCEDURE subselector
     * Basically it leaves out the rows that were not labeled as "<ALL>".
     */
    public VoltTable[] aggregateProcedureStats(VoltTable[] baseStats) {
        VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("TIMESTAMP", VoltType.BIGINT),
                new VoltTable.ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
                new VoltTable.ColumnInfo("HOSTNAME", VoltType.STRING),
                new VoltTable.ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID),
                new VoltTable.ColumnInfo("PARTITION_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING),
                new VoltTable.ColumnInfo("INVOCATIONS", VoltType.BIGINT),
                new VoltTable.ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT),
                new VoltTable.ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT),
                new VoltTable.ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT),
                new VoltTable.ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT),
                new VoltTable.ColumnInfo("MIN_RESULT_SIZE", VoltType.INTEGER),
                new VoltTable.ColumnInfo("MAX_RESULT_SIZE", VoltType.INTEGER),
                new VoltTable.ColumnInfo("AVG_RESULT_SIZE", VoltType.INTEGER),
                new VoltTable.ColumnInfo("MIN_PARAMETER_SET_SIZE", VoltType.INTEGER),
                new VoltTable.ColumnInfo("MAX_PARAMETER_SET_SIZE", VoltType.INTEGER),
                new VoltTable.ColumnInfo("AVG_PARAMETER_SET_SIZE", VoltType.INTEGER),
                new VoltTable.ColumnInfo("ABORTS", VoltType.BIGINT),
                new VoltTable.ColumnInfo("FAILURES", VoltType.BIGINT),
                new VoltTable.ColumnInfo("TRANSACTIONAL", VoltType.TINYINT),
                new VoltTable.ColumnInfo("COMPOUND", VoltType.TINYINT));
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
            if (baseStats[0].getString("STATEMENT").equalsIgnoreCase("<ALL>")) {
                result.addRow(
                        baseStats[0].getLong("TIMESTAMP"),
                        baseStats[0].getLong(VoltSystemProcedure.CNAME_HOST_ID),
                        baseStats[0].getString("HOSTNAME"),
                        baseStats[0].getLong(VoltSystemProcedure.CNAME_SITE_ID),
                        baseStats[0].getLong("PARTITION_ID"),
                        baseStats[0].getString("PROCEDURE"),
                        baseStats[0].getLong("INVOCATIONS"),
                        baseStats[0].getLong("TIMED_INVOCATIONS"),
                        baseStats[0].getLong("MIN_EXECUTION_TIME"),
                        baseStats[0].getLong("MAX_EXECUTION_TIME"),
                        baseStats[0].getLong("AVG_EXECUTION_TIME"),
                        baseStats[0].getLong("MIN_RESULT_SIZE"),
                        baseStats[0].getLong("MAX_RESULT_SIZE"),
                        baseStats[0].getLong("AVG_RESULT_SIZE"),
                        baseStats[0].getLong("MIN_PARAMETER_SET_SIZE"),
                        baseStats[0].getLong("MAX_PARAMETER_SET_SIZE"),
                        baseStats[0].getLong("AVG_PARAMETER_SET_SIZE"),
                        baseStats[0].getLong("ABORTS"),
                        baseStats[0].getLong("FAILURES"),
                        (byte) baseStats[0].getLong("TRANSACTIONAL"),
                        (byte) baseStats[0].getLong("COMPOUND"));
            }
        }
        return new VoltTable[]{result};
    }

    /**
     * Produce PROCEDUREPROFILE aggregation of PROCEDURE subselector
     */
    public VoltTable[] aggregateProcedureProfileStats(VoltTable[] baseStats) {
        ProcedureProfileStatisticsTable statisticsTable = new ProcedureProfileStatisticsTable();
        aggregateProcedureStats(statisticsTable, baseStats[0]);

        return new VoltTable[]{statisticsTable.getSortedTable()};
    }

    /**
     * Produce PROCEDUREINPUT aggregation of PROCEDURE subselector
     */
    public VoltTable[] aggregateProcedureInputStats(VoltTable[] baseStats) {
        InputProcedureStatisticsTable statisticsTable = new InputProcedureStatisticsTable();
        aggregateProcedureStats(statisticsTable, baseStats[0]);

        return new VoltTable[]{statisticsTable.getSortedTable()};
    }

    /**
     * Produce PROCEDUREOUTPUT aggregation of PROCEDURE subselector
     */
    public VoltTable[] aggregateProcedureOutputStats(VoltTable[] baseStats) {
        OutputProcedureStatisticsTable statisticsTable = new OutputProcedureStatisticsTable();
        aggregateProcedureStats(statisticsTable, baseStats[0]);

        return new VoltTable[]{statisticsTable.getSortedTable()};
    }

    public void aggregateProcedureStats(ProcedureStatisticsTable procedureStatisticsTable, VoltTable baseStats) {
        baseStats.resetRowPosition();
        while (baseStats.advanceRow()) {
            // Skip non-transactional procedures for some of these rollups until
            // we figure out how to make them less confusing.
            // NB: They still show up in the raw PROCEDURE stata.
            boolean transactional = baseStats.getLong("TRANSACTIONAL") == 1;
            if (!transactional) {
                continue;
            }

            if (!baseStats.getString("STATEMENT").equalsIgnoreCase("<ALL>")) {
                continue;
            }

            String pname = baseStats.getString("PROCEDURE");
            boolean shouldDeduplicate = !readOnlyProcedureInformation.isReadOnlyProcedure(pname);

            procedureStatisticsTable.updateTable(
                    shouldDeduplicate,
                    pname,
                    baseStats.fetchRow(baseStats.getActiveRowIndex()
                    )
            );
        }
    }

    /**
     * Produce COMPOUNDPROCSUMMARY aggregation of PROCEDURE subselector.
     * Combines rows from multiple hosts into a single row per procedure.
     */
    public VoltTable[] aggregateCompoundProcSummary(VoltTable[] baseStatsArray){
        CompoundProcSummaryStatisticsTable statisticsTable = new CompoundProcSummaryStatisticsTable();
        VoltTable baseStats = baseStatsArray[0];
        baseStats.resetRowPosition();
        while (baseStats.advanceRow()) {
            if (baseStats.getLong("COMPOUND") != 0) {
                statisticsTable.updateTable(baseStats.fetchRow(baseStats.getActiveRowIndex()));
            }
        }
        return new VoltTable[] { statisticsTable.getSortedTable() };
    }

    /**
     * Produce COMPOUNDPROC subset of PROCEDURE subselector.
     * Selects only those rows applicable to compound procedures.
     */
    public VoltTable[] aggregateCompoundProcByHost(VoltTable[] baseStatsArray){
        CompoundProcStatisticsTable statisticsTable = new CompoundProcStatisticsTable();
        VoltTable baseStats = baseStatsArray[0];
        baseStats.resetRowPosition();
        while (baseStats.advanceRow()) {
            if (baseStats.getLong("COMPOUND") != 0) {
                 statisticsTable.updateTable(baseStats.fetchRow(baseStats.getActiveRowIndex()));
            }
        }
        return new VoltTable[] { statisticsTable.getSortedTable() };
    }

    /**
     * Utility routine to compute short procedure name from class name.
     * Uses same algorithm as when procedure was added to catalog.
     */
    static String getShortProcedureName(String className) {
        return ProcedureCompiler.deriveShortProcedureName(className);
    }
}
