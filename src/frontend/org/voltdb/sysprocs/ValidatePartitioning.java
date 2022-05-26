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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.primitives.Longs;

/**
 * A system procedure for validating that the rows at every partition hash correctly when hashed with the
 * hash function stored at the MPI. Optionally you can supply your own hash function which is useful
 * for test where you want to make provide an incorrect hash function to prove
 * that it is actually doing the right thing.
 *
 */
public class ValidatePartitioning extends VoltSystemProcedure {
    private static final VoltLogger HOST_LOG = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[] {
            SysProcFragmentId.PF_validatePartitioningResults,
            SysProcFragmentId.PF_validatePartitioning,
            SysProcFragmentId.PF_matchesHashinatorResults,
            SysProcFragmentId.PF_matchesHashinator
        };
    }

    @Override
    public DependencyPair
    executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId, ParameterSet params,
                        final SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_validatePartitioning) {

            final VoltTable results = constructPartitioningResultsTable();
            List<Integer> tableIds = new ArrayList<Integer>();
            List<String> tableNames = new ArrayList<String>();
            for (SnapshotTableInfo t : SnapshotUtil.getTablesToSave(context.getDatabase(), t -> !t.getIsreplicated(),
                    true)) {
                tableIds.add(t.getTableId());
                tableNames.add(t.getName());
            }
            long mispartitionedCounts[] = context.getSiteProcedureConnection().validatePartitioning(
                    Longs.toArray(tableIds), (byte[])params.toArray()[0]);

            for (int ii = 0; ii < tableNames.size(); ii++) {
                results.addRow(context.getHostId(), CoreUtils.getSiteIdFromHSId(context.getSiteId()), context.getPartitionId(), tableNames.get(ii), mispartitionedCounts[ii]);
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_validatePartitioning, results);

        } else if (fragmentId == SysProcFragmentId.PF_validatePartitioningResults) {

            assert (dependencies.size() > 0);
            final VoltTable results = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_validatePartitioning));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_validatePartitioningResults, results);

        } else if (fragmentId == SysProcFragmentId.PF_matchesHashinator) {

            final VoltTable matchesHashinator = constructHashinatorMatchesTable();

            byte [] configBytes = (byte[])params.toArray()[0];
            final long givenConfigurationSignature =
                    TheHashinator.computeConfigurationSignature(configBytes);

            matchesHashinator.addRow(
                    context.getHostId(),
                    CoreUtils.getSiteIdFromHSId(context.getSiteId()),
                    context.getPartitionId(),
                    givenConfigurationSignature == TheHashinator.getConfigurationSignature() ? (byte)1 : (byte)0);

            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_matchesHashinator, matchesHashinator);

        } else if (fragmentId == SysProcFragmentId.PF_matchesHashinatorResults) {

            assert (dependencies.size() > 0);
            final VoltTable results = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_matchesHashinator));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_matchesHashinatorResults, results);

        }
        assert (false);
        return null;
    }

    private VoltTable constructPartitioningResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[5];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo(CNAME_SITE_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo(CNAME_PARTITION_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("MISPARTITIONED_ROWS", VoltType.BIGINT);
        return new VoltTable(result_columns);
    }

    private VoltTable constructHashinatorMatchesTable() {
        ColumnInfo [] columns = new ColumnInfo[] {
                new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
                new ColumnInfo(CNAME_SITE_ID, CTYPE_ID),
                new ColumnInfo(CNAME_PARTITION_ID, CTYPE_ID),
                new ColumnInfo("HASHINATOR_MATCHES", VoltType.TINYINT)
        };
        return new VoltTable(columns);
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte config[]) throws VoltAbortException
    {
        final long startTime = System.currentTimeMillis();
        VoltTable retval[];
        if (config != null) {
            retval = performValidatePartitioningWork(config );
        } else {
            retval = performValidatePartitioningWork(
                    TheHashinator.getCurrentConfig().configBytes);
        }
        final long endTime = System.currentTimeMillis();
        final long duration = endTime -startTime;
        HOST_LOG.info("Validating partitioning took " + duration + " milliseconds");
        return retval;
    }

    private final VoltTable[] performValidatePartitioningWork(byte[] config)
    {
        ArrayList<VoltTable> results = new ArrayList<VoltTable>();
        for (VoltTable t : createAndExecuteSysProcPlan(SysProcFragmentId.PF_validatePartitioning,
                SysProcFragmentId.PF_validatePartitioningResults, config)) {
            results.add(t);
        }

        for (VoltTable t : createAndExecuteSysProcPlan(SysProcFragmentId.PF_matchesHashinator,
                SysProcFragmentId.PF_matchesHashinatorResults, config)) {
            results.add(t);
        }

        return results.toArray(new VoltTable[0]);
    }

}
