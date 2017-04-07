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

import java.util.List;
import java.util.Map;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.SettingsException;
import org.voltdb.utils.VoltTableUtil;

@ProcInfo(singlePartition = false)
public class UpdateSettings extends VoltSystemProcedure {

    private static final int DEP_updateSettingsBarrier = (int)
            SysProcFragmentId.PF_updateSettingsBarrier | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_updateSettingsBarrierAggregate = (int)
            SysProcFragmentId.PF_updateSettingsBarrierAggregate;
    private static final int DEP_updateSettings = (int)
            SysProcFragmentId.PF_updateSettings | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_updateSettingsAggregate = (int)
            SysProcFragmentId.PF_updateSettingsAggregate;

    private final static VoltLogger log = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
            SysProcFragmentId.PF_updateSettingsBarrier,
            SysProcFragmentId.PF_updateSettingsBarrierAggregate,
            SysProcFragmentId.PF_updateSettings,
            SysProcFragmentId.PF_updateSettingsAggregate
        };
    }

    public UpdateSettings() {
    }

    private VoltDBInterface getVoltDB() {
        return VoltDB.instance();
    }

    private HostMessenger getHostMessenger() {
        return VoltDB.instance().getHostMessenger();
    }

    private VoltTable getVersionResponse(int version) {
        VoltTable table = new VoltTable(new ColumnInfo[] { new ColumnInfo("VERSION", VoltType.INTEGER) });
        table.addRow(version);
        return table;
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {

        if (fragmentId == SysProcFragmentId.PF_updateSettingsBarrier) {

            DependencyPair success = new DependencyPair.TableDependencyPair(DEP_updateSettingsBarrier,
                    new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.BIGINT) } ));
            if (log.isInfoEnabled()) {
                log.info("Site " + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                        " reached settings update barrier.");
            }
            return success;

        } else if (fragmentId == SysProcFragmentId.PF_updateSettingsBarrierAggregate) {

            Object [] paramarr = params.toArray();
            byte [] settingsBytes = (byte[])paramarr[0];
            int version = ((Integer)paramarr[1]).intValue();

            ZooKeeper zk = getHostMessenger().getZK();
            Stat stat = null;
            try {
                stat = zk.setData(VoltZK.cluster_settings, settingsBytes, version);
            } catch (KeeperException | InterruptedException e) {
                String msg = "Failed to update cluster settings";
                log.error(msg,e);
                throw new SettingsException(msg, e);
            }
            log.info("Saved new cluster settings state");
            return new DependencyPair.TableDependencyPair(DEP_updateSettingsBarrierAggregate,
                    getVersionResponse(stat.getVersion()));

        } else if (fragmentId == SysProcFragmentId.PF_updateSettings) {

            Object [] paramarr = params.toArray();
            byte [] settingsBytes = (byte[])paramarr[0];
            int version = ((Integer)paramarr[1]).intValue();

            ClusterSettings settings = ClusterSettings.create(settingsBytes);
            Pair<CatalogContext, CatalogSpecificPlanner> ctgdef =
                    getVoltDB().settingsUpdate(settings, version);

            context.updateSettings(ctgdef.getFirst(), ctgdef.getSecond());

            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair.TableDependencyPair(DEP_updateSettings, result);

        } else if (fragmentId == SysProcFragmentId.PF_updateSettingsAggregate) {

            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_updateSettings));
            return new DependencyPair.TableDependencyPair(DEP_updateSettingsAggregate, result);

        } else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in UpdateSettings",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
    }

    private SynthesizedPlanFragment[] createBarrierFragment(byte[] settingsBytes, int version) {

        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_updateSettingsBarrier;
        pfs[0].outputDepId = DEP_updateSettingsBarrier;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_updateSettingsBarrierAggregate;
        pfs[1].outputDepId = DEP_updateSettingsBarrierAggregate;
        pfs[1].inputDepIds = new int[] {DEP_updateSettingsBarrier};
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.fromArrayNoCopy(new Object[] { settingsBytes, version });

        return pfs;
    }

    private SynthesizedPlanFragment[] createUpdateFragment(byte[] settingsBytes, int version) {

        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_updateSettings;
        pfs[0].outputDepId = DEP_updateSettings;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(new Object[] { settingsBytes, version });

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_updateSettingsAggregate;
        pfs[1].outputDepId = DEP_updateSettingsAggregate;
        pfs[1].inputDepIds = new int[] {DEP_updateSettings};
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        return pfs;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte[] settingsBytes) {
        ZooKeeper zk = getHostMessenger().getZK();
        Stat stat = null;
        try {
            stat = zk.exists(VoltZK.cluster_settings, false);
        } catch (KeeperException | InterruptedException e) {
            String msg = "Failed to stat cluster settings zookeeper node";
            log.error(msg, e);
            throw new VoltAbortException(msg);
        }
        final int version = stat.getVersion();

        executeSysProcPlanFragments(
                createBarrierFragment(settingsBytes, version), DEP_updateSettingsBarrierAggregate);
        return executeSysProcPlanFragments(
                createUpdateFragment(settingsBytes, version), DEP_updateSettingsAggregate);
    }
}
