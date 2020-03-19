/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.utils.VoltTableUtil;

public class UpdateLicense extends VoltSystemProcedure {

    private final static VoltLogger log = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
            SysProcFragmentId.PF_updateLicenseBarrier,
            SysProcFragmentId.PF_updateLicenseBarrierAggregate,
            SysProcFragmentId.PF_updateLicense,
            SysProcFragmentId.PF_updateLicenseAggregate
        };
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // The purpose of first fragment is to sync all sites across database to the same position.
        // So far we only update expiration date and/or max host count allowed, but in the future if
        // feature enable/disable is included, the first fragment can also do
        if (fragmentId == SysProcFragmentId.PF_updateLicenseBarrier) {
            DependencyPair success = new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicenseBarrier,
                    new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.BIGINT) } ));
            log.info("Site " + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                    " reached license update barrier.");
            return success;

        } else if (fragmentId == SysProcFragmentId.PF_updateLicenseBarrierAggregate) {
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicenseBarrierAggregate,
                    VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_updateLicenseBarrier)));

        } else if (fragmentId == SysProcFragmentId.PF_updateLicense) {
            Object [] paramarr = params.toArray();
            byte [] licenseBytes = (byte[])paramarr[0];

            // write file to disk (some temp directory)
            // use licenseAPI to verify the license validity.
            // check if we support the license change (max host count/expiration date), reject other changes.
            // once verified copy it to voltdb root directory (replace the old one)
            // replace the RealVoltDB.m_licenseAPI on each node.
            // return failure response if any of above step fails.
            // unit tests.

            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateSettings, result);

        } else if (fragmentId == SysProcFragmentId.PF_updateLicenseAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_updateLicense));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicenseAggregate, result);

        } else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in UpdateSettings",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
    }

    private SynthesizedPlanFragment[] createBarrierFragment(byte[] licenseBytes) {

        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment(SysProcFragmentId.PF_updateLicenseBarrier, true,
                ParameterSet.emptyParameterSet());

        pfs[1] = new SynthesizedPlanFragment(SysProcFragmentId.PF_updateLicenseBarrierAggregate, false,
                ParameterSet.fromArrayNoCopy(licenseBytes));

        return pfs;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte[] licenseBytes) {

        executeSysProcPlanFragments(
                createBarrierFragment(licenseBytes), SysProcFragmentId.PF_updateLicenseBarrierAggregate);
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateLicense,  SysProcFragmentId.PF_updateLicenseAggregate, licenseBytes);
    }
}
