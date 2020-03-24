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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.io.Files;

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
            return success;

        } else if (fragmentId == SysProcFragmentId.PF_updateLicenseBarrierAggregate) {
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicenseBarrierAggregate,
                    VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_updateLicenseBarrier)));

        } else if (fragmentId == SysProcFragmentId.PF_updateLicense) {
            Object [] paramarr = params.toArray();
            byte [] licenseBytes = (byte[])paramarr[0];
            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);

            if (context.isLowestSiteId()) {
                // write file to disk (some temp directory)
                File tempLicense = new File(VoltDB.instance().getVoltDBRootPath(), ".temp_content");
                try {
                    Files.write(licenseBytes, tempLicense);
                } catch (IOException e) {
                    log.info("Failed to write the new license to disk: " + e.getMessage());
                    result.addRow(VoltSystemProcedure.STATUS_FAILURE);
                    return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicense, result);
                }
                // validate the license
                LicenseApi newLicense = MiscUtils.licenseApiFactory(tempLicense.getAbsolutePath());
                if (newLicense == null) {
                    log.info("New license is invalid.");
                    tempLicense.delete();
                    result.addRow(VoltSystemProcedure.STATUS_FAILURE);
                    return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicense, result);
                }
                LicenseApi currentLicense = VoltDB.instance().getLicenseApi();
                if (MiscUtils.isLicenseChangeAllowed(newLicense, currentLicense)) {
                    // replace the staged license file in voltdb root
                    tempLicense.renameTo(new File(VoltDB.instance().getVoltDBRootPath(), "license.xml"));
                    VoltDB.instance().updateLicenseApi(newLicense);
                } else {
                    log.info("Failed to update the license, changes include license type, hard/soft expiration "
                            + "and/or features enabled are not allowed to live-update.");
                    tempLicense.delete();
                    result.addRow(VoltSystemProcedure.STATUS_FAILURE);
                    return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicense, result);
                }
            }
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicense, result);

        } else if (fragmentId == SysProcFragmentId.PF_updateLicenseAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_updateLicense));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicenseAggregate, result);

        } else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in UpdateLicense",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String license) {
        byte[] licenseBytes;
        try {
            licenseBytes = license.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            String errMsg = "Failed to encode the license: " + e.getMessage();
            log.error(errMsg);
            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_FAILURE);
            return new VoltTable[] {result};
        }
        createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateLicenseBarrier, SysProcFragmentId.PF_updateLicenseBarrierAggregate);
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateLicense,  SysProcFragmentId.PF_updateLicenseAggregate, licenseBytes);
    }
}
