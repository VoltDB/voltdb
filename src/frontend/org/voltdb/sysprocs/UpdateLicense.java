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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.collect.Maps;
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

    private VoltTable constructResultTable() {
       return new VoltTable(
                new VoltTable.ColumnInfo("HOST_ID", VoltType.BIGINT),
                new VoltTable.ColumnInfo("STATUS", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ERR_MSG", VoltType.STRING));
    }

    // return error message if found error
    private String checkError(VoltTable fragmentResponse) {
        Map<String, Set<Integer>> errorReport = Maps.newHashMap();
        while (fragmentResponse.advanceRow()) {
            // generate error report, unique error
            if (fragmentResponse.getLong("STATUS") != VoltSystemProcedure.STATUS_OK) {
                String errMsg = fragmentResponse.getString("ERR_MSG");
                int hostId = (int)fragmentResponse.getLong("HOST_ID");
                Set<Integer> reporters = errorReport.get(errMsg);
                if (reporters == null) {
                    reporters = new HashSet<Integer>();
                }
                reporters.add(hostId);
                errorReport.put(errMsg, reporters);
            }
        }
        if (!errorReport.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Entry<String, Set<Integer>> e : errorReport.entrySet()) {
                sb.append("Host ").append(e.getValue()).append(" report: ").append(e.getKey()).append("\n");
            }
            return sb.toString();
        }
        return null;
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // The purpose of first fragment is to sync all sites across database to the same position.
        if (fragmentId == SysProcFragmentId.PF_updateLicenseBarrier) {
            VoltTable result = constructResultTable();
            // The lowest site validate the new license, other sites return empty table
            if (context.isLowestSiteId()) {
                Object [] paramarr = params.toArray();
                byte [] licenseBytes = (byte[])paramarr[0];
                // Write the bytes into a temporary file in voltdb root
                File tempLicense = new File(VoltDB.instance().getVoltDBRootPath(), ".temp_content");
                try {
                    Files.write(licenseBytes, tempLicense);
                    result.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                            VoltSystemProcedure.STATUS_OK, "");
                } catch (IOException e) {
                    String errMsg = "Failed to write the new license to disk: " + e.getMessage();
                    log.info(errMsg);
                    result.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                            VoltSystemProcedure.STATUS_FAILURE,
                            errMsg);
                }
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicenseBarrier, result);

        } else if (fragmentId == SysProcFragmentId.PF_updateLicenseBarrierAggregate) {
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateLicenseBarrierAggregate,
                    VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_updateLicenseBarrier)));

        } else if (fragmentId == SysProcFragmentId.PF_updateLicense) {
            VoltTable result = constructResultTable();
            if (context.isLowestSiteId()) {
                // validate the license format and signature
                File tempLicense = new File(VoltDB.instance().getVoltDBRootPath(), ".temp_content");
                LicenseApi newLicense = MiscUtils.licenseApiFactory(tempLicense.getAbsolutePath());
                if (newLicense == null) {
                    tempLicense.delete();
                    String errMsg = "The license we try to update to is invalid";
                    log.info(errMsg);
                    result.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                            VoltSystemProcedure.STATUS_FAILURE,
                            errMsg);
                }
                // Does the new license expire?

                // Are the license changes allowed?
                LicenseApi currentLicense = VoltDB.instance().getLicenseApi();
                String error = MiscUtils.isLicenseChangeAllowed(newLicense, currentLicense);
                if (error == null) {
                    // ok to update, rename temporary file to canonical name
                    tempLicense.renameTo(new File(VoltDB.instance().getVoltDBRootPath(), "license.xml"));
                    VoltDB.instance().updateLicenseApi(newLicense);

                    result.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                            VoltSystemProcedure.STATUS_OK, "");
                } else {
                    log.info(error);
                    tempLicense.delete();
                    result.addRow(VoltDB.instance().getHostMessenger().getHostId(),
                            VoltSystemProcedure.STATUS_FAILURE,
                            error);
                }
            }
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

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte[] licenseBytes) {
        VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);

        log.info("Received user request to update database license.");
        // community is not allowed to live-update license.
        LicenseApi api = VoltDB.instance().getLicenseApi();
        if (MiscUtils.isCommunity(api)) {
            String errMsg = "License update command is not allowed in community version.";
            log.info(errMsg);
            throw new VoltAbortException(errMsg);
        }
        VoltTable[] fragmentResult =
                createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateLicenseBarrier,
                        SysProcFragmentId.PF_updateLicenseBarrierAggregate, licenseBytes);
        String errMsg = checkError(fragmentResult[0]);
        if (errMsg != null) {
            log.info(errMsg);
            throw new VoltAbortException(errMsg);
        }

        fragmentResult = createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateLicense,
                SysProcFragmentId.PF_updateLicenseAggregate);
        errMsg = checkError(fragmentResult[0]);
        if (errMsg != null) {
            log.info(errMsg);
            throw new VoltAbortException(errMsg);
        }
        log.info("License is updated successfully.");
        result.addRow(VoltSystemProcedure.STATUS_OK);
        return new VoltTable[] { result };

    }
}
