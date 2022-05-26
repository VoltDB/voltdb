/* This file is part of VoltDB.
 * Copyright (C) 2020-2022 Volt Active Data Inc.
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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.licensing.Licensing;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.io.Files;

public class UpdateLicense extends VoltNTSystemProcedure {

    private final static String tmpFileName = ".tmpLicense";
    private final static VoltLogger log = new VoltLogger("HOST");
    private final static boolean requireCompleteCluster = true;

    private static VoltTable resultTable() {
        return new VoltTable(new VoltTable.ColumnInfo("STATUS", VoltType.BIGINT),
                             new VoltTable.ColumnInfo("ERR_MSG", VoltType.STRING));
    }

    // @LicenseValidation
    //
    // On every host, write the new license to a temporary file,
    // check update validity, and return the result.
    //
    public static class LicenseValidation extends VoltNTSystemProcedure {
        public VoltTable run(byte[] licenseBytes) {
            VoltDBInterface voltdb = VoltDB.instance();

            // Write the bytes into a temporary file in voltdb root
            File tmpLicense = new File(voltdb.getVoltDBRootPath(), tmpFileName);
            try {
                Files.write(licenseBytes, tmpLicense);
            } catch (IOException e) {
                return failTable("Failed to write the new license to disk: " + e.getMessage(), null);
            }

            // Validate the license format, check whether it can be applied
            String error= voltdb.getLicensing().prepareLicenseUpdate(tmpLicense.getAbsolutePath());
            if (error != null) {
                return failTable(error, tmpLicense);
            }

            // OK so far
            return successTable();
        }
    }

    // @LiveLicenseUpdate
    //
    // On every host: rename the file to its final name, switch
    // to the new API instance, and return the result.
    //
    public static class LiveLicenseUpdate extends VoltNTSystemProcedure {
        public VoltTable run() {
            VoltDBInterface voltdb = VoltDB.instance();

            // Check our temporary file still exists
            File tmpLicense = new File(voltdb.getVoltDBRootPath(), tmpFileName);
            if (!tmpLicense.exists()) {
                return failTable("The temporary licence file created earlier no longer exists: " + tmpLicense, null);
            }

            // Apply the update
            String error = voltdb.getLicensing().applyLicenseUpdate(tmpLicense.getAbsolutePath());
            if (error != null) {
                return failTable(error, tmpLicense);
            }

            // Complete
            return successTable();
        }
    }

    // Failure response from one of the two above procs
    private static VoltTable failTable(String error, File tmpFile) {
        log.warn(error);
        if (tmpFile != null) {
            tmpFile.delete();
        }
        VoltTable vt = resultTable();
        vt.addRow(VoltSystemProcedure.STATUS_FAILURE, error);
        return vt;
    }

    // Success response from ditto
    private static VoltTable successTable() {
        VoltTable vt = resultTable();
        vt.addRow(VoltSystemProcedure.STATUS_OK, "SUCCESS");
        return vt;
    }

    // Entry point for UpdateLicense
    public VoltTable[] run(byte[] licenseBytes) throws InterruptedException, ExecutionException {
        if (!MiscUtils.isPro()) {
            return failResponse("Not supported in Community Edition");
        }

        log.info("Received user request to update database license.");
        VoltDBInterface voltdb = VoltDB.instance();

        final HostMessenger hm = voltdb.getHostMessenger();
        final ZooKeeper zk = hm.getZK();
        String errMsg = VoltZK.createActionBlocker(zk, VoltZK.licenseUpdateInProgress, CreateMode.EPHEMERAL,
                                                   log, "live license update" );
        if (errMsg != null) {
            return failResponse(errMsg);
        }

        try {
            // Initial check before we do any real work; we may or may not
            // support partial updates, but even if we do, we warn the user
            if (!voltdb.isClusterComplete()) {
                if (requireCompleteCluster) {
                    return failResponse("Updating the license requires all cluster members to be operational in the cluster. " +
                                        "Please rejoin missing members to the cluster and retry the license update");
                }
                log.warn("Updating license with some nodes not operational");
            }
            final int liveCount1 = hm.getLiveHostIds().size();

            // Validate the new license on every host
            Map<Integer, ClientResponse> result;
            result = callNTProcedureOnAllHosts("@LicenseValidation", licenseBytes).get();
            String err = checkResult(result);
            if (err != null) {
                return failResponse(err);
            }

            // Any change in number of hosts? this is a little kludgy
            // but it's a useful check on our own operation.
            final int liveCount2 = hm.getLiveHostIds().size();
            if (liveCount2 != liveCount1) {
                String msg = String.format("Live host count changed from %d to %d while preparing to update license",
                                           liveCount1, liveCount2);
                if (requireCompleteCluster) {
                    return failResponse(msg);
                }
                log.warn(msg);
            }

            // Update the new license file on every host
            result = callNTProcedureOnAllHosts("@LiveLicenseUpdate").get();
            err = checkResult(result);
            if (err != null) {
                return failResponse(err);
            }

            // If this fails we're in trouble
            try {
                zk.setData(VoltZK.license, licenseBytes, -1);
            } catch (Exception ex) {
                return failResponse("Unable to upload the new license to ZK");
            }

            return successResponse();
        } finally {
            VoltZK.removeActionBlocker(zk, VoltZK.licenseUpdateInProgress, log);
        }
    }

    // Handles the result from execution of subordinate sysprocs on
    // all live hosts.
    private String checkResult(Map<Integer, ClientResponse> results) {
        Map<String, Set<Integer>> errorReport = Maps.newHashMap();

        // Collect unique error messages and track their host ids
        for (Entry<Integer, ClientResponse> e : results.entrySet()) {
            // Request failed to execute?
            if (e.getValue().getStatus() != ClientResponse.SUCCESS) {
                String err = "Unexpected failure: status " + e.getValue().getStatus();
                addToReport(errorReport, e.getKey(), err);
            }
            // Request returned failure message? (only one row expected)
            VoltTable[] resultArray= e.getValue().getResults();
            if (resultArray.length > 0) {
                VoltTable nodeResult = resultArray[0];
                while (nodeResult.advanceRow()) {
                    if (nodeResult.getLong("STATUS") != VoltSystemProcedure.STATUS_OK) {
                        String err = nodeResult.getString("ERR_MSG");
                        addToReport(errorReport, e.getKey(), err);
                    }
                }
            }
        }
        // Build single multiline string for result
        if (!errorReport.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Entry<String, Set<Integer>> e : errorReport.entrySet()) {
                sb.append("Host ").append(e.getValue()).append(" report: ").append(e.getKey()).append("\n");
            }
            return sb.toString();
        }
        return null;
    }

    // Builds map of error message to host ids
    private void addToReport(Map<String, Set<Integer>> report, int hostId, String err) {
        Set<Integer> reporters = report.get(err);
        if (reporters == null) {
            reporters = new HashSet<>();
        }
        reporters.add(hostId);
        report.put(err, reporters);
    }

    // Failure from UpdateLicense itself
    private static VoltTable[] failResponse(String error) {
        log.warn(error);
        VoltTable vt = resultTable();
        vt.addRow(VoltSystemProcedure.STATUS_FAILURE, error );
        return new VoltTable[] { vt };
    }

    // Success of UpdateLicense itself
    private static VoltTable[] successResponse() {
        log.info("License updated successfully.");
        VoltTable vt = resultTable();
        vt.addRow(VoltSystemProcedure.STATUS_OK, "SUCCESS");
        return new VoltTable[] { vt };
    }
}
