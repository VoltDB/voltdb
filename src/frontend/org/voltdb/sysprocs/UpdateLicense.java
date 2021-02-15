/* This file is part of VoltDB.
 * Copyright (C) 2020-2021 VoltDB Inc.
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
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.licensetool.Licensing;

import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.io.Files;

public class UpdateLicense extends VoltNTSystemProcedure {

    private final static String tmpFileName = ".tmpLicense";
    private final static VoltLogger log = new VoltLogger("HOST");

    //
    // @LicenseValidation
    //
    // On every host:
    //      Write the new license to a temporary file,
    //      Check the expiration date,
    //      Check if the changes are allowed,
    //      return the result.
    public static class LicenseValidation extends VoltNTSystemProcedure {
        public VoltTable run(byte[] licenseBytes) {
            VoltDBInterface voltdb = VoltDB.instance();
            VoltTable vt = constructResultTable();
            // Write the bytes into a temporary file in voltdb root
            File tmpLicense = new File(voltdb.getVoltDBRootPath(), tmpFileName);
            try {
                Files.write(licenseBytes, tmpLicense);
            } catch (IOException e) {
                return constructFailureResponse(vt, "Failed to write the new license to disk: " + e.getMessage(), null);
            }
            // validate the license format and signature

            LicenseApi newLicense = voltdb.getLicensing().createLicenseApi(tmpLicense.getAbsolutePath());
            if (newLicense == null) {
                return constructFailureResponse(vt, "Invalid license format", tmpLicense);
            }
            // Has the new license expired already?
            Calendar today = Calendar.getInstance();
            if (newLicense.expires().before(today)) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                return constructFailureResponse(vt,
                        "Failed to update the license because new license expires at " +
                                sdf.format(newLicense.expires().getTime()),
                        tmpLicense);
            }

            // Are the changes allowed?
            LicenseApi currentLicense = voltdb.getLicensing().getLicenseApi();
            String error = voltdb.getLicensing().isLicenseChangeAllowed(newLicense, currentLicense);
            if (error != null) {
                return constructFailureResponse(vt, error, tmpLicense);
            }
            vt.addRow(VoltSystemProcedure.STATUS_OK, "");
            return vt;
        }
    }

    //
    // @LiveLicenseUpdate
    //
    // On every host:
    //      Rename the temporary file under voltdbroot to license.xml,
    //      Update the license api,
    //      Return the result.
    public static class LiveLicenseUpdate extends VoltNTSystemProcedure {
        public VoltTable run() {
            VoltDBInterface voltdb = VoltDB.instance();
            VoltTable vt = constructResultTable();
            File tmpLicense = new File(voltdb.getVoltDBRootPath(), tmpFileName);
            if (!tmpLicense.exists()) {
                return constructFailureResponse(vt, "File not found: " + tmpLicense.getAbsolutePath(), null);
            }
            File licenseF = new File(voltdb.getVoltDBRootPath(), Constants.LICENSE_FILE_NAME);
            tmpLicense.renameTo(licenseF);
            LicenseApi newLicense = voltdb.getLicensing().createLicenseApi(licenseF.getAbsolutePath());
            if (newLicense == null) { // new license bad, old license already gone: double plus ungood
                return constructFailureResponse(vt, "Invalid license format", licenseF);
            }
            voltdb.getLicensing().updateLicenseApi(newLicense);
            vt.addRow(VoltSystemProcedure.STATUS_OK, "");
            return vt;
        }
    }

    private static VoltTable constructResultTable() {
        return new VoltTable(
                new VoltTable.ColumnInfo("STATUS", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ERR_MSG", VoltType.STRING));
    }

    private static VoltTable constructFailureResponse(VoltTable vt, String err, File tmpFile) {
        log.info(err);
        if (tmpFile != null) {
            tmpFile.delete();
        }
        vt.addRow(VoltSystemProcedure.STATUS_FAILURE, err);
        return vt;
    }

    private void addToReport(Map<String, Set<Integer>> report, int hostId, String err) {
        Set<Integer> reporters = report.get(err);
        if (reporters == null) {
            reporters = new HashSet<Integer>();
        }
        reporters.add(hostId);
        report.put(err, reporters);
    }

    private String checkResult(Map<Integer, ClientResponse> results) {
        Map<String, Set<Integer>> errorReport = Maps.newHashMap();
        for (Entry<Integer, ClientResponse> e : results.entrySet()) {
            if (e.getValue().getStatus() != ClientResponse.SUCCESS) {
                String err = "Unexpected failure: status " + e.getValue().getStatus();
                addToReport(errorReport, e.getKey(), err);
            }

            VoltTable nodeResult = e.getValue().getResults()[0];
            while (nodeResult.advanceRow()) {
                if (nodeResult.getLong("STATUS") != VoltSystemProcedure.STATUS_OK) {
                    String err = nodeResult.getString("ERR_MSG");
                    addToReport(errorReport, e.getKey(), err);
                }
            }
        }
        // aggregate same type of error into one row
        if (!errorReport.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Entry<String, Set<Integer>> e : errorReport.entrySet()) {
                sb.append("Host ").append(e.getValue()).append(" report: ").append(e.getKey()).append("\n");
            }
            return sb.toString();
        }
        return null;
    }

    public VoltTable[] run(byte[] licenseBytes) throws InterruptedException, ExecutionException {
        log.info("Received user request to update database license.");
        VoltTable vt = constructResultTable();

        final ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        String errMsg = VoltZK.createActionBlocker(zk, VoltZK.licenseUpdateInProgress, CreateMode.EPHEMERAL,
                log, "live license update" );
        if (errMsg != null) {
            log.info(errMsg);
            vt.addRow(VoltSystemProcedure.STATUS_FAILURE, errMsg);
            return new VoltTable[] { vt };
        }

        try {
            // validate the new license on every host
            Map<Integer, ClientResponse> result;
            result = callNTProcedureOnAllHosts("@LicenseValidation", licenseBytes).get();
            String err = checkResult(result);
            if (err != null) {
                vt.addRow(VoltSystemProcedure.STATUS_FAILURE, err);
                return new VoltTable[] { vt };
            }

            // update the new license on every host
            result = callNTProcedureOnAllHosts("@LiveLicenseUpdate").get();
            err = checkResult(result);
            if (err != null) {
                vt.addRow(VoltSystemProcedure.STATUS_FAILURE, err);
                return new VoltTable[] { vt };
            }
            try {
                zk.setData(VoltZK.license, licenseBytes, -1);
            } catch (KeeperException | InterruptedException e) {
                vt.addRow(VoltSystemProcedure.STATUS_FAILURE, "Unable to upload the new license to ZK");
                return new VoltTable[] { vt };
            }

            log.info("License updated successfully.");
            vt.addRow(VoltSystemProcedure.STATUS_OK, "SUCCESS");
            return new VoltTable[] { vt };
        } finally {
            VoltZK.removeActionBlocker(zk, VoltZK.licenseUpdateInProgress, log);
        }

    }
}
