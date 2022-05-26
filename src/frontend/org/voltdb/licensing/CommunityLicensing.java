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

package org.voltdb.licensing;

import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.utils.MiscUtils;

import org.apache.zookeeper_voltpatches.ZooKeeper;

/**
 * This class holds miscellaneous routines in support of license
 * handling. This is for the community edition of VoltDB, and
 * many of the methods are simply stubs.
 */
public final class CommunityLicensing implements Licensing {

    @Override
    public void readLicenseFile(Configuration config) {
        if (MiscUtils.isPro()) {
            VoltDB.crashLocalVoltDB("Internal error: valid only for Community Edition");
        }
    }

    @Override
    public void logLicensingInfo() {
        // nothing
    }

    @Override
    public void stageLicenseFile() {
        // nothing
    }

    @Override
    public void validateLicense() {
        // nothing
    }

    @Override
    public void checkLicenseConsistency(ZooKeeper zk) {
        // nothing
    }

    @Override
    public boolean outputLicense(Configuration config) {
        unsupported();
        return false;
    }

    @Override
    public String getLicenseSummary() {
        unsupported();
        return null;
    }

    @Override
    public void populateLicenseInfo(VoltTable results) {
        results.addRow("TYPE", "CommunityEdition");
    }

    @Override
    public String prepareLicenseUpdate(String newLicFile) {
        unsupported();
        return "ERR";
    }

    @Override
    public String applyLicenseUpdate(String newLicFile) {
        unsupported();
        return "ERR";
    }

    @Override
    public String getLicenseType() {
        return "Community Edition";
    }

    @Override
    public boolean isTrialLicense() {
        return false;
    }

    @Override
    public String getLicensee() {
        return "VoltDB Community Edition User";
    }

    @Override
    public boolean isFeatureAllowed(String feature) {
        return false;
    }

    @Override
    public String getLicenseHash() {
        return "";
    }

    private void unsupported() {
        throw new RuntimeException("Licensing method not supported in Community Edition");
    }
}
