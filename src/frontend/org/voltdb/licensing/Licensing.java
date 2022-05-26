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

import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.apache.zookeeper_voltpatches.ZooKeeper;

/**
 * This interface supplies miscellaneous methods in support of
 * license handling. It has separate implementations for
 * community and enterprise builds.
 */
public interface Licensing {
    // VoltDB initialization
    public void readLicenseFile(Configuration config);
    public void logLicensingInfo();
    public void stageLicenseFile();
    public void validateLicense();
    public void checkLicenseConsistency(ZooKeeper zk);
    // for 'get license' CLI command
    public boolean outputLicense(Configuration config);
    // used by @SystemInformation
    public String getLicenseSummary();
    public void populateLicenseInfo(VoltTable results);
    // used by @UpdateLicense
    public String prepareLicenseUpdate(String newLicenceFile);
    public String applyLicenseUpdate(String newLicenceFile);
    // general information
    public String getLicenseType();
    public boolean isTrialLicense();
    public String getLicensee();
    public boolean isFeatureAllowed(String feature);
    public String getLicenseHash();
}
