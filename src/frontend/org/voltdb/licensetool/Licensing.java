/* This file is part of VoltDB.
 * Copyright (C) 2021 VoltDB Inc.
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

package org.voltdb.licensetool;

import org.voltdb.StartAction;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.apache.zookeeper_voltpatches.ZooKeeper;

/**
 * This interface has miscellaneous routines in support of license
 * handling. It has separate implementations for community and
 * enterprise builds.
 */
public interface Licensing {
    public LicenseApi getLicenseApi();
    public void loadLicenseApi(Configuration config);
    public LicenseApi createLicenseApi(String pathToLicense);
    public String determineEdition();
    public void logLicensingInfo();
    public void stageLicenseFile();
    public boolean validateLicense(int numberOfNodes, DrRoleType replicationRole, StartAction startAction);
    public void checkLicenseConsistency(ZooKeeper zk);
    public boolean outputLicense();
    public String isLicenseChangeAllowed(LicenseApi newLicense, LicenseApi currentLicense);
    public void updateLicenseApi(LicenseApi newApi);
    public String getLicenseSummary();
}
