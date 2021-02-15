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

import java.io.File;
import java.util.Calendar;

import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.utils.MiscUtils;

import org.apache.zookeeper_voltpatches.ZooKeeper;

/**
 * This class holds miscellaneous routines in support of license
 * handling. This is for the community edition of VoltDB, and
 * many of the methods are simply stubs.
 */
public final class CommunityLicensing implements Licensing {

    private static final String edition = "Community Edition";
    private static final String licensee = "VoltDB Community Edition User";

    private LicenseApi m_licenseApi;

    @Override
    public LicenseApi getLicenseApi() {
        return m_licenseApi;
    }

    @Override
    public void loadLicenseApi(Configuration config) {
        if (MiscUtils.isPro()) {
            VoltDB.crashLocalVoltDB("Internal error: valid only for Community Edition");
        }
        m_licenseApi = communityLicenseApi();
    }

    @Override
    public LicenseApi createLicenseApi(String pathToLicense) {
        unsupported();
        return null;
    }

    @Override
    public String determineEdition() {
        return edition;
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
    public boolean validateLicense(int numberOfNodes, DrRoleType replicationRole, StartAction startAction) {
        return true; // TODO
    }

    @Override
    public void checkLicenseConsistency(ZooKeeper zk) {
        // nothing
    }

    @Override
    public boolean outputLicense() {
        unsupported();
        return false;
    }

    @Override
    public String isLicenseChangeAllowed(LicenseApi newLicense, LicenseApi currentLicense) {
        unsupported();
        return "No";
    }

    @Override
    public void updateLicenseApi(LicenseApi newApi) {
        unsupported();
    }

    @Override
    public String getLicenseSummary() {
        unsupported();
        return null;
    }

    private void unsupported() {
        throw new RuntimeException("Licensing method not supported in " + edition);
    }

    private static LicenseApi communityLicenseApi() {
        return new LicenseApi() {
            @Override
            public boolean initializeFromFile(File license) {
                return true;
            }

            @Override
            public boolean isAnyKindOfTrial() {
                return false;
            }

            @Override
            public boolean isProTrial() {
                return false;
            }

            @Override
            public boolean isEnterpriseTrial() {
                return false;
            }

            @Override
            public int maxHostcount() {
                return Integer.MAX_VALUE;
            }

            @Override
            public Calendar expires() {
                Calendar result = Calendar.getInstance();
                result.add(Calendar.YEAR, 20); // good enough?
                return result;
            }

            @Override
            public boolean verify() {
                return true;
            }

            @Override
            public boolean isDrReplicationAllowed() {
                return false;
            }

            @Override
            public boolean isDrActiveActiveAllowed() {
                return false;
            }

            @Override
            public boolean isCommandLoggingAllowed() {
                return false;
            }

            @Override
            public boolean isAWSMarketplace() {
                return false;
            }

            @Override
            public boolean isEnterprise() {
                return false;
            }

            @Override
            public boolean isPro() {
                return false;
            }

            @Override
            public String licensee() {
                return CommunityLicensing.licensee;
            }

            @Override
            public Calendar issued() {
                Calendar result = Calendar.getInstance();
                return result;
            }

            @Override
            public String note() {
                return "";
            }

            @Override
            public boolean hardExpiration() {
                return false;
            }

            @Override
            public boolean secondaryInitialization() {
                return true;
            }

            @Override
            public String getSignature() {
                return null;
            }

            @Override
            public String getLicenseType() {
                return CommunityLicensing.edition;
            }

            @Override
            public boolean isUnrestricted() {
                return false;
            }

            @Override
            public String getIssuerCompany()
            {
                return null;
            }

            @Override
            public String getIssuerUrl()
            {
                return null;
            }

            @Override
            public String getIssuerEmail()
            {
                return null;
            }

            @Override
            public String getIssuerPhone()
            {
                return null;
            }

            @Override
            public int getVersion()
            {
                return 0;
            }

            @Override
            public int getScheme()
            {
                return 0;
            }
        };
    }
}
