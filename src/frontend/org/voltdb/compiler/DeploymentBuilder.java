/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.compiler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedHashSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.voltdb.VoltDB;
import org.voltdb.compiler.deploymentfile.AdminModeType;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.HttpdType.Jsonapi;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType.Snapshot;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.PathsType.Voltdbroot;
import org.voltdb.compiler.deploymentfile.SchemaType;
import org.voltdb.compiler.deploymentfile.SecurityProviderString;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType.Temptables;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;

public class DeploymentBuilder {
    public static final class UserInfo {
        public final String name;
        public String password;
        private final String roles[];

        public UserInfo (final String name, final String password, final String roles[]){
            this.name = name;
            this.password = password;
            this.roles = roles;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof UserInfo) {
                final UserInfo oInfo = (UserInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    int m_hostCount = 1;
    int m_sitesPerHost = 1;
    int m_replication = 0;
    boolean m_useCustomAdmin = false;
    int m_adminPort = VoltDB.DEFAULT_ADMIN_PORT;
    boolean m_adminOnStartup = false;

    final LinkedHashSet<UserInfo> m_users = new LinkedHashSet<UserInfo>();

    // zero defaults to first open port >= 8080.
    // negative one means disabled in the deployment file.
    int m_httpdPortNo = -1;
    boolean m_jsonApiEnabled = true;

    boolean m_securityEnabled = false;
    String  m_securityProvider = SecurityProviderString.HASH.value();

    private String m_snapshotPath = null;
    private int m_snapshotRetain = 0;
    private String m_snapshotPrefix = null;
    private String m_snapshotFrequency = null;
    private String m_voltRootPath = null;

    private boolean m_ppdEnabled = false;
    private String m_ppdPrefix = "none";

    private String m_internalSnapshotPath;
    private String m_commandLogPath;
    private Boolean m_commandLogSync;
    private Boolean m_commandLogEnabled;
    private Integer m_commandLogSize;
    private Integer m_commandLogFsyncInterval;
    private Integer m_commandLogMaxTxnsBeforeFsync;

    private Integer m_snapshotPriority;

    private Integer m_maxTempTableMemory = 100;

    private boolean m_elenabled;      // true if enabled; false if disabled

    // whether to allow DDL over adhoc or use full catalog updates
    private boolean m_useDDLSchema = false;

    public DeploymentBuilder() {
        this(1, 1, 0);
    }

    public DeploymentBuilder(final int sitesPerHost,
                             final int hostCount,
                             final int replication)
    {
        this(sitesPerHost, hostCount, replication, 0, false);
    }

    public DeploymentBuilder(final int sitesPerHost,
            final int hostCount, final int replication,
            final int adminPort, final boolean adminOnStartup)
    {
        m_sitesPerHost = sitesPerHost;
        m_hostCount = hostCount;
        m_replication = replication;
        m_adminPort = adminPort;
        m_adminOnStartup = adminOnStartup;

        // set default deployment stuff
        String voltRootPath = "/tmp/" + System.getProperty("user.name");
        java.io.File voltRootFile = new java.io.File(voltRootPath);
        if (!voltRootFile.exists()) {
            if (!voltRootFile.mkdir()) {
                throw new RuntimeException("Unable to create voltdbroot \"" + voltRootPath + "\" for test");
            }
        }
        if (!voltRootFile.isDirectory()) {
            throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not a directory");
        }
        if (!voltRootFile.canRead()) {
            throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not readable");
        }
        if (!voltRootFile.canWrite()) {
            throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not writable");
        }
        if (!voltRootFile.canExecute()) {
            throw new RuntimeException("voltdbroot \"" + voltRootPath + "\" for test exists but is not writable");
        }
        m_voltRootPath = voltRootPath;

    }

    public void setVoltRoot(String voltRoot) {
        assert(voltRoot != null);
        m_voltRootPath = voltRoot;
    }

    /**
     * whether to allow DDL over adhoc or use full catalog updates
     */
    public void setUseDDLSchema(boolean useIt) {
        m_useDDLSchema = useIt;
    }

    public void configureLogging(String internalSnapshotPath, String commandLogPath, Boolean commandLogSync,
            Boolean commandLogEnabled, Integer fsyncInterval, Integer maxTxnsBeforeFsync, Integer logSize) {
        m_internalSnapshotPath = internalSnapshotPath;
        m_commandLogPath = commandLogPath;
        m_commandLogSync = commandLogSync;
        m_commandLogEnabled = commandLogEnabled;
        m_commandLogFsyncInterval = fsyncInterval;
        m_commandLogMaxTxnsBeforeFsync = maxTxnsBeforeFsync;
        m_commandLogSize = logSize;
    }

    public void setEnableCommandLogging(boolean value)
    {
        m_commandLogEnabled = value;
    }

    public void setSnapshotPriority(int priority) {
        m_snapshotPriority = priority;
    }

    public void addUsers(final UserInfo users[]) {
        for (final UserInfo info : users) {
            final boolean added = m_users.add(info);
            if (!added) {
                assert(added);
            }
        }
    }

    public void removeUser(String userName) {
        Iterator<UserInfo> iter = m_users.iterator();
        while (iter.hasNext()) {
            UserInfo info = iter.next();
            if (info.name.equals(userName)) {
                iter.remove();
            }
        }
    }

    public void setHTTPDPort(int port) {
        m_httpdPortNo = port;
    }

    public void setJSONAPIEnabled(final boolean enabled) {
        m_jsonApiEnabled = enabled;
    }

    public void setSecurityEnabled(final boolean enabled) {
        m_securityEnabled = enabled;
    }

    public void setSecurityProvider(final String provider) {
        if (provider != null && !provider.trim().isEmpty()) {
            SecurityProviderString.fromValue(provider);
            m_securityProvider = provider;
        }
    }

    public void setSnapshotSettings(
            String frequency,
            int retain,
            String path,
            String prefix) {
        assert(frequency != null);
        assert(prefix != null);
        m_snapshotFrequency = frequency;
        m_snapshotRetain = retain;
        m_snapshotPrefix = prefix;
        m_snapshotPath = path;
    }

    public void setPartitionDetectionSettings(final String snapshotPath, final String ppdPrefix)
    {
        m_ppdEnabled = true;
        m_snapshotPath = snapshotPath;
        m_ppdPrefix = ppdPrefix;
    }

    public void addExport(boolean enabled) {
        m_elenabled = enabled;
    }

    public void setMaxTempTableMemory(int max)
    {
        m_maxTempTableMemory = max;
    }

    public void writeXML(String path) {
        File file;
        try {
            file = new File(path);

            final FileWriter writer = new FileWriter(file);
            writer.write(getXML());
            writer.flush();
            writer.close();
        }
        catch (final Exception e) {
            e.printStackTrace();
            assert(false);
        }
    }

    /**
     * Writes deployment.xml file to a temporary file. It is constructed from the passed parameters and the m_users
     * field.
     *
     * @param voltRoot
     * @param dinfo an instance {@link DeploymentInfo}
     * @return deployment path
     * @throws IOException
     * @throws JAXBException
     */
    public String getXML() {

        // make sure voltroot exists
        new File(m_voltRootPath).mkdirs();

        org.voltdb.compiler.deploymentfile.ObjectFactory factory =
            new org.voltdb.compiler.deploymentfile.ObjectFactory();

        // <deployment>
        DeploymentType deployment = factory.createDeploymentType();
        JAXBElement<DeploymentType> doc = factory.createDeployment(deployment);

        // <cluster>
        ClusterType cluster = factory.createClusterType();
        deployment.setCluster(cluster);
        cluster.setHostcount(m_hostCount);
        cluster.setSitesperhost(m_sitesPerHost);
        cluster.setKfactor(m_replication);
        cluster.setSchema(m_useDDLSchema ? SchemaType.DDL : SchemaType.CATALOG);

        // <paths>
        PathsType paths = factory.createPathsType();
        deployment.setPaths(paths);
        Voltdbroot voltdbroot = factory.createPathsTypeVoltdbroot();
        paths.setVoltdbroot(voltdbroot);
        voltdbroot.setPath(m_voltRootPath);

        if (m_snapshotPath != null) {
            PathsType.Snapshots snapshotPathElement = factory.createPathsTypeSnapshots();
            snapshotPathElement.setPath(m_snapshotPath);
            paths.setSnapshots(snapshotPathElement);
        }

        if (m_commandLogPath != null) {
            PathsType.Commandlog commandLogPathElement = factory.createPathsTypeCommandlog();
            commandLogPathElement.setPath(m_commandLogPath);
            paths.setCommandlog(commandLogPathElement);
        }

        if (m_internalSnapshotPath != null) {
            PathsType.Commandlogsnapshot commandLogSnapshotPathElement = factory.createPathsTypeCommandlogsnapshot();
            commandLogSnapshotPathElement.setPath(m_internalSnapshotPath);
            paths.setCommandlogsnapshot(commandLogSnapshotPathElement);
        }

        if (m_snapshotPrefix != null) {
            SnapshotType snapshot = factory.createSnapshotType();
            deployment.setSnapshot(snapshot);
            snapshot.setFrequency(m_snapshotFrequency);
            snapshot.setPrefix(m_snapshotPrefix);
            snapshot.setRetain(m_snapshotRetain);
        }

        SecurityType security = factory.createSecurityType();
        deployment.setSecurity(security);
        security.setEnabled(m_securityEnabled);
        SecurityProviderString provider = SecurityProviderString.HASH;
        if (m_securityEnabled) try {
            provider = SecurityProviderString.fromValue(m_securityProvider);
        } catch (IllegalArgumentException shouldNotHappenSeeSetter) {
        }
        security.setProvider(provider);

        if (m_commandLogSync != null || m_commandLogEnabled != null ||
                m_commandLogFsyncInterval != null || m_commandLogMaxTxnsBeforeFsync != null ||
                m_commandLogSize != null) {
            CommandLogType commandLogType = factory.createCommandLogType();
            if (m_commandLogSync != null) {
                commandLogType.setSynchronous(m_commandLogSync.booleanValue());
            }
            if (m_commandLogEnabled != null) {
                commandLogType.setEnabled(m_commandLogEnabled);
            }
            if (m_commandLogSize != null) {
                commandLogType.setLogsize(m_commandLogSize);
            }
            if (m_commandLogFsyncInterval != null || m_commandLogMaxTxnsBeforeFsync != null) {
                CommandLogType.Frequency frequency = factory.createCommandLogTypeFrequency();
                if (m_commandLogFsyncInterval != null) {
                    frequency.setTime(m_commandLogFsyncInterval);
                }
                if (m_commandLogMaxTxnsBeforeFsync != null) {
                    frequency.setTransactions(m_commandLogMaxTxnsBeforeFsync);
                }
                commandLogType.setFrequency(frequency);
            }
            deployment.setCommandlog(commandLogType);
        }

        // <partition-detection>/<snapshot>
        PartitionDetectionType ppd = factory.createPartitionDetectionType();
        deployment.setPartitionDetection(ppd);
        ppd.setEnabled(m_ppdEnabled);
        Snapshot ppdsnapshot = factory.createPartitionDetectionTypeSnapshot();
        ppd.setSnapshot(ppdsnapshot);
        ppdsnapshot.setPrefix(m_ppdPrefix);

        // <admin-mode>
        // can't be disabled, but only write out the non-default config if
        // requested by a test. otherwise, take the implied defaults (or
        // whatever local cluster overrides on the command line).
        if (m_useCustomAdmin) {
            AdminModeType admin = factory.createAdminModeType();
            deployment.setAdminMode(admin);
            admin.setPort(m_adminPort);
            admin.setAdminstartup(m_adminOnStartup);
        }

        // <systemsettings>
        SystemSettingsType systemSettingType = factory.createSystemSettingsType();
        Temptables temptables = factory.createSystemSettingsTypeTemptables();
        temptables.setMaxsize(m_maxTempTableMemory);
        systemSettingType.setTemptables(temptables);
        if (m_snapshotPriority != null) {
            SystemSettingsType.Snapshot snapshot = factory.createSystemSettingsTypeSnapshot();
            snapshot.setPriority(m_snapshotPriority);
            systemSettingType.setSnapshot(snapshot);
        }
        deployment.setSystemsettings(systemSettingType);

        // <users>
        if (m_users.size() > 0) {
            UsersType users = factory.createUsersType();
            deployment.setUsers(users);

            // <user>
            for (final UserInfo info : m_users) {
                User user = factory.createUsersTypeUser();
                users.getUser().add(user);
                user.setName(info.name);
                user.setPassword(info.password);

                // build up user/roles.
                if (info.roles.length > 0) {
                    final StringBuilder roles = new StringBuilder();
                    for (final String role : info.roles) {
                        if (roles.length() > 0)
                            roles.append(",");
                        roles.append(role.toLowerCase());
                    }
                    user.setRoles(roles.toString());
                }
            }
        }

        // <httpd>. Disabled unless port # is configured by a testcase
        HttpdType httpd = factory.createHttpdType();
        deployment.setHttpd(httpd);
        httpd.setEnabled(m_httpdPortNo != -1);
        httpd.setPort(m_httpdPortNo);
        Jsonapi json = factory.createHttpdTypeJsonapi();
        httpd.setJsonapi(json);
        json.setEnabled(m_jsonApiEnabled);

        // <export>
        ExportType export = factory.createExportType();
        deployment.setExport(export);
        export.setEnabled(m_elenabled);

        // Have some yummy boilerplate!
        String xml = null;
        try {
            JAXBContext context = JAXBContext.newInstance(DeploymentType.class);

            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
                    Boolean.TRUE);
            StringWriter writer = new StringWriter();
            marshaller.marshal(doc, writer);
            xml = writer.toString();
        }
        catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }

        return xml;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "";//getXML();
    }
}
