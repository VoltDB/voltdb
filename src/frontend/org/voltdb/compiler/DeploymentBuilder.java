/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;

import org.voltdb.VoltDB;
import org.voltdb.compiler.deploymentfile.AdminModeType;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportConfigurationType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.HttpdType.Jsonapi;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType.Snapshot;
import org.voltdb.compiler.deploymentfile.PathEntry;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.PathsType.Voltdbroot;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.compiler.deploymentfile.SchemaType;
import org.voltdb.compiler.deploymentfile.SecurityProviderString;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType.Temptables;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableMap;

public class DeploymentBuilder {
    public static final class UserInfo {
        public final String name;
        public String password;
        public final String roles[];

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

    private int m_hostCount = 1;
    private int m_sitesPerHost = 1;
    private int m_replication = 0;
    private boolean m_useCustomAdmin = false;
    private int m_adminPort = VoltDB.DEFAULT_ADMIN_PORT;
    private boolean m_adminOnStartup = false;

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
    private boolean m_commandLogEnabled = false;
    private Integer m_commandLogSize;
    private Integer m_commandLogFsyncInterval;
    private Integer m_commandLogMaxTxnsBeforeFsync;

    private Integer m_snapshotPriority;

    private Integer m_maxTempTableMemory = 100;

    private boolean m_elenabled;      // true if enabled; false if disabled
    private Properties m_elConfig;
    private String m_elExportTarget;
    String m_elloader;

    // whether to allow DDL over adhoc or use full catalog updates
    private boolean m_useDDLSchema = false;
    private Integer m_deadHostTimeout;
    private Integer m_elasticDuration;
    private Integer m_elasticThroughput;
    private Integer m_queryTimeout;

    static final org.voltdb.compiler.deploymentfile.ObjectFactory m_factory =
            new org.voltdb.compiler.deploymentfile.ObjectFactory();

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

    public void resetFromVPB(int sitesPerHost, int hostCount, int replication)
    {
        m_sitesPerHost = sitesPerHost;
        m_hostCount = hostCount;
        m_replication = replication;
    }

    DeploymentBuilder useCustomAdmin(int adminPort, boolean adminOnStartup)
    {
        m_useCustomAdmin = true;
        m_adminPort = adminPort;
        m_adminOnStartup = adminOnStartup;
        return this;
    }

    public DeploymentBuilder setVoltRoot(String voltRoot) {
        assert(voltRoot != null);
        m_voltRootPath = voltRoot;
        return this;
    }

    public DeploymentBuilder setQueryTimeout(int target) {
        m_queryTimeout = target;
        return this;
    }

    public DeploymentBuilder setDeadHostTimeout(int target) {
        m_deadHostTimeout = target;
        return this;
    }

    public DeploymentBuilder setElasticDuration(int target) {
        m_elasticDuration = target;
        return this;
    }

    public DeploymentBuilder setElasticThroughput(int target) {
        m_elasticThroughput = target;
        return this;
    }

    /**
     * whether to allow DDL over adhoc or use full catalog updates
     */
    public DeploymentBuilder setUseDDLSchema(boolean useIt) {
        m_useDDLSchema = useIt;
        return this;
    }

    public DeploymentBuilder configureLogging(String internalSnapshotPath, String commandLogPath, Boolean commandLogSync,
            boolean commandLogEnabled, Integer fsyncInterval, Integer maxTxnsBeforeFsync, Integer logSize) {
        m_internalSnapshotPath = internalSnapshotPath;
        m_commandLogPath = commandLogPath;
        m_commandLogSync = commandLogSync;
        m_commandLogEnabled = commandLogEnabled;
        m_commandLogFsyncInterval = fsyncInterval;
        m_commandLogMaxTxnsBeforeFsync = maxTxnsBeforeFsync;
        m_commandLogSize = logSize;
        return this;
    }

    public DeploymentBuilder setSnapshotPriority(int priority) {
        m_snapshotPriority = priority;
        return this;
    }

    public DeploymentBuilder addUsers(final UserInfo users[]) {
        for (final UserInfo info : users) {
            final boolean added = m_users.add(info);
            if (!added) {
                assert(added);
            }
        }
        return this;
    }

    public DeploymentBuilder removeUser(String userName) {
        Iterator<UserInfo> iter = m_users.iterator();
        while (iter.hasNext()) {
            UserInfo info = iter.next();
            if (info.name.equals(userName)) {
                iter.remove();
            }
        }
        return this;
    }

    public DeploymentBuilder setHTTPDPort(int port) {
        m_httpdPortNo = port;
        return this;
    }

    public DeploymentBuilder setJSONAPIEnabled(final boolean enabled) {
        m_jsonApiEnabled = enabled;
        return this;
    }

    public DeploymentBuilder setSecurityEnabled(final boolean enabled) {
        m_securityEnabled = enabled;
        return this;
    }

    public DeploymentBuilder setSecurityProvider(final String provider) {
        if (provider != null && !provider.trim().isEmpty()) {
            SecurityProviderString.fromValue(provider);
            m_securityProvider = provider;
        }
        return this;
    }

    public DeploymentBuilder setSnapshotSettings(
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
        return this;
    }

    public DeploymentBuilder setPartitionDetectionSettings(final String snapshotPath, final String ppdPrefix)
    {
        m_ppdEnabled = true;
        m_snapshotPath = snapshotPath;
        m_ppdPrefix = ppdPrefix;
        return this;
    }

    public DeploymentBuilder addExport(boolean enabled, String exportTarget, Properties config) {
        m_elloader = "org.voltdb.export.processors.GuestProcessor";
        m_elenabled = enabled;

        if (config == null) {
            config = new Properties();
            config.putAll(ImmutableMap.<String, String>of(
                    "type","tsv", "batched","true", "with-schema","true", "nonce","zorag", "outdir","exportdata"
                    ));
        }
        m_elConfig = config;

        if ((exportTarget != null) && !exportTarget.trim().isEmpty()) {
            m_elExportTarget = exportTarget;
        }
        return this;
    }

    public DeploymentBuilder setMaxTempTableMemory(int max)
    {
        m_maxTempTableMemory = max;
        return this;
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

    public String getXML() {
        // make sure voltroot exists
        new File(m_voltRootPath).mkdirs();

        // <deployment>
        DeploymentType deployment = m_factory.createDeploymentType();
        JAXBElement<DeploymentType> doc = m_factory.createDeployment(deployment);

        // <cluster>
        ClusterType cluster = m_factory.createClusterType();
        deployment.setCluster(cluster);
        cluster.setHostcount(m_hostCount);
        cluster.setSitesperhost(m_sitesPerHost);
        cluster.setKfactor(m_replication);
        cluster.setSchema(m_useDDLSchema ? SchemaType.DDL : SchemaType.CATALOG);

        // <paths>
        PathsType paths = m_factory.createPathsType();
        deployment.setPaths(paths);
        Voltdbroot voltdbroot = m_factory.createPathsTypeVoltdbroot();
        paths.setVoltdbroot(voltdbroot);
        voltdbroot.setPath(m_voltRootPath);

        if (m_snapshotPath != null) {
            PathEntry snapshotPathElement = m_factory.createPathEntry();
            snapshotPathElement.setPath(m_snapshotPath);
            paths.setSnapshots(snapshotPathElement);
        }

        if (m_deadHostTimeout != null) {
            HeartbeatType heartbeat = m_factory.createHeartbeatType();
            heartbeat.setTimeout(m_deadHostTimeout);
            deployment.setHeartbeat(heartbeat);
        }

        if (m_commandLogPath != null) {
            PathEntry commandLogPathElement = m_factory.createPathEntry();
            commandLogPathElement.setPath(m_commandLogPath);
            paths.setCommandlog(commandLogPathElement);
        }

        if (m_internalSnapshotPath != null) {
            PathEntry commandLogSnapshotPathElement = m_factory.createPathEntry();
            commandLogSnapshotPathElement.setPath(m_internalSnapshotPath);
            paths.setCommandlogsnapshot(commandLogSnapshotPathElement);
        }

        if (m_snapshotPrefix != null) {
            SnapshotType snapshot = m_factory.createSnapshotType();
            deployment.setSnapshot(snapshot);
            snapshot.setFrequency(m_snapshotFrequency);
            snapshot.setPrefix(m_snapshotPrefix);
            snapshot.setRetain(m_snapshotRetain);
        }

        SecurityType security = m_factory.createSecurityType();
        deployment.setSecurity(security);
        security.setEnabled(m_securityEnabled);
        SecurityProviderString provider = SecurityProviderString.HASH;
        if (m_securityEnabled) try {
            provider = SecurityProviderString.fromValue(m_securityProvider);
        } catch (IllegalArgumentException shouldNotHappenSeeSetter) {
        }
        security.setProvider(provider);

        // set the command log (which defaults to off)
        CommandLogType commandLogType = m_factory.createCommandLogType();
        commandLogType.setEnabled(m_commandLogEnabled);
        if (m_commandLogSync != null) {
            commandLogType.setSynchronous(m_commandLogSync.booleanValue());
        }
        if (m_commandLogSize != null) {
            commandLogType.setLogsize(m_commandLogSize);
        }
        if (m_commandLogFsyncInterval != null || m_commandLogMaxTxnsBeforeFsync != null) {
            CommandLogType.Frequency frequency = m_factory.createCommandLogTypeFrequency();
            if (m_commandLogFsyncInterval != null) {
                frequency.setTime(m_commandLogFsyncInterval);
            }
            if (m_commandLogMaxTxnsBeforeFsync != null) {
                frequency.setTransactions(m_commandLogMaxTxnsBeforeFsync);
            }
            commandLogType.setFrequency(frequency);
        }
        deployment.setCommandlog(commandLogType);

        // <partition-detection>/<snapshot>
        PartitionDetectionType ppd = m_factory.createPartitionDetectionType();
        deployment.setPartitionDetection(ppd);
        ppd.setEnabled(m_ppdEnabled);
        Snapshot ppdsnapshot = m_factory.createPartitionDetectionTypeSnapshot();
        ppd.setSnapshot(ppdsnapshot);
        ppdsnapshot.setPrefix(m_ppdPrefix);

        // <admin-mode>
        // can't be disabled, but only write out the non-default config if
        // requested by a test. otherwise, take the implied defaults (or
        // whatever local cluster overrides on the command line).
        if (m_useCustomAdmin) {
            AdminModeType admin = m_factory.createAdminModeType();
            deployment.setAdminMode(admin);
            admin.setPort(m_adminPort);
            admin.setAdminstartup(m_adminOnStartup);
        }

        // <systemsettings>
        SystemSettingsType systemSettingType = m_factory.createSystemSettingsType();
        Temptables temptables = m_factory.createSystemSettingsTypeTemptables();
        temptables.setMaxsize(m_maxTempTableMemory);
        systemSettingType.setTemptables(temptables);
        if (m_snapshotPriority != null) {
            SystemSettingsType.Snapshot snapshot = m_factory.createSystemSettingsTypeSnapshot();
            snapshot.setPriority(m_snapshotPriority);
            systemSettingType.setSnapshot(snapshot);
        }
        if (m_elasticThroughput != null || m_elasticDuration != null) {
            SystemSettingsType.Elastic elastic = m_factory.createSystemSettingsTypeElastic();
            if (m_elasticThroughput != null) elastic.setThroughput(m_elasticThroughput);
            if (m_elasticDuration != null) elastic.setDuration(m_elasticDuration);
            systemSettingType.setElastic(elastic);
        }
        if (m_queryTimeout != null) {
            SystemSettingsType.Query query = m_factory.createSystemSettingsTypeQuery();
            query.setTimeout(m_queryTimeout);
            systemSettingType.setQuery(query);
        }

        deployment.setSystemsettings(systemSettingType);

        // <users>
        if (m_users.size() > 0) {
            UsersType users = m_factory.createUsersType();
            deployment.setUsers(users);

            // <user>
            for (final UserInfo info : m_users) {
                User user = m_factory.createUsersTypeUser();
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
        HttpdType httpd = m_factory.createHttpdType();
        deployment.setHttpd(httpd);
        httpd.setEnabled(m_httpdPortNo != -1);
        httpd.setPort(m_httpdPortNo);
        Jsonapi json = m_factory.createHttpdTypeJsonapi();
        httpd.setJsonapi(json);
        json.setEnabled(m_jsonApiEnabled);

        // <export>
        ExportType export = m_factory.createExportType();
        deployment.setExport(export);
        export.setEnabled(m_elenabled);

        if (m_elenabled) {
            if (m_elExportTarget != null) {
                ServerExportEnum exportTarget = ServerExportEnum.fromValue(m_elExportTarget.toLowerCase());
                export.setTarget(exportTarget);
            }
            if ((m_elConfig != null) && (m_elConfig.size() > 0)) {
                ExportConfigurationType exportConfig = m_factory.createExportConfigurationType();
                List<PropertyType> configProperties = exportConfig.getProperty();

                for ( Object nameObj: m_elConfig.keySet()) {
                    PropertyType prop = m_factory.createPropertyType();
                    String name = String.class.cast(nameObj);
                    prop.setName(name);
                    prop.setValue(m_elConfig.getProperty(name));
                    configProperties.add(prop);
                }
                export.setConfiguration(exportConfig);
            }
        }

        // Have some yummy boilerplate!
        try {
            JAXBContext context = JAXBContext.newInstance(DeploymentType.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
                    Boolean.TRUE);
            StringWriter writer = new StringWriter();
            marshaller.marshal(doc, writer);
            String xml = writer.toString();
            return xml;
        }
        catch (Exception e) {
            e.printStackTrace();
            assert(false);
            return null;
        }
    }

    public String writeXMLToTempFile() {
        String xml = getXML();
        return MiscUtils.writeStringToTempFilePath(xml);
    }

    public File writeXMLToFile(String path) {
        String xml = getXML();
        File file = MiscUtils.writeStringToPath(path, xml);
        return file;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "";//getXML();
    }

    public File getPathToVoltRoot() {
        return new File(m_voltRootPath);
    }

}
