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
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.compiler.deploymentfile.AdminModeType;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.ConnectionType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.DrType;
import org.voltdb.compiler.deploymentfile.ExportConfigurationType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.HttpdType.Jsonapi;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType.Snapshot;
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
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableMap;

public class DeploymentBuilder {
    public static final class UserInfo {
        private final String m_name;
        private final String m_password;
        private final String m_roles;

        public UserInfo (final String name, final String password, final String... roles){
            m_name = name;
            assert(m_name != null);
            m_password = password;
            m_roles = (roles == null || roles.length == 0) ? null :
                StringUtils.join(roles, ',').toLowerCase();
        }

        public String getName() { return m_name; }

        public String getPassword() { return m_password; }

        /**
         * @param user
         */
        private void initUserFromInfo(User user) {
            user.setName(m_name);
            user.setPassword(m_password);
            if (m_roles != null) {
                user.setRoles(m_roles);
            }
        }

        @Override
        public int hashCode() { return m_name.hashCode(); }

        @Override
        public boolean equals(final Object other) {
            return (other instanceof UserInfo) &&
                    m_name.equals(((UserInfo)other).m_name);
        }
    }

    private int m_hostCount = 1;
    private int m_sitesPerHost = 1;
    private int m_replication = 0;
    private boolean m_useCustomAdmin = false;
    private int m_adminPort = VoltDB.DEFAULT_ADMIN_PORT;
    private boolean m_adminOnStartup = false;

    private final LinkedHashSet<UserInfo> m_users = new LinkedHashSet<UserInfo>();

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

    private final List<HashMap<String, Object>> m_elExportConnectors = new ArrayList<>();
    String m_elloader;

    // whether to allow DDL over adhoc or use full catalog updates
    private boolean m_useAdHocDDL = false;
    private Integer m_deadHostTimeout;
    private Integer m_elasticDuration;
    private Integer m_elasticThroughput;
    private Integer m_queryTimeout;

    private String m_drMasterHost;
    private Boolean m_drProducerEnabled = null;
    private Integer m_drProducerClusterId = null;

    static final org.voltdb.compiler.deploymentfile.ObjectFactory m_factory =
            new org.voltdb.compiler.deploymentfile.ObjectFactory();

    public DeploymentBuilder() { this(1, 1, 0, 0, false); }

    public DeploymentBuilder(final int sitesPerHost) { this(sitesPerHost, 1, 0, 0, false); }

    public DeploymentBuilder(final int sitesPerHost, final int hostCount)
    {
        this(sitesPerHost, hostCount, 0, 0, false);
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

    public void disableReplication() {
        m_replication = 0;
    }

    public DeploymentBuilder useCustomAdmin(int adminPort, boolean adminOnStartup)
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
    public DeploymentBuilder setUseAdHocDDL(boolean useIt) {
        m_useAdHocDDL = useIt;
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

    public DeploymentBuilder addUsers(final UserInfo... users) {
        for (final UserInfo info : users) {
            final boolean added = m_users.add(info);
            if (!added) {
                assert(added);
            }
        }
        return this;
    }

    public DeploymentBuilder removeUser(String userName) {
        // Remove the user whose name matches this dummy.
        UserInfo dummy = new UserInfo(userName, null);
        m_users.remove(dummy);
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

    public DeploymentBuilder setSecurityEnabled(final boolean enabled, boolean createAdminUser) {
        m_securityEnabled = enabled;
        if (createAdminUser) {
            addUsers(new UserInfo("defaultadmin", "admin", new String[] {"ADMINISTRATOR"}));
        }
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
        return addExport(enabled, exportTarget, config, Constants.DEFAULT_EXPORT_CONNECTOR_NAME);
    }

    public DeploymentBuilder addExport(boolean enabled, String exportTarget,
            Properties config, String target) {
        HashMap<String, Object> exportConnector = new HashMap<>();
        exportConnector.put("elLoader", "org.voltdb.export.processors.GuestProcessor");
        exportConnector.put("elEnabled", enabled);

        if (config == null) {
            config = new Properties();
            config.putAll(ImmutableMap.<String, String>of(
                    "type","tsv", "batched","true", "with-schema","true", "nonce","zorag", "outdir","exportdata"
                    ));
        }
        exportConnector.put("elConfig", config);

        if ((exportTarget != null) && !exportTarget.trim().isEmpty()) {
            exportConnector.put("elExportTarget", exportTarget);
        }
        else {
            exportConnector.put("elExportTarget", "file");
        }
        exportConnector.put("elGroup", target);
        m_elExportConnectors.add(exportConnector);
        return this;
    }

    public DeploymentBuilder setMaxTempTableMemory(int max)
    {
        m_maxTempTableMemory = max;
        return this;
    }

    public DeploymentBuilder setDRMasterHost(String drMasterHost) {
        m_drMasterHost = drMasterHost;
        return this;
    }

    public DeploymentBuilder setDRProducerEnabled(int clusterId)
    {
        m_drProducerEnabled = true;
        m_drProducerClusterId = new Integer(clusterId);
        return this;
    }

    public DeploymentBuilder setDRProducerDisabled(int clusterId)
    {
        m_drProducerEnabled = false;
        m_drProducerClusterId = new Integer(clusterId);
        return this;
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
        cluster.setSchema(m_useAdHocDDL ? SchemaType.DDL : SchemaType.CATALOG);

        // <paths>
        PathsType paths = m_factory.createPathsType();
        deployment.setPaths(paths);
        Voltdbroot voltdbroot = m_factory.createPathsTypeVoltdbroot();
        paths.setVoltdbroot(voltdbroot);
        voltdbroot.setPath(m_voltRootPath);

        if (m_snapshotPath != null) {
            PathsType.Snapshots snapshotPathElement = m_factory.createPathsTypeSnapshots();
            snapshotPathElement.setPath(m_snapshotPath);
            paths.setSnapshots(snapshotPathElement);
        }

        if (m_deadHostTimeout != null) {
            HeartbeatType heartbeat = m_factory.createHeartbeatType();
            heartbeat.setTimeout(m_deadHostTimeout);
            deployment.setHeartbeat(heartbeat);
        }

        if (m_commandLogPath != null) {
            PathsType.Commandlog commandLogPathElement = m_factory.createPathsTypeCommandlog();
            commandLogPathElement.setPath(m_commandLogPath);
            paths.setCommandlog(commandLogPathElement);
        }

        if (m_internalSnapshotPath != null) {
            PathsType.Commandlogsnapshot commandLogSnapshotPathElement =
                    m_factory.createPathsTypeCommandlogsnapshot();
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
                info.initUserFromInfo(user);
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

        for (HashMap<String,Object> exportConnector : m_elExportConnectors) {
            ExportConfigurationType exportConfig = m_factory.createExportConfigurationType();
            exportConfig.setEnabled((boolean)exportConnector.get("elEnabled") && exportConnector.get("elLoader") != null &&
                    !((String)exportConnector.get("elLoader")).trim().isEmpty());

            ServerExportEnum exportTarget = ServerExportEnum.fromValue(((String)exportConnector.get("elExportTarget")).toLowerCase());
            exportConfig.setType(exportTarget);
            if (exportTarget.equals(ServerExportEnum.CUSTOM)) {
                exportConfig.setExportconnectorclass(System.getProperty(ExportDataProcessor.EXPORT_TO_TYPE));
            }

            exportConfig.setStream((String)exportConnector.get("elGroup"));

            Properties config = (Properties)exportConnector.get("elConfig");
            if((config != null) && (config.size() > 0)) {
                List<PropertyType> configProperties = exportConfig.getProperty();

                for( Object nameObj: config.keySet()) {
                    String name = String.class.cast(nameObj);

                    PropertyType prop = m_factory.createPropertyType();
                    prop.setName(name);
                    prop.setValue(config.getProperty(name));

                    configProperties.add(prop);
                }
            }
            export.getConfiguration().add(exportConfig);
        }

        if (m_drProducerClusterId != null || (m_drMasterHost != null && !m_drMasterHost.isEmpty())) {
            DrType dr = m_factory.createDrType();
            deployment.setDr(dr);
            if (m_drProducerClusterId != null) {
                dr.setListen(m_drProducerEnabled);
                dr.setId(m_drProducerClusterId);
            }
            if (m_drMasterHost != null && !m_drMasterHost.isEmpty()) {
                ConnectionType conn = m_factory.createConnectionType();
                dr.setConnection(conn);
                conn.setSource(m_drMasterHost);
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
        try {
            File tempFile = File.createTempFile("VoltDeployment", ".xml");
            tempFile.deleteOnExit();
            MiscUtils.writeStringToFile(tempFile, xml);
            return tempFile.getPath();
        } catch (IOException e) {
            System.out.println("Failed to create deployment file.");
            e.printStackTrace();
            throw new RuntimeException(e); // Good enough for test code?
        }
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

    public String topologyString() {
        return backendName() + "-" + m_sitesPerHost + '-' + m_hostCount + '-' + m_replication;
    }

    private String backendName() {
        return backendTarget().display;
    }

    public int sites() {
        return m_sitesPerHost;
    }

    public int hosts() {
        return m_hostCount;
    }

    public int replication() {
        return m_replication;
    }

    public BackendTarget backendTarget() {
        if (this == m_forHSQLBackend) {
            return BackendTarget.HSQLDB_BACKEND;
        }
        return BackendTarget.NATIVE_EE_JNI;
    }

    static DeploymentBuilder m_forHSQLBackend = new DeploymentBuilder();

    public static DeploymentBuilder forHSQLBackend() {
        return m_forHSQLBackend;
    }

}
