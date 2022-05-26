/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import com.google_voltpatches.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.common.Constants;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.ConnectionType;
import org.voltdb.compiler.deploymentfile.ConsumerLimitType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.DiskLimitType;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.compiler.deploymentfile.DrType;
import org.voltdb.compiler.deploymentfile.ExportConfigurationType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.FeatureNameType;
import org.voltdb.compiler.deploymentfile.FeatureType;
import org.voltdb.compiler.deploymentfile.FeaturesType;
import org.voltdb.compiler.deploymentfile.FlushIntervalType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.HttpdType.Jsonapi;
import org.voltdb.compiler.deploymentfile.ImportConfigurationType;
import org.voltdb.compiler.deploymentfile.ImportType;
import org.voltdb.compiler.deploymentfile.KeyOrTrustStoreType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.PathsType.Voltdbroot;
import org.voltdb.compiler.deploymentfile.PriorityPolicyType;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType.Memorylimit;
import org.voltdb.compiler.deploymentfile.SchemaType;
import org.voltdb.compiler.deploymentfile.SecurityProviderString;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compiler.deploymentfile.ServerImportEnum;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SnmpType;
import org.voltdb.compiler.deploymentfile.SslType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType.Temptables;
import org.voltdb.compiler.deploymentfile.ThreadPoolsType;
import org.voltdb.compiler.deploymentfile.TopicType;
import org.voltdb.compiler.deploymentfile.TopicsType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.utils.NotImplementedException;

/**
 * Alternate (programmatic) interface to VoltCompiler. Give the class all of
 * the information a user would put in a VoltDB project file and it will go
 * and build the project file and run the compiler on it.
 *
 * It will also create a deployment.xml file and apply its changes to the catalog.
 */
public class VoltProjectBuilder {

    final LinkedHashSet<String> m_schemas = new LinkedHashSet<>();
    private StringBuffer transformer = new StringBuffer();

    public static final class ProcedureInfo {
        private final String roles[];
        private final Class<?> cls;
        private final String name;
        private final String sql;
        private final ProcedurePartitionData partitionData;

        public ProcedureInfo(final Class<?> cls) {
            this.roles = new String[0];
            this.cls = cls;
            this.name = cls.getSimpleName();
            this.sql = null;
            this.partitionData = null;
        }

        public ProcedureInfo(final Class<?> cls, final ProcedurePartitionData partitionInfo) {
            this.roles = new String[0];
            this.cls = cls;
            this.name = cls.getSimpleName();
            this.sql = null;
            this.partitionData = partitionInfo;
            assert(this.name != null);
        }

        public ProcedureInfo(final Class<?> cls, final ProcedurePartitionData partitionInfo,
                final String roles[]) {
            this.roles = roles;
            this.cls = cls;
            this.name = cls.getSimpleName();
            this.sql = null;
            this.partitionData = partitionInfo;
            assert(this.name != null);
        }

        public ProcedureInfo(
                final String roles[],
                final String name,
                final String sql,
                final ProcedurePartitionData partitionInfo) {
            assert(name != null);
            this.roles = roles;
            this.cls = null;
            this.name = name;
            if (sql.endsWith(";")) {
                this.sql = sql;
            }
            else {
                this.sql = sql + ";";
            }
            this.partitionData = partitionInfo;
            assert(this.name != null);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof ProcedureInfo) {
                final ProcedureInfo oInfo = (ProcedureInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    public static final class UserInfo {
        public final String name;
        public String password;
        private final String roles[];
        public boolean plaintext = true;

        public UserInfo (final String name, final String password, final String roles[], final boolean plaintext){
            this.name = name;
            this.password = password;
            this.roles = roles;
            this.plaintext = plaintext;
        }

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

    public static final class RoleInfo {
        private final String name;
        private final boolean sql;
        private final boolean sqlread;
        private final boolean admin;
        private final boolean defaultproc;
        private final boolean defaultprocread;
        private final boolean allproc;

        public RoleInfo(final String name, final boolean sql, final boolean sqlread, final boolean admin, final boolean defaultproc, final boolean defaultprocread, final boolean allproc){
            this.name = name;
            this.sql = sql;
            this.sqlread = sqlread;
            this.admin = admin;
            this.defaultproc = defaultproc;
            this.defaultprocread = defaultprocread;
            this.allproc = allproc;
        }

        public static RoleInfo[] fromTemplate(final RoleInfo other, final String... names) {
            RoleInfo[] roles = new RoleInfo[names.length];
            for (int i = 0; i < names.length; ++i) {
                roles[i] = new RoleInfo(names[i], other.sql, other.sqlread, other.admin,
                                other.defaultproc, other.defaultprocread, other.allproc);
            }
            return roles;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof RoleInfo) {
                final RoleInfo oInfo = (RoleInfo)o;
                return name.equals(oInfo.name);
            }
            return false;
        }
    }

    private static final class DeploymentInfo {
        final int hostCount;
        final int sitesPerHost;
        final int replication;
        final int clusterId;

        public DeploymentInfo(int hostCount, int sitesPerHost, int replication, int id) {
            this.hostCount = hostCount;
            this.sitesPerHost = sitesPerHost;
            this.replication = replication;
            this.clusterId = id;
        }
    }

    final LinkedHashSet<UserInfo> m_users = new LinkedHashSet<>();
    final LinkedHashSet<Class<?>> m_supplementals = new LinkedHashSet<>();

    // zero defaults to first open port >= the default port.
    // negative one means disabled in the deployment file.
    // a null HTTP port or json flag indicates that the corresponding element should
    // be omitted from the deployment XML.
    int m_httpdPortNo = -1;
    boolean m_jsonApiEnabled = true;
    boolean m_sslEnabled = false;
    boolean m_sslExternal = false;
    boolean m_sslInternal = false;
    boolean m_sslDR = false;

    String m_keystore;
    String m_keystorePassword;
    String m_certstore;
    String m_certstorePassword;

    BackendTarget m_target = BackendTarget.NATIVE_EE_JNI;
    PrintStream m_compilerDebugPrintStream = null;
    boolean m_securityEnabled = false;
    String m_securityProvider = SecurityProviderString.HASH.value();

    private String m_snapshotPath = null;
    private int m_snapshotRetain = 0;
    private String m_snapshotPrefix = null;
    private String m_snapshotFrequency = null;
    private String m_pathToDeployment = null;
    private String m_voltRootPath = null;

    private boolean m_ppdEnabled = false;
    private String m_ppdPrefix = "none";

    private Integer m_heartbeatTimeout = null;

    private Duration clockSkewInterval = null;

    private String m_internalSnapshotPath;
    private String m_commandLogPath;
    private Boolean m_commandLogSync;
    private boolean m_commandLogEnabled = false;
    private Integer m_commandLogSize;
    private Integer m_commandLogFsyncInterval;
    private Integer m_commandLogMaxTxnsBeforeFsync;

    private Boolean m_snmpEnabled = false;
    private String m_snmpTarget = null;

    private Integer m_snapshotPriority;

    private Integer m_maxTempTableMemory = 100;

    private List<String> m_diagnostics;

    private List<HashMap<String, Object>> m_ilImportConnectors = new ArrayList<>();
    private ExportType m_exportsConfiguration;

    private Integer m_deadHostTimeout = null;

    private Integer m_elasticThroughput = null;
    private Integer m_elasticDuration = null;
    private Integer m_queryTimeout = null;
    private Integer m_procedureLogThreshold = null;
    private String m_rssLimit = null;
    private String m_snmpRssLimit = null;
    private Integer m_resourceCheckInterval = null;
    private Map<FeatureNameType, String> m_featureDiskLimits;
    private Map<FeatureNameType, String> m_snmpFeatureDiskLimits;
    private FlushIntervalType m_flushIntervals = null;
    private PriorityPolicyType m_priorityPolicy = null;

    private boolean m_useDDLSchema = false;

    private String m_drMasterHost;
    private String m_drBuffers;
    private Integer m_preferredSource;
    private Boolean m_drConsumerConnectionEnabled = null;
    private String m_drConsumerSslPropertyFile = null;
    private Boolean m_drProducerEnabled = null;
    private DrRoleType m_drRole = DrRoleType.MASTER;
    private String m_drConflictRetention;

    private FeaturesType m_featureOptions;
    private TopicsType m_topicsConfiguration;
    private ThreadPoolsType m_threadPoolsConfiguration;

    public VoltProjectBuilder setClockSkewInterval(Duration interval) {
        clockSkewInterval = interval;
        return this;
    }

    public VoltProjectBuilder setQueryTimeout(int target) {
        m_queryTimeout = target;
        return this;
    }

    public VoltProjectBuilder setProcedureLogThreshold(int target) {
        m_procedureLogThreshold = target;
        return this;
    }

    public VoltProjectBuilder setRssLimit(String limit) {
        m_rssLimit = limit;
        return this;
    }

    public VoltProjectBuilder setSnmpRssLimit(String limit) {
        m_snmpRssLimit = limit;
        return this;
    }

    public VoltProjectBuilder setResourceCheckInterval(int seconds) {
        m_resourceCheckInterval = seconds;
        return this;
    }

    public VoltProjectBuilder setFeatureDiskLimits(Map<FeatureNameType, String> featureDiskLimits) {
        m_featureDiskLimits = featureDiskLimits;
        return this;
    }

    public VoltProjectBuilder setSnmpFeatureDiskLimits(Map<FeatureNameType, String> featureDiskLimits) {
        m_snmpFeatureDiskLimits = featureDiskLimits;
        return this;
    }

    public VoltProjectBuilder setElasticThroughput(int target) {
        m_elasticThroughput = target;
        return this;
    }

    public VoltProjectBuilder setElasticDuration(int target) {
        m_elasticDuration = target;
        return this;
    }

    public void setExportMode(ExportMode mode) {
        if (mode == ExportMode.ADVANCED && !VoltDB.instance().getConfig().m_isEnterprise) {
            throw new IllegalArgumentException("Attempt to set export mode to ADVANCED in community build");
        }

        if (m_featureOptions == null) {
            m_featureOptions = new FeaturesType();
        } else {
            FeatureType exportFeature = null;
            for (FeatureType feature : m_featureOptions.getFeature()) {
                if (feature.getName().equals(ExportManagerInterface.EXPORT_FEATURE)) {
                    exportFeature = feature;
                    break;
                }
            }
            if (exportFeature != null) {
                m_featureOptions.getFeature().remove(exportFeature);
            }
        }
        FeatureType exportFeature = new FeatureType();
        exportFeature.setName(ExportManagerInterface.EXPORT_FEATURE);
        exportFeature.setOption(mode.name());
        m_featureOptions.getFeature().add(exportFeature);
    }

    public ExportType getExportsConfiguration() {
        if (m_exportsConfiguration == null) {
            m_exportsConfiguration = new ExportType();
        }
        return m_exportsConfiguration;
    }

    public TopicsType getTopicsConfiguration() {
        if (m_topicsConfiguration == null) {
            // Note - Topics feature is enabled by default
            m_topicsConfiguration = new TopicsType();
        }
        return m_topicsConfiguration;
    }

    public ThreadPoolsType getThreadPoolsConfiguration() {
        if (m_threadPoolsConfiguration == null) {
            m_threadPoolsConfiguration = new ThreadPoolsType();
        }
        return m_threadPoolsConfiguration;
    }

    public boolean isTopicsEnabled() {
        return m_topicsConfiguration != null && m_topicsConfiguration.isEnabled();
    }

    public void setDeadHostTimeout(Integer deadHostTimeout) {
        m_deadHostTimeout = deadHostTimeout;
    }

    public void setUseDDLSchema(boolean useIt) {
        m_useDDLSchema = useIt;
    }

    public void configureSnmp(String target) {
        m_snmpTarget = target;
        m_snmpEnabled = true;
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

    public void configureLogging(Boolean commandLogSync, Boolean commandLogEnabled, Integer fsyncInterval, Integer maxTxnsBeforeFsync, Integer logSize) {
        m_commandLogSync = commandLogSync;
        m_commandLogEnabled = commandLogEnabled;
        m_commandLogFsyncInterval = fsyncInterval;
        m_commandLogMaxTxnsBeforeFsync = maxTxnsBeforeFsync;
        m_commandLogSize = logSize;
    }

    /**
     * Produce all catalogs this project builder knows how to produce.
     * Written to allow BenchmarkController to cause compilation of multiple
     * catalogs for benchmarks that need to update running appplications and
     * consequently need multiple benchmark controlled catalog jars.
     * @param sitesPerHost
     * @param length
     * @param kFactor
     * @param voltRoot  where to put the compiled catalogs
     * @return a list of jar filenames that were compiled. The benchmark will
     * be started using the filename at index 0.
     */
    public String[] compileAllCatalogs(
            int sitesPerHost, int length,
            int kFactor, String voltRoot)
    {
        throw new NotImplementedException("This project builder does not support compileAllCatalogs");
    }


    public void setSnapshotPriority(int priority) {
        m_snapshotPriority = priority;
    }

    public void addAllDefaults() {
        // does nothing in the base class
    }

    public void addUsers(final UserInfo users[]) {
        for (final UserInfo info : users) {
            final boolean added = m_users.add(info);
            if (!added) {
                assert(added);
            }
        }
    }

    public void clearUsers() {
        m_users.clear();
    }

    public void addRoles(final RoleInfo roles[]) {
        for (final RoleInfo info : roles) {
            transformer.append("CREATE ROLE " + info.name);
            if(info.sql || info.sqlread || info.defaultproc || info.admin || info.defaultprocread || info.allproc) {
                transformer.append(" WITH ");
                if(info.sql) {
                    transformer.append("sql,");
                }
                if(info.sqlread) {
                    transformer.append("sqlread,");
                }
                if(info.defaultproc) {
                    transformer.append("defaultproc,");
                }
                if(info.admin) {
                    transformer.append("admin,");
                }
                if(info.defaultprocread) {
                    transformer.append("defaultprocread,");
                }
                if(info.allproc) {
                    transformer.append("allproc,");
                }
                transformer.replace(transformer.length() - 1, transformer.length(), ";");
            }
            else {
                transformer.append(";");
            }
        }
    }

    public void addDRTables(final String tableNames[]) {
        for (final String drTable : tableNames) {
            transformer.append("DR TABLE " + drTable + ";");
        }
    }

    public void addSchema(final URL schemaURL) {
        assert(schemaURL != null);
        addSchema(schemaURL.getPath());
    }

    /** Creates a temporary file for the supplied schema text.
     * The file is not left open, and will be deleted upon process exit.
     */
    public static File createFileForSchema(String ddlText) throws IOException {
        File temp = File.createTempFile("literalschema", ".sql");
        temp.deleteOnExit();
        FileWriter out = new FileWriter(temp);
        out.write(ddlText);
        out.close();
        return temp;
    }

    /**
     * Adds the supplied schema by creating a temp file for it.
     */
    public void addLiteralSchema(String ddlText) throws IOException {
        File temp = createFileForSchema(ddlText);
        addSchema(URLEncoder.encode(temp.getAbsolutePath(), "UTF-8"));
    }

    /**
     * Add a schema based on a URL.
     * @param schemaURL Schema file URL
     */
    public void addSchema(String schemaURL) {
        try {
            schemaURL = URLDecoder.decode(schemaURL, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        assert(m_schemas.contains(schemaURL) == false);
        final File schemaFile = new File(schemaURL);
        assert(schemaFile != null);
        assert(schemaFile.isDirectory() == false);
        // this check below fails in some valid cases (like when the file is in a jar)
        //assert schemaFile.canRead()
        //    : "can't read file: " + schemaPath;

        m_schemas.add(schemaURL);
    }

    public void addStmtProcedure(String name, String sql) {
        addStmtProcedure(name, sql, new ProcedurePartitionData(ProcedurePartitionData.Type.MULTI));
    }

    // compatible with old deprecated syntax for test ONLY
    public void addStmtProcedure(String name, String sql, String partitionInfoString) {
        addProcedures(new ProcedureInfo(new String[0], name, sql,
                ProcedurePartitionData.fromPartitionInfoString(partitionInfoString)));
    }

    public void addStmtProcedure(String name, String sql, ProcedurePartitionData partitionData) {
        addProcedures(new ProcedureInfo(new String[0], name, sql, partitionData));
    }

    public void addMultiPartitionProcedures(final Class<?>... procedures) {
        final ArrayList<ProcedureInfo> procArray = new ArrayList<>();
        for (final Class<?> procedure : procedures) {
            procArray.add(new ProcedureInfo(procedure));
        }
        addProcedures(procArray);
    }

    public void addProcedure(final Class<?> cls) {
        addProcedures(new ProcedureInfo(cls));
    }

    public void addProcedure(final Class<?> cls, String partitionString) {
        addProcedures(new ProcedureInfo(cls,
                ProcedurePartitionData.fromPartitionInfoString(partitionString)));
    }

    public void addProcedure(final Class<?> cls, final ProcedurePartitionData partitionInfo) {
        addProcedures(new ProcedureInfo(cls, partitionInfo));
    }

    /*
     * List of procedures permitted to invoke the procedure
     */
    public void addProcedures(final ProcedureInfo... procedures) {
        final ArrayList<ProcedureInfo> procArray = new ArrayList<>();
        for (final ProcedureInfo procedure : procedures) {
            procArray.add(procedure);
        }
        addProcedures(procArray);
    }

    public void addProcedures(final Iterable<ProcedureInfo> procedures) {
        // check for duplicates and existings
        final HashSet<ProcedureInfo> newProcs = new HashSet<>();
        for (final ProcedureInfo procedure : procedures) {
            assert(newProcs.contains(procedure) == false);
            newProcs.add(procedure);
        }

        // add the procs
        for (final ProcedureInfo procedure : procedures) {

            // ALLOW clause in CREATE PROCEDURE stmt
            StringBuffer roleInfo = new StringBuffer();
            if(procedure.roles.length != 0) {
                roleInfo.append(" ALLOW ");
                for (String role : procedure.roles) {
                    roleInfo.append(role + ",");
                }
                int length = roleInfo.length();
                roleInfo.replace(length - 1, length, " ");
            }

            String partitionProcedureStatement = "";
            if(procedure.partitionData != null && procedure.partitionData.m_tableName != null) {
                String tableName = procedure.partitionData.m_tableName;
                String columnName = procedure.partitionData.m_columnName;
                String paramIndex = procedure.partitionData.m_paramIndex;

                partitionProcedureStatement = " PARTITION ON TABLE "+ tableName + " COLUMN " + columnName +
                        " PARAMETER " + paramIndex + " ";

                if (procedure.partitionData.m_tableName2 != null) {
                    String tableName2 = procedure.partitionData.m_tableName2;
                    String columnName2 = procedure.partitionData.m_columnName2;
                    String paramIndex2 = procedure.partitionData.m_paramIndex2;
                    partitionProcedureStatement += " AND ON TABLE "+ tableName2 + " COLUMN " + columnName2 +
                            " PARAMETER " + paramIndex2 + " ";
                }
            }

            if(procedure.cls != null) {
                transformer.append("CREATE PROCEDURE " + partitionProcedureStatement + roleInfo.toString() +
                        " FROM CLASS " + procedure.cls.getName() + ";");
            }
            else if(procedure.sql != null) {
                transformer.append("CREATE PROCEDURE " + procedure.name + partitionProcedureStatement + roleInfo.toString() +
                        " AS " + procedure.sql);
            }
        }
    }

    public void addSupplementalClasses(final Class<?>... supplementals) {
        final ArrayList<Class<?>> suppArray = new ArrayList<>();
        for (final Class<?> supplemental : supplementals) {
            suppArray.add(supplemental);
        }
        addSupplementalClasses(suppArray);
    }

    public void addSupplementalClasses(final Iterable<Class<?>> supplementals) {
        // check for duplicates and existings
        final HashSet<Class<?>> newSupps = new HashSet<>();
        for (final Class<?> supplemental : supplementals) {
            assert(newSupps.contains(supplemental) == false);
            assert(m_supplementals.contains(supplemental) == false);
            newSupps.add(supplemental);
        }

        // add the supplemental classes
        for (final Class<?> supplemental : supplementals) {
            m_supplementals.add(supplemental);
        }
    }

    public void addPartitionInfo(final String tableName, final String partitionColumnName) {
        transformer.append("PARTITION TABLE " + tableName + " ON COLUMN " + partitionColumnName + ";");
    }

    public void setHTTPDPort(int port) {
        m_httpdPortNo = port;
    }

    public void setJSONAPIEnabled(final boolean enabled) {
        m_jsonApiEnabled = enabled;
    }

    public void setSslEnabled(final boolean enabled) {
        m_sslEnabled = enabled;
    }

    public void setSslExternal(final boolean enabled) {
        m_sslExternal = enabled;
    }

    public void setSslInternal(final boolean enabled) {
        m_sslInternal = enabled;
    }

    public void setSslDR(final boolean enabled) {
        m_sslDR = enabled;
    }

    public void setKeyStoreInfo(final String path, final String password) {
        m_keystore = getResourcePath(path);
        m_keystorePassword = password;
    }

    public void setCertStoreInfo(final String path, final String password) {
        m_certstore = getResourcePath(path);;
        m_certstorePassword = password;
    }

    public void setSecurityEnabled(final boolean enabled, boolean createAdminUser) {
        m_securityEnabled = enabled;
        if (createAdminUser) {
            addUsers(new UserInfo[]
                    {new UserInfo("defaultadmin", "admin", new String[] {"ADMINISTRATOR"})});
        }
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

    public void setPartitionDetectionEnabled(boolean ppdEnabled) {
        m_ppdEnabled = ppdEnabled;
    }

    public void setHeartbeatTimeoutSeconds(int seconds) {
        m_heartbeatTimeout = seconds;
    }

    public void setFlushIntervals(int minimumInterval, int drFlushInterval, int exportFlushInterval) {
        org.voltdb.compiler.deploymentfile.ObjectFactory factory = new org.voltdb.compiler.deploymentfile.ObjectFactory();
        m_flushIntervals = factory.createFlushIntervalType();
        m_flushIntervals.setMinimum(minimumInterval);
        FlushIntervalType.Dr drFlush = new FlushIntervalType.Dr();
        drFlush.setInterval(drFlushInterval);
        m_flushIntervals.setDr(drFlush);
        FlushIntervalType.Export exportFlush = new FlushIntervalType.Export();
        exportFlush.setInterval(exportFlushInterval);
        m_flushIntervals.setExport(exportFlush);
    }

    // Return a priority policy that can be modified in place prior to compiling the project
    public PriorityPolicyType getPriorityPolicy() {
        if (m_priorityPolicy == null) {
            m_priorityPolicy = new PriorityPolicyType();
        }
        return m_priorityPolicy;
    }

    public void addImport(boolean enabled, String importType, String importFormat, String importBundle, Properties config) {
         addImport(enabled, importType, importFormat, importBundle, config, new Properties());
    }

    public void addImport(boolean enabled, String importType, String importFormat, String importBundle, Properties config, Properties formatConfig) {
        HashMap<String, Object> importConnector = new HashMap<>();
        importConnector.put("ilEnabled", enabled);
        importConnector.put("ilModule", importBundle);

        importConnector.put("ilConfig", config);
        if (importFormat != null) {
            importConnector.put("ilFormatter", importFormat);
        }
        if (formatConfig != null) {
            importConnector.put("ilFormatterConfig", formatConfig);
        }

        if ((importType != null) && !importType.trim().isEmpty()) {
            importConnector.put("ilImportType", importType);
        } else {
            importConnector.put("ilImportType", "custom");
        }

        m_ilImportConnectors.add(importConnector);
    }

    // Use this to update deployment with new or modified export targets
    public void clearExports() {
        getExportsConfiguration().getConfiguration().clear();
    }

    public void addExport(boolean enabled) {
        addExport(enabled, null, null);
    }

    public void addExport(boolean enabled, ServerExportEnum exportType, Properties config) {
        addExport(enabled, exportType, config, Constants.CONNECTORLESS_STREAM_TARGET_NAME);
    }

    public void addExport(boolean enabled, ServerExportEnum exportType, Properties config, String target) {
        addExport(enabled, exportType, null, config, target);
    }
    public void addExport(boolean enabled, ServerExportEnum exportType, String connectorClass, Properties config, String target) {
        org.voltdb.compiler.deploymentfile.ObjectFactory factory = new org.voltdb.compiler.deploymentfile.ObjectFactory();

        ExportConfigurationType exportConfig = factory.createExportConfigurationType();
        exportConfig.setEnabled(enabled);

        exportConfig.setType(exportType);
        if (exportType == ServerExportEnum.CUSTOM) {
            exportConfig.setExportconnectorclass(
                    connectorClass == null ? System.getProperty(ExportDataProcessor.EXPORT_TO_TYPE) : connectorClass);
        }

        exportConfig.setTarget(target);

        if (config == null) {
            config = new Properties();
            //If No config provided use file with outdir to user-specific tmp.
            config.put("outdir", "/tmp/" + System.getProperty("user.name"));
            config.putAll(ImmutableMap.<String, String>of(
                    "type","tsv", "batched","true", "with-schema","true", "nonce","zorag"
                    ));
        }
        List<PropertyType> configProperties = exportConfig.getProperty();

        for(Object nameObj: config.keySet()) {
            String name = (String) nameObj;

            PropertyType prop = factory.createPropertyType();
            prop.setName(name);
            prop.setValue(config.getProperty(name));

            configProperties.add(prop);
        }

        getExportsConfiguration().getConfiguration().add(exportConfig);
    }

    public TopicType addTopic(String topicName) {
        return addTopic(topicName, null, null);
    }

    public TopicType addTopic(String topicName, String procName, Map<String, String> properties) {
        assert !StringUtils.isBlank(topicName);
        TopicType topic = new TopicType();
        topic.setName(topicName);

        if (procName != null) {
            topic.setProcedure(procName);
        }

        if (properties != null) {
            properties.forEach((k, v) -> {
                PropertyType prop = new PropertyType();
                prop.setName(k);
                prop.setValue(v);
                topic.getProperty().add(prop);
            });
        }

        getTopicsConfiguration().getTopic().add(topic);
        return topic;
    }

    public void setCompilerDebugPrintStream(final PrintStream out) {
        m_compilerDebugPrintStream = out;
    }

    public void setMaxTempTableMemory(int max)
    {
        m_maxTempTableMemory = max;
    }

    public void setDRMasterHost(String drMasterHost) {
        m_drMasterHost = drMasterHost;
    }

    public void setDRBuffers(String drBuffers) {
        m_drBuffers = drBuffers;
    }

    public void setPreferredSource(int preferredSource) {
        m_preferredSource = preferredSource;
    }

    public void setDrConsumerConnectionEnabled() {
        m_drConsumerConnectionEnabled = true;
    }

    public void setDrConsumerConnectionDisabled() {
        m_drConsumerConnectionEnabled = false;
    }

    public void setDrConsumerSslPropertyFile(String sslPropertyFile) {
        m_drConsumerSslPropertyFile = getResourcePath(sslPropertyFile);
    }

    public void setDrProducerEnabled()
    {
        m_drProducerEnabled = true;
    }

    public void setDrProducerDisabled()
    {
        m_drProducerEnabled = false;
    }

    public void setDrNone() {
        m_drRole = DrRoleType.NONE;
    }

    public void setDrReplica() {
        m_drRole = DrRoleType.REPLICA;
    }

    public void setXDCR() {
        m_drRole = DrRoleType.XDCR;
    }

    public void setDrConflictRetention(String retention) {
        m_drConflictRetention = retention;
    }

    public boolean compile(final String jarPath) {
        return compile(jarPath, 1, 1, 0, null, 0) != null;
    }

    public boolean compile(final String jarPath,
            final int sitesPerHost,
            final int replication) {
        return compile(jarPath, sitesPerHost, 1,
                replication, null, 0) != null;
    }

    public boolean compile(final String jarPath,
            final int sitesPerHost,
            final int hostCount,
            final int replication) {
        return compile(jarPath, sitesPerHost, hostCount,
                replication, null, 0) != null;
    }

    public Catalog compile(final String jarPath,
            final int sitesPerHost,
            final int hostCount,
            final int replication, final int clusterId) {
        return compile(jarPath, sitesPerHost, hostCount,
                replication, null, clusterId);
    }

    public Catalog compile(final String jarPath,
            final int sitesPerHost,
            final int hostCount,
            final int replication,
            final String voltRoot) {
       return compile(jarPath, sitesPerHost, hostCount, replication, voltRoot, 0);
    }

    public Catalog compile(final String jarPath,
            final int sitesPerHost,
            final int hostCount,
            final int replication,
            final String voltRoot,
            final int clusterId) {
        VoltCompiler compiler = new VoltCompiler(m_drRole == DrRoleType.XDCR);
        if (compile(compiler, jarPath, voltRoot,
                       new DeploymentInfo(hostCount, sitesPerHost, replication, clusterId),
                       m_ppdEnabled, m_snapshotPath, m_ppdPrefix)) {
            return compiler.getCatalog();
        } else {
            return null;
        }
    }

    public boolean compile(
            final String jarPath, final int sitesPerHost,
            final int hostCount, final int replication,
            final String voltRoot, final int clusterId,
            final boolean ppdEnabled, final String snapshotPath,
            final String ppdPrefix)
    {
        VoltCompiler compiler = new VoltCompiler(m_drRole == DrRoleType.XDCR);
        return compile(compiler, jarPath, voltRoot,
                       new DeploymentInfo(hostCount, sitesPerHost, replication, clusterId),
                       ppdEnabled, snapshotPath, ppdPrefix);
    }

    public boolean compile(final VoltCompiler compiler,
                           final String jarPath,
                           final String voltRoot,
                           final DeploymentInfo deployment,
                           final boolean ppdEnabled,
                           final String snapshotPath,
                           final String ppdPrefix)
    {
        assert(jarPath != null);
        assert(deployment == null || deployment.sitesPerHost >= 1);
        assert(deployment == null || deployment.hostCount >= 1);
        assert(deployment == null || (deployment.clusterId >= 0 && deployment.clusterId <= 127));

        String deploymentVoltRoot = voltRoot;
        if (deployment != null) {
            if (voltRoot == null) {
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
                deploymentVoltRoot = voltRootPath;
            }
        }
        m_voltRootPath = deploymentVoltRoot;

        // Add the DDL in the transformer to the schema files before compilation
        try {
            addLiteralSchema(transformer.toString());
            transformer = new StringBuffer();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        String[] schemaPath = m_schemas.toArray(new String[0]);

        if (m_diagnostics != null) {
            compiler.enableDetailedCapture();
        }

        boolean success = false;
        success = compiler.compileFromDDL(jarPath, schemaPath);

        m_diagnostics = compiler.harvestCapturedDetail();
        if (m_compilerDebugPrintStream != null) {
            if (success) {
                compiler.summarizeSuccess(m_compilerDebugPrintStream, m_compilerDebugPrintStream, jarPath);
            } else {
                compiler.summarizeErrors(m_compilerDebugPrintStream, m_compilerDebugPrintStream);
            }
        }
        if (deployment != null) {
            compileDeploymentOnly(deploymentVoltRoot, deployment);
        }

        return success;
    }

    /** Generate a deployment file based on the options passed and set in this object.
     * @pre This VoltProjectBuilder has all its options set, except those passed as arguments to this method.
     * @param voltDbRoot
     * @param hostCount
     * @param sitesPerHost
     * @param replication
     * @param clusterId
     * @return path to deployment file that was written
     */
    public String compileDeploymentOnly(String voltDbRoot,
                                        int hostCount,
                                        int sitesPerHost,
                                        int replication,
                                        int clusterId)
    {
        DeploymentInfo deployment = new DeploymentInfo(hostCount, sitesPerHost, replication, clusterId);
        return compileDeploymentOnly(voltDbRoot, deployment);
    }

    private String compileDeploymentOnly(String voltDbRoot, DeploymentInfo deployment)
    {
        try {
            m_pathToDeployment = writeDeploymentFile(voltDbRoot, deployment);
        } catch (Exception e) {
            System.out.println("Failed to create deployment file in testcase.");
            e.printStackTrace();
            System.out.println("hostcount: " + deployment.hostCount);
            System.out.println("sitesPerHost: " + deployment.sitesPerHost);
            System.out.println("clusterId: " + deployment.clusterId);
            System.out.println("replication: " + deployment.replication);
            System.out.println("voltRoot: " + voltDbRoot);
            System.out.println("ppdEnabled: " + m_ppdEnabled);
            System.out.println("snapshotPath: " + m_snapshotPath);
            System.out.println("ppdPrefix: " + m_ppdPrefix);
            // sufficient to escape and fail test cases?
            throw new RuntimeException(e);
        }
        return m_pathToDeployment;
    }

    /**
     * Compile catalog with no deployment file generated so that the
     * internally-generated default gets used.
     *
     * @param jarPath path to output jar
     * @return true if successful
     */
    public boolean compileWithDefaultDeployment(final String jarPath) {
        VoltCompiler compiler = new VoltCompiler(false);
        return compile(compiler, jarPath, null, null, m_ppdEnabled, m_snapshotPath, m_ppdPrefix);
    }

    /**
     * After compile() has been called, a deployment file will be written. This method exposes its location so it can be
     * passed to org.voltdb.VoltDB on startup.
     * @return Returns the deployment file location.
     */
    public String getPathToDeployment() {
        if (m_pathToDeployment == null) {
            System.err.println("ERROR: Call compile() before trying to get the deployment path.");
            return null;
        } else {
            System.out.println("path to deployment is " + m_pathToDeployment);
            return m_pathToDeployment;
        }
    }

    /**
     * Utility method to take a string and put it in a file. This is used by
     * this class to write the project file to a temp file, but it also used
     * by some other tests.
     *
     * @param content The content of the file to create.
     * @return A reference to the file created or null on failure.
     */
    public static File writeStringToTempFile(final String content) {
        File tempFile;
        try {
            tempFile = File.createTempFile("myApp", ".tmp");
            // tempFile.deleteOnExit();

            final FileWriter writer = new FileWriter(tempFile);
            writer.write(content);
            writer.flush();
            writer.close();

            return tempFile;

        } catch (final IOException e) {
            e.printStackTrace();
            return null;
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
    private String writeDeploymentFile(
            String voltRoot, DeploymentInfo dinfo) throws IOException, JAXBException
    {
        org.voltdb.compiler.deploymentfile.ObjectFactory factory =
            new org.voltdb.compiler.deploymentfile.ObjectFactory();

        // <deployment>
        DeploymentType deployment = factory.createDeploymentType();
        JAXBElement<DeploymentType> doc = factory.createDeployment(deployment);

        // <cluster>
        ClusterType cluster = factory.createClusterType();
        deployment.setCluster(cluster);
        cluster.setHostcount(dinfo.hostCount);
        cluster.setSitesperhost(dinfo.sitesPerHost);
        cluster.setKfactor(dinfo.replication);
        cluster.setId(dinfo.clusterId);
        cluster.setSchema(m_useDDLSchema ? SchemaType.DDL : SchemaType.CATALOG);

        // <paths>
        PathsType paths = factory.createPathsType();
        deployment.setPaths(paths);
        if ((voltRoot != null) && !voltRoot.trim().isEmpty()) {
            Voltdbroot voltdbroot = factory.createPathsTypeVoltdbroot();
            paths.setVoltdbroot(voltdbroot);
            voltdbroot.setPath(voltRoot);
        }

        if (m_snapshotPath != null) {
            PathsType.Snapshots snapshotPathElement = factory.createPathsTypeSnapshots();
            snapshotPathElement.setPath(m_snapshotPath);
            paths.setSnapshots(snapshotPathElement);
        }

        if (m_deadHostTimeout != null) {
            HeartbeatType heartbeat = factory.createHeartbeatType();
            heartbeat.setTimeout(m_deadHostTimeout);
            deployment.setHeartbeat(heartbeat);
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
        if (m_securityEnabled) {
            try {
                provider = SecurityProviderString.fromValue(m_securityProvider);
            } catch (IllegalArgumentException shouldNotHappenSeeSetter) {
            }
        }
        security.setProvider(provider);

        // set the command log (which defaults to off)
        CommandLogType commandLogType = factory.createCommandLogType();
        commandLogType.setEnabled(m_commandLogEnabled);
        if (m_commandLogSync != null) {
            commandLogType.setSynchronous(m_commandLogSync.booleanValue());
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

        // <partition-detection>/<snapshot>
        PartitionDetectionType ppd = factory.createPartitionDetectionType();
        deployment.setPartitionDetection(ppd);
        ppd.setEnabled(m_ppdEnabled);

        // <heartbeat>
        // don't include this element if not explicitly set
        if (m_heartbeatTimeout != null) {
            HeartbeatType hb = factory.createHeartbeatType();
            deployment.setHeartbeat(hb);
            hb.setTimeout((int) m_heartbeatTimeout);
        }

        deployment.setSystemsettings(createSystemSettingsType(factory));

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
                user.setPlaintext(info.plaintext);

                // build up user/roles.
                if (info.roles.length > 0) {
                    final StringBuilder roles = new StringBuilder();
                    for (final String role : info.roles) {
                        if (roles.length() > 0) {
                            roles.append(",");
                        }
                        roles.append(role);
                    }
                    user.setRoles(roles.toString());
                }
            }
        }

        SslType ssl = factory.createSslType();
        deployment.setSsl(ssl);
        ssl.setEnabled(m_sslEnabled);
        ssl.setExternal(m_sslExternal);
        ssl.setInternal(m_sslInternal);
        ssl.setDr(m_sslDR);
        if (m_keystore!=null) {
            KeyOrTrustStoreType store = factory.createKeyOrTrustStoreType();
            store.setPath(m_keystore);
            store.setPassword(m_keystorePassword);
            ssl.setKeystore(store);
        }
        if (m_certstore!=null) {
            KeyOrTrustStoreType store = factory.createKeyOrTrustStoreType();
            store.setPath(m_certstore);
            store.setPassword(m_certstorePassword);
            ssl.setTruststore(store);
        }

        // <httpd>. Disabled unless port # is configured by a testcase
        // Omit element(s) when null.
        HttpdType httpd = factory.createHttpdType();
        deployment.setHttpd(httpd);
        httpd.setEnabled(m_httpdPortNo != -1);
        httpd.setPort(m_httpdPortNo);
        Jsonapi json = factory.createHttpdTypeJsonapi();
        httpd.setJsonapi(json);
        json.setEnabled(m_jsonApiEnabled);

        //SNMP
        SnmpType snmpType = factory.createSnmpType();
        if (m_snmpEnabled) {
            snmpType.setEnabled(true);
            snmpType.setTarget(m_snmpTarget);
            deployment.setSnmp(snmpType);
        }

        // <export>
        deployment.setExport(getExportsConfiguration());

        // <import>
        ImportType importt = factory.createImportType();
        deployment.setImport(importt);

        for (HashMap<String,Object> importConnector : m_ilImportConnectors) {
            ImportConfigurationType importConfig = factory.createImportConfigurationType();
            importConfig.setEnabled((boolean)importConnector.get("ilEnabled"));
            ServerImportEnum importType = ServerImportEnum.fromValue(((String)importConnector.get("ilImportType")).toLowerCase());
            importConfig.setType(importType);
            importConfig.setModule((String )importConnector.get("ilModule"));

            String formatter = (String) importConnector.get("ilFormatter");
            if (formatter != null) {
                importConfig.setFormat(formatter);
            }

            Properties config = (Properties)importConnector.get("ilConfig");
            if((config != null) && (config.size() > 0)) {
                String version = (String)config.get("version");
                if (version != null) {
                    importConfig.setVersion(version);
                }
                List<PropertyType> configProperties = importConfig.getProperty();

                for( Object nameObj: config.keySet()) {
                    String name = String.class.cast(nameObj);

                    PropertyType prop = factory.createPropertyType();
                    prop.setName(name);
                    prop.setValue(config.getProperty(name));

                    configProperties.add(prop);
                }
            }

            Properties formatConfig = (Properties) importConnector.get("ilFormatterConfig");
            if ((formatConfig != null) && (formatConfig.size() > 0)) {
                List<PropertyType> configProperties = importConfig.getFormatProperty();

                for (Object nameObj : formatConfig.keySet()) {
                    String name = String.class.cast(nameObj);
                    PropertyType prop = factory.createPropertyType();
                    prop.setName(name);
                    prop.setValue(formatConfig.getProperty(name));

                    configProperties.add(prop);
                }
            }

            importt.getConfiguration().add(importConfig);
        }

        DrType dr = factory.createDrType();
        deployment.setDr(dr);
        dr.setListen(m_drProducerEnabled);
        dr.setRole(m_drRole);
        if (m_drMasterHost != null && !m_drMasterHost.isEmpty()) {
            ConnectionType conn = factory.createConnectionType();
            dr.setConnection(conn);
            conn.setSource(m_drMasterHost);
            conn.setPreferredSource(m_preferredSource);
            conn.setEnabled(m_drConsumerConnectionEnabled);
            conn.setSsl(m_drConsumerSslPropertyFile);
        }
        if (m_drBuffers != null && !m_drBuffers.isEmpty()) {
            ConsumerLimitType consumerLimitType = factory.createConsumerLimitType();
            consumerLimitType.setMaxbuffers(Integer.parseInt(m_drBuffers));
            dr.setConsumerlimit(consumerLimitType);
        }
        if (m_drConflictRetention != null) {
            dr.setConflictretention(m_drConflictRetention);
        }

        deployment.setFeatures(m_featureOptions);
        setTopicsConfiguration(deployment);
        setThreadPoolsConfiguration(deployment);

        // Have some yummy boilerplate!
        File file = File.createTempFile("myAppDeployment", ".tmp");
        JAXBContext context = JAXBContext.newInstance(DeploymentType.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
                Boolean.TRUE);
        marshaller.marshal(doc, file);
        final String deploymentPath = file.getPath();
        return deploymentPath;
    }

    private void setTopicsConfiguration(DeploymentType deployment) {
        if (m_topicsConfiguration != null) {
            deployment.setTopics(m_topicsConfiguration);
        }
    }

    private void setThreadPoolsConfiguration(DeploymentType deployment) {
        if (m_threadPoolsConfiguration != null) {
            deployment.setThreadpools(m_threadPoolsConfiguration);
        }
    }

    private SystemSettingsType createSystemSettingsType(org.voltdb.compiler.deploymentfile.ObjectFactory factory)
    {
        SystemSettingsType systemSettingType = factory.createSystemSettingsType();
        Temptables temptables = factory.createSystemSettingsTypeTemptables();
        temptables.setMaxsize(m_maxTempTableMemory);
        systemSettingType.setTemptables(temptables);
        if (m_snapshotPriority != null) {
            SystemSettingsType.Snapshot snapshot = factory.createSystemSettingsTypeSnapshot();
            snapshot.setPriority(m_snapshotPriority);
            systemSettingType.setSnapshot(snapshot);
        }
        if (m_elasticThroughput != null || m_elasticDuration != null) {
            SystemSettingsType.Elastic elastic = factory.createSystemSettingsTypeElastic();
            if (m_elasticThroughput != null) {
                elastic.setThroughput(m_elasticThroughput);
            }
            if (m_elasticDuration != null) {
                elastic.setDuration(m_elasticDuration);
            }
            systemSettingType.setElastic(elastic);
        }
        if (m_queryTimeout != null) {
            SystemSettingsType.Query query = factory.createSystemSettingsTypeQuery();
            query.setTimeout(m_queryTimeout);
            systemSettingType.setQuery(query);
        }
        if (m_procedureLogThreshold != null) {
            SystemSettingsType.Procedure procedure = factory.createSystemSettingsTypeProcedure();
            procedure.setLoginfo(m_procedureLogThreshold);
            systemSettingType.setProcedure(procedure);
        }

        // Flush intervals and transaction policy
        systemSettingType.setFlushinterval(m_flushIntervals);
        systemSettingType.setPriorities(m_priorityPolicy);

        if (m_rssLimit != null || m_snmpRssLimit != null) {
            ResourceMonitorType monitorType = initializeResourceMonitorType(systemSettingType, factory);
            Memorylimit memoryLimit = factory.createResourceMonitorTypeMemorylimit();
            if (m_rssLimit != null) {
                memoryLimit.setSize(m_rssLimit);
            }
            if (m_snmpRssLimit != null) {
                memoryLimit.setAlert(m_snmpRssLimit);
            }
            monitorType.setMemorylimit(memoryLimit);
        }

        if (m_resourceCheckInterval != null) {
            ResourceMonitorType monitorType = initializeResourceMonitorType(systemSettingType, factory);
            monitorType.setFrequency(m_resourceCheckInterval);
        }

        SystemSettingsType.Clockskew clockSkewType = factory.createSystemSettingsTypeClockskew();
        if (clockSkewInterval != null) {
            clockSkewType.setInterval((int) clockSkewInterval.toMinutes());
        }
        systemSettingType.setClockskew(clockSkewType);

        setupDiskLimitType(systemSettingType, factory);

        return systemSettingType;
    }

    private void setupDiskLimitType(SystemSettingsType systemSettingsType,
            org.voltdb.compiler.deploymentfile.ObjectFactory factory) {

        Set<FeatureNameType> featureNames = new HashSet<> ();

        if (m_featureDiskLimits!= null && !m_featureDiskLimits.isEmpty()) {
            featureNames.addAll(m_featureDiskLimits.keySet());
        }
        if (m_snmpFeatureDiskLimits!= null && !m_snmpFeatureDiskLimits.isEmpty()) {
            featureNames.addAll(m_snmpFeatureDiskLimits.keySet());
        }

        if (featureNames.isEmpty()) {
            return;
        }

        DiskLimitType diskLimit = factory.createDiskLimitType();
        for (FeatureNameType featureName : featureNames) {
                DiskLimitType.Feature feature = factory.createDiskLimitTypeFeature();
                feature.setName(featureName);
                if (m_featureDiskLimits !=null && m_featureDiskLimits.containsKey(featureName)) {
                    feature.setSize(m_featureDiskLimits.get(featureName));
                }
                if (m_snmpFeatureDiskLimits !=null && m_snmpFeatureDiskLimits.containsKey(featureName)) {
                    feature.setAlert(m_snmpFeatureDiskLimits.get(featureName));
                }
                diskLimit.getFeature().add(feature);
        }

        ResourceMonitorType monitorType = initializeResourceMonitorType(systemSettingsType, factory);
        monitorType.setDisklimit(diskLimit);
    }

    private ResourceMonitorType initializeResourceMonitorType(SystemSettingsType systemSettingType,
            org.voltdb.compiler.deploymentfile.ObjectFactory factory) {
            ResourceMonitorType monitorType = systemSettingType.getResourcemonitor();
            if (monitorType == null) {
                monitorType = factory.createResourceMonitorType();
                systemSettingType.setResourcemonitor(monitorType);
            }

            return monitorType;
    }

    public File getPathToVoltRoot() {
        return new File(m_voltRootPath);
    }

    /** Provide a feedback path to monitor the VoltCompiler's plan output via harvestDiagnostics */
    public void enableDiagnostics() {
        // This empty dummy value enables the feature and provides a default fallback return value,
        // but gets replaced in the normal code path.
        m_diagnostics = new ArrayList<>();
    }

    /** Access the VoltCompiler's recent plan output, for diagnostic purposes */
    public List<String> harvestDiagnostics() {
        List<String> result = m_diagnostics;
        m_diagnostics = null;
        return result;
    }

    private String getResourcePath(String resource) {
        URL res = this.getClass().getResource(resource);
        return res == null ? resource : res.getPath();
    }

}
