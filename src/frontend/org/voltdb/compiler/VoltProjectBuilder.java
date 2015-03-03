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
import java.io.PrintStream;
import java.util.List;

import org.voltdb.compiler.CatalogBuilder.RoleInfo;
import org.voltdb.compiler.DeploymentBuilder.UserInfo;

/**
 * Alternate (programmatic) interface to VoltCompiler. Give the class all of
 * the information a user would put in a VoltDB project file and it will go
 * and build the project file and run the compiler on it.
 *
 * It will also create a deployment.xml file and apply its changes to the catalog.
 */
public class VoltProjectBuilder {
    private CatalogBuilder m_cb = new CatalogBuilder();
    private DeploymentBuilder m_db = new DeploymentBuilder();

    private String m_pathToDeployment = null;
/*
    final LinkedHashSet<String> m_schemas = new LinkedHashSet<String>();
    private StringBuffer transformer = new StringBuffer();

    public static final class ProcedureInfo {
        private final String roles[];
        private final Class<?> cls;
        private final String name;
        private final String sql;
        private final String partitionInfo;

        public ProcedureInfo(final String roles[], final Class<?> cls) {
            this.roles = roles;
            this.cls = cls;
            this.name = cls.getSimpleName();
            this.sql = null;
            this.partitionInfo = null;
            assert(this.name != null);
        }

        public ProcedureInfo(
                final String roles[],
                final String name,
                final String sql,
                final String partitionInfo) {
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
            this.partitionInfo = partitionInfo;
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
        final boolean useCustomAdmin;
        final int adminPort;
        final boolean adminOnStartup;

        public DeploymentInfo(int hostCount, int sitesPerHost, int replication,
                boolean useCustomAdmin, int adminPort, boolean adminOnStartup) {
            this.hostCount = hostCount;
            this.sitesPerHost = sitesPerHost;
            this.replication = replication;
            this.useCustomAdmin = useCustomAdmin;
            this.adminPort = adminPort;
            this.adminOnStartup = adminOnStartup;
        }
    }

    final LinkedHashSet<UserInfo> m_users = new LinkedHashSet<UserInfo>();
    final LinkedHashSet<Class<?>> m_supplementals = new LinkedHashSet<Class<?>>();

    // zero defaults to first open port >= 8080.
    // negative one means disabled in the deployment file.
    int m_httpdPortNo = -1;
    boolean m_jsonApiEnabled = true;

    BackendTarget m_target = BackendTarget.NATIVE_EE_JNI;
    PrintStream m_compilerDebugPrintStream = null;
    boolean m_securityEnabled = false;
    String m_securityProvider = SecurityProviderString.HASH.value();

    final Map<String, ProcInfoData> m_procInfoOverrides = new HashMap<String, ProcInfoData>();

    private String m_snapshotPath = null;
    private int m_snapshotRetain = 0;
    private String m_snapshotPrefix = null;
    private String m_snapshotFrequency = null;
    private String m_pathToDeployment = null;
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

    private List<String> m_diagnostics;

    private List<HashMap<String, Object>> m_elExportConnectors = new ArrayList<HashMap<String, Object>>();

    private Integer m_deadHostTimeout = null;

    private Integer m_elasticThroughput = null;
    private Integer m_elasticDuration = null;
    private Integer m_queryTimeout = null;

    private boolean m_useDDLSchema = false;

    private String m_drMasterHost;
    private Boolean m_drProducerEnabled = null;
    private Integer m_drProducerClusterId = null;
*/

    public VoltProjectBuilder setQueryTimeout(int target) {
        m_db.setQueryTimeout(target);
        return this;
    }

    public VoltProjectBuilder setElasticThroughput(int target) {
        m_db.setElasticThroughput(target);
        return this;
    }

    public VoltProjectBuilder setElasticDuration(int target) {
        m_db.setElasticDuration(target);
        return this;
    }

    public void setDeadHostTimeout(int deadHostTimeout) {
        m_db.setDeadHostTimeout(deadHostTimeout);
    }

    public void configureLogging(String internalSnapshotPath, String commandLogPath, Boolean commandLogSync,
            boolean commandLogEnabled, Integer fsyncInterval, Integer maxTxnsBeforeFsync, Integer logSize) {
        m_db.configureLogging(internalSnapshotPath, commandLogPath, commandLogSync, commandLogEnabled, fsyncInterval, maxTxnsBeforeFsync, logSize);
    }

    public void setSnapshotPriority(int priority) {
        m_db.setSnapshotPriority(priority);
    }

    public void addUsers(final UserInfo users[]) {
        m_db.addUsers(users);
    }

    public void addRoles(final RoleInfo roles[]) {
        m_cb.addRoles(roles);
/*
=======
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

    public void addSchema(final URL schemaURL) {
        assert(schemaURL != null);
        addSchema(schemaURL.getPath());
>>>>>>> master
*/
    }

    /**
     * This is test code written by Ryan, even though it was
     * committed by John.
     */
    public void addLiteralSchema(String ddlText) throws IOException {
        m_cb.addLiteralSchema(ddlText);
    }

    public void addStmtProcedure(String name, String sql) {
        m_cb.addStmtProcedure(name, sql);
    }

    public void addProcedures(final Class<?>... procedures) {
        m_cb.addProcedures(procedures);
    }

    /*
     * List of roles permitted to invoke the procedure
     */
    public void addPartitionInfo(final String tableName, final String partitionColumnName) {
        m_cb.addPartitionInfo(tableName, partitionColumnName);
    }

    public void setHTTPDPort(int port) {
        m_db.setHTTPDPort(port);
    }

    public void setSnapshotSettings(
            String frequency,
            int retain,
            String path,
            String prefix) {
        m_db.setSnapshotSettings(frequency, retain, path, prefix);
/*
=======
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

    public void addExport(boolean enabled, String exportTarget, Properties config) {
        addExport(enabled, exportTarget, config, Constants.DEFAULT_EXPORT_CONNECTOR_NAME);
    }
/*
    public void addExport(boolean enabled, String exportTarget, Properties config, String target) {
        HashMap<String, Object> exportConnector = new HashMap<String, Object>();
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
    }
*-/
    public void addExport(boolean enabled) {
        addExport(enabled, null, null);
>>>>>>> master
*/
    }

    public void setTableAsExportOnly(String name) {
        m_cb.setTableAsExportOnly(name);
    }

    public void setCompilerDebugPrintStream(final PrintStream out) {
        m_cb.setCompilerDebugPrintStream(out);
    }

    public void setMaxTempTableMemory(int max)
    {
        m_db.setMaxTempTableMemory(max);
    }

    public VoltProjectBuilder setVoltRoot(String voltRoot) {
        if (voltRoot != null) {
            m_db.setVoltRoot(voltRoot);
        }
        return this;
    }

    public void setDeploymentPath(String deploymentPath) {
        m_pathToDeployment = deploymentPath;
    }

    public void useCustomAdmin(int adminPort, boolean adminOnStartup)
    {
        m_db.useCustomAdmin(adminPort, adminOnStartup);
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

    public File getPathToVoltRoot() {
        return m_db.getPathToVoltRoot();
    }

    /** Provide a feedback path to monitor the VoltCompiler's plan output via harvestDiagnostics */
    public void enableDiagnostics() {
        // This empty dummy value enables the feature and provides a default fallback return value,
        // but gets replaced in the normal code path.
        m_cb.enableDiagnostics();
    }

    /** Access the VoltCompiler's recent plan output, for diagnostic purposes */
    public List<String> harvestDiagnostics() {
        return m_cb.harvestDiagnostics();
    }

    public DeploymentBuilder depBuilder() {
        return m_db;
    }

    public CatalogBuilder catBuilder() {
        return m_cb;
    }

}
