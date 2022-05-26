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

package org.voltdb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONException;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.TimeUtils;

import com.google_voltpatches.common.collect.ImmutableMap;

public class CatalogContext {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final class ProcedurePartitionInfo {
        VoltType type;
        int index;
        public ProcedurePartitionInfo(VoltType type, int index) {
            this.type = type;
            this.index = index;
        }
    }

    public static class CatalogInfo {
        public InMemoryJarfile m_jarfile;
        public final long m_catalogCRC;
        public final byte[] m_catalogHash;
        public final byte[] m_deploymentBytes;
        public final byte[] m_deploymentHash;
        public final UUID m_deploymentHashForConfig;
        public Catalog m_catalog;
        public ConcurrentLinkedQueue<ImmutableMap<String, ProcedureRunner>> m_preparedProcRunners;

        public CatalogInfo(byte[] catalogBytes, byte[] catalogBytesHash, byte[] deploymentBytes) {
            if (deploymentBytes == null) {
                throw new IllegalArgumentException("Can't create CatalogContext with null deployment bytes.");
            }
            if (catalogBytes == null) {
                throw new IllegalArgumentException("Can't create CatalogContext with null catalog bytes.");
            }

            try {
                m_jarfile = new InMemoryJarfile(catalogBytes);
                m_catalogCRC = m_jarfile.getCRC();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (catalogBytesHash == null) {
                m_catalogHash = m_jarfile.getSha1Hash();
            } else {
                // This is expensive to compute so if it was passed in, use it.
                m_catalogHash = catalogBytesHash;
            }

            m_deploymentBytes = deploymentBytes;
            m_deploymentHash = CatalogUtil.makeHash(deploymentBytes);
            m_deploymentHashForConfig = CatalogUtil.makeDeploymentHashForConfig(deploymentBytes);
        }

    }

    // THE CATALOG!
    public Catalog catalog;

    // PUBLIC IMMUTABLE CACHED INFORMATION
    public final Cluster cluster;
    public final Database database;
    public final CatalogMap<Procedure> procedures;
    public final CatalogMap<Table> tables;
    public final AuthSystem authSystem;
    // database settings. contains both cluster and path settings
    private final DbSettings m_dbSettings;

    public final int catalogVersion;
    public final CatalogInfo m_catalogInfo;
    // prepared catalog information in non-blocking path
    public CatalogInfo m_preparedCatalogInfo = null;

    public final long m_genId; // export generation id

    // Default procs are loaded on the fly
    public final DefaultProcedureManager m_defaultProcs;

    // Planner associated with this catalog version, Not thread-safe
    public final PlannerTool m_ptool;
    public final JdbcDatabaseMetaDataGenerator m_jdbc;
    public final HostMessenger m_messenger;

    // Some people may be interested in the JAXB rather than the raw deployment bytes.
    private DeploymentType m_memoizedDeployment;

    public long m_lastUpdateCoreDuration = -1; // in nano seconds

    // This is the Calcite schema for a single database. This object is used to update
    // the Calcite schema when the catalog is updated.
    private SchemaPlus m_schemaPlus;

    /**
     * Constructor especially used during @CatalogContext update when @param hasSchemaChange is false.
     * When @param hasSchemaChange is true, @param defaultProcManager and @param plannerTool will be created as new.
     * Otherwise, it will try to use the ones passed in to save CPU cycles for performance reason.
     * @param genId
     * @param catalog
     * @param settings
     * @param catalogInfo
     * @param version
     * @param messenger
     * @param hasSchemaChange
     * @param defaultProcManager
     * @param plannerTool
     */
    public CatalogContext(
            Catalog catalog,
            DbSettings settings,
            int version,
            long genId,
            CatalogInfo catalogInfo,
            DefaultProcedureManager defaultProcManager,
            PlannerTool plannerTool,
            HostMessenger messenger,
            boolean hasSchemaChange) {
        // check the heck out of the given params in this immutable class
        if (catalog == null) {
            throw new IllegalArgumentException("Can't create CatalogContext with null catalog.");
        } else if (settings == null) {
            throw new IllegalArgumentException("Cant't create CatalogContext with null cluster settings");
        }

        this.catalog = catalog;
        cluster = catalog.getClusters().get("cluster");
        database = cluster.getDatabases().get("database");
        procedures = database.getProcedures();
        tables = database.getTables();
        authSystem = new AuthSystem(database, cluster.getSecurityenabled());
        m_dbSettings = settings;

        catalogVersion = version;
        m_genId = genId;

        m_catalogInfo = catalogInfo;

        // If there is no schema change, default procedures will not be changed.
        // Also, the planner tool can be almost reused except updating the catalog hash string.
        // When there is schema change, we just reload every default procedure and create new planner tool
        // by applying the existing schema, which are costly in the UAC MP blocking path.
        if (hasSchemaChange) {
            m_defaultProcs = new DefaultProcedureManager(database);
            m_ptool = new PlannerTool(database, m_catalogInfo.m_catalogHash);
        } else {
            m_defaultProcs = defaultProcManager;
            m_ptool = plannerTool.updateWhenNoSchemaChange(database, m_catalogInfo.m_catalogHash);
        }

        m_jdbc = new JdbcDatabaseMetaDataGenerator(catalog, m_defaultProcs, m_catalogInfo.m_jarfile);
        m_messenger = messenger;

        if (procedures != null) {
            for (Procedure proc : procedures) {
                if (proc.getSinglepartition() && proc.getPartitiontable() != null) {
                    ProcedurePartitionInfo ppi =
                            new ProcedurePartitionInfo(VoltType.get((byte)proc.getPartitioncolumn().getType()),
                                                       proc.getPartitionparameter());
                    proc.setAttachment(ppi);
                }
            }
        }

        m_memoizedDeployment = null;
    }

    /**
     * Constructor of @CatalogConext used when creating brand-new instances.
     * @param catalog
     * @param settings
     * @param version
     * @param genId
     * @param catalogBytes
     * @param catalogBytesHash
     * @param deploymentBytes
     * @param messenger
     */
    public CatalogContext(
            Catalog catalog,
            DbSettings settings,
            int version,
            long genId,
            byte[] catalogBytes,
            byte[] catalogBytesHash,
            byte[] deploymentBytes,
            HostMessenger messenger) {
        this(catalog, settings, version, genId,
             new CatalogInfo(catalogBytes, catalogBytesHash, deploymentBytes),
             null, null, messenger, true);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public ClusterSettings getClusterSettings() {
        return m_dbSettings.getCluster();
    }

    public CatalogMap<Table> getTables() {
        return tables;
    }

    public NodeSettings getNodeSettings() {
        return m_dbSettings.getNodeSetting();
    }

    public DbSettings getDbSettings() {
        return m_dbSettings;
    }

    public Catalog getNewCatalog(String diffCommands) {
        Catalog newCatalog = catalog.deepCopy();
        newCatalog.execute(diffCommands);
        return newCatalog;
    }

    public Database getDatabase() {
        return database;
    }

    /**
     * Get the Calcite schema associated with the default database
     */
    public SchemaPlus getSchemaPlus() {
        return m_schemaPlus;
    }

    public CatalogContext update(
            boolean isForReplay,
            Catalog newCatalog,
            int nextCatalogVersion,
            long genId,
            CatalogInfo catalogInfo,
            HostMessenger messenger,
            boolean hasSchemaChange) {
        assert(newCatalog != null);
        assert(catalogInfo != null);

        if (!isForReplay) {
            catalogInfo = m_preparedCatalogInfo;
            newCatalog = catalogInfo.m_catalog;
        }

        CatalogContext retval =
            new CatalogContext(
                    newCatalog,
                    this.m_dbSettings,
                    nextCatalogVersion, // version increment
                    genId,
                    catalogInfo,
                    m_defaultProcs,
                    m_ptool,
                    messenger,
                    hasSchemaChange);
        return retval;
    }

    public ImmutableMap<String, ProcedureRunner> getPreparedUserProcedureRunners(SiteProcedureConnection site) {

        ImmutableMap<String, ProcedureRunner> userProcRunner = m_catalogInfo.m_preparedProcRunners.poll();

        if (userProcRunner == null) {
            // somehow there is no prepared user procedure runner map left, then prepare it again

            CatalogMap<Procedure> catalogProcedures = database.getProcedures();
            try {
                userProcRunner = LoadedProcedureSet.loadUserProcedureRunners(catalogProcedures,
                                                                        m_catalogInfo.m_jarfile.getLoader(),
                                                                        null, null);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        for (ProcedureRunner runner: userProcRunner.values()) {
            // swap site and initiate the statistics
            runner.initSiteAndStats(site);
        }

        return userProcRunner;
    }

    public enum CatalogJarWriteMode {
        START_OR_RESTART,
        CATALOG_UPDATE,
        RECOVER
    }

    /**
     * Write, replace or update the catalog jar based on different cases. This function
     * assumes any IOException should lead to fatal crash.
     * @param path
     * @param name
     * @throws IOException
     */
    public Runnable writeCatalogJarToFile(String path, String name, CatalogJarWriteMode mode) throws IOException {
        File catalogFile = new File(path, name);
        File catalogTmpFile = new File(path, name + ".tmp");

        if (mode == CatalogJarWriteMode.CATALOG_UPDATE) {
            // This means a @UpdateCore case, the asynchronous writing of
            // jar file has finished, rename the jar file
            catalogFile.delete();
            catalogTmpFile.renameTo(catalogFile);
            return null;
        }

        if (mode == CatalogJarWriteMode.START_OR_RESTART) {
            // This happens in the beginning of ,
            // when the catalog jar does not yet exist. Though the contents
            // written might be a default one and could be overwritten later
            // by @UAC, @UpdateClasses, etc.
            return m_catalogInfo.m_jarfile.writeToFile(catalogFile);
        }

        if (mode == CatalogJarWriteMode.RECOVER) {
            // we must overwrite the file (the file may have been changed)
            catalogFile.delete();
            if (catalogTmpFile.exists()) {
                // If somehow the catalog temp jar is not cleaned up, then delete it
                catalogTmpFile.delete();
            }

            return m_catalogInfo.m_jarfile.writeToFile(catalogFile);
        }

        VoltDB.crashLocalVoltDB("Unsupported mode to write catalog jar", true, null);
        return null;
    }

    /**
     * Given a class name in the catalog jar, loads it from the jar, even if the
     * jar is served from an URL and isn't in the classpath.
     *
     * @param procedureClassName The name of the class to load.
     * @return A java Class variable associated with the class.
     * @throws ClassNotFoundException if the class is not in the jar file.
     */
    public Class<?> classForProcedureOrUDF(String procedureClassName)
            throws LinkageError, ExceptionInInitializerError, ClassNotFoundException {
        return classForProcedureOrUDF(procedureClassName, m_catalogInfo.m_jarfile.getLoader());
    }

    public static Class<?> classForProcedureOrUDF(String procedureClassName, ClassLoader loader)
            throws LinkageError, ExceptionInInitializerError, ClassNotFoundException {
        // this is a safety mechanism to prevent catalog classes overriding VoltDB stuff
        if (procedureClassName.startsWith("org.voltdb.")) {
            return Class.forName(procedureClassName);
        }

        // look in the catalog for the file
        return Class.forName(procedureClassName, true, loader);
    }

    // Generate helpful status messages based on configuration present in the
    // catalog.  Used to generated these messages at startup and after an
    // @UpdateApplicationCatalog
    SortedMap<String, String> getDebuggingInfoFromCatalog(boolean verbose) {
        SortedMap<String, String> logLines = new TreeMap<>();

        // topology
        Deployment deployment = cluster.getDeployment().iterator().next();
        int hostCount = m_dbSettings.getCluster().hostcount();
        if (verbose) {
            Map<Integer, Integer> sphMap;
            try {
                sphMap = m_messenger.getSitesPerHostMapFromZK();
            } catch (KeeperException | InterruptedException | JSONException e) {
                hostLog.warn("Failed to get sitesperhost information from Zookeeper", e);
                sphMap = null;
            }
            int kFactor = deployment.getKfactor();
            if (sphMap == null) {
                logLines.put("deployment1",
                        String.format("Cluster has %d hosts with leader hostname: \"%s\". [unknown] local sites count. K = %d.",
                                hostCount, VoltDB.instance().getConfig().m_leader, kFactor));
                logLines.put("deployment2", "Unable to retrieve partition information from the cluster.");
            } else {
                int localSitesCount = sphMap.get(m_messenger.getHostId());
                logLines.put("deployment1",
                        String.format("Cluster has %d hosts with leader hostname: \"%s\". %d local sites count. K = %d.",
                                hostCount, VoltDB.instance().getConfig().m_leader, localSitesCount, kFactor));

                int totalSitesCount = 0;
                for (Map.Entry<Integer, Integer> e : sphMap.entrySet()) {
                    totalSitesCount += e.getValue();
                }
                int replicas = kFactor + 1;
                int partitionCount = totalSitesCount / replicas;
                logLines.put("deployment2",
                        String.format("The entire cluster has %d %s of%s %d logical partition%s.",
                                replicas,
                                replicas > 1 ? "copies" : "copy",
                                        partitionCount > 1 ? " each of the" : "",
                                                partitionCount,
                                                partitionCount > 1 ? "s" : ""));
            }
        }

        // voltdb root
        logLines.put("voltdbroot", "Using \"" + VoltDB.instance().getVoltDBRootPath() + "\" for voltdbroot directory.");

        // partition detection
        if (cluster.getNetworkpartition()) {
            logLines.put("partition-detection", "Detection of network partitions in the cluster is enabled.");
        }
        else {
            logLines.put("partition-detection", "Detection of network partitions in the cluster is not enabled.");
        }

        // security info
        if (cluster.getSecurityenabled()) {
            logLines.put("sec-enabled", "Client authentication is enabled.");
        }
        else {
            logLines.put("sec-enabled", "Client authentication is not enabled. Anonymous clients accepted.");
        }

        // auto snapshot info
        SnapshotSchedule ssched = database.getSnapshotschedule().get("default");
        if (ssched == null || !ssched.getEnabled()) {
            logLines.put("snapshot-schedule1", "No schedule set for automated snapshots.");
        }
        else {
            TimeUnit unit = TimeUtils.convertTimeUnit(ssched.getFrequencyunit());
            String msg = "[unknown frequency]";
            if (unit != null) {
                msg = String.format("%s %s", ssched.getFrequencyvalue(), unit.name().toLowerCase());
            }
            logLines.put("snapshot-schedule1", "Automatic snapshots enabled, saved to " + VoltDB.instance().getSnapshotPath() +
                         " and named with prefix '" + ssched.getPrefix() + "'.");
            logLines.put("snapshot-schedule2", "Database will retain a history of " + ssched.getRetain() +
                         " snapshots, generated every " + msg + ".");
        }

        return logLines;
    }

    public InMemoryJarfile getCatalogJar() {
        return m_catalogInfo.m_jarfile;
    }

    public long getCatalogCRC() {
        return m_catalogInfo.m_catalogCRC;
    }

    public byte[] getCatalogHash()
    {
        return m_catalogInfo.m_catalogHash;
    }

    public byte[] getDeploymentHash() {
        return m_catalogInfo.m_deploymentHash;
    }

    /**
     * @param catalogHash
     * @param deploymentHash
     * @return true if the prepared catalog mismatch the catalog update invocation, false otherwise
     */
    public boolean checkMismatchedPreparedCatalog(byte[] catalogHash, byte[] deploymentHash) {
        if (m_preparedCatalogInfo == null) {
            // this is the replay case that does not prepare the catalog
            return false;
        }

        if (!Arrays.equals(m_preparedCatalogInfo.m_catalogHash, catalogHash) ||
            !Arrays.equals(m_preparedCatalogInfo.m_deploymentHash, deploymentHash)) {
            return true;
        }

        return false;
    }

    /**
     * Get the JAXB XML Deployment object, which is memoized
     */
    public DeploymentType getDeployment()
    {
        if (m_memoizedDeployment == null) {
            m_memoizedDeployment = CatalogUtil.getDeployment(
                    new ByteArrayInputStream(m_catalogInfo.m_deploymentBytes));
            // This should NEVER happen
            if (m_memoizedDeployment == null) {
                VoltDB.crashLocalVoltDB("The internal deployment bytes are invalid.  This should never occur; please contact VoltDB support with your logfiles.");
            }
        }
        return m_memoizedDeployment;
    }

    /**
     * Safe wrapper around the above. Various unit tests set up an
     * empty m_deploymentBytes, which getDeployment cannot deal with.
     * This variant does a precheck; it should be used only if the
     * caller is prepared to handle a null return.
     */
    public DeploymentType getDeploymentSafely() {
        byte[] b = m_catalogInfo.m_deploymentBytes;
        if (b == null || b.length == 0) {
            return null;
        }
        return getDeployment();
    }

    /**
     * Get the XML Deployment bytes
     */
    public byte[] getDeploymentBytes()
    {
        return m_catalogInfo.m_deploymentBytes;
    }

    public String getCatalogLogString() {
        return String.format("Catalog: catalog hash %s, deployment hash %s, version %d",
                                Encoder.hexEncode(m_catalogInfo.m_catalogHash).substring(0, 10),
                                Encoder.hexEncode(m_catalogInfo.m_deploymentHash).substring(0, 10),
                                catalogVersion);
    }

    /**
     * Get the raw bytes of a catalog file for shipping around.
     */
    public byte[] getCatalogJarBytes() throws IOException {
        if (m_catalogInfo.m_jarfile == null) {
            return null;
        }
        return m_catalogInfo.m_jarfile.getFullJarBytes();
    }


    /**
     * Get a file/entry (as bytes) given a key/path in the source jar.
     *
     * @param key In-jar path to file.
     * @return byte[] or null if the file doesn't exist.
     */
    public byte[] getFileInJar(String key) {
        return m_catalogInfo.m_jarfile.get(key);
    }

    /**
     * Set the Calcite schema associated with the default database
     * @param schemaPlus the updated schema
     */
    public void setSchemaPlus(SchemaPlus schemaPlus) {
        m_schemaPlus = schemaPlus;
    }
}
