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

package org.voltdb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;
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
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.VoltFile;

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


    // THE CATALOG!
    public final Catalog catalog;

    // PUBLIC IMMUTABLE CACHED INFORMATION
    public final Cluster cluster;
    public final Database database;
    public final CatalogMap<Procedure> procedures;
    public final CatalogMap<Table> tables;
    public final AuthSystem authSystem;
    public final int catalogVersion;
    private final byte[] catalogHash;
    private final long catalogCRC;
    private final byte[] deploymentBytes;
    public final byte[] deploymentHash;
    public final long m_transactionId;
    public long m_uniqueId;
    public final JdbcDatabaseMetaDataGenerator m_jdbc;
    // Default procs are loaded on the fly
    // The DPM knows which default procs COULD EXIST
    //  and also how to get SQL for them.
    public final DefaultProcedureManager m_defaultProcs;

    /*
     * Planner associated with this catalog version.
     * Not thread-safe, should only be accessed by AsyncCompilerAgent
     */
    public final PlannerTool m_ptool;

    // PRIVATE
    private final InMemoryJarfile m_jarfile;

    // Some people may be interested in the JAXB rather than the raw deployment bytes.
    private DeploymentType m_memoizedDeployment;

    public CatalogContext(
            long transactionId,
            long uniqueId,
            Catalog catalog,
            byte[] catalogBytes,
            byte[] deploymentBytes,
            int version)
    {
        m_transactionId = transactionId;
        m_uniqueId = uniqueId;
        // check the heck out of the given params in this immutable class
        assert(catalog != null);
        if (catalog == null) {
            throw new RuntimeException("Can't create CatalogContext with null catalog.");
        }

        assert(deploymentBytes != null);
        if (deploymentBytes == null) {
            throw new RuntimeException("Can't create CatalogContext with null deployment bytes.");
        }

        assert(catalogBytes != null);
        if (catalogBytes != null) {
            try {
                m_jarfile = new InMemoryJarfile(catalogBytes);
                catalogCRC = m_jarfile.getCRC();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.catalogHash = CatalogUtil.makeCatalogOrDeploymentHash(catalogBytes);
        }
        else {
            throw new RuntimeException("Can't create CatalogContext with null catalog bytes.");
        }

        this.catalog = catalog;
        cluster = catalog.getClusters().get("cluster");
        database = cluster.getDatabases().get("database");
        procedures = database.getProcedures();
        tables = database.getTables();
        authSystem = new AuthSystem(database, cluster.getSecurityenabled());

        this.deploymentBytes = deploymentBytes;
        this.deploymentHash = CatalogUtil.makeCatalogOrDeploymentHash(deploymentBytes);
        m_memoizedDeployment = null;

        m_defaultProcs = new DefaultProcedureManager(database);

        m_jdbc = new JdbcDatabaseMetaDataGenerator(catalog, m_defaultProcs, m_jarfile);
        m_ptool = new PlannerTool(cluster, database, catalogHash);
        catalogVersion = version;

        if (procedures != null) {
            for (Procedure proc : procedures) {
                if (proc.getSinglepartition()) {
                    ProcedurePartitionInfo ppi = new ProcedurePartitionInfo(VoltType.get((byte)proc.getPartitioncolumn().getType()), proc.getPartitionparameter());
                    proc.setAttachment(ppi);
                }
            }
        }
    }

    public CatalogContext update(
            long txnId,
            long uniqueId,
            byte[] catalogBytes,
            String diffCommands,
            boolean incrementVersion,
            byte[] deploymentBytes)
    {
        Catalog newCatalog = catalog.deepCopy();
        newCatalog.execute(diffCommands);
        int incValue = incrementVersion ? 1 : 0;
        // If there's no new catalog bytes, preserve the old one rather than
        // bashing it
        byte[] bytes = catalogBytes;
        if (bytes == null) {
            try {
                bytes = this.getCatalogJarBytes();
            } catch (IOException e) {
                // Failure is not an option
                hostLog.fatal(e.getMessage());
            }
        }
        // Ditto for the deploymentBytes
        byte[] depbytes = deploymentBytes;
        if (depbytes == null) {
            depbytes = this.deploymentBytes;
        }
        CatalogContext retval =
            new CatalogContext(
                    txnId,
                    uniqueId,
                    newCatalog,
                    bytes,
                    depbytes,
                    catalogVersion + incValue);
        return retval;
    }

    /**
     * Get a file/entry (as bytes) given a key/path in the source jar.
     *
     * @param key In-jar path to file.
     * @return byte[] or null if the file doesn't exist.
     */
    public byte[] getFileInJar(String key) {
        return m_jarfile.get(key);
    }

    /**
     * Write the original JAR file to the specified path/name
     * @param path
     * @param name
     * @throws IOException
     */
    public Runnable writeCatalogJarToFile(String path, String name) throws IOException
    {
        File catalog_file = new VoltFile(path, name);
        if (catalog_file.exists())
        {
            catalog_file.delete();
        }
        return m_jarfile.writeToFile(catalog_file);
    }

    /**
     * Get the raw bytes of a catalog file for shipping around.
     */
    public byte[] getCatalogJarBytes() throws IOException {
        if (m_jarfile == null) {
            return null;
        }
        return m_jarfile.getFullJarBytes();
    }

    /**
     * Get the JAXB XML Deployment object, which is memoized
     */
    public DeploymentType getDeployment()
    {
        if (m_memoizedDeployment == null) {
            m_memoizedDeployment = CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
            // This should NEVER happen
            if (m_memoizedDeployment == null) {
                VoltDB.crashLocalVoltDB("The internal deployment bytes are invalid.  This should never occur; please contact VoltDB support with your logfiles.");
            }
        }
        return m_memoizedDeployment;
    }

    /**
     * Get the XML Deployment bytes
     */
    public byte[] getDeploymentBytes()
    {
        return deploymentBytes;
    }

    /**
     * Given a class name in the catalog jar, loads it from the jar, even if the
     * jar is served from a url and isn't in the classpath.
     *
     * @param procedureClassName The name of the class to load.
     * @return A java Class variable assocated with the class.
     * @throws ClassNotFoundException if the class is not in the jar file.
     */
    public Class<?> classForProcedure(String procedureClassName) throws ClassNotFoundException {
        //System.out.println("Loading class " + procedureClassName);

        // this is a safety mechanism to prevent catalog classes overriding voltdb stuff
        if (procedureClassName.startsWith("org.voltdb."))
            return Class.forName(procedureClassName);

        // look in the catalog for the file
        return m_jarfile.getLoader().loadClass(procedureClassName);
    }

    // Generate helpful status messages based on configuration present in the
    // catalog.  Used to generated these messages at startup and after an
    // @UpdateApplicationCatalog
    SortedMap<String, String> getDebuggingInfoFromCatalog()
    {
        SortedMap<String, String> logLines = new TreeMap<String, String>();

        // topology
        Deployment deployment = cluster.getDeployment().iterator().next();
        int hostCount = deployment.getHostcount();
        int sitesPerHost = deployment.getSitesperhost();
        int kFactor = deployment.getKfactor();
        logLines.put("deployment1",
                String.format("Cluster has %d hosts with leader hostname: \"%s\". %d sites per host. K = %d.",
                hostCount, VoltDB.instance().getConfig().m_leader, sitesPerHost, kFactor));

        int replicas = kFactor + 1;
        int partitionCount = sitesPerHost * hostCount / replicas;
        logLines.put("deployment2",
                String.format("The entire cluster has %d %s of%s %d logical partition%s.",
                replicas,
                replicas > 1 ? "copies" : "copy",
                partitionCount > 1 ? " each of the" : "",
                partitionCount,
                partitionCount > 1 ? "s" : ""));

        // voltdb root
        logLines.put("voltdbroot", "Using \"" + cluster.getVoltroot() + "\" for voltdbroot directory.");

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
            final String frequencyUnitString = ssched.getFrequencyunit().toLowerCase();
            final char frequencyUnit = frequencyUnitString.charAt(0);
            String msg = "[unknown frequency]";
            switch (frequencyUnit) {
            case 's':
                msg = String.valueOf(ssched.getFrequencyvalue()) + " seconds";
                break;
            case 'm':
                msg = String.valueOf(ssched.getFrequencyvalue()) + " minutes";
                break;
            case 'h':
                msg = String.valueOf(ssched.getFrequencyvalue()) + " hours";
                break;
            }
            logLines.put("snapshot-schedule1", "Automatic snapshots enabled, saved to " + ssched.getPath() +
                         " and named with prefix '" + ssched.getPrefix() + "'.");
            logLines.put("snapshot-schedule2", "Database will retain a history of " + ssched.getRetain() +
                         " snapshots, generated every " + msg + ".");
        }

        return logLines;
    }

    public long getCatalogCRC() {
        return catalogCRC;
    }

    public byte[] getCatalogHash()
    {
        return catalogHash;
    }
}
