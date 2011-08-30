/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.File;
import java.io.IOException;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.VoltFile;

public class CatalogContext {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // THE CATALOG!
    public final Catalog catalog;

    // PUBLIC IMMUTABLE CACHED INFORMATION
    public final Cluster cluster;
    public final Database database;
    public final CatalogMap<Procedure> procedures;
    public final CatalogMap<Site> sites;
    public final AuthSystem authSystem;
    public final int numberOfPartitions;
    public final int numberOfExecSites;
    public final int numberOfNodes;
    public final SiteTracker siteTracker;
    public final int catalogVersion;
    public final long catalogCRC;
    public final long deploymentCRC;
    public long m_transactionId;
    public final JdbcDatabaseMetaDataGenerator m_jdbc;

    // PRIVATE
    //private final String m_path;
    private final InMemoryJarfile m_jarfile;

    public CatalogContext(
            long transactionId,
            Catalog catalog,
            byte[] catalogBytes,
            long deploymentCRC,
            int version,
            long prevCRC) {
        m_transactionId = transactionId;
        // check the heck out of the given params in this immutable class
        assert(catalog != null);
        if (catalog == null)
            throw new RuntimeException("Can't create CatalogContext with null catalog.");

        //m_path = pathToCatalogJar;
        long tempCRC = 0;
        if (catalogBytes != null) {
            try {
                m_jarfile = new InMemoryJarfile(catalogBytes);
                tempCRC = m_jarfile.getCRC();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            catalogCRC = tempCRC;
        }
        else {
            m_jarfile = null;
            catalogCRC = prevCRC;
        }

        this.catalog = catalog;
        cluster = catalog.getClusters().get("cluster");
        database = cluster.getDatabases().get("database");
        procedures = database.getProcedures();
        authSystem = new AuthSystem(database, cluster.getSecurityenabled());
        sites = cluster.getSites();
        siteTracker = new SiteTracker(cluster.getSites());
        this.deploymentCRC = deploymentCRC;
        m_jdbc = new JdbcDatabaseMetaDataGenerator(catalog);

        // count nodes
        numberOfNodes = cluster.getHosts().size();

        // count exec sites
        int execSiteCount = 0;
        for (Site site : sites) {
            if (site.getPartition() != null) {
                assert (site.getIsexec());
                execSiteCount++;
            }
        }
        numberOfExecSites = execSiteCount;

        // count partitions
        numberOfPartitions = cluster.getPartitions().size();
        catalogVersion = version;
    }

    public CatalogContext update(
            long txnId,
            byte[] catalogBytes,
            String diffCommands,
            boolean incrementVersion,
            long deploymentCRC) {
        Catalog newCatalog = catalog.deepCopy();
        newCatalog.execute(diffCommands);
        int incValue = incrementVersion ? 1 : 0;
        long realDepCRC = deploymentCRC > 0 ? deploymentCRC : this.deploymentCRC;
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
        CatalogContext retval =
            new CatalogContext(
                    txnId,
                    newCatalog,
                    bytes,
                    realDepCRC,
                    catalogVersion + incValue,
                    catalogCRC);
        return retval;
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
    void logDebuggingInfoFromCatalog()
    {
        VoltLogger hostLog = new VoltLogger("HOST");
        if (cluster.getSecurityenabled()) {
            hostLog.info("Client authentication is enabled.");
        }
        else {
            hostLog.info("Client authentication is not enabled. Anonymous clients accepted.");
        }

        // auto snapshot info
        SnapshotSchedule ssched = database.getSnapshotschedule().get("default");
        if (ssched == null || !ssched.getEnabled()) {
            hostLog.info("No schedule set for automated snapshots.");
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
            hostLog.info("Automatic snapshots enabled, saved to " + ssched.getPath() +
                         " and named with prefix '" + ssched.getPrefix() + "'.");
            hostLog.info("Database will retain a history of " + ssched.getRetain() +
                         " snapshots, generated every " + msg + ".");
        }
    }
}
