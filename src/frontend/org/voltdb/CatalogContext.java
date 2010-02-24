/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.utils.JarClassLoader;

public class CatalogContext {

    /** Pass this to constructor for catalog path in tests */
    public static final String NO_PATH = "EMPTY_PATH";

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

    // PRIVATE
    private final String m_path;
    private final JarClassLoader m_catalogClassLoader;

    public CatalogContext(Catalog catalog, String pathToCatalogJar) {
        // check the heck out of the given params in this immutable class
        assert(catalog != null);
        assert(pathToCatalogJar != null);
        if (catalog == null)
            throw new RuntimeException("Can't create CatalogContext with null catalog.");
        if (pathToCatalogJar == null)
            throw new RuntimeException("Can't create CatalogContext with null jar path.");

        m_path = pathToCatalogJar;
        if (pathToCatalogJar.startsWith(NO_PATH) == false)
            m_catalogClassLoader = new JarClassLoader(pathToCatalogJar);
        else
            m_catalogClassLoader = null;
        this.catalog = catalog;
        cluster = catalog.getClusters().get("cluster");
        database = cluster.getDatabases().get("database");
        procedures = database.getProcedures();
        authSystem = new AuthSystem(database, cluster.getSecurityenabled());
        sites = cluster.getSites();

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
    }

    public CatalogContext deepCopy() {
        return new CatalogContext(catalog.deepCopy(), m_path);
    }

    public CatalogContext update(String pathToNewJar, String diffCommands) {
        Catalog newCatalog = catalog.deepCopy();
        newCatalog.execute(diffCommands);
        CatalogContext retval = new CatalogContext(newCatalog, pathToNewJar);
        return retval;
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
        // this is a safety mechanism to prevent catalog classes overriding voltdb stuff
        if (procedureClassName.startsWith("org.voltdb."))
            return Class.forName(procedureClassName);

        // look in the catalog for the file
        return m_catalogClassLoader.loadClass(procedureClassName);
    }
}
