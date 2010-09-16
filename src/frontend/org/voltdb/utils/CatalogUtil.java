/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.ClusterCompiler;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.xml.sax.SAXException;

/**
 *
 */
public abstract class CatalogUtil {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final String CATALOG_FILENAME = "catalog.txt";

    public static String loadCatalogFromJar(String pathToCatalog, VoltLogger log) {
        assert(pathToCatalog != null);

        String serializedCatalog = null;
        try {
            InMemoryJarfile jarfile = new InMemoryJarfile(pathToCatalog);
            byte[] serializedCatalogBytes = jarfile.get(CATALOG_FILENAME);
            serializedCatalog = new String(serializedCatalogBytes, "UTF-8");
        } catch (Exception e) {
            if (log != null)
                log.l7dlog( Level.FATAL, LogKeys.host_VoltDB_CatalogReadFailure.name(), e);
            return null;
        }

        return serializedCatalog;
    }

    /**
     * Get a unique id for a plan fragment by munging the indices of it's parents
     * and grandparents in the catalog.
     *
     * @param frag Catalog fragment to identify
     * @return unique id for fragment
     */
    public static long getUniqueIdForFragment(PlanFragment frag) {
        long retval = 0;
        CatalogType parent = frag.getParent();
        retval = ((long) parent.getParent().getRelativeIndex()) << 32;
        retval += ((long) parent.getRelativeIndex()) << 16;
        retval += frag.getRelativeIndex();

        return retval;
    }

    /**
     *
     * @param catalogTable
     * @return An empty table with the same schema as a given catalog table.
     */
    public static VoltTable getVoltTable(Table catalogTable) {
        List<Column> catalogColumns = CatalogUtil.getSortedCatalogItems(catalogTable.getColumns(), "index");

        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[catalogColumns.size()];

        int i = 0;
        for (Column catCol : catalogColumns) {
            columns[i++] = new VoltTable.ColumnInfo(catCol.getTypeName(), VoltType.get((byte)catCol.getType()));
        }

        return new VoltTable(columns);
    }

    /**
     * Given a set of catalog items, return a sorted list of them, sorted by
     * the value of a specified field. The field is specified by name. If the
     * field doesn't exist, trip an assertion. This is primarily used to sort
     * a table's columns or a procedure's parameters.
     *
     * @param <T> The type of item to sort.
     * @param items The set of catalog items.
     * @param sortFieldName The name of the field to sort on.
     * @return A list of catalog items, sorted on the specified field.
     */
    public static <T extends CatalogType> List<T> getSortedCatalogItems(CatalogMap<T> items, String sortFieldName) {
        assert(items != null);
        assert(sortFieldName != null);

        // build a treemap based on the field value
        TreeMap<Object, T> map = new TreeMap<Object, T>();
        boolean hasField = false;
        for (T item : items) {
            // check the first time through for the field
            if (hasField == false)
                hasField = item.getFields().contains(sortFieldName);
            assert(hasField == true);

            map.put(item.getField(sortFieldName), item);
        }

        // create a sorted list from the map
        ArrayList<T> retval = new ArrayList<T>();
        for (T item : map.values()) {
            retval.add(item);
        }

        return retval;
    }

    /**
     * For a given Table catalog object, return the PrimaryKey Index catalog object
     * @param catalogTable
     * @return The index representing the primary key.
     * @throws Exception if the table does not define a primary key
     */
    public static Index getPrimaryKeyIndex(Table catalogTable) throws Exception {

        // We first need to find the pkey constraint
        Constraint catalog_constraint = null;
        for (Constraint c : catalogTable.getConstraints()) {
            if (c.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                catalog_constraint = c;
                break;
            }
        }
        if (catalog_constraint == null) {
            throw new Exception("ERROR: Table '" + catalogTable.getTypeName() + "' does not have a PRIMARY KEY constraint");
        }

        // And then grab the index that it is using
        return (catalog_constraint.getIndex());
    }

    /**
     * Return all the of the primary key columns for a particular table
     * If the table does not have a primary key, then the returned list will be empty
     * @param catalogTable
     * @return An ordered list of the primary key columns
     */
    public static Collection<Column> getPrimaryKeyColumns(Table catalogTable) {
        Collection<Column> columns = new ArrayList<Column>();
        Index catalog_idx = null;
        try {
            catalog_idx = CatalogUtil.getPrimaryKeyIndex(catalogTable);
        } catch (Exception ex) {
            // IGNORE
            return (columns);
        }
        assert(catalog_idx != null);

        for (ColumnRef catalog_col_ref : getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
            columns.add(catalog_col_ref.getColumn());
        }
        return (columns);
    }

    /**
     * Convert a Table catalog object into the proper SQL DDL, including all indexes,
     * constraints, and foreign key references.
     * @param catalog_tbl
     * @return SQL Schema text representing the table.
     */
    public static String toSchema(Table catalog_tbl) {
        assert(!catalog_tbl.getColumns().isEmpty());
        final String spacer = "   ";

        Set<Index> skip_indexes = new HashSet<Index>();
        Set<Constraint> skip_constraints = new HashSet<Constraint>();

        String ret = "CREATE TABLE " + catalog_tbl.getTypeName() + " (";

        // Columns
        String add = "\n";
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType col_type = VoltType.get((byte)catalog_col.getType());

            // this next assert would be great if we dealt with default values well
            //assert(! ((catalog_col.getDefaultvalue() == null) && (catalog_col.getNullable() == false) ) );

            ret += add + spacer + catalog_col.getTypeName() + " " +
                   col_type.toSQLString() +
                   (col_type == VoltType.STRING && catalog_col.getSize() > 0 ? "(" + catalog_col.getSize() + ")" : "");

            // Default value
            String defaultvalue = catalog_col.getDefaultvalue();
            //VoltType defaulttype = VoltType.get((byte)catalog_col.getDefaulttype());
            boolean nullable = catalog_col.getNullable();
            // TODO: Shouldn't have to check whether the string contains "null"
            if (defaultvalue != null && defaultvalue.toLowerCase().equals("null") && nullable) {
                defaultvalue = null;
            }
            else { // XXX: if (defaulttype != VoltType.VOLTFUNCTION) {
                // TODO: Escape strings properly
                defaultvalue = "'" + defaultvalue + "'";
            }
            ret += " DEFAULT " + (defaultvalue != null ? defaultvalue : "NULL") +
                   (!nullable ? " NOT NULL" : "");

            // Single-column constraints
            for (ConstraintRef catalog_const_ref : catalog_col.getConstraints()) {
                Constraint catalog_const = catalog_const_ref.getConstraint();
                ConstraintType const_type = ConstraintType.get(catalog_const.getType());

                // Check if there is another column in our table with the same constraint
                // If there is, then we need to add it to the end of the table definition
                boolean found = false;
                for (Column catalog_other_col : catalog_tbl.getColumns()) {
                    if (catalog_other_col.equals(catalog_col)) continue;
                    if (catalog_other_col.getConstraints().getIgnoreCase(catalog_const.getTypeName()) != null) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    switch (const_type) {
                        case FOREIGN_KEY: {
                            Table catalog_fkey_tbl = catalog_const.getForeignkeytable();
                            Column catalog_fkey_col = null;
                            for (ColumnRef ref : catalog_const.getForeignkeycols()) {
                                catalog_fkey_col = ref.getColumn();
                                break; // Nasty hack to get first item
                            }

                            assert(catalog_fkey_col != null);
                            ret += " REFERENCES " + catalog_fkey_tbl.getTypeName() + " (" + catalog_fkey_col.getTypeName() + ")";
                            skip_constraints.add(catalog_const);
                            break;
                        }
                        default:
                            // Nothing for now
                    }
                }
            }

            add = ",\n";
        }

        // Constraints
        for (Constraint catalog_const : catalog_tbl.getConstraints()) {
            if (skip_constraints.contains(catalog_const)) continue;
            ConstraintType const_type = ConstraintType.get(catalog_const.getType());

            // Primary Keys / Unique Constraints
            if (const_type == ConstraintType.PRIMARY_KEY || const_type == ConstraintType.UNIQUE) {
                Index catalog_idx = catalog_const.getIndex();
                IndexType idx_type = IndexType.get(catalog_idx.getType());
                String idx_suffix = idx_type.getSQLSuffix();

                ret += add + spacer +
                       (!idx_suffix.isEmpty() ? "CONSTRAINT " + catalog_const.getTypeName() + " " : "") +
                       (const_type == ConstraintType.PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE") + " (";

                String col_add = "";
                for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                    ret += col_add + catalog_colref.getColumn().getTypeName();
                    col_add = ", ";
                } // FOR
                ret += ")";
                skip_indexes.add(catalog_idx);

            // Foreign Key
            } else if (const_type == ConstraintType.FOREIGN_KEY) {
                Table catalog_fkey_tbl = catalog_const.getForeignkeytable();
                String col_add = "";
                String our_columns = "";
                String fkey_columns = "";
                for (ColumnRef catalog_colref : catalog_const.getForeignkeycols()) {
                    // The name of the ColumnRef is the column in our base table
                    Column our_column = catalog_tbl.getColumns().getIgnoreCase(catalog_colref.getTypeName());
                    assert(our_column != null);
                    our_columns += col_add + our_column.getTypeName();

                    Column fkey_column = catalog_colref.getColumn();
                    assert(fkey_column != null);
                    fkey_columns += col_add + fkey_column.getTypeName();

                    col_add = ", ";
                }
                ret += add + spacer + "CONSTRAINT " + catalog_const.getTypeName() + " " +
                                      "FOREIGN KEY (" + our_columns + ") " +
                                      "REFERENCES " + catalog_fkey_tbl.getTypeName() + " (" + fkey_columns + ")";
            }
            skip_constraints.add(catalog_const);
        }
        ret += "\n);\n";

        // All other Indexes
        for (Index catalog_idx : catalog_tbl.getIndexes()) {
            if (skip_indexes.contains(catalog_idx)) continue;

            ret += "CREATE INDEX " + catalog_idx.getTypeName() +
                   " ON " + catalog_tbl.getTypeName() + " (";
            add = "";
            for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                ret += add + catalog_colref.getColumn().getTypeName();
                add = ", ";
            }
            ret += ");\n";
        }

        return ret;
    }

    /**
     * Return true if a table is a streamed / export-only table
     * This function is duplicated in CatalogUtil.h
     * @param database
     * @param table
     * @return true if a table is export-only or false otherwise
     */
    public static boolean isTableExportOnly(org.voltdb.catalog.Database database,
                                            org.voltdb.catalog.Table table)
    {
        // no export, no export only tables
        if (database.getConnectors().size() == 0) {
            return false;
        }

        // there is one well-known-named connector
        org.voltdb.catalog.Connector connector = database.getConnectors().get("0");

        // iterate the connector tableinfo list looking for tableIndex
        // tableInfo has a reference to a table - can compare the reference
        // to the desired table by looking at the relative index. ick.
        for (org.voltdb.catalog.ConnectorTableInfo tableInfo : connector.getTableinfo()) {
            if (tableInfo.getTable().getRelativeIndex() == table.getRelativeIndex()) {
                return tableInfo.getAppendonly();
            }
        }
        return false;
    }

    /**
     * Return true if a table is the source table for a materialized view.
     */
    public static boolean isTableMaterializeViewSource(org.voltdb.catalog.Database database,
                                                       org.voltdb.catalog.Table table)
    {
        CatalogMap<Table> tables = database.getTables();
        for (Table t : tables) {
            Table matsrc = t.getMaterializer();
            if ((matsrc != null) && (matsrc.getRelativeIndex() == table.getRelativeIndex())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parse the deployment.xml file and add its data into the catalog.
     * @param catalog Catalog to be updated.
     * @param pathToDeployment Path to the deployment.xml file.
     */
    public static boolean compileDeployment(Catalog catalog, String deploymentURL) {
        DeploymentType deployment = parseDeployment(deploymentURL);

        // wasn't a valid xml deployment file
        if (deployment == null) {
            hostLog.error("Not a valid XML deployment file at URL: " + deploymentURL);
            return false;
        }

        if (!validateDeployment(catalog, deployment)) {
            return false;
        }

        // set the cluster info
        setClusterInfo(catalog, deployment.getCluster());

        // set the users info
        setUsersInfo(catalog, deployment.getUsers());

        // set the HTTPD info
        setHTTPDInfo(catalog, deployment.getHttpd());

        return true;
    }

    /**
     * Parses the deployment XML file.
     * @param deploymentURL Path to the deployment.xml file.
     * @return a reference to the root <deployment> element.
     */
    public static DeploymentType parseDeployment(String deploymentURL) {
        // get the URL/path for the deployment and prep an InputStream
        InputStream deployIS = null;
        try {
            URL deployURL = new URL(deploymentURL);
            deployIS = deployURL.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            try {
                deployIS = new FileInputStream(deploymentURL);
            } catch (FileNotFoundException e) {
                deployIS = null;
            }
        } catch (IOException ioex) {
            deployIS = null;
        }

        // make sure the file exists
        if (deployIS == null) {
            hostLog.error("Could not locate deployment info at given URL: " + deploymentURL);
            return null;
        } else {
            hostLog.info("URL of deployment info: " + deploymentURL);
        }

        // get deployment info from xml file
        return getDeployment(deployIS);
    }

    /**
     * Get a reference to the root <deployment> element from the deployment.xml file.
     * @param pathToDeployment Path to the deployment.xml file.
     * @return Returns a reference to the root <deployment> element.
     */
    @SuppressWarnings("unchecked")
    private static DeploymentType getDeployment(InputStream deployIS) {
        try {
            JAXBContext jc = JAXBContext.newInstance("org.voltdb.compiler.deploymentfile");
            // This schema shot the sheriff.
            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);

            // This is ugly, but I couldn't get CatalogUtil.class.getResource("../compiler/DeploymentFileSchema.xsd")
            // to work and gave up.
            Schema schema = sf.newSchema(VoltDB.class.getResource("compiler/DeploymentFileSchema.xsd"));

            Unmarshaller unmarshaller = jc.createUnmarshaller();
            // But did not shoot unmarshaller!
            unmarshaller.setSchema(schema);
            JAXBElement<DeploymentType> result =
                (JAXBElement<DeploymentType>) unmarshaller.unmarshal(deployIS);
            DeploymentType deployment = result.getValue();
            return deployment;
        } catch (JAXBException e) {
            // Convert some linked exceptions to more friendly errors.
            if (e.getLinkedException() instanceof java.io.FileNotFoundException) {
                hostLog.error(e.getLinkedException().getMessage());
                return null;
            } else if (e.getLinkedException() instanceof org.xml.sax.SAXParseException) {
                hostLog.error("Error schema validating deployment.xml file. " + e.getLinkedException().getMessage());
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (SAXException e) {
            hostLog.error("Error schema validating deployment.xml file. " + e.getMessage());
            return null;
        }
    }

    /**
     * Validate the contents of the deployment.xml file. This is for things like making sure users aren't being added to
     * non-existent groups, not for validating XML syntax.
     * @param catalog Catalog to be validated against.
     * @param deployment Reference to root <deployment> element of deployment file to be validated.
     * @return Returns true if the deployment file is valid.
     */
    private static boolean validateDeployment(Catalog catalog, DeploymentType deployment) {
        if (deployment.getUsers() == null) {
            return true;
        }

        Cluster cluster = catalog.getClusters().get("cluster");
        Database database = cluster.getDatabases().get("database");
        Set<String> validGroups = new HashSet<String>();
        for (Group group : database.getGroups()) {
            validGroups.add(group.getTypeName());
        }

        for (UsersType.User user : deployment.getUsers().getUser()) {
            if (user.getGroups() == null)
                continue;

            for (String group : user.getGroups().split(",")) {
                group = group.trim();
                if (!validGroups.contains(group)) {
                    hostLog.error("Cannot assign user \"" + user.getName() + "\" to non-existent group \"" + group +
                            "\"");
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Set cluster info in the catalog.
     * @param catalog The catalog to be updated.
     * @param cluster A reference to the <cluster> element of the deployment.xml file.
     */
    private static void setClusterInfo(Catalog catalog, ClusterType cluster) {
        int hostCount = cluster.getHostcount();
        int sitesPerHost = cluster.getSitesperhost();
        String leader = cluster.getLeader();
        int kFactor = cluster.getKfactor();

        ClusterConfig config = new ClusterConfig(hostCount, sitesPerHost, kFactor, leader);
        hostLog.l7dlog(Level.INFO, LogKeys.compiler_VoltCompiler_LeaderAndHostCountAndSitesPerHost.name(),
                new Object[] {config.getLeaderAddress(), config.getHostCount(), config.getSitesPerHost()}, null);

        if (!config.validate()) {
            hostLog.error(config.getErrorMsg());
        } else {
            ClusterCompiler.compile(catalog, config);
        }
    }

    /**
     * Set user info in the catalog.
     * @param catalog The catalog to be updated.
     * @param users A reference to the <users> element of the deployment.xml file.
     */
    private static void setUsersInfo(Catalog catalog, UsersType users) {
        if (users == null) {
            return;
        }

        // TODO: The database name is not available in deployment.xml (it is defined in project.xml). However, it must
        // always be named "database", so I've temporarily hardcoded it here until a more robust solution is available.
        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");

        for (UsersType.User user : users.getUser()) {
            org.voltdb.catalog.User catUser = db.getUsers().add(user.getName());
            byte passwordHash[] = extractPassword(user.getPassword());
            catUser.setShadowpassword(Encoder.hexEncode(passwordHash));

            // process the @groups comma separated list
            if (user.getGroups() != null) {
                String grouplist[] = user.getGroups().split(",");
                for (final String group : grouplist) {
                    final GroupRef groupRef = catUser.getGroups().add(group);
                    final Group catalogGroup = db.getGroups().get(group);
                    if (catalogGroup != null) {
                        groupRef.setGroup(catalogGroup);
                    }
                }
            }
        }
    }

    private static void setHTTPDInfo(Catalog catalog, HttpdType httpd) {
        // defaults
        int httpdPort = 0;
        boolean jsonEnabled = true;

        Cluster cluster = catalog.getClusters().get("cluster");

        // if the httpd info is available, use it
        if (httpd != null) {
           httpdPort = httpd.getPort();
           // if the json api info is available, use it
           HttpdType.Jsonapi jsonapi = httpd.getJsonapi();
           if (jsonapi != null)
               jsonEnabled = jsonapi.getEnabled().equalsIgnoreCase("true");
        }

        // set the catalog info
        cluster.setHttpdportno(httpdPort);
        cluster.setJsonapi(jsonEnabled);
    }

    /** Read a hashed password from password. */
    private static byte[] extractPassword(String password) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (final NoSuchAlgorithmException e) {
            hostLog.l7dlog(Level.FATAL, LogKeys.compiler_VoltCompiler_NoSuchAlgorithm.name(), e);
            System.exit(-1);
        }
        final byte passwordHash[] = md.digest(md.digest(password.getBytes()));
        return passwordHash;
    }

}