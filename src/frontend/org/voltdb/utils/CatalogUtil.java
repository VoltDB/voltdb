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

package org.voltdb.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
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
import java.util.zip.CRC32;

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
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.ClusterCompiler;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.deploymentfile.AdminModeType;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.CommandLogType.Frequency;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PathEntry;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType.Temptables;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
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

    public static String loadCatalogFromJar(byte[] catalogBytes, VoltLogger log) {
        assert(catalogBytes != null);

        String serializedCatalog = null;
        try {
            InMemoryJarfile jarfile = new InMemoryJarfile(catalogBytes);
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
     * Serialize a file into bytes. Used to serialize catalog and deployment
     * file for UpdateApplicationCatalog on the client.
     *
     * @param path
     * @return a byte array of the file
     * @throws IOException
     *             If there are errors reading the file
     */
    public static byte[] toBytes(File path) throws IOException {
        FileInputStream fin = new FileInputStream(path);
        byte[] buffer = new byte[(int) fin.getChannel().size()];
        try {
            if (fin.read(buffer) == -1) {
                throw new IOException("File " + path.getAbsolutePath() + " is empty");
            }
        } finally {
            fin.close();
        }
        return buffer;
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
     * Return true if a table is a streamed / export table
     * This function is duplicated in CatalogUtil.h
     * @param database
     * @param table
     * @return true if a table is export or false otherwise
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

    public static long compileDeploymentAndGetCRC(Catalog catalog, String deploymentURL, boolean crashOnFailedValidation) {
        DeploymentType deployment = CatalogUtil.parseDeployment(deploymentURL);
        if (deployment == null) {
            return -1;
        }
        return compileDeploymentAndGetCRC(catalog, deployment, crashOnFailedValidation, false);
    }

    public static long compileDeploymentStringAndGetCRC(Catalog catalog, String deploymentString, boolean crashOnFailedValidation) {
        DeploymentType deployment = CatalogUtil.parseDeploymentFromString(deploymentString);
        if (deployment == null) {
            return -1;
        }
        return compileDeploymentAndGetCRC(catalog, deployment, crashOnFailedValidation, true);
    }

    /**
     * Parse the deployment.xml file and add its data into the catalog.
     * @param catalog Catalog to be updated.
     * @param deployment Parsed representation of the deployment.xml file.
     * @param crashOnFailedValidation
     * @param printLog Whether or not to print the cluster configuration.
     * @return CRC of the deployment contents (>0) or -1 on failure.
     */
    public static long compileDeploymentAndGetCRC(Catalog catalog,
                                                  DeploymentType deployment,
                                                  boolean crashOnFailedValidation,
                                                  boolean printLog) {

        if (!validateDeployment(catalog, deployment)) {
            return -1;
        }

        // add our hacky Deployment to the catalog
        catalog.getClusters().get("cluster").getDeployment().add("deployment");

        // set the cluster info
        setClusterInfo(catalog, deployment, printLog);

        //Set the snapshot schedule
        setSnapshotInfo( catalog, deployment.getSnapshot());

        //set path and path overrides
        // NOTE: this must be called *AFTER* setClusterInfo and setSnapshotInfo
        // because path locations for snapshots and partition detection don't
        // exist in the catalog until after those portions of the deployment
        // file are handled.
        setPathsInfo(catalog, deployment.getPaths(), crashOnFailedValidation,
                     printLog);

        // set the users info
        setUsersInfo(catalog, deployment.getUsers());

        // set the HTTPD info
        setHTTPDInfo(catalog, deployment.getHttpd());

        setExportInfo( catalog, deployment.getExport());

        setCommandLogInfo( catalog, deployment.getCommandlog());

        return getDeploymentCRC(deployment);
    }

    /*
     * Command log element is created in setPathsInfo
     */
    private static void setCommandLogInfo(Catalog catalog, CommandLogType commandlog) {
        int fsyncInterval = 200;
        int maxTxnsBeforeFsync = Integer.MAX_VALUE;
        boolean enabled = false;
        boolean sync = false;
        int logSizeMb = 1024;
        org.voltdb.catalog.CommandLog config = catalog.getClusters().get("cluster").getLogconfig().get("log");
        if (commandlog != null) {
            logSizeMb = commandlog.getLogsize();
            sync = commandlog.isSynchronous();
            enabled = commandlog.isEnabled();
            Frequency freq = commandlog.getFrequency();
            if (freq != null) {
                long maxTxnsBeforeFsyncTemp = freq.getTransactions();
                if (maxTxnsBeforeFsyncTemp < 1 || maxTxnsBeforeFsyncTemp > Integer.MAX_VALUE) {
                    throw new RuntimeException("Invalid command log max txns before fsync (" + maxTxnsBeforeFsync
                            + ") specified. Supplied value must be between 1 and (2^31 - 1) txns");
                }
                maxTxnsBeforeFsync = (int)maxTxnsBeforeFsyncTemp;
                fsyncInterval = freq.getTime();
                if (fsyncInterval < 1 | fsyncInterval > 5000) {
                    throw new RuntimeException("Invalid command log fsync interval(" + fsyncInterval
                            + ") specified. Supplied value must be between 1 and 5000 milliseconds");
                }
            }
        }
        config.setEnabled(enabled);
        config.setSynchronous(sync);
        config.setFsyncinterval(fsyncInterval);
        config.setMaxtxns(maxTxnsBeforeFsync);
        config.setLogsize(logSizeMb);
    }

    public static long getDeploymentCRC(String deploymentURL) {
        DeploymentType deployment = parseDeployment(deploymentURL);

        // wasn't a valid xml deployment file
        if (deployment == null) {
            hostLog.error("Not a valid XML deployment file at URL: " + deploymentURL);
            return -1;
        }

        return getDeploymentCRC(deployment);
    }

    /**
     * This code is not really tenable, and should be replaced with some
     * XML normalization code, but for now it should work and be pretty
     * tolerant of XML documents with different formatting for the same
     * values.
     * @return A positive CRC for the deployment contents
     */
    static long getDeploymentCRC(DeploymentType deployment) {
        StringBuilder sb = new StringBuilder();

        sb.append(" CLUSTER ");
        ClusterType ct = deployment.getCluster();
        sb.append(ct.getHostcount()).append(",");
        sb.append(ct.getKfactor()).append(",");
        sb.append(ct.getSitesperhost()).append(",");

        sb.append(" PARTITIONDETECTION ");
        PartitionDetectionType pdt = deployment.getPartitionDetection();
        if (pdt != null) {
            sb.append(pdt.isEnabled()).append(",");
            PartitionDetectionType.Snapshot st = pdt.getSnapshot();
            assert(st != null);
            sb.append(st.getPrefix()).append(",");
        }

        sb.append(" ADMINMODE ");
        AdminModeType amt = deployment.getAdminMode();
        if (amt != null)
        {
            sb.append(amt.getPort()).append(",");
            sb.append(amt.isAdminstartup()).append("\n");
        }

        sb.append(" HEARTBEATCONFIG ");
        HeartbeatType hbt = deployment.getHeartbeat();
        if (hbt != null)
        {
            sb.append(hbt.getTimeout()).append("\n");
        }

        sb.append(" USERS ");
        UsersType ut = deployment.getUsers();
        if (ut != null) {
            List<User> users = ut.getUser();
            for (User u : users) {
                sb.append(" USER ");
                sb.append(u.getName()).append(",");
                sb.append(u.getGroups()).append(",");
                sb.append(u.getPassword()).append(",");
            }
        }
        sb.append("\n");

        sb.append(" HTTPD ");
        HttpdType ht = deployment.getHttpd();
        if (ht != null) {
            HttpdType.Jsonapi jt = ht.getJsonapi();
            if (jt != null) {
                sb.append(jt.isEnabled()).append(",");
            }
            sb.append(ht.isEnabled());
            sb.append(ht.getPort());
        }

        sb.append(" SYSTEMSETTINGS ");
        SystemSettingsType sst = deployment.getSystemsettings();
        if (sst != null)
        {
            sb.append(" TEMPTABLES ");
            Temptables ttt = sst.getTemptables();
            if (ttt != null)
            {
                sb.append(ttt.getMaxsize()).append("\n");
            }
        }

        byte[] data = null;
        try {
            data = sb.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {}

        CRC32 crc = new CRC32();
        crc.update(data);

        long retval = crc.getValue();
        retval = Math.abs(retval);
        if (retval < 0)
            return 0;
        return retval;
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

        return getDeployment(deployIS);
    }

    /**
     * Parses the deployment XML string.
     * @param deploymentString The deployment file content.
     * @return a reference to the root <deployment> element.
     */
    public static DeploymentType parseDeploymentFromString(String deploymentString) {
        ByteArrayInputStream byteIS;
        try {
            byteIS = new ByteArrayInputStream(deploymentString.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            hostLog.warn("Unable to read deployment string: " + e.getMessage());
            return null;
        }
        // get deployment info from xml file
        return getDeployment(byteIS);
    }

    /**
     * Get a reference to the root <deployment> element from the deployment.xml file.
     * @param deployIS
     * @return Returns a reference to the root <deployment> element.
     */
    @SuppressWarnings("unchecked")
    private static DeploymentType getDeployment(InputStream deployIS) {
        try {
            JAXBContext jc = JAXBContext.newInstance("org.voltdb.compiler.deploymentfile");
            // This schema shot the sheriff.
            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(VoltDB.class.getResource("compiler/DeploymentFileSchema.xsd"));
            Unmarshaller unmarshaller = jc.createUnmarshaller();
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
     * @param leader The leader hostname
     * @param catalog The catalog to be updated.
     * @param printLog Whether or not to print cluster configuration.
     */
    private static void setClusterInfo(Catalog catalog, DeploymentType deployment,
                                       boolean printLog) {
        ClusterType cluster = deployment.getCluster();
        int hostCount = cluster.getHostcount();
        int sitesPerHost = cluster.getSitesperhost();
        int kFactor = cluster.getKfactor();

        ClusterConfig config = new ClusterConfig(hostCount, sitesPerHost, kFactor);
        if (printLog) {
            hostLog.l7dlog(Level.INFO,
                           LogKeys.compiler_VoltCompiler_LeaderAndHostCountAndSitesPerHost.name(),
                           new Object[] { config.getHostCount(),
                                          VoltDB.instance().getConfig().m_leader,
                                          config.getSitesPerHost(),
                                          config.getReplicationFactor() },
                           null);
        }
        int replicas = config.getReplicationFactor() + 1;
        int partitionCount = config.getSitesPerHost() * config.getHostCount() / replicas;
        if (printLog) {
            hostLog.info(String.format("The entire cluster has %d %s of%s %d logical partition%s.",
                                       replicas,
                                       replicas > 1 ? "copies" : "copy",
                                       partitionCount > 1 ? " each of the" : "",
                                       partitionCount,
                                       partitionCount > 1 ? "s" : ""));
        }

        if (!config.validate()) {
            hostLog.error(config.getErrorMsg());
        } else {
            ClusterCompiler.compile(catalog, config);
            Cluster catCluster = catalog.getClusters().get("cluster");
            // copy the deployment info that is currently not recorded anywhere else
            Deployment catDeploy = catCluster.getDeployment().get("deployment");
            catDeploy.setHostcount(hostCount);
            catDeploy.setSitesperhost(sitesPerHost);
            catDeploy.setKfactor(kFactor);
            // copy partition detection configuration from xml to catalog
            if (deployment.getPartitionDetection() != null && deployment.getPartitionDetection().isEnabled()) {
                catCluster.setNetworkpartition(true);
                CatalogMap<SnapshotSchedule> faultsnapshots = catCluster.getFaultsnapshots();
                SnapshotSchedule sched = faultsnapshots.add("CLUSTER_PARTITION");
                sched.setPrefix(deployment.getPartitionDetection().getSnapshot().getPrefix());
                if (printLog) {
                    hostLog.info("Detection of network partitions in the cluster is enabled.");
                }
            }
            else {
                catCluster.setNetworkpartition(false);
                if (printLog) {
                    hostLog.info("Detection of network partitions in the cluster is not enabled.");
                }
            }

            // copy admin mode configuration from xml to catalog
            if (deployment.getAdminMode() != null)
            {
                catCluster.setAdminport(deployment.getAdminMode().getPort());
                catCluster.setAdminstartup(deployment.getAdminMode().isAdminstartup());
            }
            else
            {
                // encode the default values
                catCluster.setAdminport(VoltDB.DEFAULT_ADMIN_PORT);
                catCluster.setAdminstartup(false);
            }

            setSystemSettings(deployment, catDeploy);

            if (deployment.getHeartbeat() != null)
            {
                catCluster.setHeartbeattimeout(deployment.getHeartbeat().getTimeout());
            }
            else
            {
                // default to 10 seconds
                catCluster.setHeartbeattimeout(10);
            }
        }
    }

    private static void setSystemSettings(DeploymentType deployment,
                                          Deployment catDeployment)
    {
        // Create catalog Systemsettings
        Systemsettings syssettings =
            catDeployment.getSystemsettings().add("systemsettings");
        int maxtemptablesize = 100;
        int snapshotpriority = 6;
        if (deployment.getSystemsettings() != null)
        {
            Temptables temptables = deployment.getSystemsettings().getTemptables();
            if (temptables != null)
            {
                maxtemptablesize = temptables.getMaxsize();
            }
            SystemSettingsType.Snapshot snapshot = deployment.getSystemsettings().getSnapshot();
            if (snapshot != null) {
                snapshotpriority = snapshot.getPriority();
            }
        }
        syssettings.setMaxtemptablesize(maxtemptablesize);
        syssettings.setSnapshotpriority(snapshotpriority);
    }

    private static void validateDirectory(String type, File path, boolean crashOnFailedValidation) {
        String error = null;
        do {
            if (!path.exists()) {
                error = "Specified " + type + " \"" + path + "\" does not exist"; break;
            }
            if (!path.isDirectory()) {
                error = "Specified " + type + " \"" + path + "\" is not a directory"; break;
            }
            if (!path.canRead()) {
                error = "Specified " + type + " \"" + path + "\" is not readable"; break;
            }
            if (!path.canWrite()) {
                error = "Specified " + type + " \"" + path + "\" is not writable"; break;
            }
            if (!path.canExecute()) {
                error = "Specified " + type + " \"" + path + "\" is not executable"; break;
            }
        } while(false);
        if (error != null) {
            if (crashOnFailedValidation) {
                hostLog.fatal(error);
                VoltDB.crashVoltDB();
            } else {
                hostLog.warn(error);
            }
        }
    }

    /**
     * Set deployment time settings for export
     * @param catalog The catalog to be updated.
     * @param exportsType A reference to the <exports> element of the deployment.xml file.
     */
    private static void setExportInfo(Catalog catalog, ExportType exportType) {
        if (exportType == null) {
            return;
        }

        boolean adminstate = exportType.isEnabled();
        String connector = "org.voltdb.export.processors.RawProcessor";
        if (exportType.getClazz() != null) {
            connector = exportType.getClazz();
        }

        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        org.voltdb.catalog.Connector catconn = db.getConnectors().get("0");
        if (catconn == null) {
            if (adminstate) {
                hostLog.info("Export configuration enabled in deployment file however no export " +
                        "tables are present in the project file. Export disabled.");
            }
            return;
        }

        catconn.setLoaderclass(connector);
        catconn.setEnabled(adminstate);

        if (!adminstate) {
            hostLog.info("Export configuration is present and is " +
               "configured to be disabled. Export will be disabled.");
        }
    }

    /**
     * Set the auto-snapshot settings in the catalog from the deployment file
     * @param catalog The catalog to be updated.
     * @param snapshot A reference to the <snapshot> element of the deployment.xml file.
     */
    private static void setSnapshotInfo(Catalog catalog, SnapshotType snapshotSettings) {
        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        SnapshotSchedule schedule = db.getSnapshotschedule().add("default");
        if (snapshotSettings != null)
        {
            schedule.setEnabled(snapshotSettings.isEnabled());
            String frequency = snapshotSettings.getFrequency();
            if (!frequency.endsWith("s") &&
                    !frequency.endsWith("m") &&
                    !frequency.endsWith("h")) {
                hostLog.fatal(
                        "Snapshot frequency " + frequency +
                        " needs to end with time unit specified" +
                        " that is one of [s, m, h] (seconds, minutes, hours)");
                VoltDB.crashVoltDB();
            }

            int frequencyInt = 0;
            String frequencySubstring = frequency.substring(0, frequency.length() - 1);
            try {
                frequencyInt = Integer.parseInt(frequencySubstring);
            } catch (Exception e) {
                hostLog.fatal("Frequency " + frequencySubstring +
                        " is not an integer ");
                VoltDB.crashVoltDB();
            }

            String prefix = snapshotSettings.getPrefix();
            if (prefix == null || prefix.isEmpty()) {
                hostLog.fatal("Snapshot prefix " + prefix +
                " is not a valid prefix ");
                VoltDB.crashVoltDB();
            }

            if (prefix.contains("-") || prefix.contains(",")) {
                hostLog.fatal("Snapshot prefix " + prefix +
                " cannot include , or - ");
                VoltDB.crashVoltDB();
            }

            int retain = snapshotSettings.getRetain();
            if (retain < 1) {
                hostLog.fatal("Snapshot retain value " + retain +
                        " is not a valid value. Must be 1 or greater.");
                VoltDB.crashVoltDB();
            }

            schedule.setFrequencyunit(
                    frequency.substring(frequency.length() - 1, frequency.length()));
            schedule.setFrequencyvalue(frequencyInt);
            schedule.setPrefix(prefix);
            schedule.setRetain(retain);
        }
        else
        {
            schedule.setEnabled(false);
        }
    }

    private static File getFeaturePath(PathsType paths, PathEntry pathEntry,
                                       File voltDbRoot,
                                       String pathDescription, String defaultPath)
    {
        File featurePath;
        if (paths == null || pathEntry == null) {
            featurePath = new VoltFile(voltDbRoot, defaultPath);
        } else {
            featurePath = new VoltFile(pathEntry.getPath());
            if (!featurePath.isAbsolute())
            {
                featurePath = new VoltFile(voltDbRoot, pathEntry.getPath());
            }
        }
        if (!featurePath.exists()) {
            hostLog.info("Creating " + pathDescription + " directory: " +
                         featurePath.getAbsolutePath());
            if (!featurePath.mkdirs()) {
                hostLog.fatal("Failed to create " + pathDescription + " directory \"" +
                              featurePath + "\"");
            }
        }
        return featurePath;
    }

    /**
     * Set voltroot path, and set the path overrides for export overflow, partition, etc.
     * @param catalog The catalog to be updated.
     * @param paths A reference to the <paths> element of the deployment.xml file.
     * @param printLog Whether or not to print paths info.
     */
    private static void setPathsInfo(Catalog catalog, PathsType paths, boolean crashOnFailedValidation,
                                     boolean printLog) {
        File voltDbRoot;
        final Cluster cluster = catalog.getClusters().get("cluster");
        // Handle default voltdbroot (and completely missing "paths" element).
        if (paths == null || paths.getVoltdbroot() == null || paths.getVoltdbroot().getPath() == null) {
            voltDbRoot = new VoltFile("voltdbroot");
            if (!voltDbRoot.exists()) {
                hostLog.info("Creating voltdbroot directory: " + voltDbRoot.getAbsolutePath());
                if (!voltDbRoot.mkdir()) {
                    hostLog.fatal("Failed to create voltdbroot directory \"" + voltDbRoot + "\"");
                }
            }
        } else {
            voltDbRoot = new VoltFile(paths.getVoltdbroot().getPath());
        }

        validateDirectory("volt root", voltDbRoot, crashOnFailedValidation);
        if (printLog) {
            hostLog.info("Using \"" + voltDbRoot.getAbsolutePath() + "\" for voltdbroot directory.");
        }

        PathEntry path_entry = null;
        if (paths != null)
        {
            path_entry = paths.getSnapshots();
        }
        File snapshotPath =
            getFeaturePath(paths, path_entry, voltDbRoot,
                           "snapshot", "snapshots");
        validateDirectory("snapshot path", snapshotPath, crashOnFailedValidation);

        path_entry = null;
        if (paths != null)
        {
            path_entry = paths.getExportoverflow();
        }
        File exportOverflowPath =
            getFeaturePath(paths, path_entry, voltDbRoot, "export overflow",
                           "export_overflow");
        validateDirectory("export overflow", exportOverflowPath, crashOnFailedValidation);

        // only use these directories in the enterprise version
        File commandLogPath = null;
        File commandLogSnapshotPath = null;

        path_entry = null;
        if (paths != null)
        {
            path_entry = paths.getCommandlog();
        }
        if (VoltDB.instance().getConfig().m_isEnterprise) {
            commandLogPath =
                    getFeaturePath(paths, path_entry, voltDbRoot, "command log", "command_log");
            validateDirectory("command log", commandLogPath, crashOnFailedValidation);
        }
        else {
            // dumb defaults if you ask for logging in community version
            commandLogPath = new VoltFile(voltDbRoot, "command_log");
        }

        path_entry = null;
        if (paths != null)
        {
            path_entry = paths.getCommandlogsnapshot();
        }
        if (VoltDB.instance().getConfig().m_isEnterprise) {
            commandLogSnapshotPath =
                getFeaturePath(paths, path_entry, voltDbRoot, "command log snapshot", "command_log_snapshot");
            validateDirectory("command log snapshot", commandLogSnapshotPath, crashOnFailedValidation);
        }
        else {
            // dumb defaults if you ask for logging in community version
            commandLogSnapshotPath = new VoltFile(voltDbRoot, "command_log_snapshot");;
        }

        //Set the volt root in the catalog
        catalog.getClusters().get("cluster").setVoltroot(voltDbRoot.getPath());

        //Set the auto-snapshot schedule path if there are auto-snapshots
        SnapshotSchedule schedule = cluster.getDatabases().
            get("database").getSnapshotschedule().get("default");
        if (schedule != null) {
            schedule.setPath(snapshotPath.getPath());
        }

        //Update the path in the schedule for ppd
        schedule = cluster.getFaultsnapshots().get("CLUSTER_PARTITION");
        if (schedule != null) {
            schedule.setPath(snapshotPath.getPath());
        }

        //Also set the export overflow directory
        cluster.setExportoverflow(exportOverflowPath.getPath());

        //Set the command log paths, also creates the command log entry in the catalog
        final org.voltdb.catalog.CommandLog commandLogConfig = cluster.getLogconfig().add("log");
        commandLogConfig.setInternalsnapshotpath(commandLogSnapshotPath.getPath());
        commandLogConfig.setLogpath(commandLogPath.getPath());
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
        int httpdPort = -1;
        boolean jsonEnabled = false;

        Cluster cluster = catalog.getClusters().get("cluster");

        // if the httpd info is available, use it
        if (httpd != null && httpd.isEnabled()) {
           httpdPort = httpd.getPort();
           HttpdType.Jsonapi jsonapi = httpd.getJsonapi();
           if (jsonapi != null)
               jsonEnabled = jsonapi.isEnabled();
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
