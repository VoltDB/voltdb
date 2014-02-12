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
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.mindrot.BCrypt;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.deploymentfile.AdminModeType;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.CommandLogType.Frequency;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportConfigurationType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PathEntry;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType.Temptables;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
import org.voltdb.compilereport.IndexAnnotation;
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.compilereport.StatementAnnotation;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.StmtTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.xml.sax.SAXException;

import com.google_voltpatches.common.base.Charsets;
import org.voltdb.SystemProcedureCatalog;

/**
 *
 */
public abstract class CatalogUtil {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final String CATALOG_FILENAME = "catalog.txt";
    public static final String CATALOG_BUILDINFO_FILENAME = "buildinfo.txt";

    /**
     * Load a catalog from the jar bytes.
     *
     * @param catalogBytes
     * @param log
     * @return The serialized string of the catalog content.
     * @throws Exception
     *             If the catalog cannot be loaded because it's incompatible, or
     *             if there is no version information in the catalog.
     */
    public static String loadCatalogFromJar(byte[] catalogBytes, VoltLogger log) throws IOException {
        assert(catalogBytes != null);

        String serializedCatalog = null;
        String voltVersionString = null;
        InMemoryJarfile jarfile = new InMemoryJarfile(catalogBytes);
        byte[] serializedCatalogBytes = jarfile.get(CATALOG_FILENAME);

        if (null == serializedCatalogBytes) {
            throw new IOException("Database catalog not found - please build your application using the current version of VoltDB.");
        }

        serializedCatalog = new String(serializedCatalogBytes, "UTF-8");

        // Get Volt version string
        byte[] buildInfoBytes = jarfile.get(CATALOG_BUILDINFO_FILENAME);
        if (buildInfoBytes == null) {
            throw new IOException("Catalog build information not found - please build your application using the current version of VoltDB.");
        }
        String buildInfo = new String(buildInfoBytes, "UTF-8");
        String[] buildInfoLines = buildInfo.split("\n");
        if (buildInfoLines.length != 5) {
            throw new IOException("Catalog built with an old version of VoltDB - please build your application using the current version of VoltDB.");
        }
        voltVersionString = buildInfoLines[0].trim();

        // Check if it's compatible
        if (!isCatalogCompatible(voltVersionString)) {
            throw new IOException("Catalog compiled with '" + voltVersionString + "' is not compatible with the current version of VoltDB (" +
                    VoltDB.instance().getVersionString() + ") - " + " please build your application using the current version of VoltDB.");
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
                String idx_suffix = IndexType.getSQLSuffix(catalog_idx.getType());

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

            String jsonstring = catalog_idx.getExpressionsjson();

            if (jsonstring.isEmpty()) {
                for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                    ret += add + catalog_colref.getColumn().getTypeName();
                    add = ", ";
                }
            } else {
                List<AbstractExpression> indexedExprs = null;
                try {
                    indexedExprs = AbstractExpression.fromJSONArrayString(jsonstring,
                            StmtTableScan.getStmtTableScan(catalog_tbl));
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                for (AbstractExpression expr : indexedExprs) {
                    ret += add + expr.explain(catalog_tbl.getTypeName());
                    add = ", ";
                }
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

    /**
     * Check if a catalog compiled with the given version of VoltDB is
     * compatible with the current version of VoltDB.
     *
     * @param catalogVersionStr
     *            The version string of the VoltDB that compiled the catalog.
     * @return true if it's compatible, false otherwise.
     */

    public static boolean isCatalogCompatible(String catalogVersionStr)
    {
        if (catalogVersionStr == null || catalogVersionStr.isEmpty()) {
            return false;
        }

        //Check that it is a properly formed verstion string
        int[] catalogVersion = MiscUtils.parseVersionString(catalogVersionStr);
        if (catalogVersion == null) {
            throw new IllegalArgumentException("Invalid version string " + catalogVersionStr);
        }

        if (!catalogVersionStr.equals(VoltDB.instance().getVersionString())) {
            return false;
        }

        return true;
    }

    public static long compileDeploymentAndGetCRC(Catalog catalog, String deploymentURL,
            boolean crashOnFailedValidation, boolean isPlaceHolderCatalog) {
        DeploymentType deployment = CatalogUtil.parseDeployment(deploymentURL);
        if (deployment == null) {
            return -1;
        }
        return compileDeploymentAndGetCRC(catalog, deployment, crashOnFailedValidation, isPlaceHolderCatalog);
    }

    public static long compileDeploymentStringAndGetCRC(Catalog catalog, String deploymentString,
            boolean crashOnFailedValidation, boolean isPlaceHolderCatalog) {
        DeploymentType deployment = CatalogUtil.parseDeploymentFromString(deploymentString);
        if (deployment == null) {
            return -1;
        }
        return compileDeploymentAndGetCRC(catalog, deployment, crashOnFailedValidation, isPlaceHolderCatalog);
    }

    /**
     * Parse the deployment.xml file and add its data into the catalog.
     * @param catalog Catalog to be updated.
     * @param deployment Parsed representation of the deployment.xml file.
     * @param crashOnFailedValidation
     * @param isPlaceHolderCatalog if the catalog is isPlaceHolderCatalog and we are verifying only deployment xml.
     * @return CRC of the deployment contents (>0) or -1 on failure.
     */
    public static long compileDeploymentAndGetCRC(Catalog catalog,
                                                  DeploymentType deployment,
            boolean crashOnFailedValidation, boolean isPlaceHolderCatalog)    {

        if (!validateDeployment(catalog, deployment)) {
            return -1;
        }

        // add our hacky Deployment to the catalog
        catalog.getClusters().get("cluster").getDeployment().add("deployment");

        // set the cluster info
        setClusterInfo(catalog, deployment);

        //Set the snapshot schedule
        setSnapshotInfo( catalog, deployment.getSnapshot());

        //Set enable security
        setSecurityEnabled(catalog, deployment.getSecurity());

        //set path and path overrides
        // NOTE: this must be called *AFTER* setClusterInfo and setSnapshotInfo
        // because path locations for snapshots and partition detection don't
        // exist in the catalog until after those portions of the deployment
        // file are handled.
        setPathsInfo(catalog, deployment.getPaths(), crashOnFailedValidation);

        // set the users info
        setUsersInfo(catalog, deployment.getUsers());

        // set the HTTPD info
        setHTTPDInfo(catalog, deployment.getHttpd());

        if (!isPlaceHolderCatalog) {
            setExportInfo(catalog, deployment.getExport());
        }

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
        // enterprise voltdb defaults to CL enabled if not specified in the XML
        if (MiscUtils.isPro()) {
            enabled = true;
        }
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
        StringBuilder sb = new StringBuilder(1024);

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
            if (st != null) {
                sb.append(st.getPrefix()).append(",");
            }
        }

        sb.append(" SECURITY ");
        SecurityType st = deployment.getSecurity();
        if (st != null) {
            sb.append(st.isEnabled());
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
                sb.append(Arrays.toString(mergeUserRoles(u).toArray()));
                sb.append(",").append(u.getPassword()).append(",");
                sb.append(u.isPlaintext()).append(",");
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

        sb.append(" EXPORT ");
        ExportType export = deployment.getExport();
        if( export != null) {
            sb.append(" ENABLE ").append(export.isEnabled());
            // mimic what is done when the catalog is built, which
            // ignores anything else within the export XML stanza
            // when enabled is false
            if (export.isEnabled()) {
                ServerExportEnum exportTarget = export.getTarget();
                if (exportTarget != null) {
                    sb.append( "TARGET ").append(exportTarget.name());
                    if (exportTarget.name().equalsIgnoreCase("CUSTOM")) {
                        sb.append(" EXPORTCONNECTORCLASS ").append(export.getExportconnectorclass());
                    }
                }
                ExportConfigurationType config = export.getConfiguration();
                if (config != null) {
                    List<PropertyType> props = config.getProperty();
                    if( props != null && !props.isEmpty()) {
                        sb.append(" CONFIGURATION");
                        int propCnt = 0;
                        for( PropertyType prop: props) {
                            if( propCnt++ > 0) {
                                sb.append(",");
                            }
                            sb.append(" ").append(prop.getName());
                            sb.append(": ").append(prop.getValue());
                        }
                    }
                }
            }
            sb.append("\n");
        }

        byte[] data = null;
        try {
            data = sb.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            hostLog.error("CRCing deployment file to determine" +
                    " compatibility and determined deployment file is"+
                    " not valid UTF-8. File must be UTF-8 encoded.");
            data = new byte[]{0x0}; // should generate a CRC mismatch.
        }

        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(data);

        long retval = crc.getValue();
        return Math.abs(retval);
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
    public static DeploymentType getDeployment(InputStream deployIS) {
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
            if (user.getGroups() == null && user.getRoles() == null)
                continue;

            for (String group : mergeUserRoles(user)) {
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
    private static void setClusterInfo(Catalog catalog, DeploymentType deployment) {
        ClusterType cluster = deployment.getCluster();
        int hostCount = cluster.getHostcount();
        int sitesPerHost = cluster.getSitesperhost();
        int kFactor = cluster.getKfactor();

        ClusterConfig config = new ClusterConfig(hostCount, sitesPerHost, kFactor);

        if (!config.validate()) {
            hostLog.error(config.getErrorMsg());
        } else {
            Cluster catCluster = catalog.getClusters().get("cluster");
            // copy the deployment info that is currently not recorded anywhere else
            Deployment catDeploy = catCluster.getDeployment().get("deployment");
            catDeploy.setHostcount(hostCount);
            catDeploy.setSitesperhost(sitesPerHost);
            catDeploy.setKfactor(kFactor);
            // copy partition detection configuration from xml to catalog
            String defaultPPDPrefix = "partition_detection";
            if (deployment.getPartitionDetection() != null) {
                if (deployment.getPartitionDetection().isEnabled()) {
                    catCluster.setNetworkpartition(true);
                    CatalogMap<SnapshotSchedule> faultsnapshots = catCluster.getFaultsnapshots();
                    SnapshotSchedule sched = faultsnapshots.add("CLUSTER_PARTITION");
                    if (deployment.getPartitionDetection().getSnapshot() != null) {
                        sched.setPrefix(deployment.getPartitionDetection().getSnapshot().getPrefix());
                    }
                    else {
                        sched.setPrefix(defaultPPDPrefix);
                    }
                }
                else {
                    catCluster.setNetworkpartition(false);
                }
            }
            else {
                // Default partition detection on
                catCluster.setNetworkpartition(true);
                CatalogMap<SnapshotSchedule> faultsnapshots = catCluster.getFaultsnapshots();
                SnapshotSchedule sched = faultsnapshots.add("CLUSTER_PARTITION");
                sched.setPrefix(defaultPPDPrefix);
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
        int elasticPauseTime = 50;
        int elasticThroughput = 2;
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
            SystemSettingsType.Elastic elastic = deployment.getSystemsettings().getElastic();
            if (elastic != null) {
                elasticPauseTime = deployment.getSystemsettings().getElastic().getDuration();
                elasticThroughput = deployment.getSystemsettings().getElastic().getThroughput();
            }
        }
        syssettings.setMaxtemptablesize(maxtemptablesize);
        syssettings.setSnapshotpriority(snapshotpriority);
        syssettings.setElasticpausetime(elasticPauseTime);
        syssettings.setElasticthroughput(elasticThroughput);
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
                VoltDB.crashLocalVoltDB(error, false, null);
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

        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        org.voltdb.catalog.Connector catconn = db.getConnectors().get("0");
        if (catconn == null) {
            if (adminstate) {
                hostLog.info("Export configuration enabled in deployment file however no export " +
                        "tables are present in the project file. Export disabled.");
            }
            return;
        }

        // on-server export always uses the guest processor
        String connector = "org.voltdb.export.processors.GuestProcessor";
        catconn.setLoaderclass(connector);
        catconn.setEnabled(adminstate);

        String exportClientClassName = null;

        switch(exportType.getTarget()) {
            case FILE: exportClientClassName = "org.voltdb.exportclient.ExportToFileClient"; break;
            case JDBC: exportClientClassName = "org.voltdb.exportclient.JDBCExportClient"; break;
            //Validate that we can load the class.
            case CUSTOM:
                try {
                    CatalogUtil.class.getClassLoader().loadClass(exportType.getExportconnectorclass());
                    exportClientClassName = exportType.getExportconnectorclass();
                }
                catch (ClassNotFoundException ex) {
                    hostLog.error(
                            "Custom Export failed to configure, failed to load " +
                            " export plugin class: " + exportType.getExportconnectorclass() +
                            " Disabling export.");
                exportType.setEnabled(false);
                return;
            }
            break;
        }

        // this is OK as the deployment file XML schema does not allow for
        // export configuration property names that begin with underscores
        if (exportClientClassName != null && exportClientClassName.trim().length() > 0) {
            ConnectorProperty prop = catconn.getConfig().add(ExportDataProcessor.EXPORT_TO_TYPE);
            prop.setName(ExportDataProcessor.EXPORT_TO_TYPE);
            //Override for tests
            String dexportClientClassName = System.getProperty(ExportDataProcessor.EXPORT_TO_TYPE, exportClientClassName);
            prop.setValue(dexportClientClassName);
        }

        ExportConfigurationType exportConfiguration = exportType.getConfiguration();
        if (exportConfiguration != null) {

            List<PropertyType> configProperties = exportConfiguration.getProperty();
            if (configProperties != null && ! configProperties.isEmpty()) {

                for( PropertyType configProp: configProperties) {
                    ConnectorProperty prop = catconn.getConfig().add(configProp.getName());
                    prop.setName(configProp.getName());
                    prop.setValue(configProp.getValue());
                }
            }
        }

        if (!adminstate) {
            hostLog.info("Export configuration is present and is " +
               "configured to be disabled. Export will be disabled.");
        } else {
            hostLog.info("Export is configured and enabled with type=" + exportType.getTarget());
            if (exportConfiguration != null && exportConfiguration.getProperty() != null) {
                hostLog.info("Export configuration properties are: ");
                for (PropertyType configProp : exportConfiguration.getProperty()) {
                    if (!configProp.getName().equalsIgnoreCase("password")) {
                        hostLog.info("Export Configuration Property NAME=" + configProp.getName() + " VALUE=" + configProp.getValue());
                    }
                }
            }
        }
    }

    /**
     * Set the security setting in the catalog from the deployment file
     * @param catalog the catalog to be updated
     * @param securityEnabled security element of the deployment xml
     */
    private static void setSecurityEnabled( Catalog catalog, SecurityType securityEnabled) {
        Cluster cluster = catalog.getClusters().get("cluster");
        boolean enabled = false;
        if (securityEnabled != null) {
            enabled = securityEnabled.isEnabled();
        }
        cluster.setSecurityenabled(enabled);
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
                hostLog.error(
                        "Snapshot frequency " + frequency +
                        " needs to end with time unit specified" +
                        " that is one of [s, m, h] (seconds, minutes, hours)" +
                        " Defaulting snapshot frequency to 10m.");
                frequency = "10m";
            }

            int frequencyInt = 0;
            String frequencySubstring = frequency.substring(0, frequency.length() - 1);
            try {
                frequencyInt = Integer.parseInt(frequencySubstring);
            } catch (Exception e) {
                hostLog.error("Frequency " + frequencySubstring +
                        " is not an integer. Defaulting frequency to 10m.");
                frequency = "10m";
                frequencyInt = 10;
            }

            String prefix = snapshotSettings.getPrefix();
            if (prefix == null || prefix.isEmpty()) {
                hostLog.error("Snapshot prefix " + prefix +
                " is not a valid prefix. Using prefix of 'SNAPSHOTNONCE' ");
                prefix = "SNAPSHOTNONCE";
            }

            if (prefix.contains("-") || prefix.contains(",")) {
                String oldprefix = prefix;
                prefix = prefix.replaceAll("-", "_");
                prefix = prefix.replaceAll(",", "_");
                hostLog.error("Snapshot prefix " + oldprefix + " cannot include , or -." +
                        " Using the prefix: " + prefix + " instead.");
            }

            int retain = snapshotSettings.getRetain();
            if (retain < 1) {
                hostLog.error("Snapshot retain value " + retain +
                        " is not a valid value. Must be 1 or greater." +
                        " Defaulting snapshot retain to 1.");
                retain = 1;
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
    private static void setPathsInfo(Catalog catalog, PathsType paths, boolean crashOnFailedValidation) {
        File voltDbRoot;
        final Cluster cluster = catalog.getClusters().get("cluster");
        // Handles default voltdbroot (and completely missing "paths" element).
        voltDbRoot = getVoltDbRoot(paths);

        validateDirectory("volt root", voltDbRoot, crashOnFailedValidation);

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
     * Get a File object representing voltdbroot. Create directory if missing.
     * Use paths if non-null to get override default location.
     *
     * @param paths override paths or null
     * @return File object for voltdbroot
     */
    public static File getVoltDbRoot(PathsType paths) {
        File voltDbRoot;
        if (paths == null || paths.getVoltdbroot() == null || paths.getVoltdbroot().getPath() == null) {
            voltDbRoot = new VoltFile("voltdbroot");
            if (!voltDbRoot.exists()) {
                hostLog.info("Creating voltdbroot directory: " + voltDbRoot.getAbsolutePath());
                if (!voltDbRoot.mkdir()) {
                    hostLog.fatal("Failed to create voltdbroot directory \"" + voltDbRoot.getAbsolutePath() + "\"");
                }
            }
        } else {
            voltDbRoot = new VoltFile(paths.getVoltdbroot().getPath());
        }
        return voltDbRoot;
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

        SecureRandom sr = new SecureRandom();
        for (UsersType.User user : users.getUser()) {

            String sha1hex = user.getPassword();
            if (user.isPlaintext()) {
                sha1hex = extractPassword(user.getPassword());
            }
            org.voltdb.catalog.User catUser = db.getUsers().add(user.getName());

            String hashedPW =
                    BCrypt.hashpw(
                            sha1hex,
                            BCrypt.gensalt(BCrypt.GENSALT_DEFAULT_LOG2_ROUNDS,sr));
            catUser.setShadowpassword(hashedPW);

            // process the @groups and @roles comma separated list
            for (final String role : mergeUserRoles(user)) {
                final GroupRef groupRef = catUser.getGroups().add(role);
                final Group catalogGroup = db.getGroups().get(role);
                if (catalogGroup != null) {
                    groupRef.setGroup(catalogGroup);
                }
            }
        }
    }

    /**
     * Takes the list of roles specified in the groups, and roles user
     * attributes and merges the into one set that contains no duplicates
     * @param user an instance of {@link UsersType.User}
     * @return a {@link Set} of role name
     */
    public static Set<String> mergeUserRoles(final UsersType.User user) {
        Set<String> roles = new TreeSet<String>();
        if (user == null) return roles;

        if (user.getGroups() != null && !user.getGroups().trim().isEmpty()) {
            String [] grouplist = user.getGroups().trim().split(",");
            for (String group: grouplist) {
                if( group == null || group.trim().isEmpty()) continue;
                roles.add(group.trim());
            }
        }

        if (user.getRoles() != null && !user.getRoles().trim().isEmpty()) {
            String [] rolelist = user.getRoles().trim().split(",");
            for (String role: rolelist) {
                if( role == null || role.trim().isEmpty()) continue;
                roles.add(role.trim());
            }
        }

        return roles;
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

    /** Read a hashed password from password.
     *  SHA-1 hash it once to match what we will get from the wire protocol
     *  and then hex encode it
     * */
    private static String extractPassword(String password) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (final NoSuchAlgorithmException e) {
            hostLog.l7dlog(Level.FATAL, LogKeys.compiler_VoltCompiler_NoSuchAlgorithm.name(), e);
            System.exit(-1);
        }
        final byte passwordHash[] = md.digest(password.getBytes(Charsets.UTF_8));
        return Encoder.hexEncode(passwordHash);
    }

    public static void
        uploadCatalogToZK(ZooKeeper zk, int catalogVersion, long txnId, long uniqueId, byte[] catalogHash, byte catalogBytes[])
                throws KeeperException, InterruptedException {
        ByteBuffer versionAndBytes = ByteBuffer.allocate(catalogBytes.length + 20 + 20);
        versionAndBytes.putInt(catalogVersion);
        versionAndBytes.putLong(txnId);
        versionAndBytes.putLong(uniqueId);
        versionAndBytes.put(catalogHash);
        versionAndBytes.put(catalogBytes);
        zk.create(VoltZK.catalogbytes,
                versionAndBytes.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static void
        setCatalogToZK(ZooKeeper zk, int catalogVersion, long txnId, long uniqueId, byte[] catalogHash, byte catalogBytes[])
            throws KeeperException, InterruptedException {
        ByteBuffer versionAndBytes = ByteBuffer.allocate(catalogBytes.length + 20 + 20);
        versionAndBytes.putInt(catalogVersion);
        versionAndBytes.putLong(txnId);
        versionAndBytes.putLong(uniqueId);
        versionAndBytes.put(catalogHash);
        versionAndBytes.put(catalogBytes);
        zk.setData(VoltZK.catalogbytes,
                versionAndBytes.array(), -1);
    }

    public static class CatalogAndIds {
        public final long txnId;
        public final long uniqueId;
        public final int version;
        public final byte hash[];
        public final byte bytes[];

        public CatalogAndIds(long txnId, long uniqueId, int catalogVersion, byte[] catalogHash, byte[] catalogBytes) {
            this.txnId = txnId;
            this.uniqueId = uniqueId;
            this.version = catalogVersion;
            this.hash = catalogHash;
            this.bytes = catalogBytes;
        }
    }

    public static CatalogAndIds getCatalogFromZK(ZooKeeper zk) throws KeeperException, InterruptedException {
        ByteBuffer versionAndBytes =
                ByteBuffer.wrap(zk.getData(VoltZK.catalogbytes, false, null));
        int version = versionAndBytes.getInt();
        long catalogTxnId = versionAndBytes.getLong();
        long catalogUniqueId = versionAndBytes.getLong();
        byte[] catalogHash = new byte[20]; // sha-1 hash size
        versionAndBytes.get(catalogHash);
        byte[] catalogBytes = new byte[versionAndBytes.remaining()];
        versionAndBytes.get(catalogBytes);
        versionAndBytes = null;
        return new CatalogAndIds(catalogTxnId, catalogUniqueId, version, catalogHash, catalogBytes);
    }

    /**
     * Given plan graphs and a SQL stmt, compute a bi-directonal usage map between
     * schema (indexes, table & views) and SQL/Procedures.
     * Use "annotation" objects to store this extra information in the catalog
     * during compilation and catalog report generation.
     */
    public static void updateUsageAnnotations(Database db,
                                              Statement stmt,
                                              AbstractPlanNode topPlan,
                                              AbstractPlanNode bottomPlan)
    {
        Collection<String> tablesRead = new TreeSet<String>();
        Collection<String> tablesUpdated = new TreeSet<String>();
        Collection<String> indexes = new TreeSet<String>();
        if (topPlan != null) {
            topPlan.getTablesAndIndexes(tablesRead, tablesUpdated, indexes);
        }
        if (bottomPlan != null) {
            bottomPlan.getTablesAndIndexes(tablesRead, tablesUpdated, indexes);
        }

        // make useage only in either read or updated, not both
        tablesRead.removeAll(tablesUpdated);

        for (Table table : db.getTables()) {
            for (String indexName : indexes) {
                Index index = table.getIndexes().get(indexName);
                if (index != null) {
                    updateIndexUsageAnnotation(index, stmt);
                }
            }
            if (tablesRead.contains(table.getTypeName())) {
                updateTableUsageAnnotation(table, stmt, true);
                tablesRead.remove(table.getTypeName());
            }
            if (tablesUpdated.contains(table.getTypeName())) {
                updateTableUsageAnnotation(table, stmt, false);
                tablesUpdated.remove(table.getTypeName());
            }
        }

        assert(tablesRead.size() == 0);
        assert(tablesUpdated.size() == 0);
    }

    private static void updateIndexUsageAnnotation(Index index, Statement stmt) {
        Procedure proc = (Procedure) stmt.getParent();
        // skip CRUD generated procs
        if (proc.getDefaultproc()) {
            return;
        }

        IndexAnnotation ia = (IndexAnnotation) index.getAnnotation();
        if (ia == null) {
            ia = new IndexAnnotation();
            index.setAnnotation(ia);
        }
        ia.statementsThatUseThis.add(stmt);
        ia.proceduresThatUseThis.add(proc);

        ProcedureAnnotation pa = (ProcedureAnnotation) proc.getAnnotation();
        if (pa == null) {
            pa = new ProcedureAnnotation();
            proc.setAnnotation(pa);
        }
        pa.indexesUsed.add(index);

        StatementAnnotation sa = (StatementAnnotation) stmt.getAnnotation();
        if (sa == null) {
            sa = new StatementAnnotation();
            stmt.setAnnotation(sa);
        }
        sa.indexesUsed.add(index);
    }

    private static void updateTableUsageAnnotation(Table table, Statement stmt, boolean read) {
        Procedure proc = (Procedure) stmt.getParent();
        // skip CRUD generated procs
        if (proc.getDefaultproc()) {
            return;
        }

        TableAnnotation ta = (TableAnnotation) table.getAnnotation();
        if (ta == null) {
            ta = new TableAnnotation();
            table.setAnnotation(ta);
        }
        if (read) {
            ta.statementsThatReadThis.add(stmt);
            ta.proceduresThatReadThis.add(proc);
        }
        else {
            ta.statementsThatUpdateThis.add(stmt);
            ta.proceduresThatUpdateThis.add(proc);
        }

        ProcedureAnnotation pa = (ProcedureAnnotation) proc.getAnnotation();
        if (pa == null) {
            pa = new ProcedureAnnotation();
            proc.setAnnotation(pa);
        }
        if (read) {
            pa.tablesRead.add(table);
        }
        else {
            pa.tablesUpdated.add(table);
        }

        StatementAnnotation sa = (StatementAnnotation) stmt.getAnnotation();
        if (sa == null) {
            sa = new StatementAnnotation();
            stmt.setAnnotation(sa);
        }
        if (read) {
            sa.tablesRead.add(table);
        }
        else {
            sa.tablesUpdated.add(table);
        }
    }

    /**
     * Get all normal tables from the catalog. A normal table is one that's NOT a materialized
     * view, nor an export table. For the lack of a better name, I call it normal.
     * @param catalog         Catalog database
     * @param isReplicated    true to return only replicated tables,
     *                        false to return all partitioned tables
     * @return A list of tables
     */
    public static List<Table> getNormalTables(Database catalog, boolean isReplicated) {
        List<Table> tables = new ArrayList<Table>();
        for (Table table : catalog.getTables()) {
            if ((table.getIsreplicated() == isReplicated) &&
                table.getMaterializer() == null &&
                !CatalogUtil.isTableExportOnly(catalog, table)) {
                tables.add(table);
            }
        }
        return tables;
    }

    /**
     * Iterate through all the tables in the catalog, find a table with an id that matches the
     * given table id, and return its name.
     *
     * @param catalog  Catalog database
     * @param tableId  table id
     * @return table name associated with the given table id (null if no association is found)
     */
    public static String getTableNameFromId(Database catalog, int tableId) {
        String tableName = null;
        for (Table table: catalog.getTables()) {
            if (table.getRelativeIndex() == tableId) {
                tableName = table.getTypeName();
            }
        }
        return tableName;
    }

    // Calculate the width of an index:
    // -- if the index is a pure-column index, return number of columns in the index
    // -- if the index is an expression index, return number of expressions used to create the index
    public static int getCatalogIndexSize(Index index) {
        int indexSize = 0;
        String jsonstring = index.getExpressionsjson();

        if (jsonstring.isEmpty()) {
            indexSize = getSortedCatalogItems(index.getColumns(), "index").size();
        } else {
            try {
                indexSize = AbstractExpression.fromJSONArrayString(jsonstring, null).size();
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return indexSize;
    }

    /**
     * Return if given proc is durable if its a sysproc SystemProcedureCatalog is consulted. All non sys procs are all
     * durable.
     *
     * @param procName
     * @return true if proc is durable for non sys procs return true (durable)
     */
    public static boolean isDurableProc(String procName) {
        //For sysprocs look at sysproc catalog.
        if (procName.charAt(0) == '@') {
            SystemProcedureCatalog.Config sysProc = SystemProcedureCatalog.listing.get(procName);
            if (sysProc != null) {
                return sysProc.isDurable();
            }
        }
        return true;
    }

}
