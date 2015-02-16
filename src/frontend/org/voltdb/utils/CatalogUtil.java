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

package org.voltdb.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.mindrot.BCrypt;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.SystemProcedureCatalog;
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
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.VoltCompiler;
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
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.compiler.deploymentfile.ReplicationType;
import org.voltdb.compiler.deploymentfile.SchemaType;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ConstraintType;
import org.xml.sax.SAXException;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import java.util.Properties;

import org.voltdb.export.ExportManager;

/**
 *
 */
public abstract class CatalogUtil {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final String CATALOG_FILENAME = "catalog.txt";
    public static final String CATALOG_BUILDINFO_FILENAME = "buildinfo.txt";

    private static JAXBContext m_jc;
    private static Schema m_schema;
    static {
        try {
            // This schema shot the sheriff.
            m_jc = JAXBContext.newInstance("org.voltdb.compiler.deploymentfile");
            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            m_schema = sf.newSchema(VoltDB.class.getResource("compiler/DeploymentFileSchema.xsd"));
        } catch (JAXBException ex) {
            m_jc = null;
            m_schema = null;
            hostLog.error("Failed to create JAXB Context for deployment.", ex);
        } catch (SAXException e) {
            m_jc = null;
            m_schema = null;
            hostLog.error("Error schema validating deployment.xml file. " + e.getMessage());
        }
    }

    /**
     * Load a catalog from the jar bytes.
     *
     * @param catalogBytes
     * @return Pair containing updated InMemoryJarFile and upgraded version (or null if it wasn't upgraded)
     * @throws IOException
     *             If the catalog cannot be loaded because it's incompatible, or
     *             if there is no version information in the catalog.
     */
    public static Pair<InMemoryJarfile, String> loadAndUpgradeCatalogFromJar(byte[] catalogBytes)
        throws IOException
    {
        // Throws IOException on load failure.
        InMemoryJarfile jarfile = loadInMemoryJarFile(catalogBytes);
        // Let VoltCompiler do a version check and upgrade the catalog on the fly.
        // I.e. jarfile may be modified.
        VoltCompiler compiler = new VoltCompiler();
        String upgradedFromVersion = compiler.upgradeCatalogAsNeeded(jarfile);
        return new Pair<InMemoryJarfile, String>(jarfile, upgradedFromVersion);
    }

    /**
     * Convenience method to extract the catalog commands from an InMemoryJarfile as a string
     */
    public static String getSerializedCatalogStringFromJar(InMemoryJarfile jarfile)
    {
        byte[] serializedCatalogBytes = jarfile.get(CatalogUtil.CATALOG_FILENAME);
        String serializedCatalog = new String(serializedCatalogBytes, Constants.UTF8ENCODING);
        return serializedCatalog;
    }

    /**
     * Get the catalog build info from the jar bytes.
     * Performs sanity checks on the build info and version strings.
     *
     * @param jarfile in-memory catalog jar file
     * @return build info lines
     * @throws IOException If the catalog or the version string cannot be loaded.
     */
    public static String[] getBuildInfoFromJar(InMemoryJarfile jarfile)
            throws IOException
    {
        // Read the raw build info bytes.
        byte[] buildInfoBytes = jarfile.get(CATALOG_BUILDINFO_FILENAME);
        if (buildInfoBytes == null) {
            throw new IOException("Catalog build information not found - please build your application using the current version of VoltDB.");
        }

        // Convert the bytes to a string and split by lines.
        String buildInfo;
        buildInfo = new String(buildInfoBytes, Constants.UTF8ENCODING);
        String[] buildInfoLines = buildInfo.split("\n");

        // Sanity check the number of lines and the version string.
        if (buildInfoLines.length < 1) {
            throw new IOException("Catalog build info has no version string.");
        }
        String versionFromCatalog = buildInfoLines[0].trim();
        if (!CatalogUtil.isCatalogVersionValid(versionFromCatalog)) {
            throw new IOException(String.format(
                    "Catalog build info version (%s) is bad.", versionFromCatalog));
        }

        // Trim leading/trailing whitespace.
        for (int i = 0; i < buildInfoLines.length; ++i) {
            buildInfoLines[i] = buildInfoLines[i].trim();
        }

        return buildInfoLines;
    }

    /**
     * Load an in-memory catalog jar file from jar bytes.
     *
     * @param catalogBytes
     * @param log
     * @return The in-memory jar containing the loaded catalog.
     * @throws IOException If the catalog cannot be loaded.
     */
    public static InMemoryJarfile loadInMemoryJarFile(byte[] catalogBytes)
            throws IOException
    {
        assert(catalogBytes != null);

        InMemoryJarfile jarfile = new InMemoryJarfile(catalogBytes);
        byte[] serializedCatalogBytes = jarfile.get(CATALOG_FILENAME);

        if (null == serializedCatalogBytes) {
            throw new IOException("Database catalog not found - please build your application using the current version of VoltDB.");
        }

        return jarfile;
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
            if (hasField == false) {
                hasField = ArrayUtils.contains(item.getFields(), sortFieldName);
            }
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

    public static NavigableSet<Table> getExportTables(Database db) {
        ImmutableSortedSet.Builder<Table> exportTables = ImmutableSortedSet.naturalOrder();
        for (Connector connector : db.getConnectors()) {
            for (ConnectorTableInfo tinfo : connector.getTableinfo()) {
                exportTables.add(tinfo.getTable());
            }
        }
        return exportTables.build();
    }

    public static NavigableSet<String> getExportTableNames(Database db) {
        ImmutableSortedSet.Builder<String> exportTables = ImmutableSortedSet.naturalOrder();
        for (Connector connector : db.getConnectors()) {
            for (ConnectorTableInfo tinfo : connector.getTableinfo()) {
                exportTables.add(tinfo.getTable().getTypeName());
            }
        }
        return exportTables.build();
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
        for (Connector connector : database.getConnectors()) {
            // iterate the connector tableinfo list looking for tableIndex
            // tableInfo has a reference to a table - can compare the reference
            // to the desired table by looking at the relative index. ick.
            for (ConnectorTableInfo tableInfo : connector.getTableinfo()) {
                if (tableInfo.getTable().getRelativeIndex() == table.getRelativeIndex()) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String getExportTargetIfExportTableOrNullOtherwise(org.voltdb.catalog.Database database,
                                                                    org.voltdb.catalog.Table table)
    {
        for (Connector connector : database.getConnectors()) {
            // iterate the connector tableinfo list looking for tableIndex
            // tableInfo has a reference to a table - can compare the reference
            // to the desired table by looking at the relative index. ick.
            for (ConnectorTableInfo tableInfo : connector.getTableinfo()) {
                if (tableInfo.getTable().getRelativeIndex() == table.getRelativeIndex()) {
                    return connector.getTypeName();
                }
            }
        }
        return null;
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
        Object[] catalogVersion = MiscUtils.parseVersionString(catalogVersionStr);
        if (catalogVersion == null) {
            throw new IllegalArgumentException("Invalid version string " + catalogVersionStr);
        }

        if (!catalogVersionStr.equals(VoltDB.instance().getVersionString())) {
            return false;
        }

        return true;
    }

    /**
     * Check if a catalog version string is valid.
     *
     * @param catalogVersionStr
     *            The version string of the VoltDB that compiled the catalog.
     * @return true if it's valid, false otherwise.
     */

    public static boolean isCatalogVersionValid(String catalogVersionStr)
    {
        // Do we have a version string?
        if (catalogVersionStr == null || catalogVersionStr.isEmpty()) {
            return false;
        }

        //Check that it is a properly formed version string
        Object[] catalogVersion = MiscUtils.parseVersionString(catalogVersionStr);
        if (catalogVersion == null) {
            return false;
        }

        // It's valid.
        return true;
    }

    public static String compileDeployment(Catalog catalog, String deploymentURL,
            boolean isPlaceHolderCatalog)
    {
        DeploymentType deployment = CatalogUtil.parseDeployment(deploymentURL);
        if (deployment == null) {
            return "Error parsing deployment file: " + deploymentURL;
        }
        return compileDeployment(catalog, deployment, isPlaceHolderCatalog);
    }

    public static String compileDeploymentString(Catalog catalog, String deploymentString,
            boolean isPlaceHolderCatalog)
    {
        DeploymentType deployment = CatalogUtil.parseDeploymentFromString(deploymentString);
        if (deployment == null) {
            return "Error parsing deployment string";
        }
        return compileDeployment(catalog, deployment, isPlaceHolderCatalog);
    }

    /**
     * Parse the deployment.xml file and add its data into the catalog.
     * @param catalog Catalog to be updated.
     * @param deployment Parsed representation of the deployment.xml file.
     * @param isPlaceHolderCatalog if the catalog is isPlaceHolderCatalog and we are verifying only deployment xml.
     * @return String containing any errors parsing/validating the deployment. NULL on success.
     */
    public static String compileDeployment(Catalog catalog,
            DeploymentType deployment,
            boolean isPlaceHolderCatalog)
    {
        String errmsg = null;

        try {
            validateDeployment(catalog, deployment);

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
            setPathsInfo(catalog, deployment.getPaths());

            // set the users info
            // We'll skip this when building the dummy catalog on startup
            // so that we don't spew misleading user/role warnings
            if (!isPlaceHolderCatalog) {
                setUsersInfo(catalog, deployment.getUsers());
            }

            // set the HTTPD info
            setHTTPDInfo(catalog, deployment.getHttpd());

            if (!isPlaceHolderCatalog) {
                setExportInfo(catalog, deployment.getExport());
            }

            setCommandLogInfo( catalog, deployment.getCommandlog());
        }
        catch (Exception e) {
            // Anything that goes wrong anywhere in trying to handle the deployment file
            // should return an error, and let the caller decide what to do (crash or not, for
            // example)
            errmsg = "Error validating deployment configuration: " + e.getMessage();
            hostLog.error(errmsg);
            return errmsg;
        }

        return null;
    }

    /*
     * Command log element is created in setPathsInfo
     */
    private static void setCommandLogInfo(Catalog catalog, CommandLogType commandlog) {
        int fsyncInterval = 200;
        int maxTxnsBeforeFsync = Integer.MAX_VALUE;
        org.voltdb.catalog.CommandLog config = catalog.getClusters().get("cluster").getLogconfig().get("log");
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
        config.setEnabled(commandlog.isEnabled());
        config.setSynchronous(commandlog.isSynchronous());
        config.setFsyncinterval(fsyncInterval);
        config.setMaxtxns(maxTxnsBeforeFsync);
        config.setLogsize(commandlog.getLogsize());
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
        byteIS = new ByteArrayInputStream(deploymentString.getBytes(Constants.UTF8ENCODING));
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
            if (m_jc == null || m_schema == null) {
                throw new RuntimeException("Error schema validation.");
            }
            Unmarshaller unmarshaller = m_jc.createUnmarshaller();
            unmarshaller.setSchema(m_schema);
            JAXBElement<DeploymentType> result =
                (JAXBElement<DeploymentType>) unmarshaller.unmarshal(deployIS);
            DeploymentType deployment = result.getValue();
            // move any deprecated standalone export elements to the default target
            ExportType export = deployment.getExport();
            if (export != null && export.getTarget() != null) {
                if (export.getConfiguration().size() > 1) {
                    hostLog.error("Invalid schema, cannot use deprecated export syntax with multiple configuration tags.");
                    return null;
                }
                //OLD syntax use target as type.
                if (export.getConfiguration().isEmpty()) {
                    // this code is for RestRoundtripTest
                    export.getConfiguration().add(new ExportConfigurationType());
                }
                ExportConfigurationType exportConfig = export.getConfiguration().get(0);
                if (export.isEnabled() != null) {
                    exportConfig.setEnabled(export.isEnabled());
                }
                if (export.getTarget() != null) {
                    exportConfig.setType(export.getTarget());
                }
                if (export.getExportconnectorclass() != null) {
                    exportConfig.setExportconnectorclass(export.getExportconnectorclass());
                }
                //Set target to default name.
                exportConfig.setStream(Constants.DEFAULT_EXPORT_CONNECTOR_NAME);
            }

            populateDefaultDeployment(deployment);
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
        }
    }

    public static void populateDefaultDeployment(DeploymentType deployment) {
            //partition detection
            PartitionDetectionType pd = deployment.getPartitionDetection();
            if (pd == null) {
                pd = new PartitionDetectionType();
                deployment.setPartitionDetection(pd);
            }
            if (pd.getSnapshot() == null) {
                PartitionDetectionType.Snapshot sshot = new PartitionDetectionType.Snapshot();
                pd.setSnapshot(sshot);
            }
            //admin mode
            if (deployment.getAdminMode() == null) {
                AdminModeType amode = new AdminModeType();
                deployment.setAdminMode(amode);
            }
            //heartbeat
            if (deployment.getHeartbeat() == null) {
                HeartbeatType hb = new HeartbeatType();
                deployment.setHeartbeat(hb);
            }
            //httpd
            HttpdType httpd = deployment.getHttpd();
            if (httpd == null) {
                httpd = new HttpdType();
                //-1 means find next port from 8080
                httpd.setPort(-1);
                deployment.setHttpd(httpd);
            }
            //jsonApi
            HttpdType.Jsonapi jsonApi = httpd.getJsonapi();
            if (jsonApi == null) {
                jsonApi = new HttpdType.Jsonapi();
                httpd.setJsonapi(jsonApi);
            }
            //replication
            if (deployment.getReplication() == null) {
                ReplicationType repl = new ReplicationType();
                deployment.setReplication(repl);
            }
            //snapshot
            if (deployment.getSnapshot() == null) {
                SnapshotType snap = new SnapshotType();
                snap.setEnabled(false);
                deployment.setSnapshot(snap);
            }
            //Security
            if (deployment.getSecurity() == null) {
                SecurityType sec = new SecurityType();
                deployment.setSecurity(sec);
            }
            //Paths
            if (deployment.getPaths() == null) {
                PathsType paths = new PathsType();
                deployment.setPaths(paths);
            }
            //create paths entries
            PathsType paths = deployment.getPaths();
            if (paths.getVoltdbroot() == null) {
                PathsType.Voltdbroot root = new PathsType.Voltdbroot();
                paths.setVoltdbroot(root);
            }
            //snapshot
            if (paths.getSnapshots() == null) {
                PathsType.Snapshots snap = new PathsType.Snapshots();
                paths.setSnapshots(snap);
            }
            if (paths.getCommandlog() == null) {
                //cl
                PathsType.Commandlog cl = new PathsType.Commandlog();
                paths.setCommandlog(cl);
            }
            if (paths.getCommandlogsnapshot() == null) {
                //cl snap
                PathsType.Commandlogsnapshot clsnap = new PathsType.Commandlogsnapshot();
                paths.setCommandlogsnapshot(clsnap);
            }
            if (paths.getExportoverflow() == null) {
                //export overflow
                PathsType.Exportoverflow exp = new PathsType.Exportoverflow();
                paths.setExportoverflow(exp);
            }

            //Command log info
            if (deployment.getCommandlog() == null) {
                boolean enabled = false;
                if (MiscUtils.isPro()) {
                    enabled = true;
                }
                CommandLogType cl = new CommandLogType();
                cl.setEnabled(enabled);
                Frequency freq = new Frequency();
                cl.setFrequency(freq);
                deployment.setCommandlog(cl);
            }
            //System settings
            SystemSettingsType ss = deployment.getSystemsettings();
            if (deployment.getSystemsettings() == null) {
                ss = new SystemSettingsType();
                deployment.setSystemsettings(ss);
            }
            SystemSettingsType.Elastic sse = ss.getElastic();
            if (sse == null) {
               sse = new SystemSettingsType.Elastic();
               ss.setElastic(sse);
            }
            SystemSettingsType.Query query = ss.getQuery();
            if (query == null) {
               query = new SystemSettingsType.Query();
               ss.setQuery(query);
            }
            SystemSettingsType.Snapshot snap = ss.getSnapshot();
            if (snap == null) {
               snap = new SystemSettingsType.Snapshot();
               ss.setSnapshot(snap);
            }
            SystemSettingsType.Temptables tt = ss.getTemptables();
            if (tt == null) {
               tt = new SystemSettingsType.Temptables();
               ss.setTemptables(tt);
            }
    }

    /**
     * Given the deployment object generate the XML
     * @param deployment
     * @return XML of deployment object.
     * @throws IOException
     */
    public static String getDeployment(DeploymentType deployment) throws IOException {
        try {
            if (m_jc == null || m_schema == null) {
                throw new RuntimeException("Error schema validation.");
            }
            Marshaller marshaller = m_jc.createMarshaller();
            marshaller.setSchema(m_schema);
            StringWriter sw = new StringWriter();
            marshaller.marshal(new JAXBElement(new QName("","deployment"), DeploymentType.class, deployment), sw);
            return sw.toString();
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
        }
    }

    /**
     * Validate the contents of the deployment.xml file.
     * This is for validating VoltDB requirements, not XML schema correctness
     * @param catalog Catalog to be validated against.
     * @param deployment Reference to root <deployment> element of deployment file to be validated.
     * @throw throws a RuntimeException if any validation fails
     */
    private static void validateDeployment(Catalog catalog, DeploymentType deployment) {
        if (deployment.getSecurity() != null && deployment.getSecurity().isEnabled()) {
            if (deployment.getUsers() == null) {
                String msg = "Cannot enable security without defining at least one user in the built-in ADMINISTRATOR role in the deployment file.";
                throw new RuntimeException(msg);
            }

            boolean foundAdminUser = false;
            for (UsersType.User user : deployment.getUsers().getUser()) {
                if (user.getRoles() == null)
                    continue;

                for (String role : extractUserRoles(user)) {
                    if (role.equalsIgnoreCase("ADMINISTRATOR")) {
                        foundAdminUser = true;
                        break;
                    }
                }
            }

            if (!foundAdminUser) {
                String msg = "Cannot enable security without defining at least one user in the built-in ADMINISTRATOR role in the deployment file.";
                throw new RuntimeException(msg);
            }
        }
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
            throw new RuntimeException(config.getErrorMsg());
        } else {
            Cluster catCluster = catalog.getClusters().get("cluster");
            // copy the deployment info that is currently not recorded anywhere else
            Deployment catDeploy = catCluster.getDeployment().get("deployment");
            catDeploy.setHostcount(hostCount);
            catDeploy.setSitesperhost(sitesPerHost);
            catDeploy.setKfactor(kFactor);
            // copy partition detection configuration from xml to catalog
            String defaultPPDPrefix = "partition_detection";
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

            catCluster.setAdminport(deployment.getAdminMode().getPort());
            catCluster.setAdminstartup(deployment.getAdminMode().isAdminstartup());

            setSystemSettings(deployment, catDeploy);

            catCluster.setHeartbeattimeout(deployment.getHeartbeat().getTimeout());

            // copy schema modification behavior from xml to catalog
            if (cluster.getSchema() != null) {
                catCluster.setUseddlschema(cluster.getSchema() == SchemaType.DDL);
            }
            else {
                // Don't think we can get here, deployment schema guarantees a default value
                hostLog.warn("Schema modification setting not found. " +
                        "Forcing default behavior of UpdateCatalog to modify database schema.");
                catCluster.setUseddlschema(false);
            }
        }
    }

    private static void setSystemSettings(DeploymentType deployment,
                                          Deployment catDeployment)
    {
        // Create catalog Systemsettings
        Systemsettings syssettings =
            catDeployment.getSystemsettings().add("systemsettings");

        syssettings.setTemptablemaxsize(deployment.getSystemsettings().getTemptables().getMaxsize());
        syssettings.setSnapshotpriority(deployment.getSystemsettings().getSnapshot().getPriority());
        syssettings.setElasticduration(deployment.getSystemsettings().getElastic().getDuration());
        syssettings.setElasticthroughput(deployment.getSystemsettings().getElastic().getThroughput());
        syssettings.setQuerytimeout(deployment.getSystemsettings().getQuery().getTimeout());
    }

    private static void validateDirectory(String type, File path) {
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
            throw new RuntimeException(error);
        }
    }

    private static Properties checkExportProcessorConfiguration(ExportConfigurationType exportConfiguration) {
        // on-server export always uses the guest processor
        String exportClientClassName = null;

        switch(exportConfiguration.getType()) {
            case FILE: exportClientClassName = "org.voltdb.exportclient.ExportToFileClient"; break;
            case JDBC: exportClientClassName = "org.voltdb.exportclient.JDBCExportClient"; break;
            case KAFKA: exportClientClassName = "org.voltdb.exportclient.KafkaExportClient"; break;
            case RABBITMQ: exportClientClassName = "org.voltdb.exportclient.RabbitMQExportClient"; break;
            case HTTP: exportClientClassName = "org.voltdb.exportclient.HttpExportClient"; break;
            //Validate that we can load the class.
            case CUSTOM:
                exportClientClassName = exportConfiguration.getExportconnectorclass();
                if (exportConfiguration.isEnabled()) {
                    try {
                        CatalogUtil.class.getClassLoader().loadClass(exportClientClassName);
                    }
                    catch (ClassNotFoundException ex) {
                        String msg =
                                "Custom Export failed to configure, failed to load" +
                                " export plugin class: " + exportConfiguration.getExportconnectorclass() +
                                " Disabling export.";
                        hostLog.error(msg);
                        throw new DeploymentCheckException(msg);
                    }
                }
            break;
        }

        Properties processorProperties = new Properties();

        if (exportClientClassName != null && exportClientClassName.trim().length() > 0) {
            String dexportClientClassName = System.getProperty(ExportDataProcessor.EXPORT_TO_TYPE, exportClientClassName);
            //Override for tests
            if (dexportClientClassName != null && dexportClientClassName.trim().length() > 0 &&
                    exportConfiguration.getType().equals(ServerExportEnum.CUSTOM)) {
                processorProperties.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, dexportClientClassName);
            } else {
                processorProperties.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, exportClientClassName);
            }
        }

        if (exportConfiguration != null) {
            List<PropertyType> configProperties = exportConfiguration.getProperty();
            if (configProperties != null && ! configProperties.isEmpty()) {

                for( PropertyType configProp: configProperties) {
                    String key = configProp.getName();
                    String value = configProp.getValue();
                    if (!key.toLowerCase().contains("passw")) {
                        processorProperties.setProperty(key, value.trim());
                    } else {
                        //Dont trim passwords
                        processorProperties.setProperty(key, value);
                    }
                }
            }
        }

        if (!exportConfiguration.isEnabled()) {
            return processorProperties;
        }

        // Instantiate the Guest Processor
        Class<?> processorClazz = null;
        try {
            processorClazz = Class.forName(ExportManager.PROCESSOR_CLASS);
        } catch (ClassNotFoundException e) {
            throw new DeploymentCheckException("Export is a PRO version only feature");
        }
        ExportDataProcessor processor = null;
        try {
            processor = (ExportDataProcessor)processorClazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            hostLog.error("Unable to instantiate export processor", e);
            throw new DeploymentCheckException("Unable to instantiate export processor", e);
        }
        try {
            processor.addLogger(hostLog);

            processorProperties.put(ExportManager.CONFIG_CHECK_ONLY, "true");
            processor.checkProcessorConfig(processorProperties);
            processor.shutdown();
        } catch (Exception e) {
            hostLog.error("Export processor failed its configuration check", e);
            throw new DeploymentCheckException("Export processor failed its configuration check: " + e.getMessage(), e);
        }

        processorProperties.remove(ExportManager.CONFIG_CHECK_ONLY);
        return processorProperties;
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
        List<String> streamList = new ArrayList<String>();
        boolean noEmptyTarget = (exportType.getConfiguration().size() != 1);
        for (ExportConfigurationType exportConfiguration : exportType.getConfiguration()) {

            boolean connectorEnabled = exportConfiguration.isEnabled();
            // Get the stream name from the xml attribute "stream"
            // Should default to Constants.DEFAULT_EXPORT_CONNECTOR_NAME if not specified
            String streamName = exportConfiguration.getStream();
            if (noEmptyTarget && (streamName == null || streamName.trim().isEmpty()) ) {
                    throw new RuntimeException("stream must be specified along with type in export configuration.");
            }

            if (connectorEnabled) {
                if (streamList.contains(streamName)) {
                    throw new RuntimeException("Multiple connectors can not be assigned to single export stream: " +
                            streamName + ".");
                }
                else {
                    streamList.add(streamName);
                }
            }
            boolean defaultConnector = streamName.equals(Constants.DEFAULT_EXPORT_CONNECTOR_NAME);

            Database db = catalog.getClusters().get("cluster").getDatabases().get("database");

            org.voltdb.catalog.Connector catconn = db.getConnectors().get(streamName);
            if (catconn == null) {
                if (connectorEnabled) {
                    if (defaultConnector) {
                        hostLog.info("Export configuration enabled and provided for the default export " +
                                     "stream in deployment file, however, no export " +
                                     "tables are assigned to the default stream. " +
                                     "Export stream will be disabled.");
                    }
                    else {
                        hostLog.info("Export configuration enabled and provided for export stream " +
                                     streamName +
                                     " in deployment file however no export " +
                                     "tables are assigned to the this stream. " +
                                     "Export stream " + streamName + " will be disabled.");
                    }
                }
                continue;
            }
            Properties processorProperties = checkExportProcessorConfiguration(exportConfiguration);
            for (String name: processorProperties.stringPropertyNames()) {
                ConnectorProperty prop = catconn.getConfig().add(name);
                prop.setName(name);
                prop.setValue(processorProperties.getProperty(name));
            }

            // on-server export always uses the guest processor
            catconn.setLoaderclass(ExportManager.PROCESSOR_CLASS);
            catconn.setEnabled(connectorEnabled);

            if (!connectorEnabled) {
                if (defaultConnector) {
                    hostLog.info("Export configuration for the default export stream is present and is " +
                            "configured to be disabled. The default export stream will be disabled.");
                }
                else {
                    hostLog.info("Export configuration for export stream " + streamName + " is present and is " +
                                 "configured to be disabled. Export stream " + streamName + " will be disabled.");
                }
            } else {
                if (defaultConnector) {
                    hostLog.info("Default export stream is configured and enabled with type=" + exportConfiguration.getType());
                }
                else {
                    hostLog.info("Export stream " + streamName + " is configured and enabled with type=" + exportConfiguration.getType());
                }
                if (exportConfiguration.getProperty() != null) {
                    if (defaultConnector) {
                        hostLog.info("Default export stream configuration properties are: ");
                    }
                    else {
                        hostLog.info("Export stream " + streamName + " configuration properties are: ");
                    }
                    for (PropertyType configProp : exportConfiguration.getProperty()) {
                        if (!configProp.getName().toLowerCase().contains("password")) {
                            hostLog.info("Export Configuration Property NAME=" + configProp.getName() + " VALUE=" + configProp.getValue());
                        }
                    }
                }
            }
        }
    }

    /**
     * Set the security setting in the catalog from the deployment file
     * @param catalog the catalog to be updated
     * @param security security element of the deployment xml
     */
    private static void setSecurityEnabled( Catalog catalog, SecurityType security) {
        Cluster cluster = catalog.getClusters().get("cluster");
        Database database = cluster.getDatabases().get("database");

        cluster.setSecurityenabled(security.isEnabled());
        database.setSecurityprovider(security.getProvider().value());
    }

    /**
     * Set the auto-snapshot settings in the catalog from the deployment file
     * @param catalog The catalog to be updated.
     * @param snapshot A reference to the <snapshot> element of the deployment.xml file.
     */
    private static void setSnapshotInfo(Catalog catalog, SnapshotType snapshotSettings) {
        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        SnapshotSchedule schedule = db.getSnapshotschedule().add("default");
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

    /**
     * Set voltroot path, and set the path overrides for export overflow, partition, etc.
     * @param catalog The catalog to be updated.
     * @param paths A reference to the <paths> element of the deployment.xml file.
     * @param printLog Whether or not to print paths info.
     */
    private static void setPathsInfo(Catalog catalog, PathsType paths) {
        File voltDbRoot;
        final Cluster cluster = catalog.getClusters().get("cluster");
        // Handles default voltdbroot (and completely missing "paths" element).
        voltDbRoot = getVoltDbRoot(paths);
        //Snapshot
        File snapshotPath = getSnapshot(paths.getSnapshots(), voltDbRoot);
        //export overflow
        File exportOverflowPath = getExportOverflow(paths.getExportoverflow(), voltDbRoot);
        // only use these directories in the enterprise version
        File commandLogPath = null;
        File commandLogSnapshotPath = null;
        commandLogPath = getCommandLog(paths.getCommandlog(), voltDbRoot);
        commandLogSnapshotPath = getCommandLogSnapshot(paths.getCommandlogsnapshot(), voltDbRoot);

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
                if (!voltDbRoot.mkdirs()) {
                    hostLog.fatal("Failed to create voltdbroot directory \"" + voltDbRoot.getAbsolutePath() + "\"");
                }
            }
        } else {
            voltDbRoot = new VoltFile(paths.getVoltdbroot().getPath());
            if (!voltDbRoot.exists()) {
                hostLog.info("Creating voltdbroot directory: " + voltDbRoot.getAbsolutePath());
                if (!voltDbRoot.mkdirs()) {
                    hostLog.fatal("Failed to create voltdbroot directory \"" + voltDbRoot.getAbsolutePath() + "\"");
                }
            }
        }
        validateDirectory("volt root", voltDbRoot);

        return voltDbRoot;
    }

    public static File getSnapshot(PathsType.Snapshots paths, File voltDbRoot) {
        File snapshotPath;
        snapshotPath = new File(paths.getPath());
        if (!snapshotPath.isAbsolute())
        {
            snapshotPath = new VoltFile(voltDbRoot, paths.getPath());
        }

        if (!snapshotPath.exists()) {
            hostLog.info("Creating snapshot path directory: " +
                         snapshotPath.getAbsolutePath());
            if (!snapshotPath.mkdirs()) {
                hostLog.fatal("Failed to create snapshot path directory \"" +
                              snapshotPath + "\"");
            }
        }
        validateDirectory("snapshot path", snapshotPath);

        return snapshotPath;

    }

    public static File getCommandLog(PathsType.Commandlog paths, File voltDbRoot) {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            // dumb defaults if you ask for logging in community version
            File commandlogPath =  new VoltFile(voltDbRoot, "command_log");
            return commandlogPath;
        }
        File commandlogPath;
        commandlogPath = new File(paths.getPath());
        if (!commandlogPath.isAbsolute())
        {
            commandlogPath = new VoltFile(voltDbRoot, paths.getPath());
        }

        if (!commandlogPath.exists()) {
            hostLog.info("Creating command log directory: " +
                         commandlogPath.getAbsolutePath());
            if (!commandlogPath.mkdirs()) {
                hostLog.fatal("Failed to create command log path directory \"" +
                              commandlogPath + "\"");
            }
        }
        validateDirectory("command log", commandlogPath);

        return commandlogPath;

    }

    public static File getCommandLogSnapshot(PathsType.Commandlogsnapshot paths, File voltDbRoot) {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            // dumb defaults if you ask for logging in community version
            File commandlogSnapshotPath = new VoltFile(voltDbRoot, "command_log_snapshot");
            return commandlogSnapshotPath;
        }

        File commandlogSnapshotPath;
        commandlogSnapshotPath = new File(paths.getPath());
        if (!commandlogSnapshotPath.isAbsolute())
        {
            commandlogSnapshotPath = new VoltFile(voltDbRoot, paths.getPath());
        }

        if (!commandlogSnapshotPath.exists()) {
            hostLog.info("Creating command log snapshot directory: " +
                         commandlogSnapshotPath.getAbsolutePath());
            if (!commandlogSnapshotPath.mkdirs()) {
                hostLog.fatal("Failed to create command log snapshot path directory \"" +
                              commandlogSnapshotPath + "\"");
            }
        }
        validateDirectory("command log snapshot", commandlogSnapshotPath);

        return commandlogSnapshotPath;

    }

    public static File getExportOverflow(PathsType.Exportoverflow paths, File voltDbRoot) {
        File exportOverflowPath;
        exportOverflowPath = new File(paths.getPath());
        if (!exportOverflowPath.isAbsolute())
        {
            exportOverflowPath = new VoltFile(voltDbRoot, paths.getPath());
        }

        if (!exportOverflowPath.exists()) {
            hostLog.info("Creating export overflow directory: " +
                         exportOverflowPath.getAbsolutePath());
            if (!exportOverflowPath.mkdirs()) {
                hostLog.fatal("Failed to create export overflow path directory \"" +
                              exportOverflowPath + "\"");
            }
        }
        validateDirectory("export overflow", exportOverflowPath);
        return exportOverflowPath;

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

        // The database name is not available in deployment.xml (it is defined
        // in project.xml). However, it must always be named "database", so
        // I've temporarily hardcoded it here until a more robust solution is
        // available.
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
            for (final String role : extractUserRoles(user)) {
                final Group catalogGroup = db.getGroups().get(role);
                // if the role doesn't exist, ignore it.
                if (catalogGroup != null) {
                    final GroupRef groupRef = catUser.getGroups().add(role);
                    groupRef.setGroup(catalogGroup);
                }
                else {
                    hostLog.warn("User \"" + user.getName() +
                            "\" is assigned to non-existent role \"" + role + "\" " +
                            "and may not have the expected database permissions.");
                }
            }
        }
    }

    /**
     * Takes the list of roles specified in the roles user
     * attributes and returns a set from the comma-separated list
     * @param user an instance of {@link UsersType.User}
     * @return a {@link Set} of role name
     */
    private static Set<String> extractUserRoles(final UsersType.User user) {
        Set<String> roles = new TreeSet<String>();
        if (user == null) return roles;

        if (user.getRoles() != null && !user.getRoles().trim().isEmpty()) {
            String [] rolelist = user.getRoles().trim().split(",");
            for (String role: rolelist) {
                if( role == null || role.trim().isEmpty()) continue;
                roles.add(role.trim().toLowerCase());
            }
        }

        return roles;
    }

    private static void setHTTPDInfo(Catalog catalog, HttpdType httpd) {
        Cluster cluster = catalog.getClusters().get("cluster");

        // set the catalog info
        cluster.setHttpdportno(httpd.getPort());
        cluster.setJsonapi(httpd.getJsonapi().isEnabled());
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

    /**
     * This code appeared repeatedly.  Extract method to take bytes for the catalog
     * or deployment file, do the irritating exception crash test, jam the bytes in,
     * and get the SHA-1 hash.
     */
    public static byte[] makeCatalogOrDeploymentHash(byte[] inbytes)
    {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            VoltDB.crashLocalVoltDB("Bad JVM has no SHA-1 hash.", true, e);
        }
        md.update(inbytes);
        byte[] hash = md.digest();
        assert(hash.length == 20); // sha-1 length
        return hash;
    }

    private static ByteBuffer makeCatalogVersionAndBytes(
                int catalogVersion,
                long txnId,
                long uniqueId,
                byte[] catalogBytes,
                byte[] deploymentBytes)
    {
        ByteBuffer versionAndBytes =
            ByteBuffer.allocate(
                    4 +  // catalog bytes length
                    catalogBytes.length +
                    4 +  // deployment bytes length
                    deploymentBytes.length +
                    4 +  // catalog version
                    8 +  // txnID
                    8 +  // unique ID
                    20 + // catalog SHA-1 hash
                    20   // deployment SHA-1 hash
                    );
        versionAndBytes.putInt(catalogVersion);
        versionAndBytes.putLong(txnId);
        versionAndBytes.putLong(uniqueId);
        versionAndBytes.put(makeCatalogOrDeploymentHash(catalogBytes));
        versionAndBytes.put(makeCatalogOrDeploymentHash(deploymentBytes));
        versionAndBytes.putInt(catalogBytes.length);
        versionAndBytes.put(catalogBytes);
        versionAndBytes.putInt(deploymentBytes.length);
        versionAndBytes.put(deploymentBytes);
        return versionAndBytes;
    }

    /**
     *  Attempt to create the ZK node and write the catalog/deployment bytes
     *  to ZK.  Used during the initial cluster deployment discovery and
     *  distribution.
     */
    public static void writeCatalogToZK(ZooKeeper zk,
                int catalogVersion,
                long txnId,
                long uniqueId,
                byte[] catalogBytes,
                byte[] deploymentBytes)
        throws KeeperException, InterruptedException
    {
        ByteBuffer versionAndBytes = makeCatalogVersionAndBytes(catalogVersion,
                txnId, uniqueId, catalogBytes, deploymentBytes);
        zk.create(VoltZK.catalogbytes,
                versionAndBytes.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Update the catalog/deployment contained in ZK.  Someone somewhere must have
     * called writeCatalogToZK earlier in order to create the ZK node.
     */
    public static void updateCatalogToZK(ZooKeeper zk,
            int catalogVersion,
            long txnId,
            long uniqueId,
            byte[] catalogBytes,
            byte[] deploymentBytes)
        throws KeeperException, InterruptedException
    {
        ByteBuffer versionAndBytes = makeCatalogVersionAndBytes(catalogVersion,
                txnId, uniqueId, catalogBytes, deploymentBytes);
        zk.setData(VoltZK.catalogbytes, versionAndBytes.array(), -1);
    }

    public static class CatalogAndIds {
        public final long txnId;
        public final long uniqueId;
        public final int version;
        private final byte[] catalogHash;
        private final byte[] deploymentHash;
        public final byte[] catalogBytes;
        public final byte[] deploymentBytes;

        private CatalogAndIds(long txnId,
                long uniqueId,
                int catalogVersion,
                byte[] catalogHash,
                byte[] deploymentHash,
                byte[] catalogBytes,
                byte[] deploymentBytes)
        {
            this.txnId = txnId;
            this.uniqueId = uniqueId;
            this.version = catalogVersion;
            this.catalogHash = catalogHash;
            this.deploymentHash = deploymentHash;
            this.catalogBytes = catalogBytes;
            this.deploymentBytes = deploymentBytes;
        }

        public byte[] getCatalogHash()
        {
            return catalogHash.clone();
        }

        public byte[] getDeploymentHash()
        {
            return deploymentHash.clone();
        }

        @Override
        public String toString()
        {
            return String.format("Catalog: TXN ID %d, catalog hash %s, deployment hash %s\n",
                    txnId,
                    Encoder.hexEncode(catalogHash).substring(0, 10),
                    Encoder.hexEncode(deploymentHash).substring(0, 10));
        }
    }

    /**
     * Retrieve the catalog and deployment configuration from zookeeper.
     * NOTE: In general, people who want the catalog and/or deployment should
     * be getting it from the current CatalogContext, available from
     * VoltDB.instance().  This is primarily for startup and for use by
     * @UpdateApplicationCatalog.  If you think this is where you need to
     * be getting catalog or deployment from, consider carefully if that's
     * really what you want to do. --izzy 12/8/2014
     */
    public static CatalogAndIds getCatalogFromZK(ZooKeeper zk) throws KeeperException, InterruptedException {
        ByteBuffer versionAndBytes =
                ByteBuffer.wrap(zk.getData(VoltZK.catalogbytes, false, null));
        int version = versionAndBytes.getInt();
        long catalogTxnId = versionAndBytes.getLong();
        long catalogUniqueId = versionAndBytes.getLong();
        byte[] catalogHash = new byte[20]; // sha-1 hash size
        versionAndBytes.get(catalogHash);
        byte[] deploymentHash = new byte[20]; // sha-1 hash size
        versionAndBytes.get(deploymentHash);
        int catalogLength = versionAndBytes.getInt();
        byte[] catalogBytes = new byte[catalogLength];
        versionAndBytes.get(catalogBytes);
        int deploymentLength = versionAndBytes.getInt();
        byte[] deploymentBytes = new byte[deploymentLength];
        versionAndBytes.get(deploymentBytes);
        versionAndBytes = null;
        return new CatalogAndIds(catalogTxnId, catalogUniqueId, version, catalogHash,
                deploymentHash, catalogBytes, deploymentBytes);
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
        Map<String, StmtTargetTableScan> tablesRead = new TreeMap<String, StmtTargetTableScan>();
        Collection<String> indexes = new TreeSet<String>();
        if (topPlan != null) {
            topPlan.getTablesAndIndexes(tablesRead, indexes);
        }
        if (bottomPlan != null) {
            bottomPlan.getTablesAndIndexes(tablesRead, indexes);
        }

        String updated = "";
        if ( ! stmt.getReadonly()) {
            updated = topPlan.getUpdatedTable();
            if (updated == null) {
                updated = bottomPlan.getUpdatedTable();
            }
            assert(updated.length() > 0);
        }

        Set<String> readTableNames = tablesRead.keySet();
        stmt.setTablesread(StringUtils.join(readTableNames, ","));
        stmt.setTablesupdated(updated);

        Set<String> tableDotIndexNames = new TreeSet<>();

        for (Table table : db.getTables()) {
            if (readTableNames.contains(table.getTypeName())) {
                readTableNames.remove(table.getTypeName());
                for (String indexName : indexes) {
                    Index index = table.getIndexes().get(indexName);
                    if (index != null) {
                        tableDotIndexNames.add(table.getTypeName() + "." + index.getTypeName());
                    }
                }
            }
        }

        String indexString = StringUtils.join(tableDotIndexNames, ",");
        stmt.setIndexesused(indexString);

        assert(tablesRead.size() == 0);
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

    /**
     * Build an empty catalog jar file.
     * @return jar file or null (on failure)
     * @throws IOException on failure to create temporary jar file
     */
    public static File createTemporaryEmptyCatalogJarFile() throws IOException {
        File emptyJarFile = File.createTempFile("catalog-empty", ".jar");
        emptyJarFile.deleteOnExit();
        VoltCompiler compiler = new VoltCompiler();
        if (!compiler.compileEmptyCatalog(emptyJarFile.getAbsolutePath())) {
            return null;
        }
        return emptyJarFile;
    }

    public static class DeploymentCheckException extends RuntimeException {

        private static final long serialVersionUID = 6741313621335268608L;

        public DeploymentCheckException() {
            super();
        }

        public DeploymentCheckException(String message, Throwable cause) {
            super(message, cause);
        }

        public DeploymentCheckException(String message) {
            super(message);
        }

        public DeploymentCheckException(Throwable cause) {
            super(cause);
        }

    }

}
