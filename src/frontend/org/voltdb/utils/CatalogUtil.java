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

package org.voltdb.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.json_voltpatches.JSONException;
import org.mindrot.BCrypt;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.VoltPort;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZKUtil.ZKCatalogStatus;
import org.voltdb.CatalogContext;
import org.voltdb.DefaultProcedureManager;
import org.voltdb.HealthMonitor;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.RealVoltDB;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.TableType;
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
import org.voltdb.catalog.Priorities;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Property;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.ThreadPool;
import org.voltdb.catalog.Topic;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.deploymentfile.AvroType;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.CommandLogType.Frequency;
import org.voltdb.compiler.deploymentfile.ConnectionType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.compiler.deploymentfile.DrType;
import org.voltdb.compiler.deploymentfile.ExportConfigurationType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.FlushIntervalType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.HttpdType;
import org.voltdb.compiler.deploymentfile.ImportConfigurationType;
import org.voltdb.compiler.deploymentfile.ImportType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.PriorityPolicyType;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType;
import org.voltdb.compiler.deploymentfile.SchemaType;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compiler.deploymentfile.ServerImportEnum;
import org.voltdb.compiler.deploymentfile.SnapshotType;
import org.voltdb.compiler.deploymentfile.SnmpType;
import org.voltdb.compiler.deploymentfile.SslType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compiler.deploymentfile.ThreadPoolsType;
import org.voltdb.compiler.deploymentfile.TopicType;
import org.voltdb.compiler.deploymentfile.TopicsType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.e3.topics.TopicProperties;
import org.voltdb.e3.topics.TopicRetention;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportManager;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.importer.ImportDataProcessor;
import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.importer.formatter.FormatterBuilder;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.iv2.PriorityPolicy;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.snmp.DummySnmpTrapSender;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.xml.sax.SAXException;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.collect.Maps;

/**
 *
 */
public abstract class CatalogUtil {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final String CATALOG_FILENAME = "catalog.txt";
    public static final String CATALOG_BUILDINFO_FILENAME = "buildinfo.txt";
    public static final String CATALOG_REPORT_FILENAME = "catalog-report.html";
    public static final String CATALOG_EMPTY_DDL_FILENAME = "ddl.sql";

    public static final String SIGNATURE_TABLE_NAME_SEPARATOR = "|";
    public static final String SIGNATURE_DELIMITER = ",";

    public static final String ADMIN = "administrator";

    // DR conflicts export table name prefix
    public static final String DR_CONFLICTS_PARTITIONED_EXPORT_TABLE = "VOLTDB_AUTOGEN_XDCR_CONFLICTS_PARTITIONED";
    public static final String DR_CONFLICTS_REPLICATED_EXPORT_TABLE = "VOLTDB_AUTOGEN_XDCR_CONFLICTS_REPLICATED";
    // DR conflicts export group name
    public static final String DR_CONFLICTS_TABLE_EXPORT_GROUP = "VOLTDB_XDCR_CONFLICTS";
    public static final String DEFAULT_DR_CONFLICTS_EXPORT_TYPE = "csv";
    public static final String DEFAULT_DR_CONFLICTS_NONCE = "LOG";
    public static final String DEFAULT_DR_CONFLICTS_DIR = "xdcr_conflicts";
    public static final String DR_HIDDEN_COLUMN_NAME = "dr_clusterid_timestamp";
    public static final String VIEW_HIDDEN_COLUMN_NAME = "count_star";
    public static final String MIGRATE_HIDDEN_COLUMN_NAME = "migrate_column";

    // Hash Mismatch log string
    public static final String MATCHED_STATEMENTS = "matching statements and parameters";
    public static final String MISMATCHED_STATEMENTS = "mismatched statements";
    public static final String MISMATCHED_PARAMETERS = "mismatched parameters";

    final static Pattern JAR_EXTENSION_RE  = Pattern.compile("(?:.+)\\.jar/(?:.+)" ,Pattern.CASE_INSENSITIVE);
    public final static Pattern XML_COMMENT_RE = Pattern.compile("<!--.+?-->",Pattern.MULTILINE|Pattern.DOTALL);
    public final static Pattern HOSTCOUNT_RE = Pattern.compile(
            "\\bhostcount\\s*=\\s*(?:\"\\s*\\d+\\s*\"|'\\s*\\d+\\s*')",Pattern.MULTILINE);

    public static final VoltTable.ColumnInfo DR_HIDDEN_COLUMN_INFO =
            new VoltTable.ColumnInfo(DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT);
    public static final VoltTable.ColumnInfo VIEW_HIDDEN_COLUMN_INFO =
            new VoltTable.ColumnInfo(VIEW_HIDDEN_COLUMN_NAME, VoltType.BIGINT);
    public static final VoltTable.ColumnInfo MIGRATE_HIDDEN_COLUMN_INFO =
            new VoltTable.ColumnInfo(MIGRATE_HIDDEN_COLUMN_NAME, VoltType.BIGINT);

    // Map of all of the hidden columns from column name to type
    public static final Map<String, VoltType> HIDDEN_COLUMNS = ImmutableMap.of(DR_HIDDEN_COLUMN_NAME, VoltType.BIGINT,
            VIEW_HIDDEN_COLUMN_NAME, VoltType.BIGINT, MIGRATE_HIDDEN_COLUMN_NAME, VoltType.BIGINT);

    public static final String ROW_LENGTH_LIMIT = "row.length.limit";
    public static final int EXPORT_INTERNAL_FIELD_Length = 41; // 8 * 5 + 1;

    public final static String[] CATALOG_DEFAULT_ARTIFACTS = {
            VoltCompiler.AUTOGEN_DDL_FILE_NAME,
            CATALOG_BUILDINFO_FILENAME,
            CATALOG_REPORT_FILENAME,
            CATALOG_EMPTY_DDL_FILENAME,
            CATALOG_FILENAME,
    };

    private static boolean m_exportEnabled = false;
    public static final String CATALOG_FILE_NAME = "catalog.jar";
    public static final String STAGED_CATALOG_PATH = Constants.CONFIG_DIR + File.separator + "staged-catalog.jar";
    public static final String VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME = "voltdbbundlelocation";

    public static JAXBContext m_jc;
    public static Schema m_schema;
    // 52mb - 4k, leave space for zookeeper header
    public static final int MAX_CATALOG_CHUNK_SIZE = VoltPort.MAX_MESSAGE_LENGTH - 4 * 1024;
    public static final int CATALOG_BUFFER_HEADER = 4 +  // version number
                                                    8 +  // generation Id
                                                    4 +  // deployment bytes length
                                                    4 +  // catalog bytes length
                                                    20;  // catalog SHA-1 hash
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
     * @param isXDCR
     * @return Pair containing updated InMemoryJarFile and upgraded version (or null if it wasn't upgraded)
     * @throws IOException
     *             If the catalog cannot be loaded because it's incompatible, or
     *             if there is no version information in the catalog.
     */
    public static Pair<InMemoryJarfile, String> loadAndUpgradeCatalogFromJar(byte[] catalogBytes, boolean isXDCR)
        throws IOException
    {
        // Throws IOException on load failure.
        InMemoryJarfile jarfile = loadInMemoryJarFile(catalogBytes);

        return loadAndUpgradeCatalogFromJar(jarfile, isXDCR);
    }

    /**
     * Load a catalog from the InMemoryJarfile.
     *
     * @param jarfile
     * @return Pair containing updated InMemoryJarFile and upgraded version (or null if it wasn't upgraded)
     * @throws IOException if there is no version information in the catalog.
     */
    public static Pair<InMemoryJarfile, String> loadAndUpgradeCatalogFromJar(InMemoryJarfile jarfile, boolean isXDCR)
        throws IOException
    {
        // Let VoltCompiler do a version check and upgrade the catalog on the fly.
        // I.e. jarfile may be modified.
        VoltCompiler compiler = new VoltCompiler(isXDCR);
        String upgradedFromVersion = compiler.upgradeCatalogAsNeeded(jarfile);
        return new Pair<>(jarfile, upgradedFromVersion);
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
            throw new IOException("Catalog build information not found - please build your application " +
                    "using the current version of VoltDB.");
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
     * Get the auto generated DDL from the catalog jar.
     *
     * @param jarfile in-memory catalog jar file
     * @return Auto generated DDL stored in catalog.jar
     * @throws IOException If the catalog or the auto generated ddl cannot be loaded.
     */
    public static String getAutoGenDDLFromJar(InMemoryJarfile jarfile)
            throws IOException
    {
        // Read the raw auto generated ddl bytes.
        byte[] ddlBytes = jarfile.get(VoltCompiler.AUTOGEN_DDL_FILE_NAME);
        if (ddlBytes == null) {
            throw new IOException("Auto generated schema DDL not found - please make sure the database is " +
                    "initialized with valid schema.");
        }
        String ddl = new String(ddlBytes, StandardCharsets.UTF_8);
        return ddl.trim();
    }

    /**
     * Removes the default voltdb artifact files from catalog and returns the resulltant
     * jar file. This will contain dependency files needed for generated stored procs
     *
     * @param jarfile in-memory catalog jar file
     * @return In-memory jar file containing dependency files for stored procedures
     */
    public static InMemoryJarfile getCatalogJarWithoutDefaultArtifacts(final InMemoryJarfile jarfile) {
        InMemoryJarfile cloneJar = jarfile.deepCopy();
        for (String entry : CATALOG_DEFAULT_ARTIFACTS) {
            cloneJar.remove(entry);
        }
        return cloneJar;
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
        if (!jarfile.containsKey(CATALOG_FILENAME)) {
            throw new IOException("Database catalog not found - please build your application " +
                    "using the current version of VoltDB.");
        }

        return jarfile;
    }

    /**
     * Tell if a table is a single table view without a COUNT(*) column.
     * In that case, the table will have a hidden COUNT(*) column which we need to
     * include in the snapshot.
     * @param table The table to check.
     * @return true if we need to snapshot a hidden COUNT(*) column.
     */
    public static boolean needsViewHiddenColumn(Table table) {
        // The table must be a view, and it can only have one source table.
        // We do not care if the source table is persistent or streamed.

        // If the table is not a materialized view, skip.
        if (table.getMaterializer() == null) {
            return false;
        }

        // If the table is a materialized view, then search if there is a COUNT(*) column.
        // We only allow the users to omit the COUNT(*) column if the view is built on
        // one single table. So we do not check again here.
        for (Column c : table.getColumns()) {
            if (ExpressionType.get(c.getAggregatetype()) == ExpressionType.AGGREGATE_COUNT_STAR) {
                return false;
            }
        }
        return true;
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
     * @param catalogTable a catalog table providing the schema
     * @return An empty table with the same schema as a given catalog table.
     */
    public static VoltTable getVoltTable(Table catalogTable) {
        List<Column> catalogColumns = CatalogUtil.getSortedCatalogItems(catalogTable.getColumns(), "index");

        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[catalogColumns.size()];

        int i = 0;
        for (Column catCol : catalogColumns) {
            columns[i++] = catalogColumnToInfo(catCol);
        }

        return new VoltTable(columns);
    }

    /**
     *
     * @param catalogTable a catalog table providing the schema
     * @param hiddenColumnInfos variable-length ColumnInfo objects for hidden columns
     * @return An empty table with the same schema as a given catalog table.
     */
    public static VoltTable getVoltTable(Table catalogTable, VoltTable.ColumnInfo... hiddenColumns) {
        List<Column> catalogColumns = CatalogUtil.getSortedCatalogItems(catalogTable.getColumns(), "index");

        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[catalogColumns.size() + hiddenColumns.length];

        int i = 0;
        for (Column catCol : catalogColumns) {
            columns[i++] = catalogColumnToInfo(catCol);
        }
        for (VoltTable.ColumnInfo hiddenColumnInfo : hiddenColumns) {
            columns[i++] = hiddenColumnInfo;
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
        List<T> retval = new ArrayList<>();
        getSortedCatalogItems(items, sortFieldName, retval);
        return retval;
    }

    /**
     * A getSortedCatalogItems variant with the result list filled in-place
     * @param <T> The type of item to sort.
     * @param items The set of catalog items.
     * @param sortFieldName The name of the field to sort on.
     * @param result An output list of catalog items, sorted on the specified field.
     */
    public static <T extends CatalogType> void getSortedCatalogItems(CatalogMap<T> items,
            String sortFieldName, List<T> result) {
        assert (items != null);
        assert (sortFieldName != null);

        // build a treemap based on the field value
        TreeMap<Object, T> map = new TreeMap<>();
        boolean hasField = false;
        for (T item : items) {
            // check the first time through for the field
            if (hasField == false) {
                hasField = ArrayUtils.contains(item.getFields(), sortFieldName);
            }
            assert (hasField == true);

            map.put(item.getField(sortFieldName), item);
        }

        result.addAll(map.values());
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
            throw new Exception("ERROR: Table '" + catalogTable.getTypeName() +
                    "' does not have a PRIMARY KEY constraint");
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
        Collection<Column> columns = new ArrayList<>();
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

    public static NavigableSet<Table> getExportTablesExcludeViewOnly(CatalogMap<Connector> connectors) {
        ImmutableSortedSet.Builder<Table> exportTables = ImmutableSortedSet.naturalOrder();
        for (Connector connector : connectors) {
            for (ConnectorTableInfo tinfo : connector.getTableinfo()) {
                Table t = tinfo.getTable();
                if (!TableType.needsExportDataSource(t.getTabletype())) {
                    continue;
                }
                exportTables.add(t);
            }
        }
        return exportTables.build();
    }

    // Return all the tables that are persistent streams, i.e. all tables creating PBDs,
    // regardless of whether a connector exists
    public static NavigableMap<String, Table> getAllStreamsExcludingViews(Database db) {
        ImmutableSortedMap.Builder<String, Table> exportTables = ImmutableSortedMap
                .orderedBy(String.CASE_INSENSITIVE_ORDER);
        for (Table t : db.getTables()) {
            if (!TableType.needsExportDataSource(t.getTabletype())) {
                continue;
            }
            exportTables.put(t.getTypeName(), t);
        }
        return exportTables.build();
    }

    public static CatalogMap<Connector> getConnectors(CatalogContext catalogContext) {
        return catalogContext.database.getConnectors();
    }

    public static CatalogMap<ThreadPool> getThreadPools(CatalogContext catalogContext) {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Deployment deployment = cluster.getDeployment().get("deployment");
        return deployment.getThreadpools();
    }

    public static boolean hasEnabledConnectors(CatalogMap<Connector> connectors) {
        for (Connector conn : connectors) {
            if (conn.getEnabled() && !conn.getTableinfo().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public static List<String> getDisabledConnectors() {
        List<String> disabledConnectors = new LinkedList<>();
        for (Connector conn : getConnectors(VoltDB.instance().getCatalogContext())) {
            if (!conn.getEnabled() && !conn.getTableinfo().isEmpty()) {
                disabledConnectors.add(conn.getTypeName());
            }
        }
        return disabledConnectors;
    }

    public static boolean hasExportedTables(CatalogMap<Connector> connectors) {
        for (Connector conn : connectors) {
            if (!conn.getTableinfo().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public static void dumpConnectors(VoltLogger logger, CatalogMap<Connector> connectors) {

        if (!logger.isDebugEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder("Connectors:\n");
        for (Connector conn : connectors) {
            sb.append("\tname:    " + conn.getTypeName() + "\n");
            sb.append("\tenabled: " + conn.getEnabled() + "\n");
            if (conn.getTableinfo().isEmpty()) {
                sb.append("\tno tables ...\n");
            }
            else {
                sb.append("\ttables:\n");
                for (ConnectorTableInfo ti : conn.getTableinfo()) {
                    sb.append("\t\t table name: " + ti.getTypeName() + "\n");
                }
            }
        }
        logger.debug(sb.toString());
    }

    /**
     * Test if a table with {@code name} is involved with export. This will also return {@code false} for topics
     *
     * @param db   {@link Database} instance
     * @param name of table
     * @return {@code true} if a table with {@code name} is involved with export
     */
    public static boolean isExportTable(Database db, String name) {
        Table table = db.getTables().get(name);
        if (table != null) {
            boolean isTopic = isTopic(db, name);
            int type = table.getTabletype();
            if (TableType.isInvalidType(type)) {
                return isStream(db, table) && !isTopic;
            }
            return TableType.PERSISTENT.get() != type && !isTopic;
        }
        return false;
    }

    /**
     * Test if a table with {@code name} is a topic
     *
     * @param db    {@link Database} instance
     * @param       name of table
     * @return      {@code true} if a table with {@code name} is a topic
     */
    public static boolean isTopic(Database db, String name) {
        Topic topic = db.getTopics().get(name);
        return topic != null;
    }

    /**
     * Return true if a table was explicitly declared as a STREAM in the DDL
     *
     * @param database
     * @param table
     * @return true if a table is export or false otherwise
     */
    public static boolean isStream(org.voltdb.catalog.Database database,
                                            org.voltdb.catalog.Table table) {
        int type = table.getTabletype();
        // Starting from V9.2 we should not read pre-9.0 snapshots
        // However, many JUnit tests mock the environment with invalid table types
        // assert(!TableType.isInvalidType(type));
        if (TableType.isInvalidType(type)) {
            // This implementation uses connectors instead of just looking at the tableType
            // because snapshots or catalogs from pre-9.0 versions (DR) will not have this new tableType field.
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
            // Found no connectors
            return false;
        } else {
            return TableType.isStream(type);
        }
    }

    public static boolean isExportEnabled() {
        return m_exportEnabled;
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
     * Return list of materialized view for table.
     */
    public static List<Table> getMaterializeViews(org.voltdb.catalog.Database database,
                                                       org.voltdb.catalog.Table table)
    {
        ArrayList<Table> tlist = new ArrayList<>();
        CatalogMap<Table> tables = database.getTables();
        for (Table t : tables) {
            Table matsrc = t.getMaterializer();
            if ((matsrc != null) && (matsrc.getRelativeIndex() == table.getRelativeIndex())) {
                tlist.add(t);
            }
        }
        return tlist;
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
     * @param isPlaceHolderCatalog if the catalog is isPlaceHolderCatalog and we are verifying
     *        only deployment xml.
     * @return String containing any errors parsing/validating the deployment. NULL on success.
     */
    public static String compileDeployment(Catalog catalog,
            DeploymentType deployment,
            boolean isPlaceHolderCatalog)
    {
        String errmsg = null;

        try {
            validateDeployment(catalog, deployment);
            validateAvro(deployment);
            validateThreadPoolsConfiguration(deployment.getThreadpools());
            Cluster catCluster = getCluster(catalog);

            // add our hacky Deployment to the catalog
            if (catCluster.getDeployment().get("deployment") == null) {
                catCluster.getDeployment().add("deployment");
            }

            // set the cluster info
            setClusterInfo(catalog, deployment);

            //Set the snapshot schedule
            setSnapshotInfo(catalog, deployment.getSnapshot());

            //Set enable security
            setSecurityEnabled(catalog, deployment.getSecurity());

            // set the users info
            // We'll skip this when building the dummy catalog on startup
            // so that we don't spew misleading user/role warnings
            if (!isPlaceHolderCatalog) {
                setUsersInfo(catalog, deployment.getUsers());
            }

            // set the HTTPD info
            setHTTPDInfo(catalog, deployment.getHttpd(), deployment.getSsl());

            DrType drType = deployment.getDr();
            Integer drFlushInterval = drType == null ? null : drType.getFlushInterval();
            if (drFlushInterval == null) {
                drFlushInterval = deployment.getSystemsettings().getFlushinterval().getDr().getInterval();
            }
            if (drFlushInterval < catCluster.getGlobalflushinterval()) {
                hostLog.warn("DR flush interval (" + drFlushInterval + "ms) in the configuration " +
                        "is smaller than the global minimum (" + catCluster.getGlobalflushinterval() + "ms)");
            }
            if (drFlushInterval <= 0) {
                throw new RuntimeException("DR Flush Interval must be greater than zero");
            }

            setDrInfo(catalog, deployment.getDr(), deployment.getCluster(), drFlushInterval, isPlaceHolderCatalog);

            int exportFlushInterval = deployment.getSystemsettings().getFlushinterval().getExport().getInterval();
            catCluster.setExportflushinterval(exportFlushInterval);
            if (exportFlushInterval < catCluster.getGlobalflushinterval()) {
                hostLog.warn("Export flush interval (" + exportFlushInterval + "ms) in the configuration " +
                        "is smaller than the global minimum (" + catCluster.getGlobalflushinterval() + "ms)");
            }
            if (exportFlushInterval <= 0) {
                throw new RuntimeException("Export Flush Interval must be greater than zero");
            }
            if (!isPlaceHolderCatalog) {
                setExportInfo(catalog, deployment.getExport(), deployment.getThreadpools());
                setTopics(catalog, deployment.getTopics());
                setImportInfo(catalog, deployment.getImport());
                setSnmpInfo(deployment.getSnmp());
            }

            setCommandLogInfo( catalog, deployment.getCommandlog());
            //This is here so we can update our local list of paths.
            //I would not have needed this if validateResourceMonitorInfo didnt exist here.
            VoltDB.instance().loadLegacyPathProperties(deployment);

            setupPaths(deployment.getPaths());
            validateResourceMonitorInfo(deployment);
        } catch (Exception e) {
            // Anything that goes wrong anywhere in trying to handle the deployment file
            // should return an error, and let the caller decide what to do (crash or not, for
            // example)
            errmsg = "Error validating deployment configuration: " + e.getMessage();
            hostLog.error(errmsg);
            return errmsg;
        }

        return null;
    }

    private static void validateResourceMonitorInfo(DeploymentType deployment) {
        // call resource monitor ctor so that it does all validations.
        new HealthMonitor(deployment.getSystemsettings(), new DummySnmpTrapSender());
    }


    /*
     * Command log element is created in setPathsInfo
     */
    private static void setCommandLogInfo(Catalog catalog, CommandLogType commandlog) {
        int fsyncInterval = 200;
        int maxTxnsBeforeFsync = Integer.MAX_VALUE;
        org.voltdb.catalog.CommandLog config = getCluster(catalog).getLogconfig().get("log");
        if (config == null) {
            config = getCluster(catalog).getLogconfig().add("log");
        }

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
            DeploymentType deployment = unmarshalDeployment(deployIS);
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

    /**
     * Get a reference to the root <deployment> element from the deployment.xml file.
     * @param deployIS
     * @return Returns a reference to the root <deployment> element.
     */
    public static DeploymentType unmarshalDeployment(InputStream deployIS) throws JAXBException {
            Unmarshaller unmarshaller = m_jc.createUnmarshaller();
            unmarshaller.setSchema(m_schema);
            JAXBElement<DeploymentType> result =
                (JAXBElement<DeploymentType>) unmarshaller.unmarshal(deployIS);
            DeploymentType deployment = result.getValue();
            return deployment;
    }

    public static void populateDefaultDeployment(DeploymentType deployment) {
        //partition detection
        PartitionDetectionType pd = deployment.getPartitionDetection();
        if (pd == null) {
            pd = new PartitionDetectionType();
            deployment.setPartitionDetection(pd);
        }
        //heartbeat
        if (deployment.getHeartbeat() == null) {
            HeartbeatType hb = new HeartbeatType();
            deployment.setHeartbeat(hb);
        }

        SslType ssl = deployment.getSsl();
        if (ssl == null) {
            ssl = new SslType();
            deployment.setSsl(ssl);
        }
        //httpd
        HttpdType httpd = deployment.getHttpd();
        if (httpd == null) {
            httpd = new HttpdType();
            // Find next available port starting with the default
            httpd.setPort(Constants.HTTP_PORT_AUTO);
            deployment.setHttpd(httpd);
        }
        //jsonApi
        HttpdType.Jsonapi jsonApi = httpd.getJsonapi();
        if (jsonApi == null) {
            jsonApi = new HttpdType.Jsonapi();
            httpd.setJsonapi(jsonApi);
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
        if (paths.getDroverflow() == null) {
            final PathsType.Droverflow droverflow = new PathsType.Droverflow();
            paths.setDroverflow(droverflow);
        }
        if (paths.getLargequeryswap() == null) {
            final PathsType.Largequeryswap largequeryswap = new PathsType.Largequeryswap();
            paths.setLargequeryswap(largequeryswap);
        }
        if (paths.getExportcursor() == null) {
            final PathsType.Exportcursor exportCursor = new PathsType.Exportcursor();
            paths.setExportcursor(exportCursor);
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
        SystemSettingsType.Procedure procedure = ss.getProcedure();
        if (procedure == null) {
            procedure = new SystemSettingsType.Procedure();
            ss.setProcedure(procedure);
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
        if (ss.getClockskew() == null) {
            ss.setClockskew(new SystemSettingsType.Clockskew());
        }
        ResourceMonitorType rm = ss.getResourcemonitor();
        if (rm == null) {
            rm = new ResourceMonitorType();
            ss.setResourcemonitor(rm);
        }
        ResourceMonitorType.Memorylimit mem = rm.getMemorylimit();
        if (mem == null) {
            mem = new ResourceMonitorType.Memorylimit();
            rm.setMemorylimit(mem);
        }
        FlushIntervalType fi = ss.getFlushinterval();
        if (fi == null) {
            fi = new FlushIntervalType();
            ss.setFlushinterval(fi);
        }
        if (fi.getExport() == null) {
            fi.setExport(new FlushIntervalType.Export());
        }
        if (fi.getDr() == null) {
            fi.setDr(new FlushIntervalType.Dr());
        }
    }

    /**
     * Computes a MD5 digest (128 bits -> 2 longs -> UUID which is comprised of
     * two longs) of a deployment file stripped of all comments and its hostcount
     * attribute set to 0.
     *
     * @param deploymentBytes
     * @return MD5 digest for for configuration
     */
    public static UUID makeDeploymentHashForConfig(byte[] deploymentBytes) {
        String normalized = new String(deploymentBytes, StandardCharsets.UTF_8);
        Matcher matcher = XML_COMMENT_RE.matcher(normalized);
        normalized = matcher.replaceAll("");
        matcher = HOSTCOUNT_RE.matcher(normalized);
        normalized = matcher.replaceFirst("hostcount=\"0\"");
        return Digester.md5AsUUID(normalized);
    }

    /**
     * Given the deployment object generate the XML
     * @param deployment
     * @return XML of deployment object.
     * @throws IOException
     */
    public static String getDeployment(DeploymentType deployment) throws IOException {
        return getDeployment(deployment, false);
    }

    /**
     * Given the deployment object generate the XML
     *
     * @param deployment
     * @param indent
     * @return XML of deployment object.
     * @throws IOException
     */
    public static String getDeployment(DeploymentType deployment, boolean indent) throws IOException {
        try {
            if (m_jc == null || m_schema == null) {
                throw new RuntimeException("Error schema validation.");
            }
            Marshaller marshaller = m_jc.createMarshaller();
            marshaller.setSchema(m_schema);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.valueOf(indent));
            StringWriter sw = new StringWriter();
            marshaller.marshal(new JAXBElement<>(new QName("","deployment"), DeploymentType.class, deployment), sw);
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
                String msg = "Cannot enable security without defining at least one user in " +
                        "the built-in ADMINISTRATOR role in the deployment file.";
                throw new RuntimeException(msg);
            }

            boolean foundAdminUser = false;
            for (UsersType.User user : deployment.getUsers().getUser()) {
                if (user.getRoles() == null) {
                    continue;
                }

                for (String role : extractUserRoles(user)) {
                    if (role.equalsIgnoreCase(ADMIN)) {
                        foundAdminUser = true;
                        break;
                    }
                }
            }

            if (!foundAdminUser) {
                String msg = "Cannot enable security without defining at least one user in " +
                        "the built-in ADMINISTRATOR role in the deployment file.";
                throw new RuntimeException(msg);
            }
        }
    }

    private static void validateAvro(DeploymentType deployment) throws MalformedURLException {
        AvroType avro = deployment.getAvro();
        if (avro == null) {
            return;
        }

        String registryUrl = avro.getRegistry();
        assert registryUrl != null : "deployment syntax must require URL";
        try {
            new URL(registryUrl);
        }
        catch (MalformedURLException ex) {
            throw new RuntimeException(
                    "Failed to parse schema registry url in <avro>: " + ex.getMessage());
        }
    }

    public final static Map<String, String> asClusterSettingsMap(DeploymentType depl) {
        return ImmutableMap.<String,String>builder()
                .put(ClusterSettings.HOST_COUNT, Integer.toString(depl.getCluster().getHostcount()))
                .put(ClusterSettings.CANGAMANGA, Integer.toString(depl.getSystemsettings().getQuery().getTimeout()))
                .build();
    }

    public final static Map<String,String> asNodeSettingsMap(DeploymentType depl) {
        PathsType paths = depl.getPaths();
        return ImmutableMap.<String,String>builder()
                .put(NodeSettings.VOLTDBROOT_PATH_KEY, paths.getVoltdbroot().getPath())
                .put(NodeSettings.CL_PATH_KEY, paths.getCommandlog().getPath())
                .put(NodeSettings.CL_SNAPSHOT_PATH_KEY, paths.getCommandlogsnapshot().getPath())
                .put(NodeSettings.SNAPSHOT_PATH_KEY, paths.getSnapshots().getPath())
                .put(NodeSettings.EXPORT_OVERFLOW_PATH_KEY, paths.getExportoverflow().getPath())
                .put(NodeSettings.DR_OVERFLOW_PATH_KEY, paths.getDroverflow().getPath())
                .put(NodeSettings.LARGE_QUERY_SWAP_PATH_KEY, paths.getLargequeryswap().getPath())
                .put(NodeSettings.LOCAL_SITES_COUNT_KEY, Integer.toString(depl.getCluster().getSitesperhost()))
                .put(NodeSettings.EXPORT_CURSOR_PATH_KEY, paths.getExportcursor().getPath())
                .build();
    }

    public final static DbSettings asDbSettings(String deploymentURL) {
        return asDbSettings(parseDeployment(deploymentURL));
    }

    public final static DbSettings asDbSettings(DeploymentType depl) {
        return new DbSettings(depl);
    }

    /**
     * Set cluster info in the catalog.
     * @param leader The leader hostname
     * @param catalog The catalog to be updated.
     * @param printLog Whether or not to print cluster configuration.
     */
    private static void setClusterInfo(Catalog catalog, DeploymentType deployment) {
        ClusterType cluster = deployment.getCluster();
        int kFactor = cluster.getKfactor();

        Cluster catCluster = getCluster(catalog);
        // copy the deployment info that is currently not recorded anywhere else
        Deployment catDeploy = catCluster.getDeployment().get("deployment");
        catDeploy.setKfactor(kFactor);
        if (deployment.getPartitionDetection().isEnabled()) {
            catCluster.setNetworkpartition(true);
        }
        else {
            catCluster.setNetworkpartition(false);
        }

        setSystemSettings(deployment, catDeploy);
        setThreadPools(deployment, catDeploy);

        catCluster.setHeartbeattimeout(deployment.getHeartbeat().getTimeout());
        catCluster.setGlobalflushinterval(deployment.getSystemsettings().getFlushinterval().getMinimum());
        if (catCluster.getGlobalflushinterval() <= 0) {
            throw new RuntimeException("SystemSettings FlushInterval Minimum must be greater than zero");
        }

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

    private static void setSystemSettings(DeploymentType deployment,
                                          Deployment catDeployment)
    {
        // Create catalog Systemsettings
        Systemsettings syssettings = catDeployment.getSystemsettings().get("systemsettings");
        if (syssettings == null) {
            syssettings = catDeployment.getSystemsettings().add("systemsettings");
        }

        syssettings.setTemptablemaxsize(deployment.getSystemsettings().getTemptables().getMaxsize());
        syssettings.setSnapshotpriority(deployment.getSystemsettings().getSnapshot().getPriority());
        syssettings.setElasticduration(deployment.getSystemsettings().getElastic().getDuration());
        syssettings.setElasticthroughput(deployment.getSystemsettings().getElastic().getThroughput());
        syssettings.setQuerytimeout(deployment.getSystemsettings().getQuery().getTimeout());

        // Set the priorities
        Priorities priorities = syssettings.getPriorities().get("priorities");
        if (priorities == null) {
            priorities = syssettings.getPriorities().add("priorities");
        }
        PriorityPolicyType pp = PriorityPolicy.getPolicyFromDeployment(deployment);
        if (pp.isEnabled()) {
            priorities.setEnabled(true);
            priorities.setMaxwait(pp.getMaxwait());
            priorities.setBatchsize(pp.getBatchsize());
            priorities.setDrpriority(pp.getDr().getPriority());
            priorities.setSnapshotpriority(pp.getSnapshot().getPriority());
        }
    }

    private static void setThreadPools(DeploymentType deployment, Deployment catDeployment) {
        // build the ThreadPool catalog from scratch.
        catDeployment.getThreadpools().clear();
        ThreadPoolsType threadPoolsType = deployment.getThreadpools();
        if (threadPoolsType == null) {
            return;
        }

        for (ThreadPoolsType.Pool threadPool : threadPoolsType.getPool()) {
            ThreadPool catalogThreadPool = catDeployment.getThreadpools().add(threadPool.getName());
            catalogThreadPool.setName(threadPool.getName());
            // TODO: thread pool size upper-bound?
            catalogThreadPool.setSize(threadPool.getSize());
        }
    }

    public static void validateDirectory(String type, File path) {
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
            case KAFKA: exportClientClassName = "org.voltdb.exportclient.kafka.KafkaExportClient"; break;
            case HTTP: exportClientClassName = "org.voltdb.exportclient.HttpExportClient"; break;
            case ELASTICSEARCH: exportClientClassName = "org.voltdb.exportclient.ElasticSearchHttpExportClient"; break;
            // Validate that we can load the class.
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
                    if (key.toLowerCase().contains("passw")) {
                        // Don't trim password
                        processorProperties.setProperty(key, value);
                    } else {
                        processorProperties.setProperty(key, value.trim());
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
            throw new DeploymentCheckException("Export is being used in wrong version of VoltDB software.");
        }
        ExportDataProcessor processor = null;
        try {
            processor = (ExportDataProcessor)processorClazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            hostLog.error("Unable to instantiate export processor", e);
            throw new DeploymentCheckException("Unable to instantiate export processor", e);
        }
        try {
            processorProperties.put(ExportManager.CONFIG_CHECK_ONLY, "true");
            processor.checkProcessorConfig(processorProperties);
            processor.shutdown();
        } catch (IllegalArgumentException iae) {
            // don't print a stack trace for expected IllegalArgumentExceptions
            hostLog.error("Export processor failed its configuration check: " + iae.getMessage());
            throw new DeploymentCheckException("Export processor failed its configuration check: " + iae.getMessage(), iae);
        } catch (Exception e) {
            hostLog.error("Export processor failed its configuration check", e);
            throw new DeploymentCheckException("Export processor failed its configuration check: " + e.getMessage(), e);
        }

        processorProperties.remove(ExportManager.CONFIG_CHECK_ONLY);


        return processorProperties;
    }

    public static class ImportConfiguration {
        private final int m_priority;
        private final Properties m_moduleProps;
        private final FormatterBuilder m_formatterBuilder;

        public ImportConfiguration(String formatName, int priority, Properties moduleProps, Properties formatterProps) {
            m_priority = priority;
            m_moduleProps = moduleProps;
            m_formatterBuilder = new FormatterBuilder(formatName, formatterProps);

            //map procedures and formatters to topics for kafka 10
            String importBundleJar = m_moduleProps.getProperty(ImportDataProcessor.IMPORT_MODULE);
            if (importBundleJar.indexOf("kafkastream10") > -1) {
                String topics = moduleProps.getProperty("topics");
                if (!StringUtil.isEmpty(topics)) {
                    String procedure = moduleProps.getProperty("procedure");
                    List<String> topicList = Arrays.asList(topics.split("\\s*,\\s*"));
                    if (!topicList.isEmpty()) {
                        Map<String, String> procedures = Maps.newHashMap();
                        Map<String, FormatterBuilder> formatters = Maps.newHashMap();
                        m_moduleProps.put(ImportDataProcessor.KAFKA10_PROCEDURES, procedures);
                        m_moduleProps.put(ImportDataProcessor.KAFKA10_FORMATTERS, formatters);
                        RealVoltDB db = (RealVoltDB)VoltDB.instance();
                        m_moduleProps.setProperty(ImportDataProcessor.VOLTDB_HOST_COUNT,
                                Integer.toString(db.getHostCount()));

                        for (String topic : topicList) {
                            if (procedure != null && !procedure.trim().isEmpty()) {
                                procedures.put(topic, procedure);
                                formatters.put(topic, m_formatterBuilder);
                            }
                        }
                    }
                }
            }
        }

        public int getPriority() {
            return m_priority;
        }

        public Properties getmoduleProperties() {
            return m_moduleProps;
        }

        public Properties getformatterProperties(){
            return m_formatterBuilder.getFormatterProperties();
        }

        public void setFormatterFactory(AbstractFormatterFactory formatterFactory) {
            m_formatterBuilder.setFormatterFactory(formatterFactory);
        }

        public FormatterBuilder getFormatterBuilder() {
            return m_formatterBuilder;
        }

        @SuppressWarnings("unchecked")
        public  Map<String, FormatterBuilder> getFormatterBuilders() {
            Map<String, FormatterBuilder> builders = (Map<String, FormatterBuilder>)m_moduleProps.get(
                                ImportDataProcessor.KAFKA10_FORMATTERS);
            if (builders == null) {
                builders = Maps.newHashMap();
                String importBundleJar = m_moduleProps.getProperty(ImportDataProcessor.IMPORT_MODULE);
                builders.put(importBundleJar, m_formatterBuilder);
            }
            return builders;
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_priority, m_moduleProps, m_formatterBuilder);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof ImportConfiguration)) {
                return false;
            }
            ImportConfiguration other = (ImportConfiguration) o;
            if (this.m_priority != other.m_priority) {
                return false;
            }
            return m_moduleProps.equals(other.m_moduleProps);
        }

        //merge Kafka 10 importer configurations: store formatters and stored procedures by brokers and group
        //into the properties. Also merge the topics list.
        @SuppressWarnings("unchecked")
        public void mergeProperties(Properties props) {
            Map<String, String> procedures = (Map<String, String>)
                    m_moduleProps.get(ImportDataProcessor.KAFKA10_PROCEDURES);
            Map<String, String> newProcedures = (Map<String, String>) props.get(ImportDataProcessor.KAFKA10_PROCEDURES);
            procedures.putAll(newProcedures);

            Map<String, FormatterBuilder> formatters =
                    (Map<String, FormatterBuilder>) m_moduleProps.get(ImportDataProcessor.KAFKA10_FORMATTERS);
            Map<String, FormatterBuilder> newFormatters =
                    (Map<String, FormatterBuilder>) props.get(ImportDataProcessor.KAFKA10_FORMATTERS);
            formatters.putAll(newFormatters);

            //merge topics
            String topics = m_moduleProps.getProperty("topics") + "," + props.getProperty("topics");
            m_moduleProps.put("topics", topics);
            hostLog.info("merging Kafka importer properties, topics:" + m_moduleProps.getProperty("topics"));
        }

        @SuppressWarnings("unchecked")
        public boolean checkProcedures(CatalogContext catalogContext, VoltLogger importLog, String configName) {
            String procedure = m_moduleProps.getProperty(ImportDataProcessor.IMPORT_PROCEDURE);
            if (procedure == null) {
                importLog.info("Importer " + configName + " has no procedures. The importer will be disabled.");
                return false;
            }
            Procedure catProc = catalogContext.procedures.get(procedure);
            if (catProc == null) {
                catProc = catalogContext.m_defaultProcs.checkForDefaultProcedure(procedure);
            }
            String msg = "Importer " + configName + " procedure %s is missing. will disable this importer " +
                    "until the procedure becomes available.";
            if( catProc == null) {
                importLog.info(String.format(msg, procedure));
                return false;
            }
            Map<String, String> procedures = (Map<String, String>)
                    m_moduleProps.get(ImportDataProcessor.KAFKA10_PROCEDURES);
            if (procedures == null) {
                return true;
            }

            for (String pr : procedures.values()) {
                catProc = catalogContext.procedures.get(pr);
                if (catProc == null) {
                    catProc = catalogContext.m_defaultProcs.checkForDefaultProcedure(pr);
                }
                if( catProc == null) {
                    importLog.info(String.format(msg,  procedure));
                    return false;
                }
            }

            return true;
        }
    }

    private static String buildBundleURL(String bundle, boolean alwaysBundle) {
        String modulePrefix = "osgi|";
        InputStream is;
        try {
            //Make sure we can load stream
            is = (new URL(bundle)).openStream();
        } catch (Exception ex) {
            is = null;
        }
        String bundleUrl = bundle;
        if (is == null) {
            try {
                String bundlelocation = System.getProperty(VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME);
                if (bundlelocation == null || bundlelocation.trim().length() == 0) {
                    String rpath = CatalogUtil.class.getProtectionDomain().getCodeSource().
                            getLocation().toURI().getPath();
                    hostLog.info("Module base is: " + rpath + "/../bundles/");
                    String bpath = (new File(rpath)).getParent() + "/../bundles/" + bundleUrl;
                    is = new FileInputStream(new File(bpath));
                    bundleUrl = "file:" + bpath;
                } else {
                    String bpath = bundlelocation + "/" + bundleUrl;
                    is = new FileInputStream(new File(bpath));
                    bundleUrl = "file:" + bpath;
                }
            } catch (URISyntaxException | FileNotFoundException ex) {
                is = null;
            }
        }

        if (is != null) {
            try {
                is.close();
            } catch (IOException ex) {
            }
        } else if (!alwaysBundle) {
            //Not a URL try as a class
            try {
                CatalogUtil.class.getClassLoader().loadClass(bundleUrl);
                modulePrefix = "class|";
            }
            catch (ClassNotFoundException ex2) {
                String msg =
                        "Import failed to configure, failed to load module by URL or classname provided" +
                        " import module: " + bundleUrl;
                hostLog.error(msg);
                throw new DeploymentCheckException(msg);
            }
        } else {
            String msg =
                    "Import failed to configure, failed to load module by URL or classname provided" +
                    " format module: " + bundleUrl;
            hostLog.error(msg);
            throw new DeploymentCheckException(msg);
        }
        return (alwaysBundle ? bundleUrl : modulePrefix + bundleUrl);
    }

    /**
     * Build Importer configuration optionally log deprecation or any other messages.
     * @param importConfiguration deployment configuration.
     * @param validation if we are validating configuration log any deprecation messages. This
     *        avoids double logging of deprecated or any other messages we would introduce in here.
     * @return
     */
    private static ImportConfiguration buildImportProcessorConfiguration(ImportConfigurationType importConfiguration,
            boolean validation) {
        String importBundleUrl = importConfiguration.getModule();

        if (!importConfiguration.isEnabled()) {
            return null;
        }
        switch(importConfiguration.getType()) {
            case CUSTOM:
                break;
            case KAFKA:
                String version = importConfiguration.getVersion().trim();
                if ("8".equals(version)) {
                    if (validation) {
                        hostLog.warn("Kafka importer version 0.8 has been deprecated.");
                    }
                    importBundleUrl = "kafkastream.jar";
                } else if ("10".equals(version)) {
                    importBundleUrl = "kafkastream10.jar";
                } else {
                    throw new DeploymentCheckException("Kafka " + version + " is not supported.");
                }
                break;
            case KINESIS:
                importBundleUrl = "kinesisstream.jar";
                break;
            default:
                throw new DeploymentCheckException("Import Configuration type must be specified.");
        }

        Properties moduleProps = new Properties();
        Properties formatterProps = new Properties();

        String formatBundle = importConfiguration.getFormat();
        String formatName = null;
        if (formatBundle != null && formatBundle.trim().length() > 0) {
            if ("csv".equalsIgnoreCase(formatBundle) || "tsv".equalsIgnoreCase(formatBundle)) {
                formatName = formatBundle;
                formatBundle = "voltcsvformatter.jar";
            } else if (JAR_EXTENSION_RE.matcher(formatBundle).matches()) {
                int typeIndex = formatBundle.lastIndexOf("/");
                formatName = formatBundle.substring(typeIndex + 1);
                formatBundle = formatBundle.substring(0, typeIndex);
            } else {
                throw new DeploymentCheckException("Import format " + formatBundle + " not valid.");
            }
            formatterProps.setProperty(ImportDataProcessor.IMPORT_FORMATTER, buildBundleURL(formatBundle, true));
        }

        if (importBundleUrl != null && importBundleUrl.trim().length() > 0) {
            moduleProps.setProperty(ImportDataProcessor.IMPORT_MODULE, buildBundleURL(importBundleUrl, false));
        }

        List<PropertyType> importProperties = importConfiguration.getProperty();
        if (importProperties != null && ! importProperties.isEmpty()) {
            for( PropertyType prop: importProperties) {
                String key = prop.getName();
                String value = prop.getValue();
                if (!key.toLowerCase().contains("passw")) {
                    moduleProps.setProperty(key, value.trim());
                } else {
                    //Don't trim passwords
                    moduleProps.setProperty(key, value);
                }
            }
        }

        List<PropertyType> formatProperties = importConfiguration.getFormatProperty();
        if (formatProperties != null && ! formatProperties.isEmpty()) {
            for( PropertyType prop: formatProperties) {
                formatterProps.setProperty(prop.getName(), prop.getValue());
            }
        }

        return new ImportConfiguration(formatName, importConfiguration.getPriority(), moduleProps, formatterProps);
    }

    /**
     * Set deployment time settings for export
     * @param exportType A reference to the <exports> element of the deployment.xml file.
     * @param catalog The catalog to be updated.
     * @param threadPoolsType
     */
    private static void setExportInfo(Catalog catalog, ExportType exportType, ThreadPoolsType threadPoolsType) {
        final Cluster cluster = getCluster(catalog);
        Database db = cluster.getDatabases().get("database");
        if (DrRoleType.XDCR.value().equals(cluster.getDrrole())) {
            // add default export configuration to DR conflict table
            String retention = cluster.getConflictretention();
            exportType = addExportConfigToDRConflictsTable(exportType, retention);
        }

        if (exportType == null) {
            return;
        }

        Set<String> targetSet = new HashSet<>();

        for (ExportConfigurationType exportConfiguration : exportType.getConfiguration()) {

            boolean connectorEnabled = exportConfiguration.isEnabled();
            // target name is case insensitive
            String targetName = exportConfiguration.getTarget().toUpperCase();
            if (connectorEnabled) {
                m_exportEnabled = true;
                if (!targetSet.add(targetName)) {
                    throw new RuntimeException("Multiple connectors can not be assigned to single export target: " +
                            targetName + ".");
                }
            }

            Properties processorProperties = checkExportProcessorConfiguration(exportConfiguration);

            // Check if the thread pool name set in the export target configuration exists
            getConfiguredThreadPoolSize(exportConfiguration, threadPoolsType);
            org.voltdb.catalog.Connector catconn = db.getConnectors().getIgnoreCase(targetName);

            if (catconn == null) {
                if (connectorEnabled) {
                    hostLog.info("Export configuration enabled and provided for export target " + targetName
                            + " in deployment file however no export tables are assigned to this target. "
                            + "Export target " + targetName + " will be disabled.");
                }
                continue;
            }

            // checking rowLengthLimit
            int rowLengthLimit = Integer.parseInt(processorProperties.getProperty(ROW_LENGTH_LIMIT,"0"));
            if (rowLengthLimit > 0) {
                for (ConnectorTableInfo catTableinfo : catconn.getTableinfo()) {
                    Table tableref = catTableinfo.getTable();
                    int rowLength = Boolean.parseBoolean(processorProperties.getProperty("skipinternals", "false")) ?
                            0 : EXPORT_INTERNAL_FIELD_Length;
                    for (Column catColumn: tableref.getColumns()) {
                        rowLength += catColumn.getSize();
                    }
                    if (rowLength > rowLengthLimit) {
                        hostLog.error("Export configuration for export target " + targetName + " has" +
                                "configured to has row length limit " + rowLengthLimit +
                                ". But the export table " + tableref.getTypeName() +
                                " has estimated row length " + rowLength +
                                ".");
                        throw new RuntimeException("Export table " + tableref.getTypeName() +
                                " row length is " + rowLength + ", exceeding configurated limitation " +
                                rowLengthLimit + ".");
                    }
                }
            }


            for (String name: processorProperties.stringPropertyNames()) {
                ConnectorProperty prop = catconn.getConfig().get(name);
                if (prop == null) {
                    prop = catconn.getConfig().add(name);
                }
                prop.setName(name);
                prop.setValue(processorProperties.getProperty(name));
            }

            // on-server export always uses the guest processor
            catconn.setLoaderclass(ExportManager.PROCESSOR_CLASS);
            catconn.setEnabled(connectorEnabled);
            catconn.setThreadpoolname(exportConfiguration.getThreadpool());

            if (!connectorEnabled) {
                hostLog.info("Export configuration for export target " + targetName + " is present and is " +
                             "configured to be disabled. Export target " + targetName + " will be disabled.");
            } else {
                hostLog.info("Export target " + targetName + " is configured and enabled with type=" +
                        exportConfiguration.getType());
                if (exportConfiguration.getProperty() != null) {
                    hostLog.info("Export target " + targetName + " configuration properties are: ");
                    for (PropertyType configProp : exportConfiguration.getProperty()) {
                        if (!configProp.getName().toLowerCase().contains("password")) {
                            hostLog.info("Export Configuration Property NAME=" + configProp.getName() +
                                    " VALUE=" + configProp.getValue());
                        }
                    }
                }
            }
        }
    }

    /**
     * Set deployment time settings for import
     * @param catalog The catalog to be updated.
     * @param importType A reference to the <exports> element of the deployment.xml file.
     */
    private static void setImportInfo(Catalog catalog, ImportType importType) {
        if (importType == null) {
            return;
        }
        List<ImportConfigurationType> kafkaConfigs = new ArrayList<>();
        for (ImportConfigurationType importConfiguration : importType.getConfiguration()) {

            boolean connectorEnabled = importConfiguration.isEnabled();
            if (!connectorEnabled) {
                continue;
            }

            if (importConfiguration.getType().equals(ServerImportEnum.KAFKA)) {
                kafkaConfigs.add(importConfiguration);
            }
            buildImportProcessorConfiguration(importConfiguration, true);
        }
        validateKafkaConfig(kafkaConfigs);
    }

    /**
     * Check whether two Kafka configurations have both the same topic and group id. If two configurations
     * have the same group id and overlapping sets of topics, a RuntimeException will be thrown.
     * @param configs All parsed Kafka configurations
     */
    private static void validateKafkaConfig(List<ImportConfigurationType> configs) {
        if (configs.isEmpty()) {
            return;
        }
        // We associate each group id with the set of topics that belong to it
        HashMap<String, HashSet<String>> groupidToTopics = new HashMap<>();
        for (ImportConfigurationType config : configs) {
            String groupid = "";
            HashSet<String> topics = new HashSet<>();
            // Fetch topics and group id from each configuration
            for (PropertyType pt : config.getProperty()) {
                if (pt.getName().equals("topics")) {
                    topics.addAll(Arrays.asList(pt.getValue().split("\\s*,\\s*")));
                } else if (pt.getName().equals("groupid")) {
                    groupid = pt.getValue();
                }
            }
            if (groupidToTopics.containsKey(groupid)) {
                // Under this group id, we first union the set of already-stored topics with the set
                // of newly-seen topics.
                HashSet<String> union = new HashSet<>(groupidToTopics.get(groupid));
                union.addAll(topics);
                if (union.size() == (topics.size() + groupidToTopics.get(groupid).size())) {
                    groupidToTopics.put(groupid, union);
                } else {
                    // If the size of the union doesn't equal to the sum of sizes of newly-seen topic set and
                    // already-stored topic set, those two sets must overlap with each other, which means that
                    // there must be two configurations having the same group id and overlapping sets of topics.
                    // Thus, we throw the RuntimeException.
                    throw new RuntimeException("Invalid import configuration. Two Kafka entries have the " +
                            "same groupid and topic.");
                }
            } else {
                groupidToTopics.put(groupid, topics);
            }
        }
    }

    /**
     * Set {@code Topic} instances in catalog, and connect their streams or tables.
     * <p>
     * Historically topics were created by a CREATE TOPIC DDL command, which
     * has been replaced by deployment file specification. However, inline encoding
     * requires sending {@code Topic} instances to the EE.
     * <p>
     * Note: on @UpdateClasses this is called with deployment already compiled in the
     * catalog. Overwrite the existing objects (FIXME: not efficient).
     * <p>
     * Note: STREAM objects are kept 'connector-less' in order to defer creating
     * {@link E3ExportDataSource} instances until they are associated with a topic.
     * This is because STREAM topics can be stored encoded (topic.store.encoded = true)
     * and it is not possible to change this property on the fly. This behavior does not
     * apply to tables since tables exporting to topic cannot be stored encoded.
     *
     * @param catalog {@code Catalog} being built
     * @param topicsType {@code TopicsType} from deployment
     */
    private static void setTopics(Catalog catalog, TopicsType topicsType) {
        final Cluster cluster = getCluster(catalog);
        Database db = cluster.getDatabases().get("database");

        if (topicsType == null) {
            return;
        }

        // Build a map of streams/tables referring to topics - multiple streams/tables to same topic is caught here,
        // not in validation. Since catalog rejects homonyms differing by case, perform case-insensitive checks.
        Map<String, Table> topicStreams = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Table table : db.getTables()) {
            if (!StringUtils.isBlank(table.getTopicname())) {
                Table curTable = topicStreams.put(table.getTopicname(), table);
                if (curTable != null) {
                    throw new RuntimeException(String.format("Streams %s and %s refer to the same topic %s",
                            curTable.getTypeName(), table.getTypeName(), table.getTopicname()));
                }
            }
        }

        // Detect topic duplicates here since we have an update logic below: note case-insensitive check
        HashSet<String> topics = new HashSet<>();

        // Create topics, connecting them to streams or tables as we go
        CatalogMap<Topic> cataTopics = db.getTopics();
        cataTopics.clear();
        for (TopicType topic : topicsType.getTopic()) {
            if (!topics.add(topic.getName().toUpperCase())) {
                throw new RuntimeException(String.format("Topic %s conflicts with other homonym topics in deployment", topic.getName()));
            }
            Topic cataTopic = cataTopics.add(topic.getName());
            String procName = topic.getProcedure();
            if (!StringUtils.isBlank(procName)) {
                cataTopic.setProcedurename(procName);
                cataTopic.setPriority(topic.getPriority());
            }
            if (topic.isOpaque()) {
                /*
                 * The opaque topic will be partitioned only if the OPAQUE_PARTITIONED property
                 * is true.
                 */
                cataTopic.setIsopaque(true);
                cataTopic.setIssingle(true);
            }
            String roles = topic.getAllow();
            if (!StringUtils.isBlank(roles)) {
                cataTopic.setRoles(roles);
            }
            TopicRetention retention = TopicRetention.parse(topic.getRetention());
            retention.toTopic(cataTopic);

            /*
             * Copy the properties: note special processing for single-partition opaque
             * topics, caching the "single" property in the catalog object. This was required
             * when transitioning from DDL to deployment. It was decided to make the "single"
             * a property but existing code relied heavily on Topic.getIssingle().
             */
            CatalogMap<Property> cataProps = cataTopic.getProperties();
            for (PropertyType prop : topic.getProperty()) {
                if (TopicProperties.Key.OPAQUE_PARTITIONED.name().equals(prop.getName()) && cataTopic.getIsopaque()) {
                    cataTopic.setIssingle(!Boolean.parseBoolean(prop.getValue()));
                }
                cataProps.add(prop.getName()).setValue(prop.getValue());
            }

            // Historical: topic format is a property
            String formatName = topic.getFormat();
            if (!StringUtils.isBlank(formatName)) {
                cataProps.add(TopicProperties.Key.TOPIC_FORMAT.name()).setValue(formatName);
            }

            // Connect stream/tables to topic
            String streamName = null;
            Table table = topicStreams.get(cataTopic.getTypeName());
            if (table != null) {
                streamName = table.getTypeName();

                // On DDL update, streams exporting to topics are set to connectorless.
                // If stream exports to topic, change stream type: this will enable creating
                // the data sources for this STREAM.
                //
                // Note that on LOAD CLASSES the table type is already set
                if (TableType.isConnectorLessStream(table.getTabletype())) {
                    table.setTabletype(TableType.STREAM.get());
                }

                // Transfer stream column info to topic as properties (historical from previous topic DDL)
                String keyColumns = table.getTopickeycolumnnames();
                if (!StringUtils.isBlank(keyColumns)) {
                    cataProps.add(TopicProperties.Key.CONSUMER_KEY.name()).setValue(keyColumns);
                }
                String valColumns = table.getTopicvaluecolumnnames();
                if (!StringUtils.isBlank(valColumns)) {
                    cataProps.add(TopicProperties.Key.CONSUMER_VALUE.name()).setValue(valColumns);
                }
            }
            else if (topic.isOpaque()) {
                // Streams are automatically created for opaque topics
                streamName = topic.getName().toUpperCase();
            }
            cataTopic.setStreamname(streamName);
        }

        // Handle the cases where the topic does not exist in deployment:
        // - Any STREAM referring to it must be converted to connectorless in order to drop
        //   the data sources.
        // - Reject any persistent TABLE referring to a non-existent topic.
        Set<String> orphanTables = new HashSet<>();
        for (Table table : topicStreams.values()) {
            int tableType = table.getTabletype();
            if (cataTopics.get(table.getTopicname()) != null || TableType.isConnectorLessStream(tableType)) {
                continue;
            }

            if (TableType.isStream(tableType)) {
                table.setTabletype(TableType.CONNECTOR_LESS_STREAM.get());
            }
            else {
                orphanTables.add(table.getTypeName());
            }
        }

        if (!orphanTables.isEmpty()) {
            throw new RuntimeException(String.format(
                    "The following tables export to topics that are missing in deployment: %s", orphanTables));
        }
    }

    /**
     * Validate Snmp Configuration.
     * @param snmpType
     */
    private static void setSnmpInfo(SnmpType snmpType) {
        if (snmpType == null || !snmpType.isEnabled()) {
            return;
        }
        //Validate Snmp Configuration.
        if (snmpType.getTarget() == null || snmpType.getTarget().trim().length() == 0) {
            throw new IllegalArgumentException("Target must be specified for SNMP configuration.");
        }
        if (snmpType.getAuthkey() != null && snmpType.getAuthkey().length() < 8) {
            throw new IllegalArgumentException("SNMP Authkey must be > 8 characters.");
        }
        if (snmpType.getPrivacykey() != null && snmpType.getPrivacykey().length() < 8) {
            throw new IllegalArgumentException("SNMP Privacy Key must be > 8 characters.");
        }
    }

    public static Map<String, ImportConfiguration> getImportProcessorConfig(ImportType importType) {
        Map<String, ImportConfiguration> processorConfig = new HashMap<>();
        if (importType == null) {
            return processorConfig;
        }
        int i = 0;
        for (ImportConfigurationType importConfiguration : importType.getConfiguration()) {

            boolean connectorEnabled = importConfiguration.isEnabled();
            if (!connectorEnabled) {
                continue;
            }

            ImportConfiguration processorProperties = buildImportProcessorConfiguration(importConfiguration, false);

            processorConfig.put(importConfiguration.getModule() + i++, processorProperties);
        }
        mergeKafka10ImportConfigurations(processorConfig);
        return processorConfig;
    }

    /**
     * aggregate Kafka10 importer configurations.One importer per brokers and kafka group.
     * Formatters and stored procedures can vary by topics.
     */
    private static void mergeKafka10ImportConfigurations(Map<String, ImportConfiguration> processorConfig) {
        if (processorConfig.isEmpty()) {
            return;
        }

        Map<String, ImportConfiguration> kafka10ProcessorConfigs = new HashMap<>();
        Iterator<Map.Entry<String, ImportConfiguration>> iter = processorConfig.entrySet().iterator();
        while (iter.hasNext()) {
            String configName = iter.next().getKey();
            ImportConfiguration importConfig = processorConfig.get(configName);
            Properties properties = importConfig.getmoduleProperties();

            String importBundleJar = properties.getProperty(ImportDataProcessor.IMPORT_MODULE);
            Preconditions.checkNotNull(importBundleJar,
                    "Import source is undefined or custom import plugin class missing.");
            //handle special cases for kafka 10 and maybe late versions
            String[] bundleJar = importBundleJar.split("kafkastream");
            if (bundleJar.length > 1) {
                String version = bundleJar[1].substring(0, bundleJar[1].indexOf(".jar"));
                if (!version.isEmpty()) {
                    int versionNumber = Integer.parseInt(version);
                    if (versionNumber == 10) {
                        kafka10ProcessorConfigs.put(configName, importConfig);
                        iter.remove();
                    }
                }
            }
        }

        if (kafka10ProcessorConfigs.isEmpty()) {
            return;
        }

        Map<String, ImportConfiguration> mergedConfigs = new HashMap<>();
        iter = kafka10ProcessorConfigs.entrySet().iterator();
        while (iter.hasNext()) {
            ImportConfiguration importConfig = iter.next().getValue();
            Properties props = importConfig.getmoduleProperties();

            //organize the kafka10 importer by the broker list and group id
            //All importers must be configured by either broker list or zookeeper in the same group
            //otherwise, these importers can not be correctly merged.
            String brokers = props.getProperty("brokers");
            String groupid = props.getProperty("groupid", "voltdb");
            if (brokers == null) {
                brokers = props.getProperty("zookeeper");
            }
            String brokersGroup = brokers + "_" + groupid;
            ImportConfiguration config = mergedConfigs.get(brokersGroup);
            if (config == null) {
                mergedConfigs.put(brokersGroup, importConfig);
            } else {
                config.mergeProperties(props);
            }
        }
        processorConfig.putAll(mergedConfigs);
    }

    /**
     * Set the security setting in the catalog from the deployment file
     * @param catalog the catalog to be updated
     * @param security security element of the deployment xml
     */
    private static void setSecurityEnabled( Catalog catalog, SecurityType security) {
        Cluster cluster = getCluster(catalog);
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
        Database db = getDatabase(catalog);
        SnapshotSchedule schedule = db.getSnapshotschedule().get("default");
        if (schedule == null) {
            schedule = db.getSnapshotschedule().add("default");
        }
        schedule.setEnabled(snapshotSettings.isEnabled());

        int frequencyInt = 10;
        char frequencyUnit = 'm';
        String frequency = snapshotSettings.getFrequency();
        try {
            TimeUtils.TimeAndUnit tu = TimeUtils.parseTimeAndUnit(frequency, TimeUnit.SECONDS, null);
            frequencyInt = Math.toIntExact(tu.number);
            frequencyUnit = tu.flag;
        } catch (TimeUtils.InvalidTimeException | ArithmeticException ex) {
            hostLog.error("Snapshot frequency '" + frequency + "' is not valid. It should be" +
                          " an integer followed by one of [s, m, h] for seconds, minutes, hours." +
                          " Defaulting snapshot frequency to 10m.");
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

        schedule.setFrequencyunit(String.valueOf(frequencyUnit));
        schedule.setFrequencyvalue(frequencyInt);
        schedule.setPrefix(prefix);
        schedule.setRetain(retain);
    }

    /**
     * Set voltroot path, and set the path overrides for export overflow, partition, etc.
     * @param paths A reference to the <paths> element of the deployment.xml file.
     * @param printLog Whether or not to print paths info.
     */
    private static void setupPaths( PathsType paths) {
        File voltDbRoot;
        // Handles default voltdbroot (and completely missing "paths" element).
        voltDbRoot = getVoltDbRoot(paths);
        //Snapshot
        setupSnapshotPaths(paths.getSnapshots(), voltDbRoot);
        //export overflow
        setupExportOverflow(paths.getExportoverflow(), voltDbRoot);
        // only use these directories in the enterprise version
        setupCommandLog(paths.getCommandlog(), voltDbRoot);
        setupCommandLogSnapshot(paths.getCommandlogsnapshot(), voltDbRoot);
        setupDROverflow(paths.getDroverflow(), voltDbRoot);
        setupLargeQuerySwap(paths.getLargequeryswap(), voltDbRoot);
        setupTopicData(voltDbRoot);
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
        if (paths == null || paths.getVoltdbroot() == null ||
                VoltDB.instance().getVoltDBRootPath(paths.getVoltdbroot()) == null) {
            voltDbRoot = new File(VoltDB.DBROOT);
            if (!voltDbRoot.exists()) {
                hostLog.info("Creating voltdbroot directory: " + voltDbRoot.getAbsolutePath());
                if (!voltDbRoot.mkdirs()) {
                    hostLog.fatal("Failed to create voltdbroot directory \"" + voltDbRoot.getAbsolutePath() + "\"");
                }
            }
        } else {
            voltDbRoot = new File(VoltDB.instance().getVoltDBRootPath(paths.getVoltdbroot()));
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

    public static void setupSnapshotPaths(PathsType.Snapshots paths, File voltDbRoot) {
        File snapshotPath;
        snapshotPath = new File(VoltDB.instance().getSnapshotPath(paths));
        if (!snapshotPath.isAbsolute())
        {
            snapshotPath = new File(voltDbRoot, VoltDB.instance().getSnapshotPath(paths));
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
    }

    public static void setupCommandLog(PathsType.Commandlog paths, File voltDbRoot) {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            // dumb defaults if you ask for logging in community version
            return;
        }
        File commandlogPath;
        commandlogPath = new File(VoltDB.instance().getCommandLogPath(paths));
        if (!commandlogPath.isAbsolute())
        {
            commandlogPath = new File(voltDbRoot, VoltDB.instance().getCommandLogPath(paths));
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
    }

    public static void setupCommandLogSnapshot(PathsType.Commandlogsnapshot paths, File voltDbRoot) {
        if (!VoltDB.instance().getConfig().m_isEnterprise) {
            // dumb defaults if you ask for logging in community version
            new File(voltDbRoot, "command_log_snapshot"); // TODO: this does nothing?
            return;
        }

        File commandlogSnapshotPath;
        commandlogSnapshotPath = new File(VoltDB.instance().getCommandLogSnapshotPath(paths));
        if (!commandlogSnapshotPath.isAbsolute())
        {
            commandlogSnapshotPath = new File(voltDbRoot, VoltDB.instance().getCommandLogSnapshotPath(paths));
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
    }

    public static void setupExportOverflow(PathsType.Exportoverflow paths, File voltDbRoot) {
        File exportOverflowPath;
        exportOverflowPath = new File(VoltDB.instance().getExportOverflowPath(paths));
        if (!exportOverflowPath.isAbsolute())
        {
            exportOverflowPath = new File(voltDbRoot, VoltDB.instance().getExportOverflowPath(paths));
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
    }

    public static File setupDROverflow(PathsType.Droverflow paths, File voltDbRoot) {
        File drOverflowPath;
        drOverflowPath = new File(VoltDB.instance().getDROverflowPath(paths));
        if (!drOverflowPath.isAbsolute())
        {
            drOverflowPath = new File(voltDbRoot, VoltDB.instance().getDROverflowPath(paths));
        }

        if (!drOverflowPath.exists()) {
            hostLog.info("Creating DR overflow directory: " +
                         drOverflowPath.getAbsolutePath());
            if (!drOverflowPath.mkdirs()) {
                hostLog.fatal("Failed to create DR overflow path directory \"" +
                              drOverflowPath + "\"");
            }
        }
        validateDirectory("DR overflow", drOverflowPath);
        return drOverflowPath;

    }

    public static File setupLargeQuerySwap(PathsType.Largequeryswap paths, File voltDbRoot) {
        File largeQuerySwap;
        largeQuerySwap = new File(VoltDB.instance().getLargeQuerySwapPath(paths));
        if (!largeQuerySwap.isAbsolute())
        {
            largeQuerySwap = new File(voltDbRoot, VoltDB.instance().getLargeQuerySwapPath(paths));
        }

        if (!largeQuerySwap.exists()) {
            hostLog.info("Creating large query swap directory: " +
                         largeQuerySwap.getAbsolutePath());
            if (!largeQuerySwap.mkdirs()) {
                hostLog.fatal("Failed to create large query swap directory \"" +
                              largeQuerySwap + "\"");
            }
        }
        validateDirectory("large query swap", largeQuerySwap);
        return largeQuerySwap;
    }

    public static File setupTopicData(File voltDbRoot) {
        File topicsDataDir = VoltDB.instance().getTopicsDataPath();
        if (!topicsDataDir.isAbsolute()) {
            topicsDataDir = new File(voltDbRoot, topicsDataDir.getPath());
        }
        if (!topicsDataDir.exists()) {
            hostLog.info("Creating topics data directory: " + topicsDataDir.getAbsolutePath());
            if (!topicsDataDir.mkdirs()) {
                hostLog.fatal("Failed to create topics data directory \"" + topicsDataDir + "\"");
            }
        }
        validateDirectory("topics data", topicsDataDir);
        return topicsDataDir;
    }

    /**
     * Set user info in the catalog.
     * @param catalog The catalog to be updated.
     * @param users A reference to the <users> element of the deployment.xml file.
     * @throws RuntimeException when there is an user with invalid masked password.
     */
    private static void setUsersInfo(Catalog catalog, UsersType users) throws RuntimeException {
        if (users == null) {
            return;
        }

        // The database name is not available in deployment.xml (it is defined
        // in project.xml). However, it must always be named "database", so
        // I've temporarily hardcoded it here until a more robust solution is
        // available.
        Database db = getDatabase(catalog);

        SecureRandom sr = new SecureRandom();

        for (UsersType.User user : users.getUser()) {
            Set<String> roles = extractUserRoles(user);
            String sha1hex = user.getPassword();
            String sha256hex = user.getPassword();
            if (user.isPlaintext()) {
                sha1hex = extractPassword(user.getPassword(), ClientAuthScheme.HASH_SHA1);
                sha256hex = extractPassword(user.getPassword(), ClientAuthScheme.HASH_SHA256);
            } else if (user.getPassword().length() == 104) {
                int sha1len = ClientAuthScheme.getHexencodedDigestLength(ClientAuthScheme.HASH_SHA1);
                sha1hex = sha1hex.substring(0, sha1len);
                sha256hex = sha256hex.substring(sha1len);
            } else {
                // if one user has invalid password, give a warn.
                hostLog.warn("User \"" + user.getName() + "\" has invalid masked password in deployment file.");
                // throw exception disable user with invalid masked password
                throw new RuntimeException("User \"" + user.getName() +
                        "\" has invalid masked password in deployment file");
            }
            org.voltdb.catalog.User catUser = db.getUsers().get(user.getName());
            if (catUser == null) {
                catUser = db.getUsers().add(user.getName());
            }

            // generate salt only once for sha1 and sha256
            String saltGen = BCrypt.gensalt(BCrypt.GENSALT_DEFAULT_LOG2_ROUNDS,sr);
            String hashedPW =
                    BCrypt.hashpw(
                            sha1hex,
                            saltGen);
            String hashedPW256 =
                    BCrypt.hashpw(
                            sha256hex,
                            saltGen);
            catUser.setShadowpassword(hashedPW);
            catUser.setSha256shadowpassword(hashedPW256);
            //use fixed seed for comparison
            catUser.setPassword( BCrypt.hashpw(sha256hex, "$2a$10$pWO/a/OQkFyQWQDpchZdEe"));
            // process the @groups and @roles comma separated list
            for (final String role : roles) {
                final Group catalogGroup = db.getGroups().get(role);
                // if the role doesn't exist, ignore it.
                if (catalogGroup != null) {
                    GroupRef groupRef = catUser.getGroups().get(role);
                    if (groupRef == null) {
                        groupRef = catUser.getGroups().add(role);
                    }
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
        Set<String> roles = new TreeSet<>();
        if (user == null) {
            return roles;
        }

        if (user.getRoles() != null && !user.getRoles().trim().isEmpty()) {
            String [] rolelist = user.getRoles().trim().split(",");
            for (String role: rolelist) {
                if( role == null || role.trim().isEmpty()) {
                    continue;
                }
                roles.add(role.trim().toLowerCase());
            }
        }

        return roles;
    }

    private static void setHTTPDInfo(Catalog catalog, HttpdType httpd, SslType ssl) {
        Cluster cluster = getCluster(catalog);

        // set the catalog info
        int defaultPort = VoltDB.DEFAULT_HTTP_PORT;
        if (ssl !=null && ssl.isEnabled()) {
            defaultPort = VoltDB.DEFAULT_HTTPS_PORT;
        }
        cluster.setHttpdportno(httpd.getPort()==null ? defaultPort : httpd.getPort());
        cluster.setJsonapi(httpd.getJsonapi().isEnabled());
    }

    private static void setDrInfo(Catalog catalog, DrType dr, ClusterType clusterType,
            int drFlushInterval, boolean isPlaceHolderCatalog) {
        int clusterId;
        Cluster cluster = getCluster(catalog);
        final Database db = cluster.getDatabases().get("database");
        assert cluster != null;
        if (dr != null) {
            ConnectionType drConnection = dr.getConnection();
            cluster.setDrproducerenabled(dr.isListen());
            cluster.setDrproducerport(dr.getPort());
            cluster.setDrrole(dr.getRole().name().toLowerCase());
            if (dr.getRole() == DrRoleType.XDCR) {
                // Setting this for compatibility mode only, don't use in new code
                db.setIsactiveactivedred(true);
            }

            // Backward compatibility to support cluster id in DR tag
            if (clusterType.getId() == null && dr.getId() != null) {
                clusterId = dr.getId();
            } else if (clusterType.getId() != null && dr.getId() == null) {
                clusterId = clusterType.getId();
            } else if (clusterType.getId() == null && dr.getId() == null) {
                clusterId = 0;
            } else if (clusterType.getId().equals(dr.getId())) {
                clusterId = clusterType.getId();
            } else {
                throw new RuntimeException("Detected two conflicting cluster ids in deployment file, "
                        + "setting cluster id in DR tag is deprecated, please remove");
            }
            cluster.setDrflushinterval(drFlushInterval);
            if (drConnection != null) {
                String drSource = drConnection.getSource();
                cluster.setDrmasterhost(drSource);
                String sslPropertyFile = drConnection.getSsl();
                cluster.setDrconsumersslpropertyfile(sslPropertyFile);
                cluster.setDrconsumerenabled(drConnection.isEnabled());
                if (drConnection.getPreferredSource() != null) {
                    cluster.setPreferredsource(drConnection.getPreferredSource());
                } else { // reset to -1, if this is an update catalog
                    cluster.setPreferredsource(-1);
                }
                String drConsumerSSLInfo = "";
                if (sslPropertyFile != null) {
                    if (sslPropertyFile.trim().isEmpty()) {
                        drConsumerSSLInfo = " with SSL enabled";
                    }
                    else {
                        drConsumerSSLInfo = " with SSL enabled using properties in " + sslPropertyFile;
                    }
                }
                hostLog.info("Configured connection for DR replica role to host " + drSource + drConsumerSSLInfo);
            } else if (dr.getRole() == DrRoleType.XDCR) {
                // consumer should be enabled even without connection source for XDCR
                cluster.setDrconsumerenabled(true);
                cluster.setPreferredsource(-1); // reset to -1, if this is an update catalog
            }

            // conflict retention for default xdcr conflicts export
            cluster.setConflictretention(checkDrRetention(dr));

        } else { // dr == null
            cluster.setDrrole(DrRoleType.NONE.value());
            if (clusterType.getId() != null) {
                clusterId = clusterType.getId();
            } else {
                clusterId = 0;
            }
        }
        cluster.setDrclusterid(clusterId);
    }

    public static String checkDrRetention(DrType dr) {
        String result = "";
        DrRoleType role = dr.getRole();
        String retention = dr.getConflictretention();
        if (role == DrRoleType.XDCR && retention != null && !retention.isEmpty()) {
            try {
                TimeUtils.TimeAndUnit tu = TimeUtils.parseTimeAndUnit(retention, TimeUnit.SECONDS, null);
                hostLog.infoFmt("Retention for XDCR conflict files set to %s %s", tu.number, tu.unit.name().toLowerCase());
                result = retention;
            } catch (TimeUtils.InvalidTimeException | ArithmeticException ex) {
                String mess = String.format("XDCR conflict retention '%s' is not valid. It should be an integer" +
                                           " followed by one of [s, m, h, d] for seconds, minutes, hours, days.",
                                            retention);
                throw new IllegalArgumentException(mess);
            }
        }
        return result;
    }

    /** Read a hashed password from password.
     *  SHA* hash it once to match what we will get from the wire protocol
     *  and then hex encode it
     * */
    private static String extractPassword(String password, ClientAuthScheme scheme) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(ClientAuthScheme.getDigestScheme(scheme));
        } catch (final NoSuchAlgorithmException e) {
            hostLog.fatal("Password hashing algorithm not found", e);
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
    public static byte[] makeHash(byte[] inbytes)
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

    private static void makeCatalogHeader(
                SegmentedCatalog catalogAndDeployment,
                int version,
                long genId)
    {
        // Skip empty catalog
        if (catalogAndDeployment.m_data == null || catalogAndDeployment.m_data.isEmpty()) {
            return;
        }
        ByteBuffer buf = catalogAndDeployment.m_data.get(0);
        buf.position(0);
        buf.putInt(version);
        buf.putLong(genId);
        buf.putInt(catalogAndDeployment.m_deploymentLength);
        buf.putInt(catalogAndDeployment.m_catalogLength);
        buf.put(catalogAndDeployment.m_catalogHash);
        buf.position(0);
    }

    /**
     *  Attempt to create the ZK node and write the catalog/deployment bytes
     *  to ZK.  Used during the initial cluster deployment discovery and
     *  distribution.
     */
    public static void writeDeploymentToZK(ZooKeeper zk,
                                           long genId,
                                           SegmentedCatalog catalogAndDeployment,
                                           ZKCatalogStatus catalogStatus,
                                           long txnId)
        throws KeeperException, InterruptedException
    {
        // use default version 0 as start
        makeCatalogHeader(catalogAndDeployment, 0, genId);
        String path = VoltZK.catalogbytes + "/" + 0;
        ByteBuffer buffer = ByteBuffer.allocate(13);// flag (byte:1) + expected data node count (int:4) + txnId (long:8)
        buffer.put((byte)ZKCatalogStatus.PENDING.ordinal());
        buffer.putInt(catalogAndDeployment.m_data.size());
        buffer.putLong(txnId);
        zk.create(path, buffer.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        for (ByteBuffer chunk : catalogAndDeployment.m_data) {
            zk.create(ZKUtil.joinZKPath(path, "part_"), chunk.array(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
        }
        if (catalogStatus == ZKCatalogStatus.COMPLETE) {
            // mark the version node complete if requested
            buffer.position(0);
            buffer.put((byte)catalogStatus.ordinal());
            zk.setData(path, buffer.array(), -1);
        }
    }

    /**
     * Update the catalog/deployment contained in ZK. If the catalog/deployment is larger than
     * single ZK node size limit, break the object into multiple ZK nodes under the version
     * directory.
     */
    public static void updateCatalogToZK(ZooKeeper zk,
                                        int version,
                                        long genId,
                                        SegmentedCatalog catalogAndDeployment,
                                        ZKCatalogStatus catalogStatus,
                                        long txnId)
        throws KeeperException, InterruptedException
    {
        assert (catalogAndDeployment != null);

        makeCatalogHeader(catalogAndDeployment, version, genId);
        String path = VoltZK.catalogbytes + "/" + version;
        ByteBuffer buffer = ByteBuffer.allocate(13);// flag (byte:1) + expected node count (int:4) + txnId (long:8)
        buffer.put((byte)ZKCatalogStatus.PENDING.ordinal());
        buffer.putInt(catalogAndDeployment.m_data.size());
        buffer.putLong(txnId);
        Stat stat = null;
        try {
            zk.create(path, buffer.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // mark the version as incomplete
            stat = zk.exists(path, false);
            stat = zk.setData(path, buffer.array(), stat.getVersion());
            // Clear children nodes if version dir already exists
            List<String> children = zk.getChildren(path, false);
            for (String child : children) {
                ZKUtil.deleteRecursively(zk, ZKUtil.joinZKPath(path, child));
            }
        }
        for (ByteBuffer chunk : catalogAndDeployment.m_data) {
            zk.create(ZKUtil.joinZKPath(path, "part_"), chunk.array(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
        }

        if (catalogStatus == ZKCatalogStatus.COMPLETE) {
            // mark the version node complete if requested
            buffer.position(0);
            buffer.put((byte)catalogStatus.ordinal());
            zk.setData(path, buffer.array(), stat == null ? -1 : stat.getVersion());

            // delete all previous versions
            List<String> children = zk.getChildren(VoltZK.catalogbytes, false);
            for (String child : children) {
                int priorVersion = Integer.parseInt(child);
                if (priorVersion < version) {
                    ZKUtil.deleteRecursively(zk, ZKUtil.joinZKPath(VoltZK.catalogbytes, child));
                }
            }
        }
    }

    /**
     * Change the state of given ZK node that represents catalog version from PENDING to COMPLETE, delete
     * ZK nodes that those catalog version are lower than the given version.
     * No-op if the state has been COMPLETE already.
     * @param zk
     * @param version
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void publishCatalog(ZooKeeper zk, int version) throws KeeperException, InterruptedException {
        String path = VoltZK.catalogbytes + "/" + version;
        Stat stat = new Stat();
        byte[] data = zk.getData(path, false, stat);
        ByteBuffer buffer = ByteBuffer.wrap(data);// flag (byte) + expected catalog segment count (int) + txnId (long)
        byte status = buffer.get();
        if (status == (byte)ZKCatalogStatus.PENDING.ordinal()) {
            int catalogSegmentCount = buffer.getInt();
            // Rewrite the flag
            buffer.position(0);
            buffer.put((byte)ZKCatalogStatus.COMPLETE.ordinal());
            buffer.putInt(catalogSegmentCount);
            zk.setData(path, buffer.array(), stat.getVersion());

            // delete all previous versions
            List<String> children = zk.getChildren(VoltZK.catalogbytes, false);
            for (String child : children) {
                int priorVersion = Integer.parseInt(child);
                if (priorVersion < version) {
                    ZKUtil.deleteRecursively(zk, ZKUtil.joinZKPath(VoltZK.catalogbytes, child));
                }
            }
        }
    }

    /**
     * Change the state of given ZK node that represents catalog version from COMPLETE to PENDING
     * @param zk
     * @param version
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void unPublishCatalog(ZooKeeper zk, int version) throws KeeperException, InterruptedException {
        String path = VoltZK.catalogbytes + "/" + version;
        Stat stat = new Stat();
        byte[] data = zk.getData(path, false, stat);
        ByteBuffer buffer = ByteBuffer.wrap(data);// flag (byte) + expected catalog segment count (int) + txnId (long)
        byte status = buffer.get();
        if (status == (byte)ZKCatalogStatus.COMPLETE.ordinal()) {
            int catalogSegmentCount = buffer.getInt();
            // Rewrite the flag
            buffer.position(0);
            buffer.put((byte)ZKCatalogStatus.PENDING.ordinal());
            buffer.putInt(catalogSegmentCount);
            zk.setData(path, buffer.array(), stat.getVersion());
        }
    }
    public static void stageCatalogToZK(ZooKeeper zk, int version, long genId, long txnId, SegmentedCatalog catalogAndDeployment)
            throws KeeperException, InterruptedException {
        updateCatalogToZK(zk, version, genId, catalogAndDeployment, ZKCatalogStatus.PENDING, txnId);
    }

    public static class SegmentedCatalog {
        public final List<ByteBuffer> m_data;
        public final int m_deploymentLength;
        public final int m_catalogLength;
        public final byte[] m_catalogHash;

        public SegmentedCatalog(List<ByteBuffer> data, int deploymentSize, int catalogSize, byte[] catalogHash) {
            m_data = data;
            m_deploymentLength = deploymentSize;
            m_catalogLength = catalogSize;
            m_catalogHash = catalogHash;

        }

        public static SegmentedCatalog create(byte[] catalogBytes, byte[] catalogHash, byte[] deploymentBytes) {
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
            // Reserve the header space for first buffer
            int chunkOffset = CATALOG_BUFFER_HEADER;
            int totalBytes = catalogBytes.length + deploymentBytes.length + CATALOG_BUFFER_HEADER;
            int bufferSize = Math.min(MAX_CATALOG_CHUNK_SIZE, totalBytes);
            byte[] buffer = new byte[bufferSize];
            buffers.add(ByteBuffer.wrap(buffer));
            // Copy deployment bytes into first buffer (deployment file is small enough to fit in a buffer)
            System.arraycopy(deploymentBytes, 0, buffer, chunkOffset, deploymentBytes.length);
            chunkOffset += deploymentBytes.length;

            // Wrap catalog bytes into ByteBuffers
            int remaining = catalogBytes.length;
            int catalogOffset = 0;
            while (remaining > 0) {
                // The first buffer is tricky to fill
                int copyLength = Math.min(MAX_CATALOG_CHUNK_SIZE - chunkOffset, remaining);
                System.arraycopy(catalogBytes, catalogOffset, buffer, chunkOffset, copyLength);
                catalogOffset += copyLength;
                chunkOffset += copyLength;
                remaining -= copyLength;
                if (remaining > 0) {
                    bufferSize = Math.min(MAX_CATALOG_CHUNK_SIZE, remaining);
                    buffer = new byte[bufferSize];
                    buffers.add(ByteBuffer.wrap(buffer));
                    chunkOffset = 0;
                }
            }

            return new SegmentedCatalog(buffers, deploymentBytes.length, catalogBytes.length, catalogHash);
        }
    }

    public static class CatalogAndDeployment {
        public final int version;
        public final long genId;
        public final byte[] catalogBytes;
        public final byte[] catalogHash;
        public final byte[] deploymentBytes;

        public CatalogAndDeployment(
                int version,
                long genId,
                byte[] catalogBytes,
                byte[] catalogHash,
                byte[] deploymentBytes)
        {
            this.version = version;
            this.genId = genId;
            this.catalogBytes = catalogBytes;
            this.catalogHash = catalogHash;
            this.deploymentBytes = deploymentBytes;
        }

        @Override
        public String toString()
        {
            return String.format("catalog version %d, catalog hash %s, deployment hash %s",
                                version,
                                Encoder.hexEncode(catalogHash).substring(0, 10),
                                Encoder.hexEncode(deploymentBytes).substring(0, 10));
        }
    }

    /**
     * Find the latest catalog version stored under /db/catalogbytes ZK directory, the qualified version's
     * catalog status must meet the value specified by {@code expectedFlag}. If {@code expectedFlag} is
     * {@code null}, the highest catalog version stored in ZK will be returned, no matter what the status is.
     * @param zk
     * @param expectedFlag  {@code Complete}, {@code Pending} or {@code null}
     * @param expectedTxnId Ignore the check if {@code expectedTxnId} is -1
     * @return catalog version
     * @throws KeeperException
     * @throws InterruptedException
     */
    private static Pair<String, Integer> findLatestViableCatalog( ZooKeeper zk,
                                                                  ZKCatalogStatus expectedFlag,
                                                                  long expectedTxnId)
            throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(VoltZK.catalogbytes, false);
        // sort in natural order
        Collections.sort(children, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.valueOf(o1) - Integer.valueOf(o2);
            }
        });
        // find the latest version node that meets given requirements
        for (int i = children.size() - 1; i >= 0; i--) {
            String catalogPath = ZKUtil.joinZKPath(VoltZK.catalogbytes, children.get(i));
            byte[] data = zk.getData(catalogPath, false, null);
            ByteBuffer buffer = ByteBuffer.wrap(data);
            byte flag = buffer.get();
            if (expectedFlag != null && flag != (byte)expectedFlag.ordinal()) {
                continue;
            }
            int catalogSegmentCount = buffer.getInt();
            long txnId = buffer.getLong();
            // In command log replay, only one node uploads the staging catalog bytes to ZK,
            // rest of nodes use expectedTxnId to match the data.
            if (expectedTxnId != -1 && txnId != expectedTxnId) {
                continue;
            }
            return new Pair<>(catalogPath, catalogSegmentCount);
        }
        return null;
    }

    private static CatalogAndDeployment readCatalogData(ZooKeeper zk, String catalogPath, int expectedCount)
            throws KeeperException, InterruptedException {
        int deploymentBytesRead = 0;
        int catalogBytesRead = 0;
        byte[] catalogBytes = null;
        byte[] deploymentBytes = null;
        // header fields
        int version = 0;
        long genId = 0L;
        int deploymentLength = 0;
        int catalogLength = 0;
        byte[] catalogHash = new byte[20]; // sha-1 hash size

        List<String> catalogNodes = zk.getChildren(catalogPath, false);
        // updateCatalogToZK may delete the zk nodes while load catalog
        // spin loop reading it during initialization.
        if (catalogNodes.isEmpty()) {
            hostLog.info("No catalog data nodes found for path " + catalogPath);
            return null;
        }
        Collections.sort(catalogNodes);
        if (expectedCount != catalogNodes.size()) {
            hostLog.info("For " + catalogPath + " expected " + expectedCount + " catalog data nodes. Found " + catalogNodes.size());
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("expected zk catalog node count: " + expectedCount +
                        ", actual: " + catalogNodes.size());
            }
            return null;
        }

        for (int j = 0; j < catalogNodes.size(); j++) {
            ByteBuffer catalogDeploymentBytes =
                    ByteBuffer.wrap(zk.getData(ZKUtil.joinZKPath(catalogPath, catalogNodes.get(j)), false, null));
            if (j == 0) {
                // first node, get the meta data.
                version = catalogDeploymentBytes.getInt();
                genId = catalogDeploymentBytes.getLong();
                deploymentLength = catalogDeploymentBytes.getInt();
                catalogLength = catalogDeploymentBytes.getInt();
                catalogDeploymentBytes.get(catalogHash);
                deploymentBytes = new byte[deploymentLength];
                catalogBytes = new byte[catalogLength];
                // Get deployment bytes first
                deploymentBytesRead =
                        readBuffer(catalogDeploymentBytes, deploymentBytes, deploymentBytesRead, deploymentLength - deploymentBytesRead);
                // If there is still space remaining...get the catalog
                if (catalogDeploymentBytes.hasRemaining()) {
                    catalogBytesRead =
                            readBuffer(catalogDeploymentBytes, catalogBytes, catalogBytesRead, catalogLength - catalogBytesRead);
                }
                catalogDeploymentBytes = null;
                continue;
            }
            // Rest of nodes, get remaining data.
            if (deploymentBytesRead < deploymentLength) {
                deploymentBytesRead +=
                        readBuffer(catalogDeploymentBytes, deploymentBytes, deploymentBytesRead, deploymentLength - deploymentBytesRead);
            }
            if (catalogDeploymentBytes.hasRemaining() && catalogBytesRead < catalogLength) {
                catalogBytesRead += readBuffer(catalogDeploymentBytes, catalogBytes, catalogBytesRead, catalogLength - catalogBytesRead);
            }
            catalogDeploymentBytes = null;
        }
        assert (deploymentBytesRead == deploymentLength && catalogBytesRead == catalogLength);
        return new CatalogAndDeployment(version, genId, catalogBytes, catalogHash, deploymentBytes);
    }

    /**
     * Retrieve the catalog and deployment configuration from zookeeper.
     * NOTE: In general, people who want the catalog and/or deployment should
     * be getting it from the current CatalogContext, available from
     * VoltDB.instance().  This is primarily for startup and for use by
     * @UpdateCore.
     */
    public static CatalogAndDeployment getCatalogFromZK(ZooKeeper zk)
            throws KeeperException, InterruptedException {

        // read the catalog from given version
        Pair<String, Integer> latestCatalog = findLatestViableCatalog(zk, ZKCatalogStatus.COMPLETE, -1);
        if (latestCatalog == null) {
            return null;
        }
        return readCatalogData(zk, latestCatalog.getFirst(), latestCatalog.getSecond());
    }

    /**
     * Retrieve an unpublished catalog and deployment configuration from zookeeper.
     * This is primarily used by @VerifyCatalogAndWriteJar and @UpdateCore to pass
     * large bytes through zookeeper network.
     * @param zk
     * @param catalogVersion
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static CatalogAndDeployment getStagingCatalogFromZK(ZooKeeper zk, int catalogVersion)
            throws KeeperException, InterruptedException {
        String catalogPath = VoltZK.catalogbytes + "/" + catalogVersion;
        byte[] data = null;
        try {
            data = zk.getData(catalogPath, false, null);
        } catch (KeeperException.NoNodeException e) {
            hostLog.info("NoNodeException trying to get staging catalog for version " + catalogVersion);
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte flag = buffer.get();
        if (flag != (byte)ZKUtil.ZKCatalogStatus.PENDING.ordinal()) {
            hostLog.info("Flag != PENDING trying to get staging catalog for version " + catalogVersion);
            return null;
        }
        int catalogSegmentCount = buffer.getInt();
        return readCatalogData(zk, catalogPath, catalogSegmentCount);
    }

    private static int readBuffer(ByteBuffer buffer, byte[] src, int offset, int length) {
        int toRead = Math.min(length, buffer.remaining());
        buffer.get(src, offset, toRead);
        return toRead;
    }

    public static void copyUACParameters(StoredProcedureInvocation invocation, Object[] params) {
        CatalogAndDeployment cad = null;
        try {
            cad = CatalogUtil.getStagingCatalogFromZK(VoltDB.instance().getHostMessenger().getZK(), (int) params[2]);
        } catch (KeeperException | InterruptedException e) {
            VoltDB.crashLocalVoltDB("Fail to get catalog bytes while processing catalog update", true, null);
        }
        invocation.setParams(params[0],             // diff commands
                             params[1],             // expected catalog version
                             params[2],             // next catalog version
                             params[3],             // gen id
                             cad.catalogBytes,
                             params[4],             // catalog hash
                             cad.deploymentBytes,
                             params[5],             // deployment hash
                             params[6],             // work with elastic
                             params[7],             // tables must be empty
                             params[8],             // reasons for empty tables
                             params[9],             // requiresSnapshotIsolation
                             params[10],             // requireCatalogDiffCmdsApplyToEE
                             params[11],            // hasSchemaChange
                             params[12],            // requiresNewExportGeneration
                             params[13]);           // hasSecurityUserChange
    }

    public static int getZKCatalogVersion(ZooKeeper zk, long txnId) {
        Pair<String, Integer> catalogPair = null;
        try {
            catalogPair = findLatestViableCatalog(zk, null, txnId);
        } catch (KeeperException | InterruptedException ignore) {
            VoltDB.crashLocalVoltDB("Unable to retrieve ZK catalog node", false, null);
        }
        assert (catalogPair != null);
        // Split the version number part from /db/catalogbytes/<version>
        String version = catalogPair.getFirst().substring(catalogPair.getFirst().lastIndexOf('/') + 1);
        return Integer.parseInt(version);
    }

    /**
     * Given plan graphs and a SQL statement, compute a bidirectional usage map between
     * schema (indexes, table & views) and SQL/Procedures.
     * Use "annotation" objects to store this extra information in the catalog
     * during compilation and catalog report generation.
     */
    public static void updateUsageAnnotations(Database db,
                                              Statement stmt,
                                              AbstractPlanNode topPlan,
                                              AbstractPlanNode bottomPlan)
    {
        Map<String, StmtTargetTableScan> tablesRead = new TreeMap<>();
        Collection<String> indexes = new TreeSet<>();
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
     * Create a {@link Database} instance from {@code jarFile}
     *
     * @param jarFile which has a catalog file
     * @return {@link Database} generated from {@code jarFile}
     */
    public static Database getDatabaseFrom(InMemoryJarfile jarFile) {
        Catalog catalog = new Catalog();
        catalog.execute(getSerializedCatalogStringFromJar(jarFile));
        return getDatabase(catalog);
    }

    /**
     * Iterate through all the tables in the catalog, find a table with an id that matches the
     * given table id, and return its name.
     *
     * @param catalog  Catalog database
     * @param tableId  table id
     * @return table name associated with the given table id (null if no association is found)
     */
    public static Table getTableObjectNameFromId(Database catalog, int tableId) {
        for (Table table: catalog.getTables()) {
            if (table.getRelativeIndex() == tableId) {
                return table;
            }
        }
        return null;
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
     * Return if given proc is durable if its a sysproc SystemProcedureCatalog is consulted.
     * All non sys procs are all durable.
     *
     * @param procName
     * @return true if proc is durable for non sys procs return true (durable)
     */
    public static boolean isDurableProc(String procName) {
        SystemProcedureCatalog.Config sysProc = SystemProcedureCatalog.listing.get(procName);
        return sysProc == null || sysProc.isDurable();
    }

    /**
     * Build an empty catalog jar file.
     * @return jar file or null (on failure)
     * @throws IOException on failure to create temporary jar file
     * @param isXDCR
     */
    public static File createTemporaryEmptyCatalogJarFile(boolean isXDCR) throws IOException {
        File emptyJarFile = File.createTempFile("catalog-empty", ".jar");
        emptyJarFile.deleteOnExit();
        VoltCompiler compiler = new VoltCompiler(isXDCR);
        if (!compiler.compileEmptyCatalog(emptyJarFile.getAbsolutePath())) {
            return null;
        }
        return emptyJarFile;
    }

    /**
     * Get a string signature for the table represented by the args
     * @param name The name of the table
     * @param schema A sorted map of the columns in the table, keyed by column index
     * @return The table signature string.
     */
    public static String getSignatureForTable(String name, SortedMap<Integer, VoltType> schema) {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(SIGNATURE_TABLE_NAME_SEPARATOR);
        for (VoltType t : schema.values()) {
            sb.append(t.getSignatureChar());
        }
        return sb.toString();
    }

    /**
     * Deserializes a catalog DR table signature string into a map of table signatures.
     * @param signature    The signature string that includes multiple DR table signatures
     * @return A map of signatures from table names to table signatures.
     */
    public static Map<String, String> deserializeCatalogSignature(String signature) {
        Map<String, String> tableSignatures = Maps.newHashMap();
        for (String oneSig : signature.split(Pattern.quote(SIGNATURE_DELIMITER))) {
            if (!oneSig.isEmpty()) {
                final String[] parts = oneSig.split(Pattern.quote(SIGNATURE_TABLE_NAME_SEPARATOR), 2);
                tableSignatures.put(parts[0], parts[1]);
            }
        }
        return tableSignatures;
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

    /**
     * Add default configuration to DR conflicts export target if deployment file doesn't have the configuration
     *
     * @param export   list of export configuration
     * @param retention   conflict retention string
     */
    public static ExportType addExportConfigToDRConflictsTable(ExportType export, String retention) {
        if (export == null) {
            export = new ExportType();
        }
        boolean userDefineStream = false;
        for (ExportConfigurationType exportConfiguration : export.getConfiguration()) {
            if (exportConfiguration.getTarget().equals(DR_CONFLICTS_TABLE_EXPORT_GROUP)) {
                userDefineStream = true;
            }
        }

        if (!userDefineStream) {
            ExportConfigurationType defaultConfiguration = new ExportConfigurationType();
            defaultConfiguration.setEnabled(true);
            defaultConfiguration.setTarget(DR_CONFLICTS_TABLE_EXPORT_GROUP);
            defaultConfiguration.setType(ServerExportEnum.FILE);

            setExportProperty(defaultConfiguration, "type", DEFAULT_DR_CONFLICTS_EXPORT_TYPE);
            setExportProperty(defaultConfiguration, "nonce", DEFAULT_DR_CONFLICTS_NONCE);
            setExportProperty(defaultConfiguration, "outdir", DEFAULT_DR_CONFLICTS_DIR);
            setExportProperty(defaultConfiguration, "replicated", "true");
            setExportProperty(defaultConfiguration, "skipinternals", "true");

            if (retention != null && !retention.isEmpty()) {
                long retentionSecs = TimeUtils.convertTimeAndUnit(retention, TimeUnit.SECONDS, null);
                long period = 60; // same as export default
                if (retentionSecs <= 60) { // short retention needs shorter period
                    period = (retentionSecs + 1) / 2;
                }
                setExportProperty(defaultConfiguration, "retention", retention);
                setExportProperty(defaultConfiguration, "period", period + "s");
            }

            export.getConfiguration().add(defaultConfiguration);
        }
        return export;
    }

    private static void setExportProperty(ExportConfigurationType config, String name, String value) {
        PropertyType prop = new PropertyType();
        prop.setName(name);
        prop.setValue(value);
        config.getProperty().add(prop);
    }

    /*
     * Given an index return its expressions or list of indexed columns
     *
     * @param index  Catalog Index
     * @param tableScan table
     * @param indexedExprs   index expressions. This list remains empty if the index is just on simple columns.
     * @param indexedColRefs indexed columns. This list remains empty if indexedExprs is in use.
     * @return true if this is a column based index
     */
    public static boolean getCatalogIndexExpressions(Index index, StmtTableScan tableScan,
            List<AbstractExpression> indexedExprs, List<ColumnRef> indexedColRefs) {
        String exprsjson = index.getExpressionsjson();
        if (exprsjson.isEmpty()) {
            CatalogUtil.getSortedCatalogItems(index.getColumns(), "index", indexedColRefs);
        } else {
            try {
                AbstractExpression.fromJSONArrayString(exprsjson, tableScan, indexedExprs);
            } catch (JSONException e) {
                e.printStackTrace();
                assert(false);
            }
        }
        return exprsjson.isEmpty();
    }

    public static Map<String, Column> getDRTableNamePartitionColumnMapping(Database db) {
        Map<String, Column> res = new HashMap<>();
        for (Table tb : db.getTables()) {
            if (!tb.getIsreplicated() && tb.getIsdred()) {
                res.put(tb.getTypeName(), tb.getPartitioncolumn());
            }
        }
        return res;
    }

    /**
     * Creates a shallow clone of {@link DeploymentType} where all its
     * children references are copied except for {@link ClusterType}, and
     * {@link PathsType} which are newly instantiated
     * @param o
     * @return a shallow clone of {@link DeploymentType}
     */
    public static DeploymentType shallowClusterAndPathsClone(DeploymentType o) {
        DeploymentType clone = new DeploymentType();

        ClusterType other = o.getCluster();
        ClusterType cluster = new ClusterType();

        cluster.setHostcount(other.getHostcount());
        cluster.setSitesperhost(other.getSitesperhost());
        cluster.setKfactor(other.getKfactor());
        cluster.setId(other.getId());
        cluster.setSchema(other.getSchema());

        clone.setCluster(cluster);

        PathsType prev = o.getPaths();
        PathsType paths = new PathsType();

        paths.setVoltdbroot(prev.getVoltdbroot());
        paths.setSnapshots(prev.getSnapshots());
        paths.setExportoverflow(prev.getExportoverflow());
        paths.setDroverflow(prev.getDroverflow());
        paths.setCommandlog(prev.getCommandlog());
        paths.setCommandlogsnapshot(prev.getCommandlogsnapshot());
        paths.setLargequeryswap(prev.getLargequeryswap());

        clone.setPaths(paths);

        clone.setPartitionDetection(o.getPartitionDetection());
        clone.setHeartbeat(o.getHeartbeat());
        clone.setSsl(o.getSsl());
        clone.setHttpd(o.getHttpd());
        clone.setSnapshot(o.getSnapshot());
        clone.setExport(o.getExport());
        clone.setThreadpools(o.getThreadpools());
        clone.setUsers(o.getUsers());
        clone.setCommandlog(o.getCommandlog());
        clone.setSystemsettings(o.getSystemsettings());
        clone.setSecurity(o.getSecurity());
        clone.setDr(o.getDr());
        clone.setImport(o.getImport());
        clone.setSnmp(o.getSnmp());
        clone.setFeatures(o.getFeatures());
        clone.setTask(o.getTask());
        clone.setTopics(o.getTopics());
        clone.setAvro(o.getAvro());

        return clone;
    }

    /**
     * Get a deployment view that represents what needs to be displayed to VMC, which
     * reflects the paths that are used by this cluster member and the actual number of
     * hosts that belong to this cluster whether or not it was elastically expanded
     * @param deployment
     * @return adjusted deployment
     */
    public static DeploymentType updateRuntimeDeploymentPaths(DeploymentType deployment) {
        deployment = CatalogUtil.shallowClusterAndPathsClone(deployment);
        PathsType paths = deployment.getPaths();
        if (paths.getVoltdbroot() == null) {
            PathsType.Voltdbroot root = new PathsType.Voltdbroot();
            root.setPath(VoltDB.instance().getVoltDBRootPath());
            paths.setVoltdbroot(root);
        } else {
            paths.getVoltdbroot().setPath(VoltDB.instance().getVoltDBRootPath());
        }
        // Directly load path config from file
        NodeSettings pathSettings = NodeSettings.create(VoltDB.instance().getConfig().asRelativePathSettingsMap());
        //snapshot
        if (paths.getSnapshots() == null) {
            PathsType.Snapshots snap = new PathsType.Snapshots();
            snap.setPath(pathSettings.getSnapshot().toString());
            paths.setSnapshots(snap);
        } else {
            paths.getSnapshots().setPath(pathSettings.getSnapshot().toString());
        }
        if (paths.getCommandlog() == null) {
            //cl
            PathsType.Commandlog cl = new PathsType.Commandlog();
            cl.setPath(pathSettings.getCommandLog().toString());
            paths.setCommandlog(cl);
        } else {
            paths.getCommandlog().setPath(pathSettings.getCommandLog().toString());
        }
        if (paths.getCommandlogsnapshot() == null) {
            //cl snap
            PathsType.Commandlogsnapshot clsnap = new PathsType.Commandlogsnapshot();
            clsnap.setPath(pathSettings.getCommandLogSnapshot().toString());
            paths.setCommandlogsnapshot(clsnap);
        } else {
            paths.getCommandlogsnapshot().setPath(pathSettings.getCommandLogSnapshot().toString());
        }
        if (paths.getExportoverflow() == null) {
            //export overflow
            PathsType.Exportoverflow exp = new PathsType.Exportoverflow();
            exp.setPath(pathSettings.getExportOverflow().toString());
            paths.setExportoverflow(exp);
        } else {
            paths.getExportoverflow().setPath(pathSettings.getExportOverflow().toString());
        }
        if (paths.getDroverflow() == null) {
            //dr overflow
            final PathsType.Droverflow droverflow = new PathsType.Droverflow();
            droverflow.setPath(pathSettings.getDROverflow().toString());
            paths.setDroverflow(droverflow);
        } else {
            paths.getDroverflow().setPath(pathSettings.getDROverflow().toString());
        }
        if (paths.getLargequeryswap() == null) {
            //large query swap
            final PathsType.Largequeryswap largequeryswap = new PathsType.Largequeryswap();
            largequeryswap.setPath(pathSettings.getLargeQuerySwap().toString());
            paths.setLargequeryswap(largequeryswap);
        } else {
            paths.getLargequeryswap().setPath(pathSettings.getLargeQuerySwap().toString());
        }
        return deployment;
    }

    public static Procedure getProcedure(String procName) {
        return VoltDB.instance().getCatalogContext().database.getProcedures().get(procName);
    }

    public static void printUserProcedureDetailShort(Procedure proc, int[] leftHashes, int[] rightHashes, int misMatchPos, int leftHostId, int rightHostId, StringBuilder sb) {
        if ((leftHashes[0] == rightHashes[0]) || (misMatchPos < 0)) {
            return;
        }
        PureJavaCrc32C crc = new PureJavaCrc32C();
        Map<Integer, String> stateMap = new HashMap<>();
        for (Statement stmt : proc.getStatements()) {
            // compute hash for determinism check
            String sqlText = stmt.getSqltext();
            crc.reset();
            crc.update(sqlText.getBytes(Constants.UTF8ENCODING));
            int hash = (int) crc.getValue();
            stateMap.put(hash, sqlText);
        }

        int pos = 0;
        for (; pos < misMatchPos - (misMatchPos % 2); pos+=2) {
            sb.append("\t").append("SQL Statement ").append(pos / 2).append(" - ").append(MATCHED_STATEMENTS).append(": ").
                    append(stateMap.get(leftHashes[pos+DeterminismHash.HEADER_OFFSET])).append("\n");
        }
        if (misMatchPos % 2 == 0) { // mismatch statement
            sb.append("\t").append("SQL Statement ").append(pos / 2).append(" - ").append(MISMATCHED_STATEMENTS).append("Host ").append(leftHostId).append(": ").
                    append(stateMap.get(leftHashes[pos+DeterminismHash.HEADER_OFFSET])).append("\n");
            sb.append("\t").append("SQL Statement ").append(pos / 2).append(" - ").append(MISMATCHED_STATEMENTS).append("Host ").append(rightHostId).append(": ").
                    append(stateMap.get(rightHashes[pos+DeterminismHash.HEADER_OFFSET])).append("\n");
        } else {  // mismatch parameter
            sb.append("\t").append("SQL Statement ").append(pos / 2).append(" - ").append(MISMATCHED_PARAMETERS).append(": ").
                    append(stateMap.get(leftHashes[pos+DeterminismHash.HEADER_OFFSET])).append("\n");
        }
    }

    /*
     * Print procedure detail, such as statement text, frag id and json plan.
     *
     * @Param proc  Catalog procedure
     */
    public static String printUserProcedureDetail(Procedure proc) {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        StringBuilder sb = new StringBuilder();
        sb.append("Procedure:" + proc.getTypeName()).append("\n");
        for (Statement stmt : proc.getStatements()) {
            // compute hash for determinism check
            String sqlText = stmt.getSqltext();
            crc.reset();
            crc.update(sqlText.getBytes(Constants.UTF8ENCODING));
            int hash = (int) crc.getValue();
            sb.append("Statement Hash: ").append(hash);
            sb.append(", Statement SQL: ").append(sqlText);
            for (PlanFragment frag : stmt.getFragments()) {
                byte[] planHash = Encoder.hexDecode(frag.getPlanhash());
                long planId = ActivePlanRepository.getFragmentIdForPlanHash(planHash);
                String stmtText = ActivePlanRepository.getStmtTextForPlanHash(planHash);
                byte[] jsonPlan = ActivePlanRepository.planForFragmentId(planId);
                sb.append("Plan Stmt Text:").append(stmtText);
                sb.append(", Plan Fragment Id:").append(planId);
                sb.append(", Json Plan:").append(new String(jsonPlan));
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printCRUDProcedureDetail(Procedure proc, LoadedProcedureSet procSet) {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        StringBuilder sb = new StringBuilder();
        sb.append("Procedure:" + proc.getTypeName()).append("\n");
        String sqlText = DefaultProcedureManager.sqlForDefaultProc(proc);
        crc.reset();
        crc.update(sqlText.getBytes(Constants.UTF8ENCODING));
        int hash = (int) crc.getValue();
        sb.append("Statement Hash: ").append(hash);
        sb.append(", Statement SQL: ").append(sqlText);
        ProcedureRunner runner = procSet.getProcByName(proc.getTypeName());
        if (runner != null) {
            for (Statement stmt : runner.getCatalogProcedure().getStatements()) {
                for (PlanFragment frag : stmt.getFragments()) {
                    byte[] planHash = Encoder.hexDecode(frag.getPlanhash());
                    long planId = ActivePlanRepository.getFragmentIdForPlanHash(planHash);
                    String stmtText = ActivePlanRepository.getStmtTextForPlanHash(planHash);
                    byte[] jsonPlan = ActivePlanRepository.planForFragmentId(planId);
                    sb.append(", Plan Fragment Id:").append(planId);
                    sb.append(", Plan Stmt Text:").append(stmtText);
                    sb.append(", Json Plan:").append(new String(jsonPlan));
                }
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    /*
     * Check if the procedure is partitioned or not
     */
    public static boolean isProcedurePartitioned(Procedure proc) {
        return proc.getSinglepartition() || proc.getPartitioncolumn2() != null;
    }

    /**
     * Return if this a compound procedure, i.e. a Java non-transactional, non-system, non-default procedure
     * <p>
     * See {@link org.voltdb.compiler.ProcedureCompiler.compileNTProcedure(VoltCompiler, Class<?>, Procedure, InMemoryJarfile)}
     *
     * @param proc {@link Procedure}
     * @return {@code true} if procedure is a compound procedure
     */
    public static boolean isCompoundProcedure(Procedure proc) {
        return !proc.getTransactional()
                && !proc.getSystemproc()
                && !proc.getDefaultproc()
                && proc.getHasjava();
    }

    public static Map<String, Table> getTimeToLiveTables(Database db) {
        Map<String, Table> ttls = Maps.newHashMap();
        for (Table t : db.getTables()) {
            if (t.getTimetolive() != null && t.getTimetolive().get(TimeToLiveVoltDB.TTL_NAME) != null) {
                ttls.put(t.getTypeName().toLowerCase(),t);
            }
        }
        return ttls;
    }

    public static boolean getIsreplicated(String tableName) {
        Table table = VoltDB.instance().getCatalogContext().tables.get(tableName);
        if (table != null) {
            return table.getIsreplicated();
        }
        return false;
    }

    public static boolean isPersistentMigrate(String tableName) {
        Table table = VoltDB.instance().getCatalogContext().tables.get(tableName);
        if (table != null) {
            return TableType.isPersistentMigrate(table.getTabletype());
        }
        return false;
    }

    public static boolean hasShadowStream(String tableName) {
        Table table = VoltDB.instance().getCatalogContext().tables.get(tableName);
        if (table != null) {
            return TableType.needsShadowStream(table.getTabletype());
        }
        return false;
    }

    /**
     * Utility method for converting a {@link Column} to a {@link VoltTable.ColumnInfo}
     *
     * @param column Source {@link Column}
     * @return {@code column} converted into a {@link VoltTable.ColumnInfo}
     */
    public static VoltTable.ColumnInfo catalogColumnToInfo(Column column) {
        return new VoltTable.ColumnInfo(column.getTypeName(), VoltType.get((byte) column.getType()));
    }

    /**
     * Check if the thread pool name set in the export target configuration exists
     * @param exportConfiguration
     * @param threadPoolsType
     */
    private static void getConfiguredThreadPoolSize(ExportConfigurationType exportConfiguration, ThreadPoolsType threadPoolsType) {
        if (threadPoolsType == null) {
            return;
        }
        String threadPoolName = exportConfiguration.getThreadpool();
        // check if the thread-pool-name exists in the deployment file <threadpools> section
        if (!StringUtil.isEmpty(threadPoolName)) {
            for (ThreadPoolsType.Pool threadPool : threadPoolsType.getPool()) {
                if (threadPool.getName().equals(threadPoolName)) {
                    return;
                }
            }

            String msg = String.format("Export target %s is configured to use a thread pool named %s, " +
                    "which does not exist in the configuration: the export target will be disabled",
                    exportConfiguration.getTarget(), threadPoolName);
            throw new DeploymentCheckException(msg);
        }
        return;
    }

    private static void validateThreadPoolsConfiguration(ThreadPoolsType threadPoolsType) {
        if (threadPoolsType == null) {
            return;
        }
        Set<String> nameSet = new HashSet<>();
        for (ThreadPoolsType.Pool threadPool : threadPoolsType.getPool()) {
            if (!nameSet.add(threadPool.getName())) {
                String msg = "Invalid thread pool configuration. Thread pool name: " + threadPool.getName() + " is not unique.";
                hostLog.error(msg);
                throw new DeploymentCheckException(msg);
            }
        }
    }

    public static Cluster getCluster(Catalog catalog) {
        return catalog.getClusters().get("cluster");
    }

    public static Database getDatabase(Catalog catalog) {
        return getCluster(catalog).getDatabases().get("database");
    }

    /**
     * Split {@code input} on {@code ,} trimming any leading or trailing white spaces
     *
     * @param input to be split
     * @return {@link Iterable} of {@link String}s from {@code input}
     */
    public static Iterable<String> splitOnCommas(String input) {
        return Splitter.on(',').trimResults().split(input);
    }

    /**
     * Test if this is an opaque topic for the partition, also checks for single partition topics
     *
     * @param topic {@link Topic} from catalog
     * @param partitionId the partition id
     * @return {@code true} if this is an opaque topic for the partition
     *      {@code false} if not opaque or nonzero partition for single partition topic
     */
    public static boolean isOpaqueTopicForPartition(Topic topic, int partitionId) {
        return topic.getIsopaque() && (!topic.getIssingle() || partitionId == 0);
    }
}
