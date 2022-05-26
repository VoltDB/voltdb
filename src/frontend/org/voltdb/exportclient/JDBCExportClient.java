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

package org.voltdb.exportclient;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManager;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.exportclient.ExportRow.ROW_OPERATION;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

import com.google_voltpatches.common.base.Predicates;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class JDBCExportClient extends ExportClientBase {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    private static final int LOG_RATE_LIMIT = 10; // seconds

    String schema_prefix;
    boolean ignoreGenerations = false;
    boolean skipInternals = false;
    boolean m_createTable = true;
    private int firstField = 0;
    private boolean m_lowercaseNames = false;
    PoolProperties m_poolProperties = new PoolProperties();
    URI m_urlId;
    static AtomicReference<Map<URI,RefCountedDS>> m_cpds =
            new AtomicReference<Map<URI,RefCountedDS>>(ImmutableMap.<URI,RefCountedDS>of());

    private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";
    private final static int SHUTDOWN_TIMEOUT_S = 60;

    private static enum DatabaseType {
        POSTGRES
        ,MYSQL
        ,ORACLE
        ,NETEZZA
        ,SQLSERVER
        ,TERADATA
        ,VERTICA
        ,VOLTDB
        ,UNRECOGNIZED;
    }
    private final Set<DatabaseType> supportsIfNotExists =
        ImmutableSet.<DatabaseType>builder()
        .add(DatabaseType.POSTGRES)
        .add(DatabaseType.MYSQL)
        .add(DatabaseType.NETEZZA)
        .add(DatabaseType.VERTICA)
        .build();

    static final class RefCountedDS {
        private final DataSource ds;
        private final int refCount;
        RefCountedDS(DataSource ds, int refCount) {
            if (ds == null) {
                throw new IllegalArgumentException("ds is null");
            }
            if (refCount < 0) {
                throw new IllegalArgumentException("refCount is less than 0");
            }
            this.ds = ds;
            this.refCount = refCount;
        }
        RefCountedDS increment() {
            return new RefCountedDS(ds, refCount+1);
        }
        RefCountedDS decrement() {
            return new RefCountedDS(ds,refCount-1);
        }
        DataSource getDataSource() {
            return ds;
        }
        int getRefCount() {
             return refCount;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((ds == null) ? 0 : ds.hashCode());
            result = prime * result + refCount;
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            RefCountedDS other = (RefCountedDS) obj;
            if (ds == null) {
                if (other.ds != null) {
                    return false;
                }
            } else if (!ds.equals(other.ds)) {
                return false;
            }
            if (refCount != other.refCount) {
                return false;
            }
            return true;
        }
        @Override
        public String toString() {
            return "RefCountedDS [ds=" + ds + ", refCount=" + refCount + "]";
        }
    }

    class JDBCDecoder extends ExportDecoderBase {

        //If the column value is longer than the limit, truncate the value to avoid flushing too much data to log.
        private static final int MAX_COLUMN_PRINT_SIZE = 1024;

        // Those 2 fields can be reset concurrently when a block timeout closes the connection
        private volatile Connection m_conn = null;
        private volatile PreparedStatement m_pstmt = null;

        private final ListeningExecutorService m_es;
        DatabaseType m_dbType = null;
        private String m_preparedStmtStr = null;
        private String m_createTableStr = null;
        private boolean m_supportsUpsert = false;
        private boolean m_supportsDuplicateKey = false;
        private boolean m_warnedOfUnsupportedOperation = false;
        private boolean m_supportsBatchUpdates;
        private boolean m_disableAutoCommits = true;
        private long m_curGenId;
        private ExportRowSchema m_curSchema;

        private final RefCountedDS m_ds;

        private final List<BatchRow> m_dataRows =  new ArrayList<>();
        private class BatchRow {
            private final ExportRow m_row;
            public BatchRow(ExportRow r) {
                m_row = r;
            }
        }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        public JDBCDecoder(AdvertisedDataSource source, RefCountedDS ds) {
            super(source);

            m_curGenId = -1;
            m_ds = ds;
            if (VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                m_es =
                        CoreUtils.getListeningSingleThreadExecutor(
                                "JDBC Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            } else {
                m_es = null;
            }
        }

        private String createTableString(DatabaseType dbType, String schemaAndTable, String identifierQuote,
                List<String> columnNames, List<Integer> columnLengths, List<VoltType> columnTypes) {
            int totalRowSize = 0;
            for (int ii = firstField; ii < columnLengths.size(); ii++) {
                totalRowSize += columnLengths.get(ii);
            }
            boolean jumboRow = totalRowSize > 64000;
            /*
             * Vertica supports 8 meg rows, so no total m_row size issues
             */
            if (dbType == DatabaseType.VERTICA) {
                jumboRow = false;
            }
            String tableNotExistsClause = " IF NOT EXISTS ";
            StringBuilder createTableQuery = new StringBuilder();
            createTableQuery.append("CREATE TABLE " +
                 (supportsIfNotExists.contains(dbType) ? tableNotExistsClause : "")
                 + schemaAndTable +
                 " (");

            for (int i = firstField; i < columnNames.size(); i++) {
                if (i != firstField) {
                    createTableQuery.append(", ");
                }

                VoltType type = columnTypes.get(i);
                final int columnLength = columnLengths.get(i);
                final String columnName = m_lowercaseNames ? columnNames.get(i).toLowerCase() : columnNames.get(i);

                final String quotedColumnName = identifierQuote + columnName +  identifierQuote;
                createTableQuery.append(quotedColumnName + " ");

                if (type == VoltType.TINYINT) {
                    switch (dbType) {
                    case POSTGRES:
                        createTableQuery.append("SMALLINT " +
                                "CONSTRAINT " + identifierQuote + columnName +
                                "_tinyint" + identifierQuote + " CHECK (-128 <= " +
                                quotedColumnName +
                                " AND " + quotedColumnName + " <= 127)");
                        break;
                    case ORACLE:
                        createTableQuery.append("NUMBER(3)");
                        break;
                    case NETEZZA:
                        createTableQuery.append("BYTEINT");
                        break;
                    case VERTICA:
                    case MYSQL:
                    case VOLTDB:
                        createTableQuery.append("TINYINT");
                        break;
                    case TERADATA:
                        createTableQuery.append("BYTEINT");
                        break;
                    default:
                        createTableQuery.append("SMALLINT");
                        break;
                    }
                } else if(columnTypes.get(i) == VoltType.TIMESTAMP) {
                    switch (dbType) {
                    case POSTGRES:
                    case ORACLE:
                    case VERTICA:
                        createTableQuery.append("TIMESTAMP WITH TIME ZONE");
                        break;
                    case SQLSERVER:
                        createTableQuery.append("DATETIMEOFFSET");
                        break;
                    default:
                        createTableQuery.append("TIMESTAMP");
                        break;
                    }
                } else if (columnTypes.get(i) == VoltType.STRING) {
                    appendStringColumn(dbType, createTableQuery, columnLength, jumboRow);
                } else if (columnTypes.get(i) == VoltType.DECIMAL) {
                    // Same deal as STRING, but currently DECIMAL's
                    // precision and scale cannot be changed, so
                    // it's not horrible to do it this way.
                    createTableQuery.append("DECIMAL(" +
                        VoltDecimalHelper.kDefaultPrecision +
                        "," + VoltDecimalHelper.kDefaultScale + ")");
                } else if (columnTypes.get(i) == VoltType.FLOAT) {
                    createTableQuery.append("DOUBLE PRECISION");
                } else if (columnTypes.get(i) == VoltType.VARBINARY) {
                    switch (dbType) {
                    case VERTICA:
                        if (columnLength > 65000) {
                            throw new RuntimeException("Vertica only supports VARBINARY up to 65000");
                        }
                        createTableQuery.append("VARBINARY(" + columnLength + ")");
                        break;
                    case POSTGRES:
                        createTableQuery.append("BYTEA");
                        break;
                    case MYSQL:
                        if (jumboRow) {
                            createTableQuery.append("MEDIUMBLOB");
                        } else {
                            createTableQuery.append("VARBINARY(" + columnLength + ")");
                        }
                        break;
                    case NETEZZA:
                        throw new RuntimeException("Netezza doesn't support a binary type");
                    case ORACLE:
                        createTableQuery.append("BLOB");
                        break;
                    case SQLSERVER:
                        if (columnLength < 8000) {
                            createTableQuery.append("VARBINARY(" + columnLength + ")");
                        } else {
                            createTableQuery.append("VARBINARY(max)");
                        }
                        break;
                    case TERADATA:
                        createTableQuery.append("VARBYTE(" + columnLength + ")");
                        break;
                    case VOLTDB:
                        createTableQuery.append("VARBINARY(" + columnLength + ")");
                        break;
                    default:
                        //Seems like most systems don't support VARBINARY beyond 64000, and there are m_row limits
                        //beyond that as well
                        if (jumboRow) {
                            createTableQuery.append("BLOB(" + columnLength + ")");
                        } else {
                            createTableQuery.append("VARBINARY(" + columnLength + ")");
                        }
                        break;
                    }
                } else if (columnTypes.get(i) == VoltType.GEOGRAPHY_POINT) {
                    appendStringColumn(dbType, createTableQuery, GeographyPointValue.getValueDisplaySize(), jumboRow);
                } else if (columnTypes.get(i) == VoltType.GEOGRAPHY) {
                    appendStringColumn(dbType, createTableQuery, GeographyValue.getValueDisplaySize(columnLength), jumboRow);
                } else {
                    if (dbType == DatabaseType.ORACLE) {
                        if (type == VoltType.SMALLINT) {
                            createTableQuery.append("NUMBER(5)");
                        } else if (type == VoltType.INTEGER) {
                            createTableQuery.append("NUMBER(10)");
                        } else if (type == VoltType.BIGINT) {
                            createTableQuery.append("NUMBER(19)");
                        } else {
                            createTableQuery.append(type.name());
                        }
                    } else {
                        createTableQuery.append(type.name());
                    }
                }
            }
            createTableQuery.append(")");
            return createTableQuery.toString();
        }

        private void initialize(long generation, String stableName, List<String> columnNames,
                List<VoltType> columnTypes, List<Integer> columnLengths) throws SQLException {
            boolean supportsBatchUpdatesTmp;
            String identifierQuoteTemp = "";
            DatabaseMetaData md = m_conn.getMetaData();
            supportsBatchUpdatesTmp = md.supportsBatchUpdates();
            String dbName = md.getDatabaseProductName();
            boolean supportsUpsert = false;
            if (dbName.equals("MySQL")) {
                m_dbType = DatabaseType.MYSQL;
                identifierQuoteTemp = "`";
                m_supportsDuplicateKey = true;
            } else if (dbName.equals("PostgreSQL")) {
                m_dbType = DatabaseType.POSTGRES;
                identifierQuoteTemp = "\"";
            } else if (dbName.equals("Oracle")) {
                m_dbType = DatabaseType.ORACLE;
                identifierQuoteTemp = "\"";
            } else if (dbName.equals("Netezza NPS")) {
                m_dbType = DatabaseType.NETEZZA;
                identifierQuoteTemp = "\"";
            } else if (dbName.equals("Microsoft SQL Server")) {
                m_dbType = DatabaseType.SQLSERVER;
                identifierQuoteTemp = "\"";
            } else if (dbName.equals("Vertica Database")) {
                m_dbType = DatabaseType.VERTICA;
                identifierQuoteTemp = "\"";
            } else if (dbName.equals("Teradata")) {
                m_dbType = DatabaseType.TERADATA;
                identifierQuoteTemp = "\"";
            } else if (dbName.equals("VoltDB")) {
                m_dbType = DatabaseType.VOLTDB;
                identifierQuoteTemp = "";
                supportsUpsert = true;
                m_disableAutoCommits = false;
            } else if (dbName.contains("HSQL")) {
                identifierQuoteTemp = "";
                m_dbType = DatabaseType.UNRECOGNIZED;
                ignoreGenerations = true;
            } else if (m_dbType == null) {
                m_dbType = DatabaseType.UNRECOGNIZED;
                identifierQuoteTemp = "\"";
            }

            String identifierQuote = identifierQuoteTemp;
            m_supportsBatchUpdates = supportsBatchUpdatesTmp;

            final String tableName = m_lowercaseNames ? stableName.toLowerCase() : stableName;

            final String schemaAndTable =
                    (schema_prefix.isEmpty() ? "" : identifierQuote + schema_prefix + identifierQuote + ".") +
                    identifierQuote + (ignoreGenerations ? "" : "E" + generation + "_") + tableName + identifierQuote;

            firstField = ExportRow.getFirstField(skipInternals);
            if (m_createTable){
                m_createTableStr = createTableString(m_dbType, schemaAndTable,
                        identifierQuote, columnNames, columnLengths, columnTypes);
            }

            String pstmtStringTmp = "INSERT INTO " + schemaAndTable + " (";
            String updateFields = new String();
            for (int i = firstField; i < columnNames.size(); i++) {
                if (i != firstField) {
                    pstmtStringTmp += ", ";
                    updateFields += ", ";
                }

                String columnName = m_lowercaseNames ? columnNames.get(i).toLowerCase() : columnNames.get(i);
                pstmtStringTmp += identifierQuote + columnName + identifierQuote;
                updateFields += identifierQuote + columnName + identifierQuote + "=?";
            }
            pstmtStringTmp += ") VALUES (";
            for (int i = firstField; i < columnNames.size(); i++) {
                if (i != firstField) {
                    pstmtStringTmp += ", ";
                }

                pstmtStringTmp += "?";
            }
            pstmtStringTmp += ")";
            if (supportsUpsert) {
                m_preparedStmtStr = "UP" + pstmtStringTmp.substring(2);
                m_supportsUpsert = true;
            }
            else if (m_supportsDuplicateKey) {
                m_preparedStmtStr = pstmtStringTmp + " ON DUPLICATE KEY UPDATE " + updateFields;
                m_supportsUpsert = true;
            }
            else {
                m_preparedStmtStr = pstmtStringTmp;
            }
            if (m_logger.isDebugEnabled()) {
                m_logger.debug(m_preparedStmtStr);
            }
        }

        private void createTable() {
            Statement stmt = null;
            try {
                stmt = m_conn.createStatement();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            try {
                m_logger.info("Creating table with statement: " + m_createTableStr);
                stmt.execute(m_createTableStr.toString());
                m_createTableStr = null;
                if (m_disableAutoCommits) {
                    m_conn.commit();
                }
            } catch (SQLException e) {
                m_logger.warn("SQL Exception when creating table.",e);
                try {
                    if (m_disableAutoCommits) {
                        m_conn.rollback();
                    }
                } catch (SQLException e1) {
                    throw new RuntimeException(e1);
                }
                if ((e.getSQLState().equals(SQLSTATE_UNIQUE_VIOLATION)) ||
                    (e.getMessage().toLowerCase().contains("already exists")) ||
                    (m_dbType == DatabaseType.ORACLE && e.getMessage().contains("ORA-00955"))) {
                    // table already exists
                    // set m_createTableStr = null, then createTable() won't be called again
                    m_logger.info("SQL Exception indicates the table already exists, proceeding with inserts");
                    m_createTableStr = null;
                } else {
                    throw new RuntimeException(e);
                }
            }
            try {
                stmt.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private void appendStringColumn(DatabaseType dbType, StringBuilder createTableQuery, int columnLength, boolean jumboRow) {
            // JDBC's TEXT type is unlimited in size
            // and should be suitable for most use cases.
            // Would be nice to create a VARCHAR with the
            // same limit as Volt, but it doesn't look like
            // we can get that information from here.
            switch (dbType) {
            case VERTICA:
                if (columnLength > 65000) {
                    throw new RuntimeException("Vertica only supports VARCHAR up to 65000");
                }
                createTableQuery.append("VARCHAR(" + columnLength + ")");
                break;
            case MYSQL:
                if (jumboRow) {
                    createTableQuery.append("MEDIUMTEXT");
                } else {
                    createTableQuery.append("VARCHAR(" + columnLength + ")");
                }
                break;
            case POSTGRES:
                createTableQuery.append("TEXT");
                break;
            case NETEZZA:
                if (columnLength > 64000) {
                    throw new RuntimeException("Netezza only supports VARCHAR up to 64000");
                }
                createTableQuery.append("VARCHAR(" + columnLength + ")");
                break;
            case ORACLE:
                if (columnLength < 4000) {
                    createTableQuery.append("VARCHAR2(" + columnLength + ")");
                } else {
                    createTableQuery.append("CLOB");
                }
                break;
            case SQLSERVER:
                if (columnLength < 8000) {
                    createTableQuery.append("VARCHAR(" + columnLength + ")");
                } else {
                    createTableQuery.append("VARCHAR(max)");
                }
                break;
            case VOLTDB:
                createTableQuery.append("VARCHAR(" + columnLength + " BYTES)");
                break;
            default:
                //Seems like most systems don't support VARCHAR beyond 64000, and there are m_row limits
                //beyond that as well
                 if (jumboRow) {
                     createTableQuery.append("CLOB(" + columnLength + ")");
                 } else {
                     createTableQuery.append("VARCHAR(" + columnLength + ")");
                 }
                break;
            }
        }

        /**
         * Detect whether the schema changed, and if yes, reset state for new statements.
         * Optimize the changes by checking if schemas actually differ, because there are
         * scenarios where the catalog changes (e.g. an export target is enabled) but the
         * schemas are unchanged.
         *
         * @throws RestartBlockException
         */
        private void checkSchemas() throws RestartBlockException {
            try {
                ExportRowSchema curSchema = getExportRowSchema();
                assert(curSchema != null);
                if (m_curGenId != curSchema.generation &&
                        !(m_curSchema != null && m_curSchema.sameSchema(curSchema))) {
                    if (m_logger.isDebugEnabled()) {
                        StringBuilder sb = new StringBuilder("Detected new schema: ")
                                .append("old = ")
                                .append(m_curGenId)
                                .append(", new = ")
                                .append(curSchema.generation);
                        m_logger.debug(sb);
                    }
                    m_curGenId = curSchema.generation;
                    m_curSchema = curSchema;
                    if (m_pstmt != null) {
                        try {
                            m_pstmt.close();
                        } catch (Exception e) {}
                        finally {
                            m_pstmt = null;
                        }
                    }
                    m_preparedStmtStr = null;
                    m_createTableStr = null;
                }
            } catch (Exception e) {
                m_logger.warn("JDBC export unable to check schemas", e);
                throw new RestartBlockException(true);
            }
        }

        @Override
        public void onBlockStart(ExportRow row) throws RestartBlockException {
            m_dataRows.clear();
            if (m_conn == null) {
                if (m_pstmt != null) {
                    try {
                        m_pstmt.close();
                    } catch (Exception e) {}
                    finally {
                        m_pstmt = null;
                    }
                }
                try {
                    m_conn = m_ds.getDataSource().getConnection();
                } catch (Exception e) {
                    m_logger.warn("JDBC export unable to connect", e);
                    closeConnection();
                    throw new RestartBlockException(true);
                }
            }
            checkSchemas();
        }

        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException {
            PreparedStatement pstmt = m_pstmt;
            Connection conn = m_conn;

            if (pstmt == null || conn == null) {
                m_logger.rateLimitedWarn(LOG_RATE_LIMIT, "Unable to commit, connection closed, probably after block timeout");
                throw new RestartBlockException(true);
            }

            try {
                if (m_supportsBatchUpdates) {
                    pstmt.executeBatch();
                }
                if (m_disableAutoCommits) {
                    conn.commit();
                }
            } catch(BatchUpdateException e){
                if (!warnClosed(conn)) {
                    logBatchErrors(e);
                }
                throw new RestartBlockException(true);
            } catch (SQLException e) {
                if (!warnClosed(conn)) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "commit() failed for row %s", Throwables.getStackTraceAsString(e));
                }
                throw new RestartBlockException(true);
            } catch (Exception e) {
                m_logger.rateLimitedError(LOG_RATE_LIMIT, "Exception while executing and committing batch %s", Throwables.getStackTraceAsString(e));
                throw new RestartBlockException(true);
            } finally{
                m_dataRows.clear();
                closeConnection();
            }
        }

        // Note: call on commit exception
        private boolean warnClosed(Connection conn) {
            try {
                if (conn.isClosed()) {
                    m_logger.rateLimitedWarn(LOG_RATE_LIMIT, "Unable to commit, connection closed, probably after block timeout");
                    return true;
                }
            } catch (Exception e) {
                // Ignore
            }
            return false;
        }

        private void logBatchErrors(BatchUpdateException e){
           int [] results = e.getUpdateCounts();
           StringBuilder builder = new StringBuilder();
           for(int i = 0; i < results.length; i++){
                if(results[i] == Statement.EXECUTE_FAILED){
                    ExportRow rowi = m_dataRows.get(i).m_row;
                    Object row[] = rowi.values;
                    for (int j = firstField; j < rowi.types.size(); j++) {
                        builder.append((j == firstField) ? "":", ");
                        formatValue(row[j], rowi.types.get(j), builder);
                    }
                    builder.append("\n");
                }
            }
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            m_logger.rateLimitedError(LOG_RATE_LIMIT, "commit() failed in table %s for row(s):\n %s",
                    builder.toString(),
                    Throwables.getStackTraceAsString(rootCause != null ? rootCause : e));
        }

        private void formatValue(Object val, VoltType columnType, StringBuilder builder){

            if(val != null){
                if (columnType == VoltType.TIMESTAMP) {
                    TimestampType timestamp = (TimestampType)val;
                    builder.append(timestamp.asJavaTimestamp().toString());
                } else if (columnType == VoltType.GEOGRAPHY_POINT) {
                    GeographyPointValue gpv = (GeographyPointValue)val;
                    builder.append(gpv.toWKT());
                } else if (columnType  == VoltType.GEOGRAPHY) {
                    GeographyValue gv = (GeographyValue)val;
                    builder.append(gv.toWKT());
                } else {
                    String formattedValue = "";
                    if (columnType == VoltType.VARBINARY) {
                        byte[] bytes = (byte[])val;
                        formattedValue = bytes.toString();
                    } else {
                        formattedValue = val.toString();
                    }
                    builder.append(formattedValue.length() < 4 ? formattedValue : StringUtils.abbreviate(formattedValue, MAX_COLUMN_PRINT_SIZE));
                }
            }
        }

        @Override
        public boolean processRow(ExportRow rowinst) throws RestartBlockException {
            if (m_preparedStmtStr == null) {
                try {
                    initialize(rowinst.generation, rowinst.tableName, rowinst.names, rowinst.types, rowinst.lengths);
                } catch (Exception e) {
                    m_logger.warn("JDBC export unable to initialize jdbc target database", e);
                    closeConnection();
                }
            }
            //We could not initialize this JDBCDecoder others may be done...
            if (m_preparedStmtStr == null) {
                throw new RestartBlockException(true);
            }

            if (rowinst.getOperation() == ROW_OPERATION.UPDATE_NEW && !m_supportsUpsert) {
                if (!m_warnedOfUnsupportedOperation) {
                    m_logger.rateLimitedWarn(LOG_RATE_LIMIT, "JDBC export skipped past a row with an operation type " +
                            rowinst.getOperation().name() + " from stream " + rowinst.tableName);
                }
                return true;
            }
            if (m_pstmt == null) {
                if (m_disableAutoCommits) {
                    try {
                        m_conn.setAutoCommit(false);
                    } catch (Exception e) {
                        m_logger.warn("JDBC export failed to reset AutoCommit in target database", e);
                        closeConnection();
                        throw new RestartBlockException(true);
                    }
                }
                if (m_createTable && m_createTableStr != null) {
                    try {
                        createTable();
                    } catch (Exception e) {
                        m_logger.warn("JDBC export unable to create table in target database", e);
                        closeConnection();
                        throw new RestartBlockException(true);
                    }
                }
                try {
                    if (m_logger.isDebugEnabled()) {
                        m_logger.debug(m_preparedStmtStr);
                    }
                    m_pstmt = m_conn.prepareStatement(m_preparedStmtStr);
                } catch (Exception e) {
                    m_logger.warn("JDBC export unable to prepare insert statement", e);
                    closeConnection();
                    throw new RestartBlockException(true);
                }
            }

            // Insert values in statement - if the statement supports duplicate keys,
            // insert each value twice at their expected positions in statement.
            Object[] row = rowinst.values;
            List<VoltType> columnTypes = rowinst.types;
            boolean restartBlock = false;
            try {
                int nCols = columnTypes.size() - firstField;

                for (int i = firstField; i < columnTypes.size(); i++) {

                    final int pstmtIndex = i + 1 - firstField;
                    final int pstmtIndex2 = pstmtIndex + nCols;

                    if (row[i] == null) {
                        m_pstmt.setNull(pstmtIndex, Types.NULL);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setNull(pstmtIndex2, Types.NULL);
                        }
                    } else if (columnTypes.get(i) == VoltType.DECIMAL) {
                        m_pstmt.setBigDecimal(pstmtIndex, (BigDecimal)row[i]);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setBigDecimal(pstmtIndex2, (BigDecimal)row[i]);
                        }
                    } else if (columnTypes.get(i) == VoltType.TINYINT) {
                        m_pstmt.setByte(pstmtIndex, (Byte)row[i]);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setByte(pstmtIndex2, (Byte)row[i]);
                        }
                    } else if (columnTypes.get(i) == VoltType.SMALLINT) {
                        m_pstmt.setShort(pstmtIndex, (Short)row[i]);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setShort(pstmtIndex2, (Short)row[i]);
                        }
                    } else if (columnTypes.get(i) == VoltType.INTEGER) {
                        m_pstmt.setInt(pstmtIndex, (Integer)row[i]);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setInt(pstmtIndex2, (Integer)row[i]);
                        }
                    } else if (columnTypes.get(i) == VoltType.BIGINT) {
                        m_pstmt.setLong(pstmtIndex, (Long)row[i]);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setLong(pstmtIndex2, (Long)row[i]);
                        }
                    } else if (columnTypes.get(i) == VoltType.FLOAT) {
                        m_pstmt.setDouble(pstmtIndex, (Double)row[i]);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setDouble(pstmtIndex2, (Double)row[i]);
                        }
                    } else if (columnTypes.get(i) == VoltType.STRING) {
                        m_pstmt.setString(pstmtIndex, (String)row[i]);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setString(pstmtIndex2, (String)row[i]);
                        }
                    } else if (columnTypes.get(i) == VoltType.TIMESTAMP) {
                        TimestampType timestamp = (TimestampType)row[i];
                        m_pstmt.setTimestamp(pstmtIndex, timestamp.asJavaTimestamp());
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setTimestamp(pstmtIndex2, timestamp.asJavaTimestamp());
                        }
                    } else if (columnTypes.get(i) == VoltType.GEOGRAPHY_POINT) {
                        GeographyPointValue gpv = (GeographyPointValue)row[i];
                        m_pstmt.setString(pstmtIndex, gpv.toWKT());
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setString(pstmtIndex2, gpv.toWKT());
                        }
                    } else if (columnTypes.get(i) == VoltType.GEOGRAPHY) {
                        GeographyValue gv = (GeographyValue)row[i];
                        m_pstmt.setString(pstmtIndex, gv.toWKT());
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setString(pstmtIndex2, gv.toWKT());
                        }
                    } else if (columnTypes.get(i) == VoltType.VARBINARY) {
                        byte[] bytes = (byte[])row[i];
                        m_pstmt.setBytes(pstmtIndex, bytes);
                        if (m_supportsDuplicateKey) {
                            m_pstmt.setBytes(pstmtIndex2, bytes);
                        }
                    }
                }

                try {
                    if (m_supportsBatchUpdates) {
                        m_pstmt.addBatch();
                        m_dataRows.add(new BatchRow(rowinst));
                    } else {
                        m_pstmt.executeUpdate();
                    }
                } catch (SQLException e) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "executeUpdate() failed in processRow() for table %s %s", (rowinst == null ? "Unknown" : rowinst.tableName), Throwables.getStackTraceAsString(e));
                    restartBlock = true;
                }
            } catch (Exception e) {
                m_logger.rateLimitedError(LOG_RATE_LIMIT, "processRow() failed in table %s, %s", (rowinst == null ? "Unknown" : rowinst.tableName), Throwables.getStackTraceAsString(e));
                restartBlock = true;
            }

            if (restartBlock) {
                closeConnection();
                throw new RestartBlockException(true);
            }

            return true;
        }

        /*
         * If there is any kind of exception from the DB call this to get a clean slate
         * and retry will recreate the connection and prepared statement.
         */
        private void closeConnection() {
            try {
                try {
                    if (m_pstmt != null) {
                        m_pstmt.close();
                    }
                } catch (Exception e) {
                    m_logger.warn("Exception closing pstmt for reset for table ", e);
                }
                try {
                    if (m_conn != null) {
                        m_conn.close();
                    }
                } catch (Exception e) {
                    m_logger.warn("Exception closing conn for reset for table ", e);
                }
            } finally {
                m_conn = null;
                m_pstmt = null;
            }
        }

        // Note: this can be called concurrently to processRow/onBlockComplertion when a block timeout occurs
        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            if (m_es == null) {
                closeConnection();
                return;
            }
            m_es.shutdown();
            try {
                if (!m_es.awaitTermination(SHUTDOWN_TIMEOUT_S, TimeUnit.SECONDS)) {
                    // In case that jdbc connection is not responsive, force out
                    forceExecutorShutdown();
                    return;
                }
            } catch (InterruptedException e) {
                // We are in the UAC path and don't want to throw exception for this condition;
                // just force the shutdown.
                m_logger.warn("Interrupted while awaiting executor shutdown on source:" + m_source);
                forceExecutorShutdown();
                return;
            }
            closeConnection();
        }

        private void forceExecutorShutdown() {
            m_logger.warn("Forcing executor shutdown on source: " + m_source);
            closeConnection();
            try {
                m_es.shutdownNow();
            } catch (Exception e) {
                m_logger.error("Failed to force executor shutdown on source: " + m_source, e);
            }
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new JDBCDecoder(source, m_cpds.get().get(m_urlId));
    }

    @Override
    public void configure(Properties config) throws Exception {
        String url = config.getProperty("jdbcurl", "").trim();
        if (url.isEmpty()) {
            throw new IllegalArgumentException("\"jdbcurl\" must not be null");
        }
        String urlId = url;
        m_poolProperties.setUrl(url);

        String user = config.getProperty("jdbcuser", "").trim();
        if (user.isEmpty()) {
            throw new IllegalArgumentException("\"jdbcuser\" must not be null");
        }
        urlId += "?jdbcuser=" + user;
        m_poolProperties.setUsername(user);

        String password = config.getProperty("jdbcpassword");
        m_poolProperties.setPassword(password);

        schema_prefix = config.getProperty("schema", "");

        m_lowercaseNames = Boolean.valueOf(config.getProperty("lowercase", "false"));
        ignoreGenerations = Boolean.valueOf(config.getProperty("ignoregenerations", "false"));
        skipInternals = Boolean.valueOf(config.getProperty("skipinternals", "false"));
        String createtable = config.getProperty("createtable", "true").trim();
        m_createTable = ("true".equalsIgnoreCase(createtable) || "yes".equalsIgnoreCase(createtable) || "1".equals(createtable));
        if(!m_createTable){
            ignoreGenerations = true;
        }
        String minPoolSize = config.getProperty("minpoolsize", "").trim();
        if (!minPoolSize.isEmpty()) {
            try {
                urlId += "&minpoolsize=" + minPoolSize;
                m_poolProperties.setMinIdle(Integer.parseInt(minPoolSize));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("\"minPoolSize\" must be integer");
            }
        }

        String maxPoolSize = config.getProperty("maxpoolsize", "").trim();
        if (!maxPoolSize.isEmpty()) {
            try {
                urlId += "&maxpoolsize=" + maxPoolSize;
                m_poolProperties.setMaxActive(Integer.parseInt(maxPoolSize));
                m_poolProperties.setMaxIdle(Integer.parseInt(maxPoolSize));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("\"maxPoolSize\" must be integer");
            }
        }

        String maxIdleTime = config.getProperty("maxidletime", "").trim();
        if (!maxIdleTime.isEmpty()) {
            try {
                urlId += "&maxidletime=" + maxIdleTime;
                m_poolProperties.setMinEvictableIdleTimeMillis(Integer.parseInt(maxIdleTime));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("\"maxIdleTime\" must be integer");
            }
        }

        String maxStatementsCached = config.getProperty("maxstatementscached", "").trim();
        int maxStatementsCachedVal;
        if (!maxStatementsCached.isEmpty()) {
            try {
                maxStatementsCachedVal = Integer.parseInt(maxStatementsCached);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("\"maxStatementsPerConnection\" must be integer");
            }
        } else {
            maxStatementsCachedVal = 50;
        }
        urlId += "&maxstatementscached=" + String.valueOf(maxStatementsCachedVal);
        m_poolProperties.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.StatementCache(max="
                + maxStatementsCachedVal + ")");

        m_poolProperties.setTestOnBorrow(true);
        if (url.startsWith("jdbc:oracle")) {
            m_poolProperties.setValidationQuery("SELECT 1 FROM DUAL");
        } else {
            m_poolProperties.setValidationQuery("SELECT 1");
        }

        /*
         * If the user didn't specify a jdbcdriver class name, set it to
         * a default value if the beginning of the url is recognized
         */

        String jdbcdriver = config.getProperty("jdbcdriver", "").trim();
        if (jdbcdriver.isEmpty()) {
            if (url.startsWith("jdbc:vertica")) {
                jdbcdriver = "com.vertica.jdbc.Driver";
            } else if (url.startsWith("jdbc:mysql")) {
                jdbcdriver = "com.mysql.jdbc.Driver";
            } else if (url.startsWith("jdbc:postgresql")) {
                jdbcdriver = "org.postgresql.Driver";
            } else if (url.startsWith("jdbc:oracle")) {
                jdbcdriver = "oracle.jdbc.OracleDriver";
            } else if (url.startsWith("jdbc:netezza")) {
                jdbcdriver = "org.netezza.Driver";
            } else if (url.startsWith("jdbc:sqlserver")) {
                jdbcdriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            } else if (url.startsWith("jdbc:teradata")) {
                jdbcdriver = "com.teradata.jdbc.TeraDriver";
            } else if (url.startsWith("jdbc:voltdb")) {
                jdbcdriver = "org.voltdb.jdbc.Driver";
            }
        }

        /*
         * Try loading the driver so there is a user-friendly exception if it cannot be loaded
         * The Tomcat Data Source will load it again later, which requires the DriverClassName to be set.
         */

        try {
            Class.forName(jdbcdriver);
        } catch (ClassNotFoundException e) {
            m_logger.warn("Exception attempting to load JDBC driver \"" + jdbcdriver + "\"", e);
            throw new RuntimeException(e);
        }

        //Dont do actual config in check mode.
        boolean configcheck = Boolean.parseBoolean(config.getProperty(ExportManager.CONFIG_CHECK_ONLY, "false"));
        if (configcheck) {
            return;
        }

        urlId += "&jdbcdriver=" + jdbcdriver;
        m_poolProperties.setDriverClassName(jdbcdriver);

        m_urlId = new URI(urlId);
        ImmutableMap.Builder<URI,RefCountedDS> builder;
        Map<URI,RefCountedDS> cpds;
        Boolean dsCreated = false;
        RefCountedDS p = null;
        do {
            if (dsCreated) {
                p.getDataSource().close();
            }

            builder = ImmutableMap.builder();
            cpds = m_cpds.get();
            p = cpds.get(m_urlId);
            builder.putAll(Maps.filterKeys(cpds,Predicates.not(Predicates.equalTo(m_urlId))));

            if (p == null) {
                DataSource ds = new DataSource();
                ds.setPoolProperties(m_poolProperties);
                p = new RefCountedDS(ds, 0);
                dsCreated = true;
            }
            builder.put(m_urlId, p.increment());
        } while (!m_cpds.compareAndSet(cpds, builder.build()));
    }

    @Override
    public void shutdown() {
        ImmutableMap.Builder<URI,RefCountedDS> builder;
        Map<URI,RefCountedDS> cpds;
        RefCountedDS p;
        if (m_cpds.get().containsKey(m_urlId)) {
            do {
                builder = ImmutableMap.builder();
                cpds = m_cpds.get();

                p = cpds.get(m_urlId).decrement();
                builder.putAll(Maps.filterKeys(cpds,Predicates.not(Predicates.equalTo(m_urlId))));
                if (p.getRefCount() > 0) {
                    builder.put(m_urlId, p);
                }
            } while(!m_cpds.compareAndSet(cpds, builder.build()));
            if (p != null && p.getRefCount() == 0) {
                p.getDataSource().close();
            }
        }
    }
}
