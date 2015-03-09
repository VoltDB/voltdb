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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.common.Constants;
import org.voltdb.types.TimestampType;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.reflect.TypeToken;

/**
 *
 * This is a single thread reader which feeds the lines after validating syntax
 * to CSVDataLoader.
 *
 */
class JDBCStatementReader extends SusceptibleRunnable {

    static final int MAX_COLUMN_SIZE  = 1 * 1024 * 1024; // 1MB
    static AtomicLong m_totalRowCount = new AtomicLong(0);
    static JDBCLoader.JDBCLoaderConfig m_config = null;
    static Client m_csvClient = null;
    long m_parsingTime = 0;
    private static final VoltLogger m_log = new VoltLogger("JDBCLOADER");
    private final CSVDataLoader m_loader;
    private final BulkLoaderErrorHandler m_errHandler;

    public static void initializeReader(JDBCLoader.JDBCLoaderConfig config, Client csvClient) {
        m_config = config;
        m_csvClient = csvClient;
    }

    public JDBCStatementReader(CSVDataLoader loader, BulkLoaderErrorHandler errorHandler) {
        m_loader = loader;
        m_errHandler = errorHandler;
    }

    private void forceClose(Connection conn, PreparedStatement stmt, ResultSet rslt) {
        if (rslt != null) try {rslt.close();} catch (Exception ignoreIt) {}
        if (stmt != null) try {stmt.close();} catch (Exception ignoreIt) {}
        if (conn != null) try {conn.close();} catch (Exception ignoreIt) {}
        try {m_loader.close();} catch (Exception ignoreIt) {}
    }

    @Override
    public void susceptibleRun() throws SQLException {

        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rslt = null;
        RowWithMetaData lineData = null;
        int columnCount = 0;

        ImporterType.Acceptor [] acceptors = null;
        Object[] columnValues = null;
        String[] stringValues = null;

        try {
            conn = DriverManager.getConnection(m_config.jdbcurl, m_config.jdbcuser, m_config.jdbcpassword);
            DatabaseMetaData dbmd = conn.getMetaData();
            int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
            if (!dbmd.supportsResultSetType(resultSetType)) {
                resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
            }
            stmt = conn.prepareStatement(
                    "select * from " + m_config.jdbctable,
                    resultSetType,
                    ResultSet.CONCUR_READ_ONLY
                    );
            stmt.setFetchSize(m_config.fetchsize);
            rslt = stmt.executeQuery();
            ResultSetMetaData mdata = rslt.getMetaData();
            columnCount = mdata.getColumnCount();

            /*
             * Each column from jdbc source must be an importable type. First
             * we determine if there is a corresponding importer type for the
             * given column. If there is one then determine the column acceptor
             * that converts the database type to the appropriate volt type, and
             * add it to the acceptors array
             */
            acceptors = new ImporterType.Acceptor[columnCount];
            for (int i = 1; i <= columnCount; ++i) {
                ImporterType type = ImporterType.forClassName(mdata.getColumnClassName(i));
                if (type == null) {
                    throw new SQLException(String.format(
                            "Unsupported data type %s for column %s",
                            mdata.getColumnTypeName(i),
                            mdata.getColumnName(i)
                            ));
                }
                acceptors[i-1] = type.getAcceptorFor(rslt, i);
            }
        } catch (Exception ex) {
            m_log.error("database query initialization failed" , ex);
            forceClose(conn,stmt,rslt);
            Throwables.propagate(ex);
        }

        StringWriter sw = new StringWriter(16384);
        PrintWriter pw = new PrintWriter(sw,true);
        CSVWriter csw = new CSVWriter(pw);
        StringBuffer sb = sw.getBuffer();

        stringValues = new String[columnCount];

        try {
            while (rslt.next()) {
                long rownum = m_totalRowCount.incrementAndGet();

                Arrays.fill(stringValues, "NULL");
                columnValues = new Object[columnCount];

                lineData = new RowWithMetaData(new String[1], rownum);

                try {
                    for (int i = 0; i < columnCount; ++i) {
                        columnValues[i] = acceptors[i].convert();
                        stringValues[i] = acceptors[i].format(columnValues[i]);
                    }

                    csw.writeNext(stringValues);
                    ((String[])lineData.rawLine)[0] = sb.toString();
                    sb.setLength(0);

                    m_loader.insertRow(lineData, columnValues);

                } catch (SQLException ex) {
                    m_errHandler.handleError(lineData, null, getExceptionAndCauseMessages(ex));
                }
            }
        } catch (InterruptedException ignoreIt) {
        }
        finally {
           forceClose(conn, stmt, rslt);
        }
        m_log.debug("JSBCLoader Done.");
    }

    public static String getExceptionAndCauseMessages(Throwable ex) {
        if (ex == null) return "";
        StringBuilder sb = new StringBuilder(8192).append(ex.getMessage());
        while (ex.getCause() != null) {
            ex = ex.getCause();
            sb.append("\n+-- Caused by: ").append(ex.getMessage());
        }
        return sb.toString();
    }

    enum ImporterType {
        BOOLEAN(TypeToken.of(Boolean.class),TypeToken.of(Boolean.TYPE)) {
            @Override
            Acceptor getAcceptorFor(ResultSet rslt, int idx) {
                return new Acceptor(rslt, idx) {
                    @Override
                    Object convert() throws SQLException {
                        Object val = m_rslt.getObject(m_idx);
                        if (m_rslt.wasNull()) return null;
                        return (Boolean)val ? (byte)1 : (byte)0;
                    }
                };
            }
        },
        BYTE(TypeToken.of(Byte.class),TypeToken.of(Byte.TYPE)),
        CHARACTER(TypeToken.of(Character.class),TypeToken.of(Character.TYPE)),
        SHORT(TypeToken.of(Short.class),TypeToken.of(Short.TYPE)),
        INTEGER(TypeToken.of(Integer.class),TypeToken.of(Integer.TYPE)),
        LONG(TypeToken.of(Long.class),TypeToken.of(Long.TYPE)),
        FLOAT(TypeToken.of(Float.class),TypeToken.of(Float.TYPE)),
        DOUBLE(TypeToken.of(Double.class),TypeToken.of(Double.TYPE)),
        DECIMAL(TypeToken.of(BigDecimal.class)),
        STRING(TypeToken.of(String.class)),
        BYTEARRAY(TypeToken.of(byte[].class)) {
            @Override
            Acceptor getAcceptorFor(ResultSet rslt, int idx) {
                return new Acceptor(rslt, idx) {
                    @Override
                    String format(Object o) {
                        return o != null ? Encoder.hexEncode((byte[])o) : "NULL";
                    }
                };
            }
        },
        DATE(TypeToken.of(Date.class)) {
            @Override
            Acceptor getAcceptorFor(ResultSet rslt, int idx) {
                return new Acceptor(rslt, idx) {
                    final SimpleDateFormat dfmt =
                            new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
                    @Override
                    Object convert() throws SQLException {
                        Object val = m_rslt.getObject(m_idx);
                        if (m_rslt.wasNull()) return null;
                        return new TimestampType((Date)val);
                    }
                    @Override
                    String format(Object o) {
                        return o != null ? dfmt.format(((TimestampType)o).asApproximateJavaDate()) : "NULL";
                    }
                };
            }
        },
        BLOB(TypeToken.of(java.sql.Blob.class)) {
            @Override
            Acceptor getAcceptorFor(ResultSet rslt, int idx) {
                return new Acceptor(rslt, idx) {
                    @Override
                    Object convert() throws SQLException {
                        Object val = null;
                        java.sql.Blob blob = null;
                        try {
                            val = m_rslt.getObject(m_idx);
                            if (m_rslt.wasNull()) return null;

                            blob = (java.sql.Blob)val;
                            if (blob.length() > MAX_COLUMN_SIZE) {
                                throw new SQLException("blobs may not be greater than " + MAX_COLUMN_SIZE);
                            }
                            return blob.getBytes(0, (int)blob.length());
                        } finally {
                            if (blob != null) {
                                try {blob.free();} catch (Exception ignoreIt) {}
                            }
                        }
                    }

                    @Override
                    String format(Object o) {
                        return o != null ? Encoder.hexEncode((byte[])o) : "NULL";
                    }
                };
            }
        };

        static class Acceptor {
            protected final ResultSet m_rslt;
            protected final int m_idx;

            public Acceptor(ResultSet rslt, int idx) {
                m_rslt = rslt;
                m_idx = idx;
            }

            /**
             * Default conversion. return it as is
             * @return return result set object as os
             * @throws SQLException
             */
            Object convert() throws SQLException {
                Object val = m_rslt.getObject(m_idx);
                if (m_rslt.wasNull()) return null;
                return val;
            }

            String format(Object o) {
                return o != null ? o.toString() : "NULL";
            }
        }

        final static class IsAssignableFromChecker implements Predicate<TypeToken<?>> {
            private final TypeToken<?> m_from;

            public IsAssignableFromChecker(String clazzName) {
                TypeToken<?> token = null;
                try {
                    token = TypeToken.of(Class.forName(clazzName));
                } catch (ClassNotFoundException e) {
                    Throwables.propagate(e);
                }
                m_from = token;
            }

            @Override
            public boolean apply(TypeToken<?> input) {
                return input.isAssignableFrom(m_from);
            }
        }

        final Set<TypeToken<?>> typeTokens;

        ImporterType(TypeToken<?>...tokens) {
            typeTokens = ImmutableSet.copyOf(tokens);
        }

        Acceptor getAcceptorFor(ResultSet rslt, int idx) {
            return new Acceptor(rslt,idx);
        }

        static ImporterType forClassName(String className) {
            IsAssignableFromChecker checker = new IsAssignableFromChecker(className);
            for (ImporterType e: values()) {
                if (FluentIterable.from(e.typeTokens).anyMatch(checker)) {
                    return e;
                }
            }
            return null;
        }
    }
}
