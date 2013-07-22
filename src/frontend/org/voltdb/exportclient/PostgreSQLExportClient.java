/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

// -*- mode: java; c-basic-offset: 4; -*-

package org.voltdb.exportclient;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;

public class PostgreSQLExportClient extends ExportClientBase {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    private static Connection conn = null;
    private static String postgres_schema_prefix;

    public PostgreSQLExportClient(boolean useAdminPorts) {
        super(useAdminPorts);
    }

    static class PostgresDecoder extends ExportDecoderBase {
        private PreparedStatement pstmt = null;

        public PostgresDecoder(AdvertisedDataSource source) {
            super(source);

            m_logger.debug("New PostgresDecoder for " + m_source.tableName);

            Statement stmt = null;
            try {
                stmt = conn.createStatement();
            } catch (SQLException e) {
                m_logger.fatal("createStatement failed for " + m_source.tableName);
                System.exit(-1);
            }

            try {
                String schemaExistsQuery = "SELECT EXISTS(SELECT * " +
                    "FROM information_schema.schemata WHERE schema_name = '" +
                    postgres_schema_prefix + m_source.m_generation + "')";

                ResultSet schemaExists = stmt.executeQuery(schemaExistsQuery);
                schemaExists.next();
                if (!schemaExists.getBoolean(1)) {
                    stmt.execute("CREATE SCHEMA " + postgres_schema_prefix + m_source.m_generation);
                    conn.commit();
                }
                schemaExists.close();
                schemaExistsQuery = null;
            } catch (SQLException e) {
                m_logger.fatal("Schema creation failed");
                System.exit(-1);
            }

            try {
                String createTableQuery = "CREATE TABLE IF NOT EXISTS " +
                    postgres_schema_prefix + m_source.m_generation +
                    "." + m_source.tableName + " (";

                for (int i = 0; i < m_source.columnNames.size(); i++) {
                    if (i != 0) {
                        createTableQuery += ", ";
                    }

                    createTableQuery += m_source.columnNames.get(i) + " ";

                    if (m_source.columnTypes.get(i) == VoltType.TINYINT) {
                        createTableQuery += "SMALLINT " +
                            "CONSTRAINT " + m_source.columnNames.get(i) +
                            "_tinyint CHECK (-128 <= " + m_source.columnNames.get(i) +
                            " AND " + m_source.columnNames.get(i) + " <= 127)";
                    } else if (m_source.columnTypes.get(i) == VoltType.STRING) {
                        // PostgreSQL's TEXT type is unlimited in size
                        // and should be suitable for most use cases.
                        // Would be nice to create a VARCHAR with the
                        // same limit as Volt, but it doesn't look like
                        // we can get that information from here.

                        createTableQuery += "TEXT";
                    } else if (m_source.columnTypes.get(i) == VoltType.DECIMAL) {
                        // Same deal as STRING, but currently DECIMAL's
                        // precision and scale cannot be changed, so
                        // it's not horrible to do it this way.

                        createTableQuery += "DECIMAL(" +
                            VoltDecimalHelper.kDefaultPrecision +
                            "," + VoltDecimalHelper.kDefaultScale + ")";
                    } else if (m_source.columnTypes.get(i) == VoltType.FLOAT) {
                        createTableQuery += "DOUBLE PRECISION";
                    } else if (m_source.columnTypes.get(i) == VoltType.VARBINARY) {
                        createTableQuery += "BYTEA";
                    } else {
                        createTableQuery += m_source.columnTypes.get(i).name();
                    }
                }

                createTableQuery += ")";

                stmt.execute(createTableQuery);
                createTableQuery = null;
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(-1);
            }

            String pstmtString = "INSERT INTO " +
                postgres_schema_prefix + m_source.m_generation + "." +
                m_source.tableName + " (";
            for (int i = 0; i < m_source.columnNames.size(); i++) {
                if (i != 0) {
                    pstmtString += ", ";
                }

                pstmtString += m_source.columnNames.get(i);
            }
            pstmtString += ") VALUES (";
            for (int i = 0; i < m_source.columnNames.size(); i++) {
                if (i != 0) {
                    pstmtString += ", ";
                }

                pstmtString += "?";
            }
            pstmtString += ")";

            try {
                pstmt = conn.prepareStatement(pstmtString);
            } catch (SQLException e) {
                m_logger.fatal("Creation of PreparedStatement for table " + m_source.tableName + " failed");
                System.exit(-1);
            }
            pstmtString = null;
        }

        @Override
        public void onBlockCompletion() {
            try {
                conn.commit();
            } catch (SQLException e) {
                m_logger.error("commit() failed for row in table " + m_source.tableName);
            }
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            m_logger.debug("In processRow for table " + m_source.tableName);

            Object[] row = null;
            try {
                row = decodeRow(rowData);
            } catch (IOException e) {
                m_logger.error("Unable to decode row for table: " + m_source.tableName);
                return false;
            }

            try {
                for (int i = 0; i < m_source.columnTypes.size(); i++) {
                    if (row[i] == null) {
                        pstmt.setNull(i + 1, Types.NULL);
                    } else if (m_source.columnTypes.get(i) == VoltType.DECIMAL) {
                        pstmt.setBigDecimal(i + 1, (BigDecimal)row[i]);
                    } else if (m_source.columnTypes.get(i) == VoltType.TINYINT) {
                        pstmt.setByte(i + 1, (Byte)row[i]);
                    } else if (m_source.columnTypes.get(i) == VoltType.SMALLINT) {
                        pstmt.setShort(i + 1, (Short)row[i]);
                    } else if (m_source.columnTypes.get(i) == VoltType.INTEGER) {
                        pstmt.setInt(i + 1, (Integer)row[i]);
                    } else if (m_source.columnTypes.get(i) == VoltType.BIGINT) {
                        pstmt.setLong(i + 1, (Long)row[i]);
                    } else if (m_source.columnTypes.get(i) == VoltType.FLOAT) {
                        pstmt.setDouble(i + 1, (Double)row[i]);
                    } else if (m_source.columnTypes.get(i) == VoltType.STRING) {
                        pstmt.setString(i + 1, (String)row[i]);
                    } else if (m_source.columnTypes.get(i) == VoltType.TIMESTAMP) {
                        TimestampType timestamp = (TimestampType)row[i];
                        pstmt.setTimestamp(i + 1, timestamp.asJavaTimestamp());
                    } else if (m_source.columnTypes.get(i) == VoltType.VARBINARY) {
                        byte[] bytes = (byte[])row[i];
                        pstmt.setBytes(i + 1, bytes);
                    }
                }

                try {
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    m_logger.error("executeUpdate() failed in processRow() for table " + m_source.tableName);
                    return false;
                }
            } catch (Exception e) {
                m_logger.error("processRow() failed in table " + m_source.tableName);
                return false;
            }
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new PostgresDecoder(source);
    }

    protected static void printHelpAndQuit(int code, boolean extended) {
        System.out.println
            ("usage: java -cp <classpath> org.voltdb.exportclient.PostgreSQLExportClient");
        System.out.println
            ("       [--help] --servers server1[:port1][,server2[:port2],...]");
        System.out.println
            ("       --connect (admin|client) [--user name] [--password password]");
        System.out.println
            ("       --pgserver server --pgdb database --pguser user");
        System.out.println
            ("       [--pgschema schema_prefix] [--pgpassword password]");

        if (extended) {
            System.out.println();

            System.out.println("Mandatory arguments:");
            System.out.println("  --servers        List of VoltDB servers to connect to");
            System.out.println("  --connect        Connect via the admin or client port");
            System.out.println("  --pgserver       PostgreSQL server to connect to");
            System.out.println("  --pgdb           PostgreSQL database to connect to");
            System.out.println("  --pguser         PostgreSQL user to connect as");
            System.out.println("Optional arguments:");
            System.out.println("  --user           VoltDB user to connect as");
            System.out.println("  --password       VoltDB password to connect with");
            System.out.println("  --pgpassword     PostgreSQL password to connect with");
            System.out.println("  --pgschema       Prefix string for the PostgreSQL schemas");
        }

        System.exit(code);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        String[] volt_servers = null;
        String volt_user = null;
        String volt_password = null;
        char connect = ' '; // either ' ', 'c' or 'a'

        String postgres_server = null;
        String postgres_database = null;
        String postgres_user = null;
        String postgres_password = null;
        Properties props = new Properties();

        for (int ii = 0; ii < args.length; ii++) {
            String arg = args[ii];
            if (arg.equals("--help")) {
                printHelpAndQuit(1, true);
            } else if (arg.equals("--connect")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: not enough args following --connect");
                    printHelpAndQuit(-1, false);
                }
                String connectStr = args[ii + 1];
                if (connectStr.equalsIgnoreCase("admin")) {
                    connect = 'a';
                } else if (connectStr.equalsIgnoreCase("client")) {
                    connect = 'c';
                } else {
                    System.err.println("Error: --connect must be one of \"admin\" or \"client\"");
                    printHelpAndQuit(-1, false);
                }
                ii++;
            } else if (arg.equals("--servers")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --servers");
                    printHelpAndQuit(-1, false);
                }
                volt_servers = args[ii + 1].split(",");
                ii++;
            } else if (arg.equals("--user")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enoguh args following --user");
                    printHelpAndQuit(-1, false);
                }
                volt_user = args[ii + 1];
                ii++;
            } else if (arg.equals("--password")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --password");
                    printHelpAndQuit(-1, false);
                }
                volt_password = args[ii + 1];
                ii++;
            } else if (arg.equals("--pgserver")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --pgserver");
                    printHelpAndQuit(-1, false);
                }
                postgres_server = args[ii + 1];
                ii++;
            } else if (arg.equals("--pgdb")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --pgdb");
                    printHelpAndQuit(-1, false);
                }
                postgres_database = args[ii + 1];
                ii++;
            } else if (arg.equals("--pgschema")) {
                postgres_schema_prefix = args[ii + 1];
                ii++;
            } else if (arg.equals("--pguser")) {
                if (args.length < ii + 1) {
                    System.err.println("Error: Not enough args following --pguser");
                    printHelpAndQuit(-1, false);
                }
                postgres_user = args[ii + 1];
                ii++;
            } else if (arg.equals("--pgpassword")) {
                postgres_password = args[ii + 1];
                ii++;
            }
        }

        if (volt_servers == null || volt_servers.length < 1) {
            System.err.println("PostgreSQLExportClient: must provide at least one VoltDB server");
            printHelpAndQuit(-1, false);
        }
        if (connect == ' ') {
            System.err.println("PostgreSQLExportClient: must specify connection type as admin or client using --connect argument");
            printHelpAndQuit(-1, false);
        }
        assert ((connect == 'c') || (connect == 'a'));
        if (volt_user == null) {
            volt_user = "";
        }
        if (volt_password == null) {
            volt_password = "";
        }

        if (postgres_server == null) {
            System.err.println("PostgreSQLExportClient: must specify a PostgreSQL server");
            printHelpAndQuit(-1, false);
        }
        if (postgres_database == null) {
            System.err.println("PostgreSQLExportClient: must specify a PostgreSQL database");
            printHelpAndQuit(-1, false);
        }
        if (postgres_user == null) {
            System.err.println("PostgreSQLExportClient: must specify a PostgreSQL user");
            printHelpAndQuit(-1, false);
        }
        if (postgres_password == null) {
            postgres_password = "";
        }
        if (postgres_schema_prefix == null) {
            postgres_schema_prefix = "export_";
        }

        PostgreSQLExportClient client = new PostgreSQLExportClient(connect == 'a');

        for (String server: volt_servers)
            client.addServerInfo(server, connect == 'a');

        client.addCredentials(volt_user, volt_password);

        String url = "jdbc:postgresql://" + postgres_server + "/" + postgres_database;
        props.setProperty("user", postgres_user);
        props.setProperty("password", postgres_password);
        try {
            conn = DriverManager.getConnection(url, props);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            m_logger.fatal("Failed to connect to PostgreSQL");
            System.exit(-1);
        }

        try {
            client.run();
        } catch (ExportClientException e) {
            m_logger.error("ExportClient.run() failed");
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                m_logger.error("PostgreSQL connection unable to be closed");
                e.printStackTrace();
            }
        }
    }
}
