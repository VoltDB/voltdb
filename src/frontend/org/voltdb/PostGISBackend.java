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

package org.voltdb;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.regex.Pattern;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

/**
 * A wrapper around a PostgreSQL database server that supports PostGIS (a
 * geospatial extension to PostgreSQL), and its JDBC connection. This class
 * can be used to execute SQL statements instead of the C++ ExecutionEngine.
 * It is currently used only by the SQL Coverage tests (and perhaps, someday,
 * the JUnit regressionsuite tests), specifically those using Geo data.
 */
public class PostGISBackend extends PostgreSQLBackend {

    // Regex pattern for a typical column (or function of a column, etc.),
    // as used in a Geospatial function
    private static final String COLUMN_PATTERN = "(\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+(::\\w+)?(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*";
    // Captures the use of LONGITUDE(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern longitudeQuery = Pattern.compile(
            "LONGITUDE\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of LATITUDE(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern latitudeQuery = Pattern.compile(
            "LATITUDE\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of CENTROID(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern centroidQuery = Pattern.compile(
            "CENTROID\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of AREA(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern areaQuery = Pattern.compile(
            "AREA\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of DISTANCE(column1,column2), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern distanceQuery = Pattern.compile(
            "DISTANCE\\s*\\((?<columns>"+COLUMN_PATTERN+","+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of CONTAINS(column1,column2), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern containsQuery = Pattern.compile(
            "CONTAINS\\s*\\((?<columns>"+COLUMN_PATTERN+","+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);

    static public PostGISBackend initializePostGISBackend(CatalogContext context)
    {
        synchronized(backendLock) {
            if (m_backend == null) {
                try {
                    if (m_permanent_db_backend == null) {
                        m_permanent_db_backend = new PostgreSQLBackend();
                    }
                    Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                    stmt.execute("drop database if exists " + m_database_name + ";");
                    stmt.execute("create database " + m_database_name + ";");
                    m_backend = new PostGISBackend(m_database_name);
                    m_backend.runDDL("create extension postgis;");
                    final String binDDL = context.database.getSchema();
                    final String ddl = Encoder.decodeBase64AndDecompress(binDDL);
                    final String[] commands = ddl.split("\n");
                    for (String command : commands) {
                        String decoded_cmd = Encoder.hexDecodeToString(command);
                        decoded_cmd = decoded_cmd.trim();
                        if (decoded_cmd.length() == 0) {
                            continue;
                        }
                        m_backend.runDDL(decoded_cmd);
                    }
                }
                catch (final Exception e) {
                    hostLog.fatal("Unable to construct PostGIS backend");
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
            }
            return (PostGISBackend) m_backend;
        }
    }

    /** Constructor specifying a (PostgreSQL/PostGIS) 'database', with default
     *  username and password. */
    public PostGISBackend(String databaseName) {
        super(databaseName);
        this.m_database_type = "PostGIS";
    }

    /** Constructor that creates a new PostGISBackend wrapping dbconn, an
     *  existing database connection. */
    public PostGISBackend(Connection dbconn) {
        super(dbconn);
//        this.dbconn = dbconn;
        this.m_database_type = "PostGIS";
    }

    /** Modify a query containing a LONGITUDE(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_X(columnName::geometry), which is an equivalent that PostGIS
     *  does support. */
    static private String transformLongitudeQuery(String dml) {
        return transformQuery(dml, longitudeQuery, "",
                "ST_X(", "::geometry", null, false, false, "column");
    }

    /** Modify a query containing a LATITUDE(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_Y(columnName::geometry), which is an equivalent that PostGIS
     *  does support. */
    static private String transformLatitudeQuery(String dml) {
        return transformQuery(dml, latitudeQuery, "",
                "ST_Y(", "::geometry", null, false, false, "column");
    }

    /** Modify a query containing a CENTROID(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_CENTROID(columnName::geometry), which is an equivalent that PostGIS
     *  does support. */
    static private String transformCentroidQuery(String dml) {
        return transformQuery(dml, centroidQuery, "",
                "ST_CENTROID(", "::geometry", null, false, false, "column");
    }

    /** Modify a query containing an AREA(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_AREA(columnName,FALSE), which is a (spherical Earth) equivalent
     *  that PostGIS does support. */
    static private String transformAreaQuery(String dml) {
        return transformQuery(dml, areaQuery, "",
                "ST_AREA(", ",FALSE", null, false, false, "column");
    }

    /** Modify a query containing a DISTANCE(column1,column2) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_DISTANCE(column1,column2,FALSE), which is a (spherical Earth)
     *  equivalent that PostGIS does support. */
    static private String transformDistanceQuery(String dml) {
        return transformQuery(dml, distanceQuery, "",
                "ST_DISTANCE(", ",FALSE", null, false, false, "columns");
    }

    /** Modify a query containing a CONTAINS(column1,column2) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_COVERS(column1,column2,FALSE), which is a (spherical Earth)
     *  equivalent that PostGIS does support. */
    static private String transformContainsQuery(String dml) {
        return transformQuery(dml, containsQuery, "",
                "ST_COVERS(", "", null, false, false, "columns");
    }

    /** For a SQL DDL statement, replace (VoltDB) keywords not supported by
     *  PostgreSQL/PostGIS with other, similar terms. */
    static public String transformDDL(String ddl) {
        // TODO: make this more robust, using a regex Pattern(s)??
        String modified_ddl = PostgreSQLBackend.transformDDL(ddl)
                .replace("GEOGRAPHY_POINT", "GEOGRAPHY(POINT,4326)")
                .replace("GEOGRAPHY,",      "GEOGRAPHY(POLYGON,4326),");
//        System.out.println("ddl         : " + ddl);
//        System.out.println("modified_ddl: " + modified_ddl);
        return modified_ddl;
    }

    /** For a SQL query, replace keywords not supported by PostgreSQL/PostGIS,
     *  or which behave differently in PostgreSQL/PostGIS than in VoltDB, with
     *  other, similar terms, so that the results will match. */
    static public String transformDML(String dml) {
        return transformCentroidQuery(
                transformAreaQuery(
                    transformDistanceQuery(
                        transformContainsQuery(
                            transformLongitudeQuery(
                                transformLatitudeQuery(
                                    PostgreSQLBackend.transformDML(dml) ))))))
                // TODO: make these more robust, using regex Patterns?
                .replace("pointFromText('POINT",     "ST_GeographyFromText('POINT")
                .replace("polygonFromText('POLYGON", "ST_GeographyFromText('POLYGON")
                .replace("asText",    "ST_AsText");
    }

    /** Modifies DDL statements in such a way that PostGIS results will match
     *  VoltDB results, and then passes the remaining work to the base class
     *  version. */
    @Override
    public void runDDL(String ddl) {
        super.runDDL(transformDDL(ddl), false);
    }

    /** Modifies queries in such a way that PostgreSQL/PostGIS results will
     *  match VoltDB results, and then passes the remaining work to the base
     *  class version. */
    @Override
    public VoltTable runDML(String dml) {
        return super.runDML(transformDML(dml), false);
    }

}
