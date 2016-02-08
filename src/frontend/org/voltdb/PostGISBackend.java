/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
    // Captures the use of NumPoints(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern numPointsQuery = Pattern.compile(
            "NumPoints\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of NumPoints(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern numInteriorRingsQuery = Pattern.compile(
            "NumInteriorRings\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of IsValid(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern isValidQuery = Pattern.compile(
            "IsValid\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of IsInvalidReason(columnName), which PostgreSQL/PostGIS
    // does not support
    private static final Pattern isInvalidReasonQuery = Pattern.compile(
            "IsInvalidReason\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Captures the use of CAST(columnName AS VARCHAR), which PostgreSQL/PostGIS
    // handles differently, when the columnName is of type GEOGRAPHY_POINT or
    // GEOGRAPHY
    private static final Pattern castGeoAsVarcharQuery = Pattern.compile(
            "CAST\\s*\\((?<column>"+COLUMN_PATTERN+")\\s*AS\\s*VARCHAR\\)",
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
                "ST_X(", "::geometry", null, false, null, "column");
    }

    /** Modify a query containing a LATITUDE(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_Y(columnName::geometry), which is an equivalent that PostGIS
     *  does support. */
    static private String transformLatitudeQuery(String dml) {
        return transformQuery(dml, latitudeQuery, "",
                "ST_Y(", "::geometry", null, false, null, "column");
    }

    /** Modify a query containing a CENTROID(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_CENTROID(columnName::geometry), which is an equivalent that PostGIS
     *  does support. */
    static private String transformCentroidQuery(String dml) {
        return transformQuery(dml, centroidQuery, "",
                "ST_CENTROID(", "::geometry", null, false, null, "column");
    }

    /** Modify a query containing an AREA(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_AREA(columnName,FALSE), which is a (spherical Earth) equivalent
     *  that PostGIS does support. */
    static private String transformAreaQuery(String dml) {
        return transformQuery(dml, areaQuery, "",
                "ST_AREA(", ",FALSE", null, false, null, "column");
    }

    /** Modify a query containing a DISTANCE(column1,column2) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_DISTANCE(column1,column2,FALSE), which is a (spherical Earth)
     *  equivalent that PostGIS does support. */
    static private String transformDistanceQuery(String dml) {
        return transformQuery(dml, distanceQuery, "",
                "ST_DISTANCE(", ",FALSE", null, false, null, "columns");
    }

    /** Modify a query containing a CONTAINS(column1,column2) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_COVERS(column1,column2,FALSE), which is a (spherical Earth)
     *  equivalent that PostGIS does support. */
    static private String transformContainsQuery(String dml) {
        return transformQuery(dml, containsQuery, "",
                "ST_COVERS(", "", null, false, null, "columns");
    }

    /** Modify a query containing an NumPoints(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_NPoints(columnName::geometry), which is an equivalent that PostGIS
     *  does support. */
    static private String transformNumPointsQuery(String dml) {
        return transformQuery(dml, numPointsQuery, "",
                "ST_NPoints(", "::geometry", null, false, null, "column");
    }

    /** Modify a query containing an NumInteriorRings(columnName) function,
     *  which PostgreSQL/PostGIS does not support, and replace it with
     *  ST_NumInteriorRings(columnName::geometry), which is an equivalent that
     *  PostGIS does support. */
    static private String transformNumInteriorRingsQuery(String dml) {
        return transformQuery(dml, numInteriorRingsQuery, "",
                "ST_NumInteriorRings(", "::geometry", null, false, null, "column");
    }

    /** Modify a query containing an IsValid(columnName) function, which
     *  PostgreSQL/PostGIS does not support, and replace it with
     *  ST_IsValid(columnName::geometry), which is an equivalent that PostGIS
     *  does support. */
    static private String transformIsValidQuery(String dml) {
        return transformQuery(dml, isValidQuery, "",
                "ST_IsValid(", "::geometry", null, false, null, "column");
    }

    /** Modify a query containing an IsInvalidReason(columnName) function,
     *  which PostgreSQL/PostGIS does not support, and replace it with
     *  ST_IsValidReason(columnName::geometry), which is an equivalent that
     *  PostGIS does support. */
    static private String transformIsInvalidReasonQuery(String dml) {
        return transformQuery(dml, isInvalidReasonQuery, "",
                "ST_IsValidReason(", "::geometry", null, false, null, "column");
    }

    /** Modify a query containing a CAST(columnName AS VARCHAR), where
     *  <i>columnName</i> is of a Geo type (GEOGRAPHY_POINT or GEOGRAPHY),
     *  for which PostgreSQL returns the WKB (well-known binary) format for
     *  that column value, unlike VoltDB, which returns the WKT (well-known
     *  text) format; so change it to: ST_AsText(columnName). */
    static private String transformCastGeoAsVarcharQuery(String dml) {
        return transformQuery(dml, castGeoAsVarcharQuery, "",
                "AsText(", ")", null, false, null, "column");
    }

    /** For a SQL DDL statement, replace (VoltDB) keywords not supported by
     *  PostgreSQL/PostGIS with other, similar terms. */
    static public String transformDDL(String ddl) {
        // 4326 is the standard, spheroidal SRIS/EPSG normally used by PostGIS;
        // we may wish to change this to use a sphere, if we can find an
        // appropriate SRIS to use, which PostGIS supports (possibly 3857?)
        String modified_ddl = PostgreSQLBackend.transformDDL(ddl)
                // TODO: make these more robust, using regex Patterns??
                .replace("GEOGRAPHY_POINT", "GEOGRAPHY(POINT,4326)")
                .replace("GEOGRAPHY,",      "GEOGRAPHY(POLYGON,4326),");
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
                                    transformNumPointsQuery(
                                        transformNumInteriorRingsQuery(
                                            transformIsValidQuery(
                                                transformIsInvalidReasonQuery(
                                                    transformCastGeoAsVarcharQuery(
                                                        PostgreSQLBackend.transformDML(dml)
               ))   )   )   )   )   )   )   )   )   )
                // TODO: make these more robust, using regex Patterns?
                .replace("pointFromText('POINT",     "ST_GeographyFromText('POINT")
                .replace("polygonFromText('POLYGON", "ST_GeographyFromText('POLYGON")
                .replace("AsText",    "ST_AsText")
                .replace("asText",    "ST_AsText");
    }

    /** Modifies DDL statements in such a way that PostGIS results will match
     *  VoltDB results, and then passes the remaining work to the base class
     *  version. */
    @Override
    public void runDDL(String ddl) {
        String modifiedDdl = transformDDL(ddl);
        debugPrintTransformSql(ddl, modifiedDdl, ddl != null && !ddl.equals(modifiedDdl));
        super.runDDL(modifiedDdl, false);
    }

    /** Modifies queries in such a way that PostgreSQL/PostGIS results will
     *  match VoltDB results, and then passes the remaining work to the base
     *  class version. */
    @Override
    public VoltTable runDML(String dml) {
        String modifiedDml = transformDML(dml);
        debugPrintTransformSql(dml, modifiedDml, dml != null && !dml.equals(modifiedDml));
        return super.runDML(modifiedDml, false);
    }

}
