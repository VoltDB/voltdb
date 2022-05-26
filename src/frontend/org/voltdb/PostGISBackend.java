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

package org.voltdb;

import java.sql.Connection;
import java.sql.Statement;
import java.util.regex.Pattern;

import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;

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

    // Captures the use of AsText(columnName)
    private static final Pattern asTextQuery = Pattern.compile(
            "AsText\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an AsText(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_AsText(columnName), which is an equivalent that PostGIS does support
    private static final QueryTransformer asTextQueryTransformer
            = new QueryTransformer(asTextQuery)
            .prefix("ST_AsText(").groups("column");

    // Captures the use of CAST(columnName AS VARCHAR)
    private static final Pattern castGeoAsVarcharQuery = Pattern.compile(
            "CAST\\s*\\((?<column>"+COLUMN_PATTERN+")\\s*AS\\s*VARCHAR\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a CAST(columnName AS VARCHAR) function,
    // when <i>columnName</i> is of a Geo type (GEOGRAPHY_POINT or GEOGRAPHY),
    // for which PostgreSQL returns the WKB (well-known binary) format for
    // that column value, unlike VoltDB, which returns the WKT (well-known
    // text) format; so change it to: ST_AsText(columnName).
    // Note: this needs to be used after asTextQueryTransformer, not before,
    // or we'll end up with ST_ST_AsText in our queries.
    private static final QueryTransformer castGeoAsVarcharQueryTransformer
            = new QueryTransformer(castGeoAsVarcharQuery)
            .prefix("ST_AsText(").suffix(")").groups("column")
            .columnType(ColumnType.GEO);

    // Captures the use of PointFromText('POINT...
    private static final Pattern pointFromTextQuery = Pattern.compile(
            "PointFromText\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a PointFromText('POINT... function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_GeographyFromText('POINT..., which is an equivalent that PostGIS
    // does support
    private static final QueryTransformer pointFromTextQueryTransformer
            = new QueryTransformer(pointFromTextQuery)
            .replacementText("ST_GeographyFromText(").useWholeMatch();

    // Captures the use of PolygonFromText('POLYGON...
    private static final Pattern polygonFromTextQuery = Pattern.compile(
            "PolygonFromText\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a PointFromText('POINT... function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_GeographyFromText('POLYGON..., which is an equivalent that PostGIS
    // does support
    private static final QueryTransformer polygonFromTextQueryTransformer
            = new QueryTransformer(polygonFromTextQuery)
            .replacementText("ST_GeographyFromText(").useWholeMatch();


    // Captures the use of LONGITUDE(columnName)
    private static final Pattern longitudeQuery = Pattern.compile(
            "LONGITUDE\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a LONGITUDE(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_X(columnName::geometry), which is an equivalent that PostGIS
    // does support
    private static final QueryTransformer longitudeQueryTransformer
            = new QueryTransformer(longitudeQuery)
            .prefix("ST_X(").suffix("::geometry").groups("column");

    // Captures the use of LATITUDE(columnName)
    private static final Pattern latitudeQuery = Pattern.compile(
            "LATITUDE\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a LONGITUDE(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_Y(columnName::geometry), which is an equivalent that PostGIS
    // does support
    private static final QueryTransformer latitudeQueryTransformer
            = new QueryTransformer(latitudeQuery)
            .prefix("ST_Y(").suffix("::geometry").groups("column");

    // Captures the use of NumPoints(columnName)
    private static final Pattern numPointsQuery = Pattern.compile(
            "NumPoints\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a NumPoints(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_NPoints(columnName::geometry), which is an equivalent
    // that PostGIS does support
    private static final QueryTransformer numPointsQueryTransformer
            = new QueryTransformer(numPointsQuery)
            .prefix("ST_NPoints(").suffix("::geometry").groups("column");

    // Captures the use of NumInteriorRings(columnName)
    private static final Pattern numInteriorRingsQuery = Pattern.compile(
            "NumInteriorRings\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a NumInteriorRings(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_NumInteriorRings(columnName::geometry), which is an equivalent
    // that PostGIS does support
    private static final QueryTransformer numInteriorRingsQueryTransformer
            = new QueryTransformer(numInteriorRingsQuery)
            .prefix("ST_NumInteriorRings(").suffix("::geometry").groups("column");

    // Captures the use of IsValid(columnName)
    private static final Pattern isValidQuery = Pattern.compile(
            "IsValid\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an IsValid(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_IsValid(columnName::geometry), which is an equivalent
    // that PostGIS does support
    private static final QueryTransformer isValidQueryTransformer
            = new QueryTransformer(isValidQuery)
            .prefix("ST_IsValid(").suffix("::geometry").groups("column");

    // Captures the use of IsInvalidReason(columnName)
    private static final Pattern isInvalidReasonQuery = Pattern.compile(
            "IsInvalidReason\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an IsInvalidReason(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_IsValidReason(columnName::geometry), which is an equivalent
    // that PostGIS does support
    private static final QueryTransformer isInvalidReasonQueryTransformer
            = new QueryTransformer(isInvalidReasonQuery)
            .prefix("ST_IsValidReason(").suffix("::geometry").groups("column");


    // Captures the use of CONTAINS(column1,column2)
    private static final Pattern containsQuery = Pattern.compile(
            "CONTAINS\\s*\\((?<columns>"+COLUMN_PATTERN+","+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a CONTAINS(column1,column2) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_COVERS(column1,column2), which is a (spherical Earth) equivalent
    // that PostGIS does support
    private static final QueryTransformer containsQueryTransformer
            = new QueryTransformer(containsQuery)
            .prefix("ST_COVERS(").groups("columns");

    // Captures the use of DISTANCE(column1,column2)
    private static final Pattern distanceQuery = Pattern.compile(
            "DISTANCE\\s*\\((?<columns>"+COLUMN_PATTERN+","+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a DISTANCE(column1,column2) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_DISTANCE(column1,column2,FALSE), which is a (spherical Earth)
    // equivalent that PostGIS does support
    private static final QueryTransformer distanceQueryTransformer
            = new QueryTransformer(distanceQuery)
            .prefix("ST_DISTANCE(").suffix(",FALSE").groups("columns");

    // Captures the use of AREA(columnName)
    private static final Pattern areaQuery = Pattern.compile(
            "AREA\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an AREA(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_AREA(columnName,FALSE), which is a (spherical Earth) equivalent
    // that PostGIS does support
    private static final QueryTransformer areaQueryTransformer
            = new QueryTransformer(areaQuery)
            .prefix("ST_AREA(").suffix(",FALSE").groups("column");

    // Captures the use of CENTROID(columnName)
    private static final Pattern centroidQuery = Pattern.compile(
            "CENTROID\\s*\\((?<column>"+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a CENTROID(columnName) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_CENTROID(columnName::geometry), which is a (spherical Earth)
    // equivalent that PostGIS does support
    private static final QueryTransformer centroidQueryTransformer
            = new QueryTransformer(centroidQuery)
            .prefix("ST_CENTROID(").suffix("::geometry").groups("column");

    // Captures the use of DWithin(column1,column2,column3)
    private static final Pattern dWithinQuery = Pattern.compile(
            "DWithin\\s*\\((?<columns>"+COLUMN_PATTERN+","+COLUMN_PATTERN+","+COLUMN_PATTERN+"\\))",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a DWithin(column1,column2,column3) function,
    // which PostgreSQL/PostGIS does not support, and replaces it with
    // ST_DWithin(column1,column2,column3,FALSE), which is a (spherical Earth)
    // equivalent that PostGIS does support
    private static final QueryTransformer dWithinQueryTransformer
            = new QueryTransformer(dWithinQuery)
            .prefix("ST_DWithin(").suffix(",FALSE").groups("columns");

    // Captures the use of GEOGRAPHY_POINT (in DDL)
    private static final Pattern geographyPointDdl = Pattern.compile(
            "(?<point>GEOGRAPHY_POINT)", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing GEOGRAPHY_POINT, which
    // PostgreSQL/PostGIS does not support, and replaces it with
    // GEOGRAPHY(POINT,4326), which is an equivalent that PostGIS does
    // support. Note: 4326 is the standard, spheroidal SRIS/EPSG normally
    // used by PostGIS; we might wish to change this to use a sphere, if
    // we can find an appropriate SRIS to use, which PostGIS supports
    // (possibly 3857?)
    private static final QueryTransformer geographyPointDdlTransformer
            = new QueryTransformer(geographyPointDdl).groups("point")
            .replacementText("GEOGRAPHY(POINT,4326)").useWholeMatch();

    // Captures the use of GEOGRAPHY (in DDL)
    private static final Pattern geographyDdl = Pattern.compile(
            "(?<polygon>GEOGRAPHY)(?!(_|\\s*\\(\\s*)POINT)", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing GEOGRAPHY, which PostgreSQL/PostGIS
    // does not support, and replaces it with GEOGRAPHY(POLYGON,4326), which
    // is an equivalent that PostGIS does support. Note: 4326 is the standard,
    // spheroidal SRIS/EPSG normally used by PostGIS; we might wish to change
    // this to use a sphere, if we can find an appropriate SRIS to use, which
    // PostGIS supports (possibly 3857?)
    private static final QueryTransformer geographyDdlTransformer
            = new QueryTransformer(geographyDdl).groups("polygon")
            .replacementText("GEOGRAPHY(POLYGON,4326)").useWholeMatch();

    static public PostGISBackend initializePostGISBackend(CatalogContext context)
    {
        synchronized(backendLock) {
            try {
                if (m_permanent_db_backend == null) {
                    m_permanent_db_backend = new PostgreSQLBackend();
                }
                if (m_backend == null) {
                    Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                    stmt.execute("drop database if exists " + m_database_name + ";");
                    stmt.execute("create database " + m_database_name + ";");
                    m_backend = new PostGISBackend(m_database_name);
                    m_backend.runDDL("create extension postgis;");
                    final String binDDL = context.database.getSchema();
                    final String ddl = CompressionService.decodeBase64AndDecompress(binDDL);
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
            }
            catch (final Exception e) {
                hostLog.fatal("Unable to construct PostGIS backend");
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
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
        this.m_database_type = "PostGIS";
    }

    /** For a SQL DDL statement, replace (VoltDB) keywords not supported by
     *  PostgreSQL/PostGIS with other, similar terms. */
    @Override
    public String transformDDL(String ddl) {
        return transformQuery(super.transformDDL(ddl),
                geographyPointDdlTransformer, geographyDdlTransformer);
    }

    /** For a SQL query, replace (VoltDB) keywords not supported by
     *  PostgreSQL/PostGIS, or which behave differently in PostgreSQL/PostGIS
     *  than in VoltDB, with other, similar terms, so that the results will match. */
    @Override
    public String transformDML(String dml) {
        return transformQuery(super.transformDML(dml),
                asTextQueryTransformer,        castGeoAsVarcharQueryTransformer,
                pointFromTextQueryTransformer, polygonFromTextQueryTransformer,
                longitudeQueryTransformer,     latitudeQueryTransformer,
                isValidQueryTransformer,       isInvalidReasonQueryTransformer,
                numPointsQueryTransformer,     numInteriorRingsQueryTransformer,
                containsQueryTransformer,      distanceQueryTransformer,
                areaQueryTransformer,          centroidQueryTransformer,
                dWithinQueryTransformer);
    }

    /** Modifies DDL statements in such a way that PostGIS results will match
     *  VoltDB results, and then passes the remaining work to the base class
     *  version. */
    @Override
    public void runDDL(String ddl) {
        String modifiedDdl = transformDDL(ddl);
        printTransformedSql(ddl, modifiedDdl);
        super.runDDL(modifiedDdl, false);
    }

    /** Modifies queries in such a way that PostgreSQL/PostGIS results will
     *  match VoltDB results, and then passes the remaining work to the base
     *  class version. */
    @Override
    public VoltTable runDML(String dml) {
        String modifiedDml = transformDML(dml);
        printTransformedSql(dml, modifiedDml);
        return super.runDML(modifiedDml, false);
    }

}
