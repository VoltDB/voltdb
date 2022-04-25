/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.hsqldb_voltpatches;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.hsqldb_voltpatches.types.Type;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;

/**
 * Implementation of calls to VoltDB functions that may have no SQL standard equivalent.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class FunctionForVoltDB extends FunctionSQL {
    private static final VoltLogger m_logger = new VoltLogger("UDF");

    public static class FunctionDescriptor {
        final private String m_name;
        final private int m_id;
        final private Type m_type;
        final private int m_typeParameter;
        final private Type[] m_paramTypes;
        final private short[] m_paramParseList;
        final private short[] m_paramParseListAlt;
        final private boolean m_isAggregate; // this shows whether this is a scalar function or aggregate function

        public String getName() {
            return m_name;
        }

        public int getId() {
            return m_id;
        }

        public Type getDataType() {
            return m_type;
        }

        public Type[] getParamTypes() {
            return m_paramTypes;
        }

        public short[] getParamParseList() {
            return m_paramParseList;
        }

        public short[] getParamParseListAlt() {
            return m_paramParseListAlt;
        }

        public boolean isAggregate() {
            return m_isAggregate;
        }

        private FunctionDescriptor(String name, Type type, int id, int typeParameter, Type[] paramTypes, short[] paramParseList) {
            this(name, type, id, typeParameter, paramTypes, paramParseList, null, false);
        }
        
        private FunctionDescriptor(String name, Type type, int id, int typeParameter, Type[] paramTypes, short[] paramParseList, boolean isAgg) {
            this(name, type, id, typeParameter, paramTypes, paramParseList, null, isAgg);
        }
        
        
        private FunctionDescriptor(String name, Type type, int id, int typeParameter, Type[] paramTypes, short[] paramParseList, short[] paramParseListAlt) {
            this(name, type, id, typeParameter, paramTypes, paramParseList, paramParseListAlt, false);
        }

        private FunctionDescriptor(String name, Type type, int id, int typeParameter, Type[] paramTypes, short[] paramParseList, short[] paramParseListAlt, boolean isAgg) {
            m_name = name;
            m_type = type;
            m_id = id;
            m_typeParameter = typeParameter;
            m_paramTypes = paramTypes;
            m_paramParseList = paramParseList;
            m_paramParseListAlt = paramParseListAlt;
            m_isAggregate = isAgg;
        }

        static final int FUNC_VOLT_ID_NOT_DEFINED               = -1;

        // These ID numbers need to be unique values for FunctionSQL.functType.
        // Assume that 1-19999 are reserved for existing HSQL functions.
        // That leaves new VoltDB-specific functions free to use values in the 20000s.
        static final int FUNC_CONCAT                     = 124;

        private static final int FUNC_VOLT_SQL_ERROR     = 20000;
        private static final int FUNC_VOLT_DECODE        = 20001;
        public static final int FUNC_VOLT_FIELD          = 20002;
        public static final int FUNC_VOLT_ARRAY_ELEMENT  = 20003;
        public static final int FUNC_VOLT_ARRAY_LENGTH   = 20004;

        static final int FUNC_VOLT_SINCE_EPOCH               = 20005;
        static final int FUNC_VOLT_SINCE_EPOCH_SECOND        = 20006;
        static final int FUNC_VOLT_SINCE_EPOCH_MILLISECOND   = 20007;
        static final int FUNC_VOLT_SINCE_EPOCH_MICROSECOND   = 20008;

        static final int FUNC_VOLT_TO_TIMESTAMP              = 20009;
        static final int FUNC_VOLT_TO_TIMESTAMP_SECOND       = 20010;
        static final int FUNC_VOLT_TO_TIMESTAMP_MILLISECOND  = 20011;
        static final int FUNC_VOLT_TO_TIMESTAMP_MICROSECOND  = 20012;

        static final int FUNC_VOLT_TRUNCATE_TIMESTAMP     = 20013;
        static final int FUNC_VOLT_TRUNCATE_YEAR          = 20014;
        static final int FUNC_VOLT_TRUNCATE_QUARTER       = 20015;
        static final int FUNC_VOLT_TRUNCATE_MONTH         = 20016;
        static final int FUNC_VOLT_TRUNCATE_DAY           = 20017;
        static final int FUNC_VOLT_TRUNCATE_HOUR          = 20018;
        static final int FUNC_VOLT_TRUNCATE_MINUTE        = 20019;
        static final int FUNC_VOLT_TRUNCATE_SECOND        = 20020;
        static final int FUNC_VOLT_TRUNCATE_MILLISECOND   = 20021;
        static final int FUNC_VOLT_TRUNCATE_MICROSECOND   = 20022;

        static final int FUNC_VOLT_FROM_UNIXTIME          = 20023;

        public static final int FUNC_VOLT_SET_FIELD       = 20024;

        static final int FUNC_VOLT_FORMAT_CURRENCY        = 20025;


        public static final int FUNC_VOLT_BITNOT          = 20026;
        public static final int FUNC_VOLT_BIT_SHIFT_LEFT  = 20027;
        public static final int FUNC_VOLT_BIT_SHIFT_RIGHT = 20028;
        public static final int FUNC_VOLT_HEX             = 20029;
        static final int FUNC_VOLT_BIN                    = 20030;

        static final int FUNC_VOLT_DATEADD                = 20031;
        static final int FUNC_VOLT_DATEADD_YEAR           = 20032;
        static final int FUNC_VOLT_DATEADD_QUARTER        = 20033;
        static final int FUNC_VOLT_DATEADD_MONTH          = 20034;
        static final int FUNC_VOLT_DATEADD_DAY            = 20035;
        static final int FUNC_VOLT_DATEADD_HOUR           = 20036;
        static final int FUNC_VOLT_DATEADD_MINUTE         = 20037;
        static final int FUNC_VOLT_DATEADD_SECOND         = 20038;
        static final int FUNC_VOLT_DATEADD_MILLISECOND    = 20039;
        static final int FUNC_VOLT_DATEADD_MICROSECOND    = 20040;
        static final int FUNC_VOLT_REGEXP_POSITION        = 20041;

        static final int FUNC_VOLT_ROUND                  = 20042;
        static final int FUNC_VOLT_STR                    = 20043;

        // our local functions for networking
        public static final int FUNC_VOLT_INET_NTOA       = 20044;
        public static final int FUNC_VOLT_INET_ATON       = 20045;
        public static final int FUNC_VOLT_INET6_NTOA      = 20046;
        public static final int FUNC_VOLT_INET6_ATON      = 20047;

        // Geospatial functions
        static final int FUNC_VOLT_POINTFROMTEXT                = 21000;
        static final int FUNC_VOLT_POLYGONFROMTEXT              = 21001;
        static final int FUNC_VOLT_CONTAINS                     = 21002;
        static final int FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS   = 21003;
        static final int FUNC_VOLT_POLYGON_NUM_POINTS           = 21004;
        static final int FUNC_VOLT_POINT_LATITUDE               = 21005;
        static final int FUNC_VOLT_POINT_LONGITUDE              = 21006;
        static final int FUNC_VOLT_POLYGON_CENTROID             = 21007;
        static final int FUNC_VOLT_POLYGON_AREA                 = 21008;
        static final int FUNC_VOLT_DISTANCE                     = 21009;    // wrapper id for distance between all geo types
        static final int FUNC_VOLT_DISTANCE_POINT_POINT         = 21010;    // distance between point and point
        static final int FUNC_VOLT_DISTANCE_POLYGON_POINT       = 21011;    // distance between polygon and point
        static final int FUNC_VOLT_ASTEXT                       = 21012;    // wrapper for asText function for all geo types
        static final int FUNC_VOLT_ASTEXT_GEOGRAPHY_POINT       = 21013;    // point to text
        static final int FUNC_VOLT_ASTEXT_GEOGRAPHY             = 21014;    // polygon to text
        static final int FUNC_VOLT_VALIDATE_POLYGON             = 21015;    // Polygon validation.
        static final int FUNC_VOLT_POLYGON_INVALID_REASON       = 21016;    // Reason a polygon may be invalid.
        static final int FUNC_VOLT_DWITHIN                      = 21017;    // wrapper id for function that evaluates if two geo objects are within
                                                                            // certain distance of each other
        static final int FUNC_VOLT_DWITHIN_POINT_POINT          = 21018;    // if two points are within certain distance of each other
        static final int FUNC_VOLT_DWITHIN_POLYGON_POINT        = 21019;    // if a polygon and a point are within certain distance of each other
        static final int FUNC_VOLT_VALIDPOLYGONFROMTEXT         = 21020;    // Like polygonFromText, but fixes invalid polygons if possible, and
                                                                            // will throw if the input is invalid and not fixable.
        static final int FUNC_VOLT_MIN_VALID_TIMESTAMP          = 21021;    // Minimum valid timestamp.
        static final int FUNC_VOLT_MAX_VALID_TIMESTAMP          = 21022;    // Maximum valid timestamp.
        static final int FUNC_VOLT_IS_VALID_TIMESTAMP           = 21023;    // Is a timestamp value in range?
        static final int FUNC_VOLT_MAKE_VALID_POLYGON           = 21024;    // Make an invalid polygon valid by reversing rings.
                                                                            // Note: This will only correct orientation errors.
        static final int FUNC_VOLT_FORMAT_TIMESTAMP             = 21025;    // Convert a timestamp to a String in a given timezone.
        public static final int FUNC_VOLT_MIGRATING             = 21026;    // Check if the row is migrating.

        static final int FUNC_VOLT_DATETIME_DIFF                = 21027;
        static final int FUNC_VOLT_DATETIME_DIFF_YEAR           = 21028;
        static final int FUNC_VOLT_DATETIME_DIFF_QUARTER        = 21029;
        static final int FUNC_VOLT_DATETIME_DIFF_MONTH          = 21030;
        static final int FUNC_VOLT_DATETIME_DIFF_WEEK           = 21031;
        static final int FUNC_VOLT_DATETIME_DIFF_DAY            = 21032;
        static final int FUNC_VOLT_DATETIME_DIFF_HOUR           = 21033;
        static final int FUNC_VOLT_DATETIME_DIFF_MINUTE         = 21034;
        static final int FUNC_VOLT_DATETIME_DIFF_SECOND         = 21035;
        static final int FUNC_VOLT_DATETIME_DIFF_MILLIS         = 21036;
        static final int FUNC_VOLT_DATETIME_DIFF_MICROS         = 21037;

        static final int FUNC_VOLT_TIME_WINDOW                       = 21038;
        static final int FUNC_VOLT_TIME_WINDOW_YEAR_START            = 21039;
        static final int FUNC_VOLT_TIME_WINDOW_QUARTER_START         = 21040;
        static final int FUNC_VOLT_TIME_WINDOW_MONTH_START           = 21041;
        static final int FUNC_VOLT_TIME_WINDOW_WEEK_START            = 21042;
        static final int FUNC_VOLT_TIME_WINDOW_DAY_START             = 21043;
        static final int FUNC_VOLT_TIME_WINDOW_HOUR_START            = 21044;
        static final int FUNC_VOLT_TIME_WINDOW_MINUTE_START          = 21045;
        static final int FUNC_VOLT_TIME_WINDOW_SECOND_START          = 21046;
        static final int FUNC_VOLT_TIME_WINDOW_MILLIS_START          = 21047;

        static final int FUNC_VOLT_TIME_WINDOW_YEAR_END            = 21048;
        static final int FUNC_VOLT_TIME_WINDOW_QUARTER_END         = 21049;
        static final int FUNC_VOLT_TIME_WINDOW_MONTH_END           = 21050;
        static final int FUNC_VOLT_TIME_WINDOW_WEEK_END            = 21051;
        static final int FUNC_VOLT_TIME_WINDOW_DAY_END             = 21052;
        static final int FUNC_VOLT_TIME_WINDOW_HOUR_END            = 21053;
        static final int FUNC_VOLT_TIME_WINDOW_MINUTE_END          = 21054;
        static final int FUNC_VOLT_TIME_WINDOW_SECOND_END          = 21055;
        static final int FUNC_VOLT_TIME_WINDOW_MILLIS_END          = 21056;

        /*
         * All VoltDB user-defined functions must have IDs in this range.
         */
        static final int FUNC_VOLT_UDF_ID_START                 = 1000000;

        /*
         * Note: The name must be all lower case.
         */
        private static final FunctionDescriptor[] instances = {

            new FunctionDescriptor("sql_error", null, FUNC_VOLT_SQL_ERROR, 0,
                    new Type[] { null, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("bit_shift_left", Type.SQL_BIGINT, FUNC_VOLT_BIT_SHIFT_LEFT, -1,
                    new Type[] { Type.SQL_BIGINT, Type.SQL_BIGINT },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("bit_shift_right", Type.SQL_BIGINT, FUNC_VOLT_BIT_SHIFT_RIGHT, -1,
                    new Type[] { Type.SQL_BIGINT, Type.SQL_BIGINT },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("decode", null, FUNC_VOLT_DECODE, 2,
                    new Type[] { null, null },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.X_REPEAT, 2, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("field", Type.SQL_VARCHAR, FUNC_VOLT_FIELD, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET}),

            new FunctionDescriptor("set_field", Type.SQL_VARCHAR, FUNC_VOLT_SET_FIELD, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("array_element", Type.SQL_VARCHAR, FUNC_VOLT_ARRAY_ELEMENT, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_INTEGER },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET}),

            new FunctionDescriptor("array_length", Type.SQL_INTEGER, FUNC_VOLT_ARRAY_LENGTH, -1,
                    new Type[] { Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.CLOSEBRACKET}),

            new FunctionDescriptor("since_epoch", Type.SQL_BIGINT, FUNC_VOLT_SINCE_EPOCH, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_TIMESTAMP },
                    new short[] {  Tokens.OPENBRACKET, Tokens.X_KEYSET, 5,
                    Tokens.SECOND, Tokens.MILLIS, Tokens.MICROS,
                    Tokens.MILLISECOND, Tokens.MICROSECOND,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("to_timestamp", Type.SQL_TIMESTAMP, FUNC_VOLT_TO_TIMESTAMP, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_BIGINT },
                    new short[] {  Tokens.OPENBRACKET, Tokens.X_KEYSET, 5,
                    Tokens.SECOND, Tokens.MILLIS, Tokens.MICROS,
                    Tokens.MILLISECOND, Tokens.MICROSECOND,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("truncate", Type.SQL_TIMESTAMP, FUNC_VOLT_TRUNCATE_TIMESTAMP, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_TIMESTAMP },
                    new short[] {  Tokens.OPENBRACKET, Tokens.X_KEYSET, 11,
                    Tokens.YEAR, Tokens.QUARTER, Tokens.MONTH, Tokens.DAY, Tokens.HOUR,
                    Tokens.MINUTE, Tokens.SECOND, Tokens.MILLIS, Tokens.MILLISECOND,
                    Tokens.MICROS, Tokens.MICROSECOND,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("from_unixtime", Type.SQL_TIMESTAMP, FUNC_VOLT_FROM_UNIXTIME, -1,
                    new Type[] { Type.SQL_BIGINT },
                    singleParamList),

            new FunctionDescriptor("format_currency", Type.SQL_VARCHAR, FUNC_VOLT_FORMAT_CURRENCY, -1,
                    new Type[] { Type.SQL_DECIMAL, Type.SQL_INTEGER},
                    doubleParamList),

            new FunctionDescriptor("round", Type.SQL_DECIMAL, FUNC_VOLT_ROUND, -1,
                    new Type[] { Type.SQL_DECIMAL, Type.SQL_INTEGER},
                    doubleParamList),

            new FunctionDescriptor("str", Type.SQL_VARCHAR, FUNC_VOLT_STR, -1,
                    new Type[] { Type.SQL_DECIMAL, Type.SQL_INTEGER, Type.SQL_INTEGER},
                    new short[] {  Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.X_OPTION, 6, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("bitnot", Type.SQL_BIGINT, FUNC_VOLT_BITNOT, -1,
                    new Type[] { Type.SQL_BIGINT },
                    singleParamList),

            new FunctionDescriptor("concat", Type.SQL_VARCHAR, FUNC_CONCAT, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.X_REPEAT, 2, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("hex", Type.SQL_VARCHAR, FUNC_VOLT_HEX, -1,
                    new Type[] { Type.SQL_BIGINT },
                    singleParamList),

            new FunctionDescriptor("bin", Type.SQL_VARCHAR, FUNC_VOLT_BIN, -1,
                    new Type[] { Type.SQL_BIGINT },
                    singleParamList),

            new FunctionDescriptor("dateadd", Type.SQL_TIMESTAMP, FUNC_VOLT_DATEADD, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_BIGINT, Type.SQL_TIMESTAMP },
                    new short[] { Tokens.OPENBRACKET, Tokens.X_KEYSET, 11, Tokens.YEAR,
                                  Tokens.QUARTER, Tokens.MONTH, Tokens.DAY, Tokens.HOUR, Tokens.MINUTE, Tokens.SECOND,
                                  Tokens.MILLIS, Tokens.MILLISECOND, Tokens.MICROS, Tokens.MICROSECOND, Tokens.COMMA,
                                  Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionDescriptor("regexp_position", Type.SQL_BIGINT, FUNC_VOLT_REGEXP_POSITION, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET}),

            new FunctionDescriptor("pointfromtext", Type.VOLT_GEOGRAPHY_POINT, FUNC_VOLT_POINTFROMTEXT, -1,
                    new Type[] { Type.SQL_VARCHAR },
                    singleParamList),

            new FunctionDescriptor("polygonfromtext", Type.VOLT_GEOGRAPHY, FUNC_VOLT_POLYGONFROMTEXT, -1,
                    new Type[] { Type.SQL_VARCHAR },
                    singleParamList),
            new FunctionDescriptor("contains", Type.SQL_BOOLEAN, FUNC_VOLT_CONTAINS, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY, Type.VOLT_GEOGRAPHY_POINT },
                    doubleParamList),

            new FunctionDescriptor("numinteriorring", Type.SQL_INTEGER, FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY },
                    singleParamList),

             // numinteriorrings is alias of numinteriorring
            new FunctionDescriptor("numinteriorrings", Type.SQL_INTEGER, FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY },
                    singleParamList),

            new FunctionDescriptor("numpoints", Type.SQL_INTEGER, FUNC_VOLT_POLYGON_NUM_POINTS, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY },
                    singleParamList),

            new FunctionDescriptor("latitude", Type.SQL_DOUBLE, FUNC_VOLT_POINT_LATITUDE, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY_POINT },
                    singleParamList),

            new FunctionDescriptor("longitude", Type.SQL_DOUBLE, FUNC_VOLT_POINT_LONGITUDE, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY_POINT },
                    singleParamList),

            new FunctionDescriptor("centroid", Type.VOLT_GEOGRAPHY_POINT, FUNC_VOLT_POLYGON_CENTROID, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY },
                    singleParamList),

            new FunctionDescriptor("area", Type.SQL_DOUBLE, FUNC_VOLT_POLYGON_AREA, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY },
                    singleParamList),

            new FunctionDescriptor("distance", Type.SQL_DOUBLE, FUNC_VOLT_DISTANCE, -1,
                    new Type[] { Type.SQL_ALL_TYPES, Type.SQL_ALL_TYPES },
                    doubleParamList),

            new FunctionDescriptor("astext", Type.SQL_VARCHAR, FUNC_VOLT_ASTEXT, -1,
                    new Type[] { Type.SQL_ALL_TYPES },
                    singleParamList),

            new FunctionDescriptor("isvalid", Type.SQL_BOOLEAN, FUNC_VOLT_VALIDATE_POLYGON, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY },
                    singleParamList),

            new FunctionDescriptor("isinvalidreason", Type.SQL_VARCHAR, FUNC_VOLT_POLYGON_INVALID_REASON, -1,
                    new Type[] { Type.VOLT_GEOGRAPHY },
                    singleParamList),

            new FunctionDescriptor("dwithin", Type.SQL_BOOLEAN, FUNC_VOLT_DWITHIN, -1,
                    new Type[] { Type.SQL_ALL_TYPES, Type.SQL_ALL_TYPES, Type.SQL_DOUBLE },
                    new short[] {  Tokens.OPENBRACKET,
                                   Tokens.QUESTION, Tokens.COMMA,
                                   Tokens.QUESTION, Tokens.COMMA,
                                   Tokens.QUESTION,
                                   Tokens.CLOSEBRACKET }),
            new FunctionDescriptor("validpolygonfromtext", Type.VOLT_GEOGRAPHY, FUNC_VOLT_VALIDPOLYGONFROMTEXT, -1,
                    new Type[] { Type.SQL_VARCHAR },
                    singleParamList),

            new FunctionDescriptor("min_valid_timestamp", Type.SQL_TIMESTAMP, FUNC_VOLT_MIN_VALID_TIMESTAMP, -1,
                    new Type[] {},
                    emptyParamList,
                    noParamList),
            new FunctionDescriptor("max_valid_timestamp", Type.SQL_TIMESTAMP, FUNC_VOLT_MAX_VALID_TIMESTAMP, -1,
                    new Type[] {},
                    emptyParamList,
                    noParamList),
            new FunctionDescriptor("is_valid_timestamp", Type.SQL_BOOLEAN, FUNC_VOLT_IS_VALID_TIMESTAMP, -1,
                    new Type[] { Type.SQL_TIMESTAMP },
                    singleParamList),

            new FunctionDescriptor("inet_ntoa", Type.SQL_VARCHAR, FUNC_VOLT_INET_NTOA, -1,
                    new Type[] { Type.SQL_BIGINT },
                    singleParamList),

            new FunctionDescriptor("inet_aton", Type.SQL_BIGINT, FUNC_VOLT_INET_ATON, -1,
                    new Type[] { Type.SQL_VARCHAR },
                    singleParamList),

            new FunctionDescriptor("inet6_aton", Type.SQL_VARBINARY, FUNC_VOLT_INET6_ATON, -1,
                    new Type[] { Type.SQL_VARCHAR },
                    singleParamList),

            new FunctionDescriptor("inet6_ntoa", Type.SQL_VARCHAR, FUNC_VOLT_INET6_NTOA, -1,
                    new Type[] { Type.SQL_VARBINARY },
                    singleParamList),

            new FunctionDescriptor("makevalidpolygon", Type.VOLT_GEOGRAPHY, FUNC_VOLT_MAKE_VALID_POLYGON, -1,
            		new Type[] { Type.VOLT_GEOGRAPHY },
            		singleParamList),

            new FunctionDescriptor("format_timestamp", Type.SQL_VARCHAR, FUNC_VOLT_FORMAT_TIMESTAMP, -1,
                    new Type[]{Type.SQL_TIMESTAMP, Type.SQL_VARCHAR},
                    doubleParamList),
                /**
                 * NOTE: this returns the set of rows that are currently not migrated, i.e.
                 * whose hidden columns are NOT NULL.
                */
            new FunctionDescriptor("migrating", Type.SQL_BOOLEAN, FUNC_VOLT_MIGRATING, -1,
                    new Type[] {},
                    emptyParamList,
                    noParamList),
            /**
             * time_window has alternate params which allows us to not specify optional param.
            */
            new FunctionDescriptor("time_window", Type.SQL_TIMESTAMP, FUNC_VOLT_TIME_WINDOW, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_INTEGER, Type.SQL_TIMESTAMP, Type.SQL_VARCHAR },
                    new short[] {  Tokens.OPENBRACKET,
                            Tokens.X_KEYSET, 10, Tokens.YEAR, Tokens.QUARTER, Tokens.MONTH, Tokens.WEEK, Tokens.DAY, Tokens.HOUR, Tokens.MINUTE, Tokens.SECOND,
                            Tokens.MILLIS, Tokens.MILLISECOND, Tokens.COMMA,
                            Tokens.QUESTION, Tokens.COMMA,
                            Tokens.QUESTION, Tokens.COMMA,
                            Tokens.X_KEYSET, 2, Tokens.START, Tokens.END, Tokens.CLOSEBRACKET },
                    new short[] {  Tokens.OPENBRACKET,
                            Tokens.X_KEYSET, 10, Tokens.YEAR, Tokens.QUARTER, Tokens.MONTH, Tokens.WEEK, Tokens.DAY, Tokens.HOUR, Tokens.MINUTE, Tokens.SECOND,
                            Tokens.MILLIS, Tokens.MILLISECOND, Tokens.COMMA,
                            Tokens.QUESTION, Tokens.COMMA,
                            Tokens.QUESTION, Tokens.CLOSEBRACKET }
                    ),
            new FunctionDescriptor("datediff", Type.SQL_BIGINT, FUNC_VOLT_DATETIME_DIFF, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_TIMESTAMP, Type.SQL_TIMESTAMP },
                    new short[] {  Tokens.OPENBRACKET,
                            Tokens.X_KEYSET, 12, Tokens.YEAR, Tokens.QUARTER, Tokens.MONTH, Tokens.WEEK, Tokens.DAY, Tokens.HOUR, Tokens.MINUTE, Tokens.SECOND,
                            Tokens.MILLIS, Tokens.MILLISECOND, Tokens.MICROS, Tokens.MICROSECOND, Tokens.COMMA,
                            Tokens.QUESTION, Tokens.COMMA,
                            Tokens.QUESTION, Tokens.CLOSEBRACKET }),
        };

        /**
         * This is the lookup table for permanently defined SQL functions.
         */
        private static Map<String, FunctionDescriptor> m_by_LC_name = new HashMap<>();
        /**
         * This is the lookup table for user defined SQL functions.
         */
        private static Map<String, FunctionDescriptor> m_defined_functions = new HashMap<>();
        private static Map<Integer, FunctionDescriptor> m_defined_functions_by_id = new HashMap<>();
        private static Map<String, Integer> m_names_to_ids = new HashMap<>();
        /**
         * This is a saved set of user defined SQL functions.
         * <ol>
         *   <li>Before we start to compile DDL, we move the m_defined_functions
         *       map to here, and reinitialize m_defined_functions to empty.</li>
         *   <li>When a new user defined function is defined, if it looks like
         *       one in m_saved_functions we reuse the saved one, but leave the
         *       saved one in the saved table as well as m_defined_functions.</li>
         *   <li>If a compilation fails we just restore the m_defined_functions to be
         *       the m_saved_functions, and no harm is done.</li>
         *   <li>If all compilations succeed, then we are committed to the new
         *       function definitions.  So, we clear the m_saved_functions.<li>
         * </ol>
         */
        private static Map<String, FunctionDescriptor> m_saved_functions = new HashMap<>();
        static {
            // These are all permanent, SQL defined functions.  So, they go
            // into m_by_LC_name
            for (FunctionDescriptor fn : instances) {
                m_by_LC_name.put(fn.m_name, fn);
            }
        }

        /**
         * Look up a function by name.  The name can have
         * any case, upper or lower.  Why we don't use a
         * case insensitive hash table is unknown.
         *
         * @param anyCase
         * @return
         */
        public static FunctionDescriptor fn_by_name(String anyCase) {
            String downCase = anyCase.toLowerCase();
            FunctionDescriptor answer;
            answer = m_by_LC_name.get(downCase);
            if (answer == null) {
                answer = m_defined_functions.get(downCase);
            }
            return answer;
        }

        public static Type getReturnType(int functionId) {
            return m_defined_functions_by_id.get(functionId).getDataType();
        }


        public int getTypeParameter() {
            return m_typeParameter;
        }

        public static void addDefinedFunction(String functionName, int functionId, FunctionDescriptor oldFd) {
            m_defined_functions.put(functionName, oldFd);
            m_defined_functions_by_id.put(functionId, oldFd);
            m_names_to_ids.put(functionName, functionId);
        }

        public static void removeOneDefinedFunction(String functionName) {
            m_defined_functions.remove(functionName);
            Integer functionId = m_names_to_ids.get(functionName);
            m_names_to_ids.remove(functionName);
            m_defined_functions_by_id.remove(functionId);
        }

        public static void clearSavedFunctions() {
            m_saved_functions = new HashMap<>();
        }

        public static void restoreSavedFunctions() {
            m_defined_functions = m_saved_functions;
            m_saved_functions = new HashMap<>();
        }

        public static void saveDefinedFunctions() {
            m_saved_functions = m_defined_functions;
            m_defined_functions = new HashMap<>();
        }

    }

    public static final int FUNC_VOLT_ID_FOR_CONTAINS = FunctionDescriptor.FUNC_VOLT_CONTAINS;

    private final FunctionDescriptor m_def;

    public static FunctionSQL newVoltDBFunction(String token) {
        FunctionDescriptor def = FunctionDescriptor.fn_by_name(token);
        if (def == null) {
            return null;
        }
        FunctionSQL function = new FunctionForVoltDB(def);
        return function;
    }

    public static Integer newVoltDBFunctionID(String token) {
        FunctionDescriptor def = FunctionDescriptor.fn_by_name(token);
        if (def == null) {
            return null;
        }
        return def.getId();
    }

    public static int getFunctionID(String token) {
        return FunctionDescriptor.fn_by_name(token).getId();
    }

    public static boolean isUserDefineAggregate(String token) {
        FunctionDescriptor def = FunctionDescriptor.fn_by_name(token);
        if (def == null) {
            return false;
        }
        return def.isAggregate();
    }

    public FunctionForVoltDB(FunctionDescriptor fn) {
        super();
        m_def     = fn;
        funcType  = m_def.getId();
        name      = m_def.getName();
        parseList = m_def.getParamParseList();
        parseListAlt = m_def.getParamParseListAlt();
        parameterArg = m_def.getTypeParameter();
    }

    @Override
    public void setArguments(Expression[] nodes) {
        //TODO; Here's where we might re-order arguments or insert implied values for functions that were implemented as aliases for other functions.
        /*
        switch (m_def.getId()) {
        default :
            break;
        }
        */
        super.setArguments(nodes);
    }

    @Override
    public Expression getFunctionExpression() {
        //TODO; Here's where we might substitute wholesale some other HSQL Expression for a function expression that is really just an alias.
        /*
        switch (m_def.getId()) {
        default :
            break;
        }
        */
        return super.getFunctionExpression();
    }

    @Override
    Object getValue(Session session, Object[] data) {
        //TODO; Here's where we implement the function for HSQL backends so it can be used for regression testing -- GOOD LUCK!
        /*
        switch (m_def.getId()) {
        default :
            break;
        }
        */
        throw Error.runtimeError(ErrorCode.U_S0500, "This FunctionForVoltDB is not implemented in HSQL backends -- or in HSQL constant-folding.");
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        Type[] paramTypes = m_def.getParamTypes();

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        switch(m_def.getId()) {
        case FunctionDescriptor.FUNC_CONCAT:
            for (int ii = 0; ii < nodes.length; ii++) {
                if (nodes[ii].dataType == null && nodes[ii].isParam) {
                    nodes[ii].dataType = Type.SQL_VARCHAR;
                }
            }
            break;
        /*
         * The types to the FIELD functions parameters are VARCHAR
         */
        case FunctionDescriptor.FUNC_VOLT_FIELD:
            if (nodes[0].dataType == null && nodes[0].isParam) {
                nodes[0].dataType = Type.SQL_VARCHAR;
            }
            if (nodes[1].dataType == null && nodes[1].isParam) {
                nodes[1].dataType = Type.SQL_VARCHAR;
            }
            break;

            /*
             * Infer parameter types to make the types of the 1st, 2nd, and (if not the last) 4th, 6th, etc.
             * arguments to DECODE as consistent as possible,
             * and the types of the 3rd, 5th, 7th, etc. and LAST arguments as consistent as possible.
             * Punt to inferring VARCHAR if the other arguments give no clue or are inconsistent
             * -- the VoltDB EE complains about NULL-typed parameters but is somewhat forgiving about
             * mixed argument types.
             */
        case FunctionDescriptor.FUNC_VOLT_DECODE:
            // Track whether parameter type hinting is needed for either key or value arguments.
            // For simplicity(?), parameters are not tracked explicitly (by position)
            // or even by category (key vs. value). So, if any parameter hinting is required at all,
            // all arguments get re-checked.
            boolean needParamType = false;
            Type inputTypeInferred = null;
            Type resultTypeInferred = null;

            for (int ii = 0; ii < nodes.length; ii++) {
                Type argType = nodes[ii].dataType;
                if (argType == null) {
                    // A param here means work to do, below.
                    if (nodes[ii].isParam || nodes[ii].valueData == null) {
                        needParamType = true;
                    }
                    continue;
                }
                // Except for the first and the optional last/"default" argument,
                // the arguments alternate between candidate inputs and candidate results.
                if ((((ii % 2) == 0) || ii == nodes.length-1) && (ii != 0)) {
                    // These arguments represent candidate result values
                    // that may hint at the result type or require hinting from the other result values.
                    if (resultTypeInferred == null) {
                        resultTypeInferred = argType; // Take the first result type hint.
                    } else if (resultTypeInferred.typeComparisonGroup != argType.typeComparisonGroup) {
                        resultTypeInferred = Type.SQL_VARCHAR; // Discard contradictory hints.
                    }
                } else {
                    // These arguments represent candidate input keys
                    // that may hint at the input type or may require hinting from the other input keys.
                    if (inputTypeInferred == null) {
                        inputTypeInferred = argType; // Take the first input type hint.
                    } else if (inputTypeInferred.typeComparisonGroup != argType.typeComparisonGroup) {
                        inputTypeInferred = Type.SQL_VARCHAR; // Discard contradictory hints, falling back to string type.
                    }
                }
            }

            // With any luck, there are no parameter "?" arguments to worry about.
            if ( ! needParamType) {
                break;
            }

            // No luck, try to infer the parameters' types.
            // Punt to guessing VARCHAR for lack of better information.
            if (inputTypeInferred == null) {
                inputTypeInferred = Type.SQL_VARCHAR;
            }
            if (resultTypeInferred == null) {
                resultTypeInferred = Type.SQL_VARCHAR;
            }

            for (int ii = 0; ii < nodes.length; ii++) {
                Type argType = nodes[ii].dataType;
                if ((argType != null) || ! (nodes[ii].isParam || nodes[ii].valueData == null)) {
                    continue;
                }
                // This is the same test as above for determining that the argument
                // is a candidate result vs. a candidate input.
                if ((((ii % 2) == 0) || ii == nodes.length-1) && (ii != 0)) {
                    nodes[ii].dataType = resultTypeInferred;
                } else {
                    nodes[ii].dataType = inputTypeInferred;
                }
            }
            break;

        case FunctionDescriptor.FUNC_VOLT_BITNOT:
            voltResolveToBigintTypesForBitwise();
            break;

        case FunctionDescriptor.FUNC_VOLT_BIT_SHIFT_LEFT:
        case FunctionDescriptor.FUNC_VOLT_BIT_SHIFT_RIGHT:
            // the first parameter has to be BigInteger
            voltResolveToBigintType(0);
            voltResolveToBigintCompatibleType(1);

            dataType = Type.SQL_BIGINT;
            break;

        case FunctionDescriptor.FUNC_VOLT_HEX:
        case FunctionDescriptor.FUNC_VOLT_BIN:
            voltResolveToBigintType(0);
            dataType = Type.SQL_VARCHAR;
            break;

        case FunctionDescriptor.FUNC_VOLT_DISTANCE:
            // validate the types of argument is valid
            if (nodes[0].dataType == null || nodes[1].dataType == null) {
                // "data type cast needed for parameter or null literal"
                throw Error.error(ErrorCode.X_42567,
                        "input type to DISTANCE function is ambiguous");
            }
            else if ((!nodes[0].dataType.isGeographyType() && !nodes[0].dataType.isGeographyPointType()) ||
                     (!nodes[1].dataType.isGeographyType() && !nodes[1].dataType.isGeographyPointType())) {
                // either of the nodes is not a valid type
                throw Error.error(ErrorCode.X_42565,
                        "The DISTANCE function computes distances between POINT-to-POINT, POINT-to-POLYGON " +
                        "and POLYGON-to-POINT only.");
            }
            else if (nodes[0].dataType.isGeographyType() && nodes[1].dataType.isGeographyType()) {
                // distance between two polygons is not supported, flag as an error
                throw Error.error(ErrorCode.X_42565, "DISTANCE between two POLYGONS not supported");
            }
            else if (nodes[0].dataType.isGeographyPointType() && nodes[1].dataType.isGeographyType()) {
                // distance between polygon-to-point and point-to-polygon is symmetric.
                // So, update the the expression for distance between point and polygon to
                // distance between polygon and point. This simplifies the logic and have to
                // handle only one case: polygon-to-point instead of two
                Expression tempNode = nodes[0];
                nodes[0] = nodes[1];
                nodes[1] = tempNode;
            }
            break;

        case FunctionDescriptor.FUNC_VOLT_DWITHIN:
            if (nodes[0].dataType == null || nodes[1].dataType == null) {
                // "data type cast needed for parameter or null literal"
                throw Error.error(ErrorCode.X_42567,
                        "input type to DWITHIN function is ambiguous");
            }
            else if ((!nodes[0].dataType.isGeographyType() && !nodes[0].dataType.isGeographyPointType()) ||
                     (!nodes[1].dataType.isGeographyType() && !nodes[1].dataType.isGeographyPointType())) {
                // first and second argument should be geography type
                throw Error.error(ErrorCode.X_42565,
                        "DWITHIN function evaulates if geographies are within specified distance of one-another for "
                                + "POINT-to-POINT, POINT-to-POLYGON and POLYGON-to-POINT geographies only.");
            }
            else if (nodes[0].dataType.isGeographyType() && nodes[1].dataType.isGeographyType()) {
                // "incompatible data type in operation"
                // distance between two polygons is not supported, flag as an error
                throw Error.error(ErrorCode.X_42565, "DWITHIN between two POLYGONS not supported");
            }
            else if (nodes[0].dataType.isGeographyPointType() && nodes[1].dataType.isGeographyType()) {
                // Distance between polygon-to-point and point-to-polygon is symmetric. Update the
                // expression for DWITHIN between point and polygon to distance between polygon
                // and point. This consolidates logic to one case: polygon-to-point
                Expression tempNode = nodes[0];
                nodes[0] = nodes[1];
                nodes[1] = tempNode;
            }

            if ((nodes[2].dataType != null) &&
                (!nodes[2].dataType.isNumberType())) {
                // "incompatible data type in operation"
                throw Error.error(ErrorCode.X_42565,
                        "input type DISTANCE to DWITHIN function must be non-negative numeric value");
            }
            break;

        case FunctionDescriptor.FUNC_VOLT_ASTEXT:
            if (nodes[0].dataType == null) {
                // "data type cast needed for parameter or null literal"
                throw Error.error(ErrorCode.X_42567,
                        "input type to ASTEXT function is ambiguous");
            }

            if (! (nodes[0].dataType.isGeographyPointType() || nodes[0].dataType.isGeographyType())) {
                // "incompatible data type in operation"
                throw Error.error(ErrorCode.X_42565,
                        "The asText function accepts only GEOGRAPHY and GEOGRAPHY_POINT types.");
            }
            break;

        // our networking specified functions
        case FunctionDescriptor.FUNC_VOLT_INET_NTOA:
            if (nodes[0].dataType != null &&
                !nodes[0].dataType.isNumberType()) {
                throw Error.error(ErrorCode.X_42561);
            }
            dataType = Type.SQL_VARCHAR;
            break;

        case FunctionDescriptor.FUNC_VOLT_INET_ATON:
            if (nodes[0].dataType != null &&
                !nodes[0].dataType.isCharacterType()) {
                throw Error.error(ErrorCode.X_42561);
            }
            dataType = Type.SQL_BIGINT;
            break;

        case FunctionDescriptor.FUNC_VOLT_INET6_ATON:
            if (nodes[0].dataType != null &&
                !nodes[0].dataType.isCharacterType()) {
                throw Error.error(ErrorCode.X_42561);
            }
            dataType = Type.SQL_VARBINARY;
            break;

        case FunctionDescriptor.FUNC_VOLT_INET6_NTOA:
            if (nodes[0].dataType != null &&
                !nodes[0].dataType.isBinaryType()) {
                throw Error.error(ErrorCode.X_42561);
            }
            dataType = Type.SQL_VARCHAR;
            break;

        default:
            break;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                if (i >= paramTypes.length) {
                 // TODO support type checking for variadic functions
                    break;
                }
                if (paramTypes[i] == null) {
                    continue; // accept all argument types
                }
                if (nodes[i].dataType == null) {
                    // assert that the ambiguous argument (e.g. '?' parameter) has the required type
                    nodes[i].dataType = paramTypes[i];
                    continue;
                }
                else if (paramTypes[i].canConvertFrom(nodes[i].dataType)) {
                    // Add support to pass in a JDBC time string constant
                    if (paramTypes[i].isDateTimeType() && nodes[i].dataType.isCharacterType()) {
                        String datetimestring = (String) nodes[i].valueData;
                        if (datetimestring != null) {
                            datetimestring = datetimestring.trim();
                            try {
                                Timestamp.valueOf(datetimestring);
                            }
                            catch (Exception e) {
                                throw Error.error(ErrorCode.X_42561);
                            }
                            nodes[i].valueData = paramTypes[i].castToType(session, nodes[i].valueData, nodes[i].dataType);
                            nodes[i].dataType = paramTypes[i];
                        }
                    } else if (paramTypes[i].isNumberType() && !nodes[i].dataType.isNumberType()) {
                        throw Error.error(ErrorCode.X_42565);
                    }
                    continue; // accept compatible argument types
                }
                throw Error.error(ErrorCode.X_42565); // incompatible data type
            }
        }

        dataType = m_def.getDataType();
        if (dataType == null && nodes.length > 0) {
            if (parameterArg < 0 || parameterArg >= nodes.length) {
                throw Error.error(ErrorCode.X_42565); // incompatible data type (so says the error -- we're missing one, actually)
            }
            Expression like_child = nodes[parameterArg];
            if (like_child != null) {
                dataType = like_child.dataType;
            }
        }
    }

    @Override
    public String getSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(m_def.getName()).append(Tokens.T_OPENBRACKET);

        switch (m_def.getId()) {
        case FunctionDescriptor.FUNC_VOLT_SINCE_EPOCH:
        case FunctionDescriptor.FUNC_VOLT_TO_TIMESTAMP:
        case FunctionDescriptor.FUNC_VOLT_TRUNCATE_TIMESTAMP: {
            int timeUnit = ((Number) nodes[0].valueData).intValue();
            sb.append(Tokens.getKeyword(timeUnit));
            break;
        }
        default:
            // If this is a nullary function, we don't want to
            // crash here.
            if (0 < nodes.length) {
                sb.append(nodes[0].getSQL());
            }
            break;
        }
        for (int ii = 1; ii < nodes.length; ii++) {
            if (nodes[ii] != null) {
                sb.append(Tokens.T_COMMA).append(nodes[ii].getSQL());
            }
            else {
                // Some functions, like regexp_position, have optional parameters.
                // The omitted optional parameters appear as a null in the function's
                // node list.  This null-preserving behavior seems to be intentional in
                // ParserSQL.readExpression, for reasons that are unclear to me.
                //   --cwolff, December 2015.
            }
        }
        sb.append(Tokens.T_CLOSEBRACKET);
        return sb.toString();
    }

    // This is the unique sequential UDF Id we assign to every UDF defined by the user.
    private static int m_udfSeqId = FunctionDescriptor.FUNC_VOLT_UDF_ID_START;

    public static int getNextFunctionId() {
        return m_udfSeqId++;
    }

    /**
     * Remove one user defined function.
     * @param functionName
     */
    public static void deregisterUserDefinedFunction(String functionName) {
        FunctionDescriptor.removeOneDefinedFunction(functionName);
    }

    /**
     * Return true iff the existing function descriptor matches
     * the given return type and parameter types.  These are all
     * HSQLDB types, not Volt types.
     *
     * @param existingFd
     * @param returnType
     * @param parameterTypes
     * @return
     */
    private static boolean functionMatches(FunctionDescriptor existingFd,
                                           Type returnType,
                                           Type[] parameterTypes) {
        if (returnType != existingFd.m_type) {
            return false;
        }
        if (parameterTypes.length != existingFd.m_paramTypes.length) {
            return false;
        }
        for (int idx = 0; idx < parameterTypes.length; idx++) {
            if (parameterTypes[idx] != existingFd.m_paramTypes[idx]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given a function name and signature, find if there is
     * an existing definition or saved defintion which matches the
     * name and signature, and return the definition.
     *
     * @param functionName
     * @param returnType
     * @param parameterType
     * @return The matching definition, or null if there is no matching definition.
     */
    private static FunctionDescriptor findFunction(String functionName,
                                                   Type returnType,
                                                   Type[] parameterType) {
        m_logger.debug("Looking for UDF " + functionName);
        FunctionDescriptor fd = FunctionDescriptor.m_by_LC_name.get(functionName);
        if (fd == null) {
            m_logger.debug("    Not defined in by_LC_name.  Maybe it's saved.");
            fd = FunctionDescriptor.m_saved_functions.get(functionName);
        }
        if (fd != null && functionMatches(fd, returnType, parameterType) ) {
            m_logger.debug("    " + functionName + " is defined or saved.  id == " + fd.getId());
            return fd;
        }
        m_logger.debug("    " + functionName + " is not defined or saved.");
        return null;
    }

    private static FunctionDescriptor makeFunctionDescriptorFromParts(String functionName,
                                                                      int functionId,
                                                                      Type returnType,
                                                                      Type[] parameterTypes,
                                                                      boolean isAggregate) {

        // A pair of parentheses + number of parameters
        int syntaxLength = 2 + parameterTypes.length;
        if (parameterTypes.length > 1) {
            // Add commas in between
            syntaxLength += parameterTypes.length - 1;
        }
        short[] syntax = new short[syntaxLength];
        syntax[0] = Tokens.OPENBRACKET;
        int idx = 1;
        for (int parId = 0; parId < parameterTypes.length; parId++) {
            if (parId > 0) {
                syntax[idx++] = Tokens.COMMA;
            }
            syntax[idx++] = Tokens.QUESTION;
        }
        syntax[syntax.length - 1] = Tokens.CLOSEBRACKET;
        return new FunctionDescriptor(functionName, returnType, functionId, -1, parameterTypes, syntax, isAggregate);
    }

    /**
     * This function registers a UDF using VoltType values for the return type and parameter types.
     *
     * @param functionName The function name.
     * @param functionId The function id.  If  this is -1 we don't have an opinion about the value.
     * @param voltReturnType The return type as a VoltType enumeration.
     * @param voltParameterTypes The parameter types as a VoltType enumeration.
     * @param isAggregate This is a scalar function or an aggregate function
     * @return
     */
    public static synchronized int registerTokenForUDF(String functionName,
                                                       int functionId,
                                                       VoltType voltReturnType,
                                                       VoltType[] voltParameterTypes,
                                                       boolean isAggregate) {
        int retFunctionId;
        Type hsqlReturnType = hsqlTypeFromVoltType(voltReturnType);
        Type[] hsqlParameterTypes = hsqlTypeFromVoltType(voltParameterTypes);
        // If the token is already registered in the map, do not bother again.
        FunctionDescriptor oldFd = findFunction(functionName, hsqlReturnType, hsqlParameterTypes);
        if (oldFd != null) {
            // This may replace functionName with itself. This will not be an error.
            FunctionDescriptor.addDefinedFunction(functionName, functionId, oldFd);
            retFunctionId = oldFd.getId();
            // If we were given a non-negative function id, it
            // was defined in the catalog.  Our re-verification here
            // should have a value which we put into the catalog sometime
            // earlier.  So, this earlier value should match the one we
            // were told to return.
            assert((functionId < 0) || (functionId == retFunctionId));
        } else {
            // if the function was not already defined, then
            //   if functionId is a valid UDF id or pre-defined SQL function id, then use it
            //   otherwise, we want a new number.
            //
            if (functionId > 0) {
                retFunctionId = functionId;
            } else {
                retFunctionId = getNextFunctionId();
            }
            FunctionDescriptor fd = makeFunctionDescriptorFromParts(functionName, retFunctionId,
                                                            hsqlReturnType, hsqlParameterTypes, isAggregate);
            // if the function id belongs to UDF, put it into the defined_function map
            if (isUserDefinedFunctionId(retFunctionId)) {
                FunctionDescriptor.addDefinedFunction(functionName, functionId, fd);
            }
            m_logger.debug(String.format("Added UDF \"%s\"(%d) with %d parameters",
                                        functionName, retFunctionId, voltParameterTypes.length));
        }
        // Ensure that m_udfSeqId is larger than all the
        // ones we've seen so far.
        if (m_udfSeqId <= retFunctionId) {
            m_udfSeqId = retFunctionId + 1;
        }
        return retFunctionId;
    }

    /**
     * Convert a VoltType to an HSQL type.
     *
     * Types are somewhat confusing.  There are three type representations, all different.
     * <ol>
     *   <li> Some types are in HSQL.  These are enumerals of the type org.hsqldb_voltpatches.types.Type.</li>
     *   <li> Some types are in VoltDB.  These are enumerals of the type org.voltdb.VoltType.</li>
     *   <li> Some types are Java class types.  These have the type Class<?>, and come from the JVM.</li>
     * <ol>
     * Neeedless to say, these three all have entirely different structures.  The HSQL types are used here
     * in HSQl.  The VoltType enumerals  are used in the rest of Volt.  In particular, the functions we need
     * to convert from VoltType to Type, like getParameterSQLTypeNumber, are not visible outside of HSQL.  So
     * we we need this function to convert one way.  Conversions the other way are possible, but not
     * currently needed.
     *
     * @param voltReturnType
     * @return
     */
    public static Type hsqlTypeFromVoltType(VoltType voltReturnType) {
        Class<?> typeClass = VoltType.classFromByteValue(voltReturnType.getValue());
        int typeNo = Types.getParameterSQLTypeNumber(typeClass);
        return Type.getDefaultTypeWithSize(typeNo);
    }

    /**
     * Map the single parameter hsqlTypeFromVoltType over an array.
     *
     * @param voltParameterTypes
     * @return
     */
    public static Type[] hsqlTypeFromVoltType(VoltType[] voltParameterTypes) {
        Type[] answer = new Type[voltParameterTypes.length];
        for (int idx = 0; idx < voltParameterTypes.length; idx++) {
            answer[idx] = hsqlTypeFromVoltType(voltParameterTypes[idx]);
        }
        return answer;
    }

    public static boolean isFunctionNameDefined(String functionName) {
        FunctionDescriptor found = FunctionDescriptor.m_by_LC_name.get(functionName);
        if (found == null) {
            found = FunctionDescriptor.m_defined_functions.get(functionName);
        }
        return (found != null);
    }

    public static boolean isUserDefinedFunctionId(int functionId) {
        return functionId >= FunctionDescriptor.FUNC_VOLT_UDF_ID_START;
    }

    public FunctionDescriptor getFunctionId() {
        return m_def;
    }

    public static Set<String> getAllUserDefinedFunctionNamesForDebugging() {
        Set<String> answer = new HashSet<>();
        for (String name : FunctionDescriptor.m_defined_functions.keySet()) {
            answer.add(name);
        }
        return answer;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (other instanceof FunctionForVoltDB == false) return false;

        FunctionForVoltDB function = (FunctionForVoltDB) other;
        if (function.getFunctionId().getId() != m_def.getId()) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int val = super.hashCode();
        val += Objects.hashCode(m_def.getId());
        return val;
    }

    /**
     * Save all the user defined functions.
     */
    public static void saveDefinedFunctions() {
        FunctionDescriptor.saveDefinedFunctions();
    }

    /**
     * Restore the saved user defined functions.  This happens
     * when a procedure compilation fails.
     */
    public static void restoreSavedFunctions() {
        FunctionDescriptor.restoreSavedFunctions();
    }

    /**
     * Forget the saved user defined functions.  This happens
     * when all procedure compilations complete, and we are committed
     * to a new catalog.
     */
    public static void clearSavedFunctions() {
        FunctionDescriptor.clearSavedFunctions();
    }
}
