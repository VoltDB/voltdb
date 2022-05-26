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

#pragma once

#include "expressions/abstractexpression.h"

// IMPORTANT: These FUNC_... values must be kept synchronized with those listed in the following hsql parser source files:
// FunctionSQL.java       -- standard SQL functions
// FunctionCustom.java    -- JDBC (Open Group SQL) functions
// FunctionForVoltDB.java -- VoltDB extensions

// Note that function names and function IDs need not correspond one-to-one.
// A function name (especially one in the Custom/JDBC/OpenGroup category) may be just an alias for another function,
// so it will use the same function ID, e.g. SUBSTR -> SUBSTRING, so SUBSTR does not need its own function id.
// A function name that is "overloaded", applying to different numbers and/or types of arguments may be optimized to map
// to different specialized implementations, each with its own function ID
// e.g. EXTRACT -> either FUNC_EXTRACT_YEAR, or FUNC_EXTRACT_MONTH, or FUNC_EXTRACT_DAY, etc.
// For completeness, FUNC_EXTRACT is defined here, but at this point, it always represents a missing translation step in the parser.

namespace voltdb {
   AbstractExpression* functionFactory(int functionId, const std::vector<AbstractExpression*>& arguments);
   static const int FUNC_POSITION_CHAR                    = 1;     // numeric
   static const int FUNC_POSITION_BINARY                  = 2;
   static const int FUNC_OCCURENCES_REGEX                 = 3;
   static const int FUNC_POSITION_REGEX                   = 4;
   static const int FUNC_EXTRACT                          = 5;
   static const int FUNC_BIT_LENGTH                       = 6;
   static const int FUNC_CHAR_LENGTH                      = 7;
   static const int FUNC_OCTET_LENGTH                     = 8;
   static const int FUNC_CARDINALITY                      = 9;
   static const int FUNC_ABS                              = 10;
   static const int FUNC_MOD                              = 11;
   static const int FUNC_LN                               = 12;
   static const int FUNC_EXP                              = 13;
   static const int FUNC_POWER                            = 14;
   static const int FUNC_SQRT                             = 15;
   static const int FUNC_FLOOR                            = 16;
   static const int FUNC_CEILING                          = 17;
   static const int FUNC_WIDTH_BUCKET                     = 20;
   static const int FUNC_SUBSTRING_CHAR                   = 21;    // string
   static const int FUNC_SUBSTRING_REG_EXPR               = 22;
   static const int FUNC_SUBSTRING_REGEX                  = 23;
   static const int FUNC_FOLD_LOWER                       = 24;
   static const int FUNC_FOLD_UPPER                       = 25;
   static const int FUNC_TRANSCODING                      = 26;
   static const int FUNC_TRANSLITERATION                  = 27;
   static const int FUNC_REGEX_TRANSLITERATION            = 28;
   static const int FUNC_TRIM_CHAR                        = 29;
   static const int FUNC_OVERLAY_CHAR                     = 30;
   static const int FUNC_CHAR_NORMALIZE                   = 31;
   static const int FUNC_SUBSTRING_BINARY                 = 32;
   static const int FUNC_TRIM_BINARY                      = 33;
   static const int FUNC_OVERLAY_BINARY                   = 40;
   static const int FUNC_CURRENT_DATE                     = 41;    // datetime
   static const int FUNC_CURRENT_TIME                     = 42;
   static const int FUNC_CURRENT_TIMESTAMP                = 43;
   static const int FUNC_LOCALTIME                        = 44;
   static const int FUNC_LOCALTIMESTAMP                   = 50;
   static const int FUNC_CURRENT_CATALOG                  = 51;    // general
   static const int FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP  = 52;
   static const int FUNC_CURRENT_PATH                     = 53;
   static const int FUNC_CURRENT_ROLE                     = 54;
   static const int FUNC_CURRENT_SCHEMA                   = 55;
   static const int FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE = 56;
   static const int FUNC_CURRENT_USER                     = 57;
   static const int FUNC_SESSION_USER                     = 58;
   static const int FUNC_SYSTEM_USER                      = 59;
   static const int FUNC_USER                             = 60;
   static const int FUNC_VALUE                            = 61;

   static const int FUNC_ISAUTOCOMMIT            = 71;
   static const int FUNC_ISREADONLYSESSION       = 72;
   static const int FUNC_ISREADONLYDATABASE      = 73;
   static const int FUNC_ISREADONLYDATABASEFILES = 74;
   static const int FUNC_DATABASE                = 75;
   static const int FUNC_IDENTITY                = 76;
   static const int FUNC_SYSDATE                 = 77;
   static const int FUNC_TIMESTAMPADD            = 78;
   static const int FUNC_TIMESTAMPDIFF           = 79;
   static const int FUNC_TRUNCATE                = 80;
   static const int FUNC_TO_CHAR                 = 81;
   static const int FUNC_TIMESTAMP               = 82;

   static const int FUNC_ACOS             = 101;
   static const int FUNC_ASIN             = 102;
   static const int FUNC_ATAN             = 103;
   static const int FUNC_ATAN2            = 104;
   static const int FUNC_COS              = 105;
   static const int FUNC_COT              = 106;
   static const int FUNC_DEGREES          = 107;
   static const int FUNC_LOG10            = 110;
   static const int FUNC_PI               = 111;
   static const int FUNC_RADIANS          = 112;
   static const int FUNC_RAND             = 113;
   static const int FUNC_ROUND            = 114;
   static const int FUNC_SIGN             = 115;
   static const int FUNC_SIN              = 116;
   static const int FUNC_TAN              = 117;
   static const int FUNC_BITAND           = 118;
   static const int FUNC_BITOR            = 119;
   static const int FUNC_BITXOR           = 120;
   static const int FUNC_ROUNDMAGIC       = 121;
   static const int FUNC_ASCII            = 122;
   static const int FUNC_CHAR             = 123;
   static const int FUNC_CONCAT           = 124;
   static const int FUNC_DIFFERENCE       = 125;
   static const int FUNC_HEXTORAW         = 126;
   static const int FUNC_LEFT             = 128;
   static const int FUNC_LOCATE           = 130;
   static const int FUNC_LTRIM            = 131;
   static const int FUNC_RAWTOHEX         = 132;
   static const int FUNC_REPEAT           = 133;
   static const int FUNC_REPLACE          = 134;
   static const int FUNC_RIGHT            = 135;
   static const int FUNC_RTRIM            = 136;
   static const int FUNC_SOUNDEX          = 137;
   static const int FUNC_SPACE            = 138;
   static const int FUNC_SUBSTR           = 139;
   static const int FUNC_DATEDIFF         = 140;
   static const int FUNC_SECONDS_MIDNIGHT = 141;
   static const int FUNC_CSC              = 142;
   static const int FUNC_SEC              = 143;

   // Function ID offsets for specializations of EXTRACT and TRIM.
   // Individual ID values are based on various Tokens.java constants
   // and need to be adjusted by these constant offsets to avoid overlap
   // with other sources of Function ID constants.
   // These are from FunctionSQL.java
   static const int SQL_EXTRACT_VOLT_FUNC_OFFSET = 1000;
   static const int SQL_TRIM_VOLT_FUNC_OFFSET = 2000;

   // These are from DTIType.java
   static const int SQL_TYPE_NUMBER_LIMIT = 256;
   static const int SQL_TIMEZONE_HOUR   = SQL_TYPE_NUMBER_LIMIT + 1;
   static const int SQL_TIMEZONE_MINUTE = SQL_TYPE_NUMBER_LIMIT + 2;
   static const int SQL_SECONDS_MIDNIGHT = SQL_TYPE_NUMBER_LIMIT + 10;

   // These are from Tokens.java.
   static const int SQL_DAY_NAME         = 607;
   static const int SQL_MONTH_NAME       = 608;
   static const int SQL_YEAR             = 321;
   static const int SQL_MONTH            = 171;
   static const int SQL_DAY              =  72;
   static const int SQL_HOUR             = 126;
   static const int SQL_MINUTE           = 167;
   static const int SQL_SECOND           = 248;
   static const int SQL_MILLIS           = 573;
   static const int SQL_MILLISECOND      = 1002;
   static const int SQL_MICROS           = 1000;
   static const int SQL_MICROSECOND      = 1001;
   static const int SQL_DAY_OF_WEEK      = 560;
   static const int SQL_DAY_OF_MONTH     = 610;
   static const int SQL_DAY_OF_YEAR      = 611;
   static const int SQL_WEEK_OF_YEAR     = 592;
   static const int SQL_QUARTER          = 609;
   static const int SQL_WEEKDAY          = 741;

   // These are from Types.java.
   static const int SQL_INTERVAL_YEAR             = 101;
   static const int SQL_INTERVAL_MONTH            = 102;
   static const int SQL_INTERVAL_DAY              = 103;
   static const int SQL_INTERVAL_HOUR             = 104;
   static const int SQL_INTERVAL_MINUTE           = 105;
   static const int SQL_INTERVAL_SECOND           = 106;

   static const int FUNC_EXTRACT_TIMEZONE_HOUR    = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_TIMEZONE_HOUR;
   static const int FUNC_EXTRACT_TIMEZONE_MINUTE  = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_TIMEZONE_MINUTE;
   static const int FUNC_EXTRACT_DAY_OF_WEEK      = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_OF_WEEK;
   static const int FUNC_EXTRACT_DAY_OF_MONTH     = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_OF_MONTH;
   static const int FUNC_EXTRACT_DAY_OF_YEAR      = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_OF_YEAR;
   static const int FUNC_EXTRACT_WEEK_OF_YEAR     = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_WEEK_OF_YEAR;
   static const int FUNC_EXTRACT_QUARTER          = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_QUARTER;
   static const int FUNC_EXTRACT_SECONDS_MIDNIGHT = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_SECONDS_MIDNIGHT;

   static const int FUNC_EXTRACT_DAY_NAME         = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_NAME;
   static const int FUNC_EXTRACT_MONTH_NAME       = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_MONTH_NAME;
   static const int FUNC_EXTRACT_YEAR             = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_YEAR;
   static const int FUNC_EXTRACT_MONTH            = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_MONTH;
   static const int FUNC_EXTRACT_DAY              = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY;
   static const int FUNC_EXTRACT_HOUR             = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_HOUR;
   static const int FUNC_EXTRACT_MINUTE           = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_MINUTE;
   static const int FUNC_EXTRACT_SECOND           = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_SECOND;

   static const int FUNC_EXTRACT_INTERVAL_YEAR             = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_INTERVAL_YEAR;
   static const int FUNC_EXTRACT_INTERVAL_MONTH            = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_INTERVAL_MONTH;
   static const int FUNC_EXTRACT_INTERVAL_DAY              = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_INTERVAL_DAY;
   static const int FUNC_EXTRACT_INTERVAL_HOUR             = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_INTERVAL_HOUR;
   static const int FUNC_EXTRACT_INTERVAL_MINUTE           = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_INTERVAL_MINUTE;
   static const int FUNC_EXTRACT_INTERVAL_SECOND           = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_INTERVAL_SECOND;
   static const int FUNC_EXTRACT_WEEKDAY                   = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_WEEKDAY;

   // VoltDB aliases (optimized implementations for existing HSQL functions)
   static const int FUNC_VOLT_SUBSTRING_CHAR_FROM              = 10000;

   // VoltDB-specific functions
   static const int FUNC_VOLT_SQL_ERROR                   = 20000;
   static const int FUNC_DECODE                           = 20001;
   static const int FUNC_VOLT_FIELD                       = 20002;
   static const int FUNC_VOLT_ARRAY_ELEMENT               = 20003;
   static const int FUNC_VOLT_ARRAY_LENGTH                = 20004;
   static const int FUNC_SINCE_EPOCH                      = 20005;
   static const int FUNC_SINCE_EPOCH_SECOND               = 20006;
   static const int FUNC_SINCE_EPOCH_MILLISECOND          = 20007;
   static const int FUNC_SINCE_EPOCH_MICROSECOND          = 20008;
   static const int FUNC_TO_TIMESTAMP                     = 20009;
   static const int FUNC_TO_TIMESTAMP_SECOND              = 20010;
   static const int FUNC_TO_TIMESTAMP_MILLISECOND         = 20011;
   static const int FUNC_TO_TIMESTAMP_MICROSECOND         = 20012;

   // VoltDB truncate timestamp function
   static const int FUNC_TRUNCATE_TIMESTAMP               = 20013; // FUNC_TRUNCATE is defined as 80 already
   static const int FUNC_TRUNCATE_YEAR                    = 20014;
   static const int FUNC_TRUNCATE_QUARTER                 = 20015;
   static const int FUNC_TRUNCATE_MONTH                   = 20016;
   static const int FUNC_TRUNCATE_DAY                     = 20017;
   static const int FUNC_TRUNCATE_HOUR                    = 20018;
   static const int FUNC_TRUNCATE_MINUTE                  = 20019;
   static const int FUNC_TRUNCATE_SECOND                  = 20020;
   static const int FUNC_TRUNCATE_MILLISECOND             = 20021;
   static const int FUNC_TRUNCATE_MICROSECOND             = 20022;

   static const int FUNC_VOLT_FROM_UNIXTIME               = 20023;

   static const int FUNC_VOLT_SET_FIELD                   = 20024;

   static const int FUNC_VOLT_FORMAT_CURRENCY             = 20025;
   // Check if the row has been migrated. Returns true when the migrating column is NOT NULL.
   // Rows that had been migrating has that column set to txn-id (i.e. not null) of the migrating process.
   static const int FUNC_VOLT_MIGRATING                   = 21026;

   static const int FUNC_VOLT_BITNOT                      = 20026;
   static const int FUNC_VOLT_BIT_SHIFT_LEFT              = 20027;
   static const int FUNC_VOLT_BIT_SHIFT_RIGHT             = 20028;
   static const int FUNC_VOLT_HEX                         = 20029;
   static const int FUNC_VOLT_BIN                         = 20030;

   static const int FUNC_VOLT_DATEADD                     = 20031;
   static const int FUNC_VOLT_DATEADD_YEAR                = 20032;
   static const int FUNC_VOLT_DATEADD_QUARTER             = 20033;
   static const int FUNC_VOLT_DATEADD_MONTH               = 20034;
   static const int FUNC_VOLT_DATEADD_DAY                 = 20035;
   static const int FUNC_VOLT_DATEADD_HOUR                = 20036;
   static const int FUNC_VOLT_DATEADD_MINUTE              = 20037;
   static const int FUNC_VOLT_DATEADD_SECOND              = 20038;
   static const int FUNC_VOLT_DATEADD_MILLISECOND         = 20039;
   static const int FUNC_VOLT_DATEADD_MICROSECOND         = 20040;
   static const int FUNC_VOLT_REGEXP_POSITION             = 20041;

   static const int FUNC_VOLT_ROUND                       = 20042;
   static const int FUNC_VOLT_STR                         = 20043;

   // our local function
   static const int FUNC_INET_NTOA                        = 20044;
   static const int FUNC_INET_ATON                        = 20045;
   static const int FUNC_INET6_NTOA                       = 20046;
   static const int FUNC_INET6_ATON                       = 20047;

   // Geospatial functions
   static const int FUNC_VOLT_POINTFROMTEXT               = 21000;
   static const int FUNC_VOLT_POLYGONFROMTEXT             = 21001;
   static const int FUNC_VOLT_CONTAINS                    = 21002;
   static const int FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS  = 21003;
   static const int FUNC_VOLT_POLYGON_NUM_POINTS          = 21004;
   static const int FUNC_VOLT_POINT_LATITUDE              = 21005;
   static const int FUNC_VOLT_POINT_LONGITUDE             = 21006;
   static const int FUNC_VOLT_POLYGON_CENTROID            = 21007;
   static const int FUNC_VOLT_POLYGON_AREA                = 21008;
   static const int FUNC_VOLT_DISTANCE                    = 21009;     // wrapper id for distance between all geo types
   static const int FUNC_VOLT_DISTANCE_POINT_POINT        = 21010;     // distance between point and point
   static const int FUNC_VOLT_DISTANCE_POLYGON_POINT      = 21011;     // distance between polygon and point
   static const int FUNC_VOLT_ASTEXT                      = 21012;     // wrapper function id for converting geography types into wkt
   static const int FUNC_VOLT_ASTEXT_GEOGRAPHY_POINT      = 21013;     // asText(<point value>)
   static const int FUNC_VOLT_ASTEXT_GEOGRAPHY            = 21014;     // asText(<geography value>)
   static const int FUNC_VOLT_IS_VALID_POLYGON            = 21015;     // Validate a polygon.
   static const int FUNC_VOLT_POLYGON_INVALID_REASON      = 21016;     // Reason a polygon is invalid.
   static const int FUNC_VOLT_DWITHIN                     = 21017;     // wrapper id for DWithin function
   static const int FUNC_VOLT_DWITHIN_POINT_POINT         = 21018;     // if two points are within certain distance of each other
   static const int FUNC_VOLT_DWITHIN_POLYGON_POINT       = 21019;     // if a polygon and a point are within certain distance of each other
   static const int FUNC_VOLT_VALIDPOLYGONFROMTEXT        = 21020;     // like polygonfromtext, but also validates the result
   static const int FUNC_VOLT_MIN_VALID_TIMESTAMP         = 21021;     // minimum valid timestamp
   static const int FUNC_VOLT_MAX_VALID_TIMESTAMP         = 21022;     // maximum valid timestamp
   static const int FUNC_VOLT_IS_VALID_TIMESTAMP          = 21023;     // is a timestamp valid
   static const int FUNC_VOLT_MAKE_VALID_POLYGON          = 21024;     // Make a polygon valid if necessary.
   static const int FUNC_VOLT_FORMAT_TIMESTAMP            = 21025;     // Convert a timestamp to a String in a given timezone.

   static const int FUNC_VOLT_DATETIME_DIFF               = 21027;
   static const int FUNC_VOLT_DATETIME_DIFF_YEAR          = 21028;
   static const int FUNC_VOLT_DATETIME_DIFF_QUARTER       = 21029;
   static const int FUNC_VOLT_DATETIME_DIFF_MONTH         = 21030;
   static const int FUNC_VOLT_DATETIME_DIFF_WEEK          = 21031;
   static const int FUNC_VOLT_DATETIME_DIFF_DAY           = 21032;
   static const int FUNC_VOLT_DATETIME_DIFF_HOUR          = 21033;
   static const int FUNC_VOLT_DATETIME_DIFF_MINUTE        = 21034;
   static const int FUNC_VOLT_DATETIME_DIFF_SECOND        = 21035;
   static const int FUNC_VOLT_DATETIME_DIFF_MILLIS        = 21036;
   static const int FUNC_VOLT_DATETIME_DIFF_MICROS        = 21037;

   static const int FUNC_VOLT_TIME_WINDOW                       = 21038;
   static const int FUNC_VOLT_TIME_WINDOW_YEAR_START            = 21039;
   static const int FUNC_VOLT_TIME_WINDOW_QUARTER_START         = 21040;
   static const int FUNC_VOLT_TIME_WINDOW_MONTH_START           = 21041;
   static const int FUNC_VOLT_TIME_WINDOW_WEEK_START            = 21042;
   static const int FUNC_VOLT_TIME_WINDOW_DAY_START             = 21043;
   static const int FUNC_VOLT_TIME_WINDOW_HOUR_START            = 21044;
   static const int FUNC_VOLT_TIME_WINDOW_MINUTE_START          = 21045;
   static const int FUNC_VOLT_TIME_WINDOW_SECOND_START          = 21046;
   static const int FUNC_VOLT_TIME_WINDOW_MILLIS_START          = 21047;

   static const int FUNC_VOLT_TIME_WINDOW_YEAR_END            = 21048;
   static const int FUNC_VOLT_TIME_WINDOW_QUARTER_END         = 21049;
   static const int FUNC_VOLT_TIME_WINDOW_MONTH_END           = 21050;
   static const int FUNC_VOLT_TIME_WINDOW_WEEK_END            = 21051;
   static const int FUNC_VOLT_TIME_WINDOW_DAY_END             = 21052;
   static const int FUNC_VOLT_TIME_WINDOW_HOUR_END            = 21053;
   static const int FUNC_VOLT_TIME_WINDOW_MINUTE_END          = 21054;
   static const int FUNC_VOLT_TIME_WINDOW_SECOND_END          = 21055;
   static const int FUNC_VOLT_TIME_WINDOW_MILLIS_END          = 21056;

    // From Tokens.java.
   static const int SQL_TRIM_LEADING                     = 149;
   static const int SQL_TRIM_TRAILING                    = 284;
   static const int SQL_TRIM_BOTH                        = 22;
   static const int SQL_START                            = 265;
   static const int SQL_END                              = 93;

   static const int FUNC_TRIM_LEADING_CHAR               = SQL_TRIM_VOLT_FUNC_OFFSET + SQL_TRIM_LEADING;
   static const int FUNC_TRIM_TRAILING_CHAR              = SQL_TRIM_VOLT_FUNC_OFFSET + SQL_TRIM_TRAILING;
   static const int FUNC_TRIM_BOTH_CHAR                  = SQL_TRIM_VOLT_FUNC_OFFSET + SQL_TRIM_BOTH;

   static const int FUNC_VOLT_UDF_ID_START                = 1000000;
#define IS_USER_DEFINED_ID(id) (FUNC_VOLT_UDF_ID_START <= (id))
}

// All of these "...functions.h" files need to be included AFTER the above definitions
// (FUNC_... constants and ...FunctionExpressionTemplates).
#include "bitwisefunctions.h"
#include "datefunctions.h"
#include "numericfunctions.h"
#include "stringfunctions.h"
#include "logicfunctions.h"
#include "jsonfunctions.h"

