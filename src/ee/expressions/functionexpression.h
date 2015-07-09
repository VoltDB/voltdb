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

#ifndef HSTOREFUNCTIONEXPRESSION_H
#define HSTOREFUNCTIONEXPRESSION_H

//#include "common/common.h"
//#include "common/serializeio.h"

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

// From FunctionSQL.java
static const int FUNC_POSITION_CHAR                    = 1;     // numeric
static const int FUNC_POSITION_BINARY                  = 2;
static const int FUNC_OCCURENCES_REGEX                 = 3;
static const int FUNC_POSITION_REGEX                   = 4;
static const int FUNC_EXTRACT                          = 5;
static const int FUNC_BIT_LENGTH                       = 6;
static const int FUNC_CHAR_LENGTH                      = 7;
static const int FUNC_OCTET_LENGTH                     = 8;
static const int FUNC_CARDINALITY                      = 9;
static const int FUNC_MAX_CARDINALITY                  = 10;
static const int FUNC_TRIM_ARRAY                       = 11;
static const int FUNC_ABS                              = 12;
static const int FUNC_MOD                              = 13;
static const int FUNC_LN                               = 14;
static const int FUNC_EXP                              = 15;
static const int FUNC_POWER                            = 16;
static const int FUNC_SQRT                             = 17;
static const int FUNC_FLOOR                            = 20;
static const int FUNC_CEILING                          = 21;
static const int FUNC_WIDTH_BUCKET                     = 22;
static const int FUNC_SUBSTRING_CHAR                   = 23;    // string
static const int FUNC_SUBSTRING_REG_EXPR               = 24;
static const int FUNC_SUBSTRING_REGEX                  = 25;
static const int FUNC_FOLD_LOWER                       = 26;
static const int FUNC_FOLD_UPPER                       = 27;
static const int FUNC_TRANSCODING                      = 28;
static const int FUNC_TRANSLITERATION                  = 29;
static const int FUNC_REGEX_TRANSLITERATION            = 30;
static const int FUNC_OVERLAY_CHAR                     = 32;
static const int FUNC_CHAR_NORMALIZE                   = 33;
static const int FUNC_SUBSTRING_BINARY                 = 40;
static const int FUNC_TRIM_BINARY                      = 41;
static const int FUNC_OVERLAY_BINARY                   = 42;
static const int FUNC_CURRENT_DATE                     = 43;    // datetime
static const int FUNC_CURRENT_TIME                     = 44;
static const int FUNC_CURRENT_TIMESTAMP                = 50;
static const int FUNC_LOCALTIME                        = 51;
static const int FUNC_LOCALTIMESTAMP                   = 52;
static const int FUNC_CURRENT_CATALOG                  = 53;    // general
static const int FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP  = 54;
static const int FUNC_CURRENT_PATH                     = 55;
static const int FUNC_CURRENT_ROLE                     = 56;
static const int FUNC_CURRENT_SCHEMA                   = 57;
static const int FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE = 58;
static const int FUNC_CURRENT_USER                     = 59;
static const int FUNC_SESSION_USER                     = 60;
static const int FUNC_SYSTEM_USER                      = 61;
static const int FUNC_USER                             = 62;
static const int FUNC_VALUE                            = 63;

// From FunctionCustom.java
static const int FUNC_ACOS                     = 71;
static const int FUNC_ACTION_ID                = 72;
static const int FUNC_ADD_MONTHS               = 73;
static const int FUNC_ASCII                    = 74;
static const int FUNC_ASIN                     = 75;
static const int FUNC_ATAN                     = 76;
static const int FUNC_ATAN2                    = 77;
static const int FUNC_BITAND                   = 78;
static const int FUNC_BITANDNOT                = 79;
static const int FUNC_BITNOT                   = 80;
static const int FUNC_BITOR                    = 81;
static const int FUNC_BITXOR                   = 82;
static const int FUNC_CHAR                     = 83;
static const int FUNC_CONCAT                   = 84;
static const int FUNC_COS                      = 85;
static const int FUNC_COT                      = 86;
static const int FUNC_CRYPT_KEY                = 87;
static const int FUNC_DATABASE                 = 88;
static const int FUNC_DATABASE_ISOLATION_LEVEL = 89;
static const int FUNC_DATABASE_NAME            = 90;
static const int FUNC_DATABASE_TIMEZONE        = 91;
static const int FUNC_DATABASE_VERSION         = 92;
static const int FUNC_DATE_ADD                 = 93;
static const int FUNC_DATE_SUB                 = 94;
static const int FUNC_DATEADD                  = 95;
static const int FUNC_DATEDIFF                 = 96;
static const int FUNC_DAYS                     = 97;
static const int FUNC_DBTIMEZONE               = 98;
static const int FUNC_DEGREES                  = 99;
static const int FUNC_DIAGNOSTICS              = 100;
static const int FUNC_DIFFERENCE               = 101;
static const int FUNC_FROM_TZ                  = 102;
static const int FUNC_HEXTORAW                 = 103;
static const int FUNC_IDENTITY                 = 104;
static const int FUNC_INSTR                    = 105;
static const int FUNC_ISAUTOCOMMIT             = 106;
static const int FUNC_ISOLATION_LEVEL          = 107;
static const int FUNC_ISREADONLYDATABASE       = 108;
static const int FUNC_ISREADONLYDATABASEFILES  = 109;
static const int FUNC_ISREADONLYSESSION        = 110;
static const int FUNC_LAST_DAY                 = 111;
static const int FUNC_LEFT                     = 112;
static const int FUNC_LOAD_FILE                = 113;
static const int FUNC_LOB_ID                   = 114;
static const int FUNC_LOCATE                   = 115;
static const int FUNC_LOG10                    = 116;
static const int FUNC_LPAD                     = 117;
static const int FUNC_MONTHS_BETWEEN           = 119;
static const int FUNC_NEW_TIME                 = 120;
static const int FUNC_NEXT_DAY                 = 121;
static const int FUNC_NUMTODSINTERVAL          = 122;
static const int FUNC_NUMTOYMINTERVAL          = 123;
static const int FUNC_PI                       = 124;
static const int FUNC_POSITION_ARRAY           = 125;
static const int FUNC_RADIANS                  = 126;
static const int FUNC_RAND                     = 127;
static const int FUNC_RAWTOHEX                 = 128;
static const int FUNC_REGEXP_MATCHES           = 129;
static const int FUNC_REGEXP_SUBSTRING         = 130;
static const int FUNC_REGEXP_SUBSTRING_ARRAY   = 131;
static const int FUNC_REPEAT                   = 132;
static const int FUNC_REPLACE                  = 133;
static const int FUNC_REVERSE                  = 134;
static const int FUNC_RIGHT                    = 135;
static const int FUNC_ROUND                    = 136;
static const int FUNC_ROUNDMAGIC               = 137;
static const int FUNC_RPAD                     = 138;
static const int FUNC_SECONDS_MIDNIGHT         = 140;
static const int FUNC_SEQUENCE_ARRAY           = 141;
static const int FUNC_SESSION_ID               = 142;
static const int FUNC_SESSION_ISOLATION_LEVEL  = 143;
static const int FUNC_SESSION_TIMEZONE         = 144;
static const int FUNC_SESSIONTIMEZONE          = 145;
static const int FUNC_SIGN                     = 146;
static const int FUNC_SIN                      = 147;
static const int FUNC_SOUNDEX                  = 148;
static const int FUNC_SORT_ARRAY               = 149;
static const int FUNC_SPACE                    = 150;
static const int FUNC_SUBSTR                   = 151;
static const int FUNC_SYS_EXTRACT_UTC          = 152;
static const int FUNC_SYSDATE                  = 153;
static const int FUNC_SYSTIMESTAMP             = 154;
static const int FUNC_TAN                      = 155;
static const int FUNC_TIMESTAMP                = 156;
static const int FUNC_TIMESTAMP_WITH_ZONE      = 157;
static const int FUNC_TIMESTAMPADD             = 158;
static const int FUNC_TIMESTAMPDIFF            = 159;
static const int FUNC_TIMEZONE                 = 160;
static const int FUNC_TO_CHAR                  = 161;
static const int FUNC_TO_DATE                  = 162;
static const int FUNC_TO_DSINTERVAL            = 163;
static const int FUNC_TO_YMINTERVAL            = 164;
static const int FUNC_TO_NUMBER                = 165;
//CONFLICT WITH VOLTDB USAGE: static const int FUNC_TO_TIMESTAMP             = 166;
static const int FUNC_TO_TIMESTAMP_TZ          = 167;
static const int FUNC_TRANSACTION_CONTROL      = 168;
static const int FUNC_TRANSACTION_ID           = 169;
static const int FUNC_TRANSACTION_SIZE         = 170;
static const int FUNC_TRANSLATE                = 171;
static const int FUNC_TRUNC                    = 172;
static const int FUNC_TRUNCATE                 = 173;
static const int FUNC_UUID                     = 174;
static const int FUNC_UNIX_TIMESTAMP           = 175;
static const int FUNC_UNIX_MILLIS              = 176;


// Function ID offsets for specializations of EXTRACT and TRIM.
// Individual ID values are based on various Tokens.java constants
// and need to be adjusted by these constant offsets to avoid overlap
// with other sources of Function ID constants.
// These are from FunctionSQL.java
static const int SQL_EXTRACT_VOLT_FUNC_OFFSET = 1000;
static const int SQL_TRIM_VOLT_FUNC_OFFSET = 2000;

// These are from Tokens.java. prefixed with SQL_
static const int SQL_DAY                        =  73;
static const int SQL_HOUR                       = 127;
static const int SQL_MINUTE                     = 169;
static const int SQL_MONTH                      = 173;
static const int SQL_SECOND                     = 250;
static const int SQL_YEAR                       = 323;
static const int SQL_DAY_OF_MONTH               = 672;
static const int SQL_DAY_OF_WEEK                = 673;
static const int SQL_DAY_NAME                   = 671;
static const int SQL_DAY_OF_YEAR                = 674;
static const int SQL_MONTH_NAME                 = 708;
static const int SQL_QUARTER                    = 722;
static const int SQL_WEEKDAY                    = 789;
static const int SQL_WEEK_OF_YEAR               = 791;

static const int FUNC_EXTRACT_DAY_OF_WEEK      = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_OF_WEEK;
static const int FUNC_EXTRACT_DAY_OF_MONTH     = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_OF_MONTH;
static const int FUNC_EXTRACT_DAY_OF_YEAR      = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_OF_YEAR;
static const int FUNC_EXTRACT_WEEK_OF_YEAR     = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_WEEK_OF_YEAR;
static const int FUNC_EXTRACT_QUARTER          = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_QUARTER;

static const int FUNC_EXTRACT_DAY_NAME         = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY_NAME;
static const int FUNC_EXTRACT_MONTH_NAME       = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_MONTH_NAME;
static const int FUNC_EXTRACT_YEAR             = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_YEAR;
static const int FUNC_EXTRACT_MONTH            = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_MONTH;
static const int FUNC_EXTRACT_DAY              = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_DAY;
static const int FUNC_EXTRACT_HOUR             = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_HOUR;
static const int FUNC_EXTRACT_MINUTE           = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_MINUTE;
static const int FUNC_EXTRACT_SECOND           = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_SECOND;

static const int FUNC_EXTRACT_WEEKDAY          = SQL_EXTRACT_VOLT_FUNC_OFFSET + SQL_WEEKDAY;

// VoltDB aliases (optimized implementations for existing HSQL functions)
static const int FUNC_VOLT_SUBSTRING_CHAR_FROM              = 10000;

// These are from FunctionForVoltDB.java.
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

static const int FUNC_VOLT_BITNOT                      = 20026;
static const int FUNC_VOLT_BIT_SHIFT_LEFT              = 20027;
static const int FUNC_VOLT_BIT_SHIFT_RIGHT             = 20028;
static const int FUNC_VOLT_HEX                         = 20029;
static const int FUNC_VOLT_BIN                         = 20030;

// From Tokens.java.
static const int SQL_TRIM_LEADING                     = 151;
static const int SQL_TRIM_TRAILING                    = 286;
static const int SQL_TRIM_BOTH                        = 23;

static const int FUNC_TRIM_LEADING_CHAR               = SQL_TRIM_VOLT_FUNC_OFFSET + SQL_TRIM_LEADING;
static const int FUNC_TRIM_TRAILING_CHAR              = SQL_TRIM_VOLT_FUNC_OFFSET + SQL_TRIM_TRAILING;
static const int FUNC_TRIM_BOTH_CHAR                  = SQL_TRIM_VOLT_FUNC_OFFSET + SQL_TRIM_BOTH;

}

// All of these "...functions.h" files need to be included AFTER the above definitions
// (FUNC_... constants and ...FunctionExpressionTemplates).
#include "bitwisefunctions.h"
#include "datefunctions.h"
#include "numericfunctions.h"
#include "stringfunctions.h"
#include "logicfunctions.h"
#include "jsonfunctions.h"

#endif
