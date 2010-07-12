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

/**
 * SQL error codes.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public interface ErrorCode {

    //
    int LAST_ERROR_HANDLE = 300;                          // temporary, used in JDBC tests only

    // neutral placeholder strings
    int TOKEN_REQUIRED = 1;                               // $$ required: $$
    int CONSTRAINT     = 2;                               // $$ table: $$

    /** @todo - all SQL-state for legacy messages to be reviewed */
    int LOCK_FILE_ACQUISITION_FAILURE = 11;               //Database lock acquisition failure

    //
    int JDBC_COLUMN_NOT_FOUND      = 28;                  // S1000 Column not found
    int JDBC_INPUTSTREAM_ERROR     = 34;                  // S1000 InputStream error
    int JDBC_INVALID_ARGUMENT      = 62;                  // S1OO0 Invalid argument in JDBC call
    int JDBC_PARAMETER_NOT_SET     = 152;                 // S1000 Parameter not set
    int JDBC_CONNECTION_NATIVE_SQL = 138;                 // S1000 Unknown JDBC escape sequence: {

    //
    int FILE_IO_ERROR               = 29;                 // S1000 File input/output error
    int WRONG_DATABASE_FILE_VERSION = 30;                 // S1000 Wrong database file version
    int DATABASE_IS_READONLY        = 31;                 // S1000 The database is in read only mode
    int DATA_IS_READONLY            = 32;                 // S1000 The table data is read only
    int ACCESS_IS_DENIED            = 33;                 // S1000 Access is denied
    int NO_DATA_IS_AVAILABLE        = 35;                 // S1000 No data is available
    int GENERAL_ERROR               = 40;                 // S1000 General error
    int DATABASE_IS_MEMORY_ONLY     = 63;                 // S1000 Database is memory only
    int OUT_OF_MEMORY               = 72;                 // S1000 Out of Memory
    int ERROR_IN_SCRIPT_FILE        = 78;                 // S1000 error in script file
    int UNSUPPORTED_FILENAME_SUFFIX = 79;                 // S1000 Unsupported suffix in file name '$$'.  (Supported suffixes:  $$)
    int COMPRESSION_SUFFIX_MISMATCH = 80;                 // S1000 Mismatch between specified compression mode '$$' and file name '$$'
    int DATABASE_IS_NON_FILE        = 82;                 // S1000 Database is non-file type
    int DATABASE_NOT_EXISTS         = 94;                 // S1000 Database does not exists
    int DATA_FILE_ERROR             = 129;                // S1000 Data File input/output error
    int GENERAL_IO_ERROR            = 98;                 // S1000 IO error

    //
    int M_ERROR_IN_BINARY_SCRIPT_1              = 96;     // \u0020table $$ row count error : $$ read, needed $$
    int M_ERROR_IN_BINARY_SCRIPT_2              = 97;     // \u0020 wrong data for insert operation
    int M_DatabaseManager_getDatabase           = 107;    // attempt to connect while db opening /closing
    int M_DatabaseScriptReader_readDDL          = 113;    // \u0020line: $$ $$
    int M_DatabaseScriptReader_readExistingData = 114;    // \u0020line: $$ $$
    int M_Message_Pair                          = 115;    // \u0020$$ $$
    int M_LOAD_SAVE_PROPERTIES                  = 116;    // \u0020$$.properties $$
    int M_HsqlProperties_load                   = 135;    // properties name is null or empty

    //
    int TEXT_TABLE_UNKNOWN_DATA_SOURCE  = 48;             // S1000 The table's data source for has not been defined
    int TEXT_TABLE_SOURCE               = 75;             // S1000 Invalid TEXT table source string
    int TEXT_FILE                       = 76;             // S1000 bad TEXT table source file - line number: $$ $$
    int TEXT_FILE_IO                    = 77;             // S1000 TEXT table source file - IO error:
    int TEXT_STRING_HAS_NEWLINE         = 145;            // S1000 end-of-line characters not allowed
    int TEXT_TABLE_HEADER               = 150;            // S1000 Header not allowed or too long
    int TEXT_SOURCE_EXISTS              = 196;            // S1000 Text source file already exists
    int TEXT_SOURCE_NO_END_SEPARATOR    = 183;            // S1000 No end sep.
    int M_TEXT_SOURCE_FIELD_ERROR       = 184;            // Error in text source field
    int M_TextCache_openning_file_error = 188;            // openning file: $$ error: $$
    int M_TextCache_closing_file_error  = 189;            // closing file: $$ error: $$
    int M_TextCache_purging_file_error  = 190;            // purging file: $$ error: $$

    //
    int M_DataFileCache_makeRow = 209;                    // error $$ reading row - file $$
    int M_DataFileCache_open    = 210;                    // error $$ opening file - file $$
    int M_DataFileCache_close   = 211;                    // error $$ closing file - file $$
    int DATA_FILE_IS_FULL       = 225;                    // S1000 Data file size limit is reached

    //
    int SERVER_TRANSFER_CORRUPTED        = 19;            // S1000 Transfer corrupted
    int SERVER_DATABASE_DISCONNECTED     = 259;           // S0504 database disconnected
    int SERVER_VERSIONS_INCOMPATIBLE     = 260;           // S0504 Client version '$$' incompatible.  \
                                                          //    HSQLDB Network-Compatibility-Version '$$' required.
    int SERVER_UNKNOWN_CLIENT            = 261;           // S0504 Network client is not a HSQLDB JDBC driver
    int SERVER_HTTP_NOT_HSQL_PROTOCOL    = 262;           // S0504 Client using HSQLDB http protocol instead of hsql
    int SERVER_INCOMPLETE_HANDSHAKE_READ = 263;           // S0504 Incomplete read of handshaking bytes
    int M_SERVER_NO_DATABASE             = 147;           // S1000 no valid database paths
    int M_SERVER_OPEN_SERVER_SOCKET_1    = 148;           // Invalid address : $$\nTry one of: $$
    int M_SERVER_OPEN_SERVER_SOCKET_2    = 149;           // Invalid address : $$
    int M_SERVER_SECURE_VERIFY_1         = 136;           // Server certificate has no Common Name
    int M_SERVER_SECURE_VERIFY_2         = 137;           // Server certificate has empty Common Name
    int M_SERVER_SECURE_VERIFY_3         = 139;           // Certificate Common Name[$$] does not match host name[$$]

    //                                                       
    int INVALID_LIMIT = 153;                              // ; in LIMIT, OFFSET or FETCH

    //
    int U_S0500 = 401;
    int X_S0501 = 301;                                    // no file name specified for source // TEXT_TABLE_SOURCE_FILENAME = 172
    int X_S0502 = 302;                                    // no value specified for field // TEXT_TABLE_SOURCE_VALUE_MISSING= 173
    int X_S0503 = 303;                                    // zero-length separator // TEXT_TABLE_SOURCE_SEPARATOR = 174
    int X_S0504           = 304;
    int X_S0521           = 320;                          // operation is not allowed on text table with data
    int X_S0522           = 321;                          // invalid statemnet - text table required
    int M_RS_EMPTY        = 410;                          // ResultSet is empty
    int M_RS_BEFORE_FIRST = 411;                          // ResultSet is positioned before first row
    int M_RS_AFTER_LAST   = 412;                          // ResultSet is positioned after last row

    // SQLSTATE definitions
    // successful completion
    int S_00000 = 0000;                                   // successful completion

    // warning
    int W_01000 = 1000;                                   // warning - generic
    int W_01001 = 1001;                                   // cursor operation conflict - 200x
    int W_01002 = 1002;                                   // disconnect error - 200x
    int W_01003 = 1003;                                   // null value eliminated in set function - 200x
    int W_01004 = 1004;                                   // string data, right truncation - 200x
    int W_01005 = 1005;                                   // insufficient item descriptor areas - 200x
    int W_01006 = 1006;                                   // privilege not revoked - 200x
    int W_01007 = 1007;                                   // privilege not granted - 200x
    int W_01009 = 1009;                                   // search condition too long for information schema - 200x
    int W_0100A = 1010;                                   // query expression too long for information schema - 200x
    int W_0100B = 1011;                                   // default value too long for information schema - 200x
    int W_0100C = 1012;                                   // result sets returned - 200x
    int W_0100D = 1013;                                   // additional result sets returned - 200x
    int W_0100E = 1014;                                   // attempt to return too many result sets - 200x
    int W_0100F = 1015;                                   // statement too long for information schema - 200x
    int W_01011 = 1016;                                   // SQL-Java path too long for information schema
    int W_0102F = 1017;                                   // array data, right truncation - 200x

    // no data - 200x
    int N_02000 = 1100;                                   // no data - generic
    int N_02001 = 1101;                                   // no data: no additional result sets returned

    // dynamic SQL error - 200x
    int X_07000 = 1200;                                   // dynamic SQL error
    int X_07001 = 1201;                                   // dynamic SQL error: using clause does not match dynamic parameter specifications
    int X_07002 = 1202;                                   // dynamic SQL error: using clause does not match target specifications
    int X_07003 = 1203;                                   // dynamic SQL error: cursor specification cannot be executed
    int X_07004 = 1204;                                   // dynamic SQL error: using clause required for dynamic parameters
    int X_07005 = 1205;                                   // dynamic SQL error: prepared statement not a cursor specification
    int X_07006 = 1206;                                   // dynamic SQL error: restricted data type attribute violation
    int X_07007 = 1207;                                   // dynamic SQL error: using clause required for result fields
    int X_07008 = 1208;                                   // dynamic SQL error: invalid descriptor count
    int X_07009 = 1209;                                   // dynamic SQL error: invalid descriptor index
    int X_0700B = 1211;                                   // dynamic SQL error: data type transform function violation
    int X_0700C = 1212;                                   // dynamic SQL error: undefined DATA value
    int X_0700D = 1213;                                   // dynamic SQL error: invalid DATA target
    int X_0700E = 1214;                                   // dynamic SQL error: invalid LEVEL value
    int X_0700F = 1215;                                   // dynamic SQL error: invalid DATETIME_INTERVAL_CODE

    // HSQLDB
    int X_07501 = 1251;                                   // statement is closed
    int X_07502 = 1252;                                   // statement is invalid
    int X_07503 = 1253;                                   // statement generates a result set
    int X_07504 = 1254;                                   // statement does not generate a result set
    int X_07505 = 1255;                                   // statement is in batch mode
    int X_07506 = 1256;                                   // statement is not in batch mode

    // connection exception - 200x
    int X_08000 = 1300;                                   // connection exception
    int X_08001 = 1301;                                   // connection exception: SQL-client unable to establish SQL-connection
    int X_08002 = 1302;                                   // connection exception: connection name in use
    int X_08003 = 1303;                                   // connection exception: connection does not exist
    int X_08004 = 1304;                                   // connection exception: SQL-server rejected establishment of SQL-connection
    int X_08006 = 1305;                                   // connection exception: connection failure
    int X_08007 = 1306;                                   // connection exception: transaction resolution unknown

    // HSQLDB connection
    int X_08501 = 1351;                                   // connection exception: timed out
    int X_08502 = 1352;                                   // warning: unsupported client info
    int X_08503 = 1353;                                   // connection exception: closed

    // triggered action exception - 200x
    int X_09000 = 1400;                                   // triggered action exception

    // feature not supported - 200x
    int X_0A000 = 1500;                                   // feature not supported
    int X_0A001 = 1501;                                   // feature not supported: multiple server transactions

    // HSQLDB feature not supported
    int X_0A501 = 1551;                                   // feature not supported:

    // invalid target type specification - 200x
    int X_0D000 = 1600;                                   // invalid target type specification

    // invalid schema name list specification - 200x
    int X_0E000 = 1700;                                   // invalid schema name list specification

    // locator exception - 2003
    int X_0F000 = 1800;                                   // locator exception
    int X_0F001 = 1801;                                   // locator exception: invalid specification

    // HSQLDB locator
    int X_0F502 = 3474;                                   // lob is no long valid
    int X_0F503 = 3475;                                   // lob stream is closed

    // resignal when handler not active - xxxx
    int X_0K000 = 1900;                                   // resignal when handler not active

    // invalid grantor - 2003
    int X_0L000 = 2000;                                   // invalid grantor

    // HSQLDB
    int X_0L501 = 2051;                                   // invalid grantor - lacks CREATE_SCHEMA privilege

    // invalid SQL-invoked procedure reference - 2003
    int X_0M000 = 2100;                                   // invalid SQL-invoked procedure reference

    // invalid role specification - 2003
    int X_0P000 = 2200;                                   // invalid role specification

    // HSQLDB
    int X_0P501 = 2251;                                   // invalid role specification - circular grant
    int X_0P502 = 2252;                                   // invalid role specification - already granted
    int X_0P503 = 2253;                                   // invalid role specification - does not have role

    // invalid transform group name specification - 2003
    int X_0S000 = 2300;                                   // invalid transform group name specification

    // target table disagrees with cursor specification - 2003
    int X_0T000 = 2400;                                   // target table disagrees with cursor specification

    // attempt to assign to non-updatable column - 2003
    int X_0U000 = 2500;                                   // attempt to assign to non-updatable column

    // attempt to assign to ordering column - 2003
    int X_0V000 = 2600;                                   // attempt to assign to ordering column

    // prohibited statement encountered during trigger execution - 2003
    int X_0W000 = 2700;                                   // prohibited statement encountered during trigger execution

    // invalid foreign server specification - xxxx
    int X_0X000 = 2800;                                   // invalid foreign server specification

    // pass-through specific condition - xxxx
    int X_0Y000 = 2900;                                   // pass-through specific condition
    int X_0Y001 = 2901;                                   // pass-through specific condition: invalid cursor option
    int X_0Y002 = 2902;                                   // pass-through specific condition: invalid cursor allocation

    // diagnostics exception - 2003
    int X_0Z000 = 3000;                                   // diagnostics exception
    int X_0Z001 = 3001;                                   // diagnostics exception: maximum number of stacked diagnostics areas exceeded

    // 04-PSM - xxxx
    int X_0Z002 = 3003;                                   // diagnostics exception: stacked diagnostics accessed without active handler

    // 04-PSM - 2003
    int X_20000 = 3100;                                   // case not found for case statement

    // cardinality violation - 2003
    int X_21000 = 3201;                                   // cardinality violation

    // data exception - xxxx
    int X_22000 = 3400;                                   // data exception - generic
    int X_22001 = 3401;                                   // data exception: string data, right truncation - 200x
    int X_22002 = 3402;                                   // data exception: null value, no indicator parameter - 200x
    int X_22003 = 3403;                                   // data exception: numeric value out of range - 200x
    int X_22004 = 3404;                                   // data exception: null value not allowed - 200x
    int X_22005 = 3405;                                   // data exception: error in assignment - 200x
    int X_22006 = 3406;                                   // data exception: invalid interval format - 200x
    int X_22007 = 3407;                                   // data exception: invalid datetime format - 200x
    int X_22008 = 3408;                                   // data exception: datetime field overflow - 200x
    int X_22009 = 3409;                                   // data exception: invalid time zone displacement value - 200x
    int X_2200B = 3410;                                   // data exception: escape character conflict - 200x
    int X_2200C = 3411;                                   // data exception: invalid use of escape character - 200x
    int X_2200D = 3412;                                   // data exception: invalid escape octet - 200x
    int X_2200E = 3413;                                   // data exception: null value in array target - 200x
    int X_2200F = 3414;                                   // data exception: zero-length character string - 200x
    int X_2200G = 3415;                                   // data exception: most specific type mismatch - 200x
    int X_2200H = 3416;                                   // data exception: sequence generator limit exceeded - 200x
    int X_2200J = 3417;                                   // data exception: nonidentical notations with the same name - xxxx
    int X_2200K = 3418;                                   // data exception: nonidentical unparsed entities with the same name - xxxx
    int X_2200L = 3419;                                   // data exception: not an XML document - xxxx
    int X_2200M = 3420;                                   // data exception: invalid XML document - xxxx
    int X_2200N = 3421;                                   // data exception: invalid XML content - xxxx
    int X_2200P = 3422;                                   // data exception: interval value out of range - 200x
    int X_2200Q = 3423;                                   // data exception: multiset value overflow - 200x
    int X_2200R = 3424;                                   // data exception: XML value overflow - xxxx
    int X_2200S = 3425;                                   // data exception: invalid comment - xxxx
    int X_2200T = 3426;                                   // data exception: invalid processing instruction - xxxx
    int X_2200U = 3427;                                   // data exception: not an XQuery document node - xxxx
    int X_2200V = 3428;                                   // data exception: invalid XQuery context item - xxxx
    int X_2200W = 3429;                                   // data exception: XQuery serialization error - xxxx
    int X_22010 = 3430;                                   // data exception: invalid indicator parameter value - 200x
    int X_22011 = 3431;                                   // data exception: substring error - 200x
    int X_22012 = 3432;                                   // data exception: division by zero - 200x
    int X_22013 = 3433;                                   // data exception: invalid preceding or following size in window function - 200x
    int X_22014 = 3434;                                   // data exception: invalid argument for NTILE function - 200x
    int X_22015 = 3435;                                   // data exception: interval field overflow - 200x
    int X_22016 = 3436;                                   // data exception: invalid argument for NTH_VALUE function - 200x
    int X_22017 = 3437;                                   // data exception: invalid data specified for datalink - xxxx
    int X_22018 = 3438;                                   // data exception: invalid character value for cast - 200x
    int X_22019 = 3439;                                   // data exception: invalid escape character - 200x
    int X_2201A = 3440;                                   // data exception: null argument passed to datalink constructor
    int X_2201B = 3441;                                   // data exception: invalid regular expression - 200x
    int X_2201C = 3442;                                   // data exception: null row not permitted in table - 200x
    int X_2201D = 3443;                                   // data exception: datalink value exceeds maximum length
    int X_2201E = 3444;                                   // data exception: invalid argument for natural logarithm - 200x
    int X_2201F = 3445;                                   // data exception: invalid argument for power function - 200x
    int X_2201G = 3446;                                   // data exception: invalid argument for width bucket function - 200x
    int X_2201J = 3447;                                   // data exception: XQuery sequence cannot be validated
    int X_2201S = 3448;                                   // data exception: invalid XQuery regular expression - 200x
    int X_2201T = 3449;                                   // data exception: invalid XQuery option flag - 200x
    int X_2201U = 3450;                                   // data exception: attempt to replace a zero-length string - 200x
    int X_2201V = 3451;                                   // data exception: invalid XQuery replacement string - 200x
    int X_2201W = 3452;                                   // data exception: invalid row count in fetch first clause - 200x
    int X_2201X = 3453;                                   // data exception: invalid row count in result offset clause - 200x
    int X_22021 = 3454;                                   // data exception: character not in repertoire - 200x
    int X_22022 = 3455;                                   // data exception: indicator overflow - 200x
    int X_22023 = 3456;                                   // data exception: invalid parameter value - 200x
    int X_22024 = 3457;                                   // data exception: unterminated C string - 200x
    int X_22025 = 3458;                                   // data exception: invalid escape sequence - 200x
    int X_22026 = 3459;                                   // data exception: string data, length mismatch - 200x
    int X_22027 = 3460;                                   // data exception: trim error - 200x
    int X_22029 = 3461;                                   // data exception: noncharacter in UCS string - 200x

    // HSQLDB data exception
    int X_22501 = 3471;                                   // value cannot be converted to target type
    int X_22511 = 3472;                                   // invalid function argument
    int X_22521 = 3473;                                   // object serialization failure

    // 04-PSM - xxxx
    int X_2202A = 3488;                                   // data exception: null value in field reference
    int X_2202D = 3489;                                   // data exception: null value substituted for mutator subject parameter - 200x
    int X_2202E = 3490;                                   // data exception: array element error
    int X_2202F = 3491;                                   // data exception: array data, right truncation - 200x
    int X_2202G = 3492;                                   // data exception: invalid repeat argument in a sample clause - 200x
    int X_2202H = 3493;                                   // data exception: invalid sample size - 200x

    // integrity constraint violation - xxxx
    int X_23000 = 3500;                                   // integrity constraint violation - 200x
    int X_23001 = 3501;                                   // integrity constraint violation: restrict violation - 200x
    int X_23501 = 8;                                      // integrity constraint violation: foreing key no action
    int X_23502 = 177;                                    // integrity constraint violation: foreign key no parent
    int X_23503 = 10;                                     // integrity constraint violation: NOT NULL check constraint
    int X_23504 = 157;                                    // integrity constraint violation: check constraint
    int X_23505 = 104;                                    // integrity constraint violation: unique constraint or index

    // invalid cursor state - xxxx
    int X_24000 = 3600;                                   // invalid cursor state - 2003
    int X_24501 = 3601;                                   // invalid cursor state: identified cursor is not open
    int X_24502 = 3602;                                   // invalid cursor state: identified cursor is already open
    int X_24504 = 3603;                                   // invalid cursor state: identifier cursor not positioned on row in UPDATE, DELETE, SET, or GET statement
    int X_24513 = 3604;                                   // invalid cursor state: cannot FETCH NEXT, PRIOR, CURRENT, or RELATIVE, cursor position is unknown
    int X_24514 = 3605;                                   // invalid cursor state: cursor disabled by previous error
    int X_24515 = 3606;                                   // invalid cursor state: all column must be set before insert

    // invalid transaction state - 200x
    int X_25000 = 3700;                                   // invalid transaction state
    int X_25001 = 3701;                                   // invalid transaction state: active SQL-transaction
    int X_25002 = 3702;                                   // invalid transaction state: branch transaction already active
    int X_25003 = 3703;                                   // invalid transaction state: inappropriate access mode for branch transaction
    int X_25004 = 3704;                                   // invalid transaction state: inappropriate isolation level for branch transaction
    int X_25005 = 3705;                                   // invalid transaction state: no active SQL-transaction for branch transaction
    int X_25006 = 3706;                                   // invalid transaction state: read-only SQL-transaction
    int X_25007 = 3707;                                   // invalid transaction state: schema and data statement mixing not supported
    int X_25008 = 3708;                                   // invalid transaction state: held cursor requires same isolation level

    // invalid SQL statement name - 2003
    int X_26000 = 3800;                                   // invalid SQL statement name

    // triggered data change violation - 2003
    int X_27000 = 3900;                                   // triggered data change violation

    // invalid authorization specification - 2003
    int X_28000 = 4000;                                   // invalid authorization specification

    // HSQLDB invalid authorization specification
    int X_28501 = 4001;                                   // invalid authorization specification - not found
    int X_28502 = 4002;                                   // invalid authorization specification - system identifier
    int X_28503 = 4003;                                   // invalid authorization specification - already exists

    // syntax error or access rule violation in direct statement
    int X_2A000 = 4100;                                   // syntax error or access rule violation in direct statement

    // dependent privilege descriptors still exist
    int X_2B000 = 4200;                                   // dependent privilege descriptors still exist

    // invalid character set name
    int X_2C000 = 4300;                                   // invalid character set name

    // invalid transaction termination
    int X_2D000 = 4400;                                   // invalid transaction termination
    int X_2D522 = 4401;                                   // invalid transaction termination: COMMIT and ROLLBACK not allowed in ATOMIC compound statement

    // invalid connection name
    int X_2E000 = 4500;                                   //invalid connection name

    // SQL routine exception
    int X_2F000 = 4600;                                   // SQL routine exception
    int X_2F002 = 4602;                                   // SQL routine exception: modifying SQL-data not permitted
    int X_2F003 = 4603;                                   // SQL routine exception: prohibited SQL-statement attempted
    int X_2F004 = 4604;                                   // SQL routine exception: reading SQL-data not permitted
    int X_2F005 = 4605;                                   // SQL routine exception: function did not execute return statement

    // invalid collation name
    int X_2H000 = 4650;                                   // invalid collation name

    // invalid SQL statement identifier
    int X_30000 = 4660;                                   // invalid SQL statement identifier

    // invalid SQL descriptor name
    int X_33000 = 4670;                                   // invalid SQL descriptor name

    // invalid cursor name
    int X_34000 = 4680;                                   // invalid cursor name

    // invalid condition number
    int X_35000 = 4690;                                   // invalid condition number

    // cursor sensitivity exception - 200x
    int X_36000 = 4700;                                   // cursor sensitivity exception
    int X_36001 = 4701;                                   // cursor sensitivity exception: request rejected
    int X_36002 = 4702;                                   // cursor sensitivity exception: request failed

    // syntax error or access rule violation in dynamic statement - xxxx
    int X_37000 = 4710;                                   // syntax error or access rule violation in dynamic statement

    // external routine exception - 2003
    int X_38000 = 4800;                                   // external routine exception
    int X_38001 = 4801;                                   // external routine exception: containing SQL not permitted
    int X_38002 = 4802;                                   // external routine exception: modifying SQL-data not permitted
    int X_38003 = 4803;                                   // external routine exception: prohibited SQL-statement attempted
    int X_38004 = 4804;                                   // external routine exception: reading SQL-data not permitted

    // external routine invocation exception - 200x
    int X_39000 = 4810;                                   // external routine invocation exception
    int X_39004 = 4811;                                   // external routine invocation exception: null value not allowed

    // savepoint exception - 200x
    int X_3B000 = 4820;                                   // savepoint exception
    int X_3B001 = 4821;                                   // savepoint exception: invalid specification
    int X_3B002 = 4822;                                   // savepoint exception: too many

    // ambiguous cursor name - 200x
    int X_3C000 = 4830;                                   // ambiguous cursor name

    // invalid catalog name - 200x
    int X_3D000 = 4840;                                   // invalid catalog name

    // invalid schema name - 200x
    int X_3F000 = 4850;                                   // invalid schema name

    // transaction rollback - 200x
    int X_40000 = 4860;                                   // transaction rollback - generic
    int X_40001 = 4861;                                   // serialization failure
    int X_40002 = 4862;                                   // integrity constraint violation
    int X_40003 = 4863;                                   // statement completion unknown
    int X_40004 = 4864;                                   // triggered action exception

    // syntax error or access rule violation - xxxx
    int X_42000 = 5000;                                   // syntax error or access rule violation - generic - 200x

    // HSQLDB database object names
    int X_42501 = 5501;                                   // user lacks privilege or object not found
    int X_42502 = 5502;                                   // dependent objects exist
    int X_42503 = 5503;                                   // system object cannot be modified
    int X_42504 = 5504;                                   // object name already exists
    int X_42505 = 5505;                                   // invalid schema name - name mismatch
    int X_42506 = 5506;                                   // invalid catalog name
    int X_42507 = 5507;                                   // admin rights required
    int X_42508 = 5508;                                   // label not found
    int X_42509 = 5509;                                   // type not found or user lacks privilege
    int X_42511 = 5511;                                   // invalid property name
    int X_42512 = 5512;                                   // invalid property value
    int X_42513 = 5513;                                   // property cannot be changed

    // constraint defintition issues
    int X_42520 = 5520;                                   // SET NULL requires nullable column
    int X_42521 = 5521;                                   // SET DEFAULT requires column default expression for
    int X_42522 = 5522;                                   // a UNIQUE constraint already exists on the set of columns
    int X_42523 = 5523;                                   // table has no primary key
    int X_42524 = 5524;                                   // constraint definition not allowed
    int X_42525 = 5525;                                   // identity definition not allowed
    int X_42526 = 5526;                                   // column is in primary key
    int X_42527 = 5527;                                   // column is in constraint
    int X_42528 = 5528;                                   // a FOREIGN KEY constraint already exists on the set of columns
    int X_42529 = 5529;                                   // a UNIQUE constraint does not exist on referenced columns
    int X_42530 = 5530;                                   // primary key definition not allowed
    int X_42531 = 5531;                                   // default expression needed
    int X_42532 = 5532;                                   // primary key already exist
    int X_42533 = 5533;                                   // is referenced by FOREIGN KEY constraint

    // other definition issues
    int X_42535 = 5535;                                   // not an identity column
    int X_42536 = 5536;                                   // column is referenced in
    int X_42537 = 5537;                                   // cannot use WITH CHECK option for read-only view
    int X_42538 = 5538;                                   // TRIGGER definition not compatible with table
    int X_42539 = 5539;                                   // cannot drop a user that is currently connected

    // DML
    int X_42541 = 5541;                                   // requires DEFAULT keyword
    int X_42542 = 5542;                                   // requires OVERRIDING clause
    int X_42543 = 5543;                                   // requires either DEFAULT keyword or OVERRIDING clause
    int X_42544 = 5544;                                   // DEFAULT keyword cannot be used as column has no DEFAULT
    int X_42545 = 5545;                                   // INSERT, UPDATE, or DELETE not permitted for view
    int X_42546 = 5546;                                   // number of target columns does not match that of query expression
    int X_42547 = 5547;                                   // merge when matched already used
    int X_42548 = 5548;                                   // merge when not matched altready used
    int X_42549 = 5549;                                   // LIMIT, OFFSET or FETCH altready used

    //
    int X_42551 = 5551;                                   // too many identifier parts

    // HSQLDB type conversion
    int X_42561 = 5561;                                   // incompatible data type in conversion
    int X_42562 = 5562;                                   // incompatible data types in combination
    int X_42563 = 5563;                                   // incompatible data type in comparison
    int X_42564 = 5564;                                   // row column count mismatch
    int X_42565 = 5565;                                   // incompatible data type in operation
    int X_42566 = 5566;                                   // interval qualifier missing
    int X_42567 = 5567;                                   // data type cast needed for parameter or null literal
    int X_42568 = 5568;                                   // data type of expression is not boolean
    int X_42569 = 5569;                                   // quoted identifier required
    int X_42570 = 5570;                                   // concatenation exceeds maximum type length

    //
    int X_42571 = 5571;                                   // NULL literal not allowed
    int X_42572 = 5572;                                   // invalid GROUP BY expression
    int X_42573 = 5573;                                   // invalid HAVING expression
    int X_42574 = 5574;                                   // expression not in aggregate or GROUP BY columns
    int X_42575 = 5575;                                   // parameter marker not allowed
    int X_42576 = 5576;                                   // invalid ORDER BY expression
    int X_42577 = 5577;                                   // duplicate name in column list
    int X_42578 = 5578;                                   // duplicate column name in derived table
    int X_42579 = 5579;                                   // duplicate update of column

    // identifiers
    int X_42581 = 5581;                                   // unexpected token
    int X_42582 = 5582;                                   // unknown token
    int X_42583 = 5583;                                   // malforemd quoted identifier
    int X_42584 = 5584;                                   // malformed string
    int X_42585 = 5585;                                   // malformed numeric constant
    int X_42586 = 5586;                                   // malformed unicode string
    int X_42587 = 5587;                                   // malformed binary string
    int X_42588 = 5588;                                   // malformed bit string
    int X_42589 = 5589;                                   // malformed comment
    int X_42590 = 5590;                                   // unexpected end of statement

    // other
    int X_42591 = 5591;                                   // cannot drop sole column of table
    int X_42592 = 5592;                                   // precision or scale out of range
    int X_42593 = 5593;                                   // column count mismatch in column name list
    int X_42594 = 5594;                                   // column count mismatch in UNION, INTERSECT, EXCEPT operation
    int X_42595 = 5595;                                   // invalid privilege specified - ALL PRIVILEGES required
    int X_42596 = 5596;                                   // schema qualifier does not match enclosing create schema statement.
    int X_42597 = 5597;                                   // number out of the valid range for sequence generator
    int X_42598 = 5598;                                   // sequence expression cannot be specified in this context

    // HSQLDB - PSM definition
    int X_42601 = 5601;                                   // repeat handler declaration
    int X_42602 = 5602;                                   // invalid statement in routine
    int X_42603 = 5603;                                   // dynamic parameter or variable required as INOUT or OUT argument
    int X_42604 = 5604;                                   // incompatible declaration attributes
    int X_42605 = 5605;                                   // routine signature already exists
    int X_42606 = 5606;                                   // routine variable name already exists
    int X_42607 = 5607;                                   // invalid SQLSTATE value

    // with check option violation - 200x
    int X_44000 = 5700;                                   // with check option violation

    // 04-PSM
    // unhandled user-defined exception - 200x
    int X_45000 = 5800;                                   // unhandled user-defined exception

    // SQL/JRT
    int X_46000 = 6000;                                   // Java execution / Java DDL - generic
    int X_46001 = 6001;                                   // invalid URL
    int X_46002 = 6002;                                   // invalid JAR name
    int X_46003 = 6003;                                   // invalid class deletion
    int X_46005 = 6004;                                   // invalid replacement
    int X_4600A = 6007;                                   // attempt to replace uninstalled JAR
    int X_4600B = 6008;                                   // attempt to remove uninstalled JAR
    int X_4600C = 6009;                                   // invalid JAR removal
    int X_4600D = 6010;                                   // invalid path
    int X_4600E = 6011;                                   // self-referencing path
    int X_46102 = 6012;                                   // invalid JAR name in path
    int X_46103 = 6013;                                   // unresolved class name

    // Unknown Error: Catch-All - xxxx
    int X_99000 = 6500;                                   // Unknown Error: Catch-All
    int X_99099 = 6501;                                   // Error converting vendor code to SQL state code

    // FDW-specific condition - xxxx
    int X_HV000 = 6600;                                   // FDW-specific condition - generic
    int X_HV001 = 6601;                                   // memory allocation error
    int X_HV002 = 6602;                                   // dynamic parameter value needed
    int X_HV004 = 6603;                                   // invalid data type
    int X_HV005 = 6604;                                   // column name not found
    int X_HV006 = 6605;                                   // invalid data type descriptors
    int X_HV007 = 6606;                                   // invalid column name
    int X_HV008 = 6607;                                   // invalid column number
    int X_HV009 = 6608;                                   // invalid use of null pointer
    int X_HV00A = 6609;                                   // invalid string format
    int X_HV00B = 6610;                                   // invalid handle
    int X_HV00C = 6611;                                   // invalid option index
    int X_HV00D = 6612;                                   // invalid option name
    int X_HV00J = 6613;                                   // option name not found
    int X_HV00K = 6614;                                   // reply handle
    int X_HV00L = 6615;                                   // unable to create execution
    int X_HV00M = 6616;                                   // unable to create reply
    int X_HV00N = 6617;                                   // unable to establish connection
    int X_HV00P = 6618;                                   // no schemas
    int X_HV00Q = 6619;                                   // schema not found
    int X_HV00R = 6620;                                   // table not found
    int X_HV010 = 6621;                                   // function sequence error
    int X_HV014 = 6622;                                   // limit on number of handles exceeded
    int X_HV021 = 6623;                                   // inconsistent descriptor information
    int X_HV024 = 6624;                                   // invalid attribute value
    int X_HV090 = 6625;                                   // invalid string length or buffer length
    int X_HV091 = 6626;                                   // invalid descriptor field identifier

    // datalink exception - 200x
    int X_HW000 = 6700;                                   // datalink exception - generic
    int X_HW001 = 6701;                                   // external file not linked
    int X_HW002 = 6702;                                   // external file already linked
    int X_HW003 = 6703;                                   // referenced file does not exist
    int X_HW004 = 6704;                                   // invalid write token
    int X_HW005 = 6705;                                   // invalid datalink construction
    int X_HW006 = 6706;                                   // invalid write permission for update
    int X_HW007 = 6707;                                   // referenced file not valid

    // CLI-specific condition - 200x
    int X_HY093 = 6800;                                   // CLI-specific condition: invalid datalink value
}
