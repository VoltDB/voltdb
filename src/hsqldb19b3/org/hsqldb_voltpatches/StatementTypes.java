/* Copyright (c) 2001-2014, The HSQL Development Group
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

/*
 * Codes based on SQL Standards for different types of statement.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.7
 * @since 1.9.0
 */
public interface StatementTypes {

    int ALLOCATE_CURSOR                  = 1;
    int ALLOCATE_DESCRIPTOR              = 2;
    int ALTER_DOMAIN                     = 3;
    int ALTER_ROUTINE                    = 17;
    int ALTER_SEQUENCE                   = 134;
    int ALTER_TYPE                       = 60;
    int ALTER_TABLE                      = 4;
    int ALTER_TRANSFORM                  = 127;
    int CREATE_ASSERTION                 = 6;
    int CALL                             = 7;
    int CREATE_CHARACTER_SET             = 8;
    int CLOSE_CURSOR                     = 9;
    int CREATE_COLLATION                 = 10;
    int COMMIT_WORK                      = 11;
    int CONNECT                          = 13;
    int DEALLOCATE_DESCRIPTOR            = 15;
    int DEALLOCATE_PREPARE               = 16;
    int DELETE_CURSOR                    = 18;
    int DELETE_WHERE                     = 19;
    int DESCRIBE                         = 20;
    int SELECT_DIRECT_SINGLE             = 21;    // identifier is SELECT
    int DISCONNECT                       = 22;
    int CREATE_DOMAIN                    = 23;
    int DROP_ASSERTION                   = 24;
    int DROP_CHARACTER_SET               = 25;
    int DROP_COLLATION                   = 26;
    int DROP_TYPE                        = 35;
    int DROP_DOMAIN                      = 27;
    int DROP_ROLE                        = 29;
    int DROP_ROUTINE                     = 30;
    int DROP_SCHEMA                      = 31;
    int DROP_SEQUENCE                    = 135;
    int DROP_TABLE                       = 32;
    int DROP_TRANSFORM                   = 116;
    int DROP_TRANSLATION                 = 33;
    int DROP_TRIGGER                     = 34;
    int DROP_CAST                        = 78;
    int DROP_ORDERING                    = 115;
    int DROP_VIEW                        = 36;
    int DYNAMIC_CLOSE                    = 37;
    int DYNAMIC_DELETE_CURSOR            = 38;
    int DYNAMIC_FETCH                    = 39;
    int DYNAMIC_OPEN                     = 40;
    int SELECT_CURSOR                    = 85;
    int SELECT_SINGLE_DYNAMIC            = 41;    // identifier is SELECT
    int DYNAMIC_UPDATE_CURSOR            = 42;
    int EXECUTE_IMMEDIATE                = 43;
    int EXECUTE                          = 44;
    int FETCH                            = 45;
    int FREE_LOCATOR                     = 98;
    int GET_DESCRIPTOR                   = 47;
    int HOLD_LOCATOR                     = 99;
    int GRANT                            = 48;
    int GRANT_ROLE                       = 49;
    int INSERT                           = 50;
    int MERGE                            = 128;
    int OPEN                             = 53;
    int PREPARABLE_DYNAMIC_DELETE_CURSOR = 54;
    int PREPARABLE_DYNAMIC_UPDATE_CURSOR = 55;
    int PREPARE                          = 56;
    int RELEASE_SAVEPOINT                = 57;
    int RETURN                           = 58;
    int REVOKE                           = 59;
    int REVOKE_ROLE                      = 129;
    int CREATE_ROLE                      = 61;
    int ROLLBACK_WORK                    = 62;
    int SAVEPOINT                        = 63;
    int CREATE_SCHEMA                    = 64;
    int CREATE_ROUTINE                   = 14;
    int SELECT_SINGLE                    = 65;    // identifier is SELECT
    int CREATE_SEQUENCE                  = 133;
    int SET_CATALOG                      = 66;
    int SET_CONNECTION                   = 67;
    int SET_CONSTRAINT                   = 68;
    int SET_DESCRIPTOR                   = 70;
    int SET_TIME_ZONE                    = 71;
    int SET_NAMES                        = 72;
    int SET_PATH                         = 69;
    int SET_ROLE                         = 73;
    int SET_SCHEMA                       = 74;
    int SET_SESSION_AUTHORIZATION        = 76;
    int SET_SESSION_CHARACTERISTICS      = 109;
    int SET_COLLATION                    = 136;
    int SET_TRANSFORM_GROUP              = 118;
    int SET_TRANSACTION                  = 75;
    int START_TRANSACTION                = 111;
    int CREATE_TABLE                     = 77;
    int CREATE_TRANSFORM                 = 117;
    int CREATE_TRANSLATION               = 79;
    int CREATE_TRIGGER                   = 80;
    int UPDATE_CURSOR                    = 81;
    int UPDATE_WHERE                     = 82;
    int CREATE_CAST                      = 52;
    int CREATE_TYPE                      = 83;
    int CREATE_ORDERING                  = 114;
    int CREATE_VIEW                      = 84;
    int ASSIGNMENT                       = 5;     // PSM
    int CASE                             = 86;
    int BEGIN_END                        = 12;
    int DROP_MODULE                      = 28;
    int FOR                              = 46;
    int IF                               = 88;
    int ITERATE                          = 102;
    int LEAVE                            = 89;
    int LOOP                             = 90;
    int RESIGNAL                         = 91;
    int REPEAT                           = 95;
    int SIGNAL                           = 92;
    int CREATE_MODULE                    = 51;
    int WHILE                            = 97;

    //
    int ALTER_FOREIGN_TABLE         = 104;
    int ALTER_USER_MAPPING          = 123;
    int DROP_FOREIGN_DATA_WRAPPER   = 121;
    int DROP_SERVER                 = 110;
    int DROP_FOREIGN_TABLE          = 105;
    int DROP_ROUTINE_MAPPING        = 131;
    int DROP_USER_MAPPING           = 124;
    int CREATE_FOREIGN_DATA_WRAPPER = 119;
    int CREATE_SERVER               = 107;
    int CREATE_FOREIGN_TABLE        = 103;
    int IMPORT_FOREIGN_SCHEMA       = 125;
    int CREATE_ROUTINE_MAPPING      = 132;
    int SET_PASSTHROUGH             = 126;
    int CREATE_USER_MAPPING         = 122;

    // hsqldb_voltpatches database
    int DATABASE_BACKUP     = 1001;
    int DATABASE_CHECKPOINT = 1002;
    int DATABASE_SHUTDOWN   = 1003;
    int DATABASE_SCRIPT     = 1004;
    int ALTER_SESSION       = 1005;

    // hsqldb_voltpatches database settings
    int SET_DATABASE_FILES_BACKUP_INCREMENT    = 1011;
    int SET_DATABASE_FILES_CACHE_ROWS          = 1012;
    int SET_DATABASE_FILES_CACHE_SIZE          = 1013;
    int SET_DATABASE_FILES_CHECK               = 1014;
    int SET_DATABASE_FILES_DEFRAG              = 1015;
    int SET_DATABASE_FILES_EVENT_LOG           = 1016;
    int SET_DATABASE_FILES_LOBS_SCALE          = 1017;
    int SET_DATABASE_FILES_LOBS_COMPRESSED     = 1018;
    int SET_DATABASE_FILES_UNUSED_TYPE_SETTING = 1019;
    int SET_DATABASE_FILES_LOG                 = 1020;
    int SET_DATABASE_FILES_LOG_SIZE            = 1021;
    int SET_DATABASE_FILES_NIO                 = 1022;
    int SET_DATABASE_FILES_READ_ONLY           = 1023;
    int SET_DATABASE_FILES_READ_ONLY_FILES     = 1024;
    int SET_DATABASE_FILES_SCALE               = 1025;
    int SET_DATABASE_FILES_SCRIPT_FORMAT       = 1026;
    int SET_DATABASE_FILES_SPACE               = 1031;
    int SET_DATABASE_FILES_TEMP_PATH           = 1032;
    int SET_DATABASE_FILES_WRITE_DELAY         = 1033;
    int SET_DATABASE_DEFAULT_INITIAL_SCHEMA    = 1034;
    int SET_DATABASE_DEFAULT_TABLE_TYPE        = 1035;
    int SET_DATABASE_AUTHENTICATION            = 1036;
    int SET_DATABASE_GC                        = 1037;
    int SET_DATABASE_PROPERTY                  = 1039;
    int SET_DATABASE_PASSWORD_CHECK            = 1040;
    int SET_DATABASE_READ_ONLY                 = 1041;
    int SET_DATABASE_READ_ONLY_FILES           = 1042;
    int SET_DATABASE_RESULT_MEMORY_ROWS        = 1046;
    int SET_DATABASE_SQL_COLLATION             = 1047;
    int SET_SESSION_SQL_IGNORECASE             = 1048;
    int SET_DATABASE_SQL_REFERENTIAL_INTEGRITY = 1049;
    int SET_DATABASE_SQL                       = 1050;
    int SET_DATABASE_TEXT_SOURCE               = 1051;
    int SET_DATABASE_TRANSACTION_CONTROL       = 1052;
    int SET_DATABASE_DEFAULT_ISOLATION_LEVEL   = 1053;
    int SET_DATABASE_TRANSACTION_CONFLICT      = 1054;
    int SET_DATABASE_UNIQUE_NAME               = 1055;
    int SET_DATABASE_PASSWORD_DIGEST           = 1056;

    // hsqldb_voltpatches user settings
    int SET_USER_LOCAL          = 1060;
    int SET_USER_INITIAL_SCHEMA = 1061;
    int SET_USER_PASSWORD       = 1062;

    // hsqldb_voltpatches session
    int TRANSACTION_LOCK_TABLE         = 1063;
    int SET_SESSION_AUTOCOMMIT         = 1064;
    int SET_SESSION_RESULT_MAX_ROWS    = 1065;
    int SET_SESSION_RESULT_MEMORY_ROWS = 1066;
    int ROLLBACK_SAVEPOINT             = 1067;
    int DECLARE_SESSION_TABLE          = 1068;

    // hsqldb_voltpatches schema
    int ALTER_INDEX                 = 1069;
    int ALTER_VIEW                  = 1070;
    int COMMENT                     = 1071;
    int CREATE_ALIAS                = 1072;
    int CREATE_INDEX                = 1073;
    int CREATE_USER                 = 1074;
    int DECLARE_VARIABLE            = 1075;
    int DROP_COLUMN                 = 1076;
    int DROP_INDEX                  = 1077;
    int DROP_CONSTRAINT             = 1078;
    int DROP_USER                   = 1079;
    int DROP_DEFAULT                = 1080;
    int ADD_COLUMN                  = 1081;
    int ADD_CONSTRAINT              = 1082;
    int ADD_DEFAULT                 = 1083;
    int ALTER_COLUMN_TYPE           = 1084;
    int ALTER_COLUMN_SEQUENCE       = 1085;
    int ALTER_COLUMN_NULL           = 1086;
    int ALTER_COLUMN_DEFAULT        = 1087;
    int ALTER_COLUMN_DROP_DEFAULT   = 1088;
    int ALTER_COLUMN_DROP_GENERATED = 1089;
    int ALTER_COLUMN_TYPE_IDENTITY  = 1090;

    //
    int EXPLAIN_PLAN             = 1191;
    int RENAME_OBJECT            = 1192;
    int SET_TABLE_INDEX          = 1193;
    int SET_TABLE_READONLY       = 1194;
    int SET_TABLE_SOURCE         = 1195;
    int SET_TABLE_SOURCE_HEADER  = 1196;
    int SET_TABLE_TYPE           = 1197;
    int SET_TABLE_CLUSTERED      = 1198;
    int SET_TABLE_NEW_TABLESPACE = 1199;
    int SET_TABLE_SET_TABLESPACE = 1200;
    int LOG_SCHEMA_STATEMENT     = 1201;

    // hsqldb_voltpatches sql implementation
    int CONDITION = 1211;                         // element of IF
    int HANDLER   = 1212;
    int DDL       = 1213;
    int CHECK     = 1214;
    int TRUNCATE  = 1215;

    //
    int CREATE_SEARCH = 1301;
    int DROP_SEARCH   = 1302;

    //
    // hsqldb_voltpatches groups
    int X_SQL_SCHEMA_DEFINITION      = 2001;
    int X_SQL_SCHEMA_MANIPULATION    = 2002;
    int X_SQL_DATA                   = 2003;
    int X_SQL_DATA_CHANGE            = 2004;
    int X_SQL_TRANSACTION            = 2005;
    int X_SQL_CONNECTION             = 2006;
    int X_SQL_CONTROL                = 2007;
    int X_SQL_SESSION                = 2008;
    int X_SQL_DIAGNOSTICS            = 2009;
    int X_SQL_DYNAMIC                = 2010;
    int X_HSQLDB_SESSION             = 2011;
    int X_HSQLDB_SCHEMA_MANIPULATION = 2012;
    int X_HSQLDB_SETTING             = 2013;
    int X_HSQLDB_DATABASE_OPERATION  = 2014;
    int X_HSQLDB_TRANSACTION         = 2015;
    int X_DYNAMIC                    = 2016;

    // Expected types of Result returned for an SQL statement
    int RETURN_ANY    = 0;
    int RETURN_COUNT  = 1;
    int RETURN_RESULT = 2;
}
