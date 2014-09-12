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

import org.hsqldb_voltpatches.lib.IntValueHashMap;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;

/**
 * Defines and enumerates reserved and non-reserved SQL keywords.<p>
 *
 * @author  Nitin Chauhan (initial work)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class Tokens {

    //
    // SQL 200n reserved words full set
    static final String        T_ABS              = "ABS";
    public static final String T_ALL              = "ALL";
    static final String        T_ALLOCATE         = "ALLOCATE";
    public static final String T_ALTER            = "ALTER";
    static final String        T_AND              = "AND";
    static final String        T_ANY              = "ANY";
    static final String        T_ARE              = "ARE";
    static final String        T_ARRAY            = "ARRAY";
    public static final String T_AS               = "AS";
    static final String        T_ASENSITIVE       = "ASENSITIVE";
    static final String        T_ASYMMETRIC       = "ASYMMETRIC";
    static final String        T_AT               = "AT";
    static final String        T_ATOMIC           = "ATOMIC";
    public static final String T_AUTHORIZATION    = "AUTHORIZATION";
    static final String        T_AVG              = "AVG";
    static final String        T_BEGIN            = "BEGIN";
    static final String        T_BETWEEN          = "BETWEEN";
    public static final String T_BIGINT           = "BIGINT";
    public static final String T_BINARY           = "BINARY";
    static final String        T_BIT_LENGTH       = "BIT_LENGTH";
    // A VoltDB extension to support varchar column in bytes.
    static final String        T_BYTES            = "BYTES"; // For VoltDB
    // End of VoltDB extension
    public static final String T_BLOB             = "BLOB";
    public static final String T_BOOLEAN          = "BOOLEAN";
    static final String        T_BOTH             = "BOTH";
    static final String        T_BY               = "BY";
    static final String        T_CALL             = "CALL";
    static final String        T_CALLED           = "CALLED";
    static final String        T_CARDINALITY      = "CARDINALITY";
    public static final String T_CASCADED         = "CASCADED";
    static final String        T_CASE             = "CASE";
    static final String        T_CAST             = "CAST";
    static final String        T_CEIL             = "CEIL";
    static final String        T_CEILING          = "CEILING";
    static final String        T_CHAR             = "CHAR";
    static final String        T_CHAR_LENGTH      = "CHAR_LENGTH";
    public static final String T_CHARACTER        = "CHARACTER";
    static final String        T_CHARACTER_LENGTH = "CHARACTER_LENGTH";
    public static final String T_CHECK            = "CHECK";
    public static final String T_CLOB             = "CLOB";
    static final String        T_CLOSE            = "CLOSE";
    static final String        T_COALESCE         = "COALESCE";
    static final String        T_COLLATE          = "COLLATE";
    static final String        T_COLLECT          = "COLLECT";
    static final String        T_COLUMN           = "COLUMN";
    public static final String T_COMMIT           = "COMMIT";
    static final String        T_CONDITION        = "CONDIITON";
    public static final String T_CONNECT          = "CONNECT";
    public static final String T_CONSTRAINT       = "CONSTRAINT";
    static final String        T_CONVERT          = "CONVERT";
    static final String        T_CORR             = "CORR";
    static final String        T_CORRESPONDING    = "CORRESPONDING";
    static final String        T_COUNT            = "COUNT";
    static final String        T_COVAR_POP        = "COVAR_POP";
    static final String        T_COVAR_SAMP       = "COVAR_SAMP";
    public static final String T_CREATE           = "CREATE";
    static final String        T_CROSS            = "CROSS";
    static final String        T_CUBE             = "CUBE";
    static final String        T_CUME_DIST        = "CUME_DIST";
    static final String        T_CURRENT          = "CURRENT";
    static final String        T_CURRENT_CATALOG  = "CURRENT_CATALOG";
    static final String        T_CURRENT_DATE     = "CURRENT_DATE";
    static final String T_CURRENT_DEFAULT_TRANSFORM_GROUP =
        "CURRENT_DEFAULT_TRANSFORM_GROUP";
    static final String T_CURRENT_PATH      = "CURRENT_PATH";
    static final String T_CURRENT_ROLE      = "CURRENT_ROLE";
    static final String T_CURRENT_SCHEMA    = "CURRENT_SCHEMA";
    static final String T_CURRENT_TIME      = "CURRENT_TIME";
    static final String T_CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    static final String T_CURRENT_TRANSFORM_GROUP_FOR_TYPE =
        "CURRENT_TRANSFORM_GROUP_FOR_TYPE";
    static final String        T_CURRENT_USER      = "CURRENT_USER";
    static final String        T_CURSOR            = "CURSOR";
    static final String        T_CYCLE             = "CYCLE";
    public static final String T_DATE              = "DATE";
    public static final String T_DAY               = "DAY";
    static final String        T_DEALLOCATE        = "DEALLOCATE";
    static final String        T_DEC               = "DEC";
    public static final String T_DECIMAL           = "DECIMAL";
    static final String        T_DECLARE           = "DECLARE";
    public static final String T_DEFAULT           = "DEFAULT";
    public static final String T_DELETE            = "DELETE";
    static final String        T_DENSE_RANK        = "DENSE_RANK";
    static final String        T_DEREF             = "DEREF";
    static final String        T_DESCRIBE          = "DESCRIBE";
    static final String        T_DETERMINISTIC     = "DETERMINISTIC";
    static final String        T_DISCONNECT        = "DISCONNECT";
    static final String        T_DISTINCT          = "DISTINCT";
    public static final String T_DO                = "DO";
    // A VoltDB extension to use FLOAT as the official DOUBLE type name
    public static final String T_DOUBLE            = "FLOAT";
    /* disable 1 line ...
    public static final String T_DOUBLE            = "DOUBLE";
    ... disabled 1 line */
    // End of VoltDB extension
    static final String        T_DROP              = "DROP";
    static final String        T_DYNAMIC           = "DYNAMIC";
    static final String        T_EACH              = "EACH";
    static final String        T_ELEMENT           = "ELEMENT";
    static final String        T_ELSE              = "ELSE";
    static final String        T_ELSEIF            = "ELSEIF";
    static final String        T_END               = "END";
    static final String        T_END_EXEC          = "END_EXEC";
    static final String        T_ESCAPE            = "ESCAPE";
    static final String        T_EVERY             = "EVERY";
    static final String        T_EXCEPT            = "EXCEPT";
    static final String        T_EXEC              = "EXEC";
    public static final String T_EXECUTE           = "EXECUTE";
    static final String        T_EXISTS            = "EXISTS";
    static final String        T_EXP               = "EXP";
    static final String        T_EXTERNAL          = "EXTERNAL";
    static final String        T_EXTRACT           = "EXTRACT";
    static final String        T_FALSE             = "FALSE";
    static final String        T_FETCH             = "FETCH";
    static final String        T_FILTER            = "FILTER";
    static final String        T_FIRST_VALUE       = "FIRST_VALUE";
    public static final String T_FLOAT             = "FLOAT";
    static final String        T_FLOOR             = "FLOOR";
    static final String        T_FOR               = "FOR";
    static final String        T_FOREIGN           = "FOREIGN";
    static final String        T_FREE              = "FREE";
    static final String        T_FROM              = "FROM";
    static final String        T_FULL              = "FULL";
    static final String        T_FUNCTION          = "FUNCTION";
    static final String        T_FUSION            = "FUSION";
    public static final String T_GET               = "GET";
    static final String        T_GLOBAL            = "GLOBAL";
    public static final String T_GRANT             = "GRANT";
    static final String        T_GROUP             = "GROUP";
    static final String        T_GROUPING          = "GROUPING";
    static final String        T_HANDLER           = "HANDLER";
    static final String        T_HAVING            = "HAVING";
    static final String        T_HOLD              = "HOLD";
    public static final String T_HOUR              = "HOUR";
    static final String        T_IDENTITY          = "IDENTITY";
    static final String        T_IF                = "IF";
    static final String        T_IN                = "IN";
    static final String        T_INDICATOR         = "INDICATOR";
    static final String        T_INNER             = "INNER";
    static final String        T_INOUT             = "INOUT";
    static final String        T_INSENSITIVE       = "INSENSITIVE";
    public static final String T_INSERT            = "INSERT";
    static final String        T_INT               = "INT";
    public static final String T_INTEGER           = "INTEGER";
    static final String        T_INTERSECT         = "INTERSECT";
    static final String        T_INTERSECTION      = "INTERSECTION";
    public static final String T_INTERVAL          = "INTERVAL";
    static final String        T_INTO              = "INTO";
    static final String        T_ITERATE           = "ITERATE";
    static final String        T_IS                = "IS";
    static final String        T_JAR               = "JAR";             // SQL/JRT
    static final String        T_JOIN              = "JOIN";
    static final String        T_LAG               = "LAG";
    static final String        T_LANGUAGE          = "LANGUAGE";
    static final String        T_LARGE             = "LARGE";
    static final String        T_LAST_VALUE        = "LAST_VALUE";
    static final String        T_LATERAL           = "LATERAL";
    static final String        T_LEAD              = "LEAD";
    static final String        T_LEADING           = "LEADING";
    static final String        T_LEAVE             = "LEAVE";
    static final String        T_LEFT              = "LEFT";
    static final String        T_LIKE              = "LIKE";
    static final String        T_LIKE_REGX         = "LIKE_REGX";
    static final String        T_LN                = "LN";
    public static final String T_LOCAL             = "LOCAL";
    static final String        T_LOCALTIME         = "LOCALTIME";
    static final String        T_LOCALTIMESTAMP    = "LOCALTIMESTAMP";
    public static final String T_LOOP              = "LOOP";
    static final String        T_LOWER             = "LOWER";
    static final String        T_MATCH             = "MATCH";
    static final String        T_MAX               = "MAX";
    static final String        T_MAX_CARDINALITY   = "MAX_CARDINALITY";
    static final String        T_MEMBER            = "MEMBER";
    static final String        T_MERGE             = "MERGE";
    static final String        T_METHOD            = "METHOD";
    static final String        T_MIN               = "MIN";
    public static final String T_MINUTE            = "MINUTE";
    static final String        T_MOD               = "MOD";
    static final String        T_MODIFIES          = "MODIFIES";
    static final String        T_MODULE            = "MODULE";
    public static final String T_MONTH             = "MONTH";
    static final String        T_MULTISET          = "MULTISET";
    static final String        T_NATIONAL          = "NATIONAL";
    static final String        T_NATURAL           = "NATURAL";
    static final String        T_NCHAR             = "NCHAR";
    static final String        T_NCLOB             = "NCLOB";
    static final String        T_NEW               = "NEW";
    public static final String T_NO                = "NO";
    public static final String T_NONE              = "NONE";
    static final String        T_NORMALIZE         = "NORMALIZE";
    static final String        T_NOT               = "NOT";
    static final String        T_NTH_VALUE         = "NTH_VALUE";
    static final String        T_NTILE             = "NTILE";
    public static final String T_NULL              = "NULL";
    public static final String T_NULLIF            = "NULLIF";
    public static final String T_NUMERIC           = "NUMERIC";
    static final String        T_OCCURRENCES_REGEX = "OCCURRENCES_REGEX";
    static final String        T_OCTET_LENGTH      = "OCTET_LENGTH";
    static final String        T_OF                = "OF";
    static final String        T_OFFSET            = "OFFSET";
    static final String        T_OLD               = "OLD";
    public static final String T_ON                = "ON";
    static final String        T_ONLY              = "ONLY";
    static final String        T_OPEN              = "OPEN";
    static final String        T_OR                = "OR";
    static final String        T_ORDER             = "ORDER";
    static final String        T_OUT               = "OUT";
    static final String        T_OUTER             = "OUTER";
    static final String        T_OVER              = "OVER";
    static final String        T_OVERLAPS          = "OVERLAPS";
    static final String        T_OVERLAY           = "OVERLAY";
    static final String        T_PARAMETER         = "PARAMETER";
    static final String        T_PARTITION         = "PARTITION";
    static final String        T_PERCENT_RANK      = "PERCENT_RANK";
    static final String        T_PERCENTILE_CONT   = "PERCENTILE_CONT";
    static final String        T_PERCENTILE_DISC   = "PERCENTILE_DISC";
    static final String        T_POSITION          = "POSITION";
    static final String        T_POSITION_REGEX    = "POSITION_REGEX";
    static final String        T_POWER             = "POWER";
    static final String        T_PRECISION         = "PRECISION";
    static final String        T_PREPARE           = "PREPARE";
    static final String        T_PRIMARY           = "PRIMARY";
    static final String        T_PROCEDURE         = "PROCEDURE";
    static final String        T_RANGE             = "RANGE";
    static final String        T_RANK              = "RANK";
    static final String        T_READS             = "READS";
    public static final String T_REAL              = "REAL";
    static final String        T_RECURSIVE         = "RECURSIVE";
    static final String        T_REF               = "REF";
    public static final String T_REFERENCES        = "REFERENCES";
    static final String        T_REFERENCING       = "REFERENCING";
    static final String        T_REGR_AVGX         = "REGR_AVGX";
    static final String        T_REGR_AVGY         = "REGR_AVGY";
    static final String        T_REGR_COUNT        = "REGR_COUNT";
    static final String        T_REGR_INTERCEPT    = "REGR_INTERCEPT";
    static final String        T_REGR_R2           = "REGR_R2";
    static final String        T_REGR_SLOPE        = "REGR_SLOPE";
    static final String        T_REGR_SXX          = "REGR_SXX";
    static final String        T_REGR_SXY          = "REGR_SXY";
    static final String        T_REGR_SYY          = "REGR_SYY";
    static final String        T_RELEASE           = "RELEASE";
    static final String        T_REPEAT            = "REPEAT";
    static final String        T_RESIGNAL          = "RESIGNAL";
    static final String        T_RESULT            = "RESULT";
    static final String        T_RETURN            = "RETURN";
    static final String        T_RETURNS           = "RETURNS";
    static final String        T_REVOKE            = "REVOKE";
    static final String        T_RIGHT             = "RIGHT";
    static final String        T_ROLLBACK          = "ROLLBACK";
    static final String        T_ROLLUP            = "ROLLUP";
    static final String        T_ROW               = "ROW";
    static final String        T_ROW_NUMBER        = "ROW_NUMBER";
    static final String        T_ROWS              = "ROWS";
    static final String        T_SAVEPOINT         = "SAVEPOINT";
    static final String        T_SCOPE             = "SCOPE";
    static final String        T_SCROLL            = "SCROLL";
    static final String        T_SEARCH            = "SEARCH";
    public static final String T_SECOND            = "SECOND";
    public static final String T_SELECT            = "SELECT";
    static final String        T_SENSITIVE         = "SENSITIVE";
    static final String        T_SESSION_USER      = "SESSION_USER";
    public static final String T_SET               = "SET";
    static final String        T_SIGNAL            = "SIGNAL";
    static final String        T_SIMILAR           = "SIMILAR";
    public static final String T_SMALLINT          = "SMALLINT";
    static final String        T_SOME              = "SOME";
    static final String        T_SPECIFIC          = "SPECIFIC";
    static final String        T_SPECIFICTYPE      = "SPECIFICTYPE";
    static final String        T_SQL               = "SQL";
    static final String        T_SQLEXCEPTION      = "SQLEXCEPTION";
    static final String        T_SQLSTATE          = "SQLSTATE";
    static final String        T_SQLWARNING        = "SQLWARNING";
    static final String        T_SQRT              = "SQRT";
    static final String        T_START             = "START";
    static final String        T_STATIC            = "STATIC";
    static final String        T_STDDEV_POP        = "STDDEV_POP";
    static final String        T_STDDEV_SAMP       = "STDDEV_SAMP";
    static final String        T_SUBMULTISET       = "SUBMULTISET";
    static final String        T_SUBSTRING         = "SUBSTRING";
    static final String        T_SUBSTRING_REGEX   = "SUBSTRING_REGEX";
    static final String        T_SUM               = "SUM";
    static final String        T_SYMMETRIC         = "SYMMETRIC";
    static final String        T_SYSTEM            = "SYSTEM";
    static final String        T_SYSTEM_USER       = "SYSTEM_USER";
    static final String        T_TABLE             = "TABLE";
    static final String        T_TABLESAMPLE       = "TABLESAMPLE";
    static final String        T_THEN              = "THEN";
    public static final String T_TIME              = "TIME";
    public static final String T_TIMESTAMP         = "TIMESTAMP";
    public static final String T_TIMEZONE_HOUR     = "TIMEZONE_HOUR";
    public static final String T_TIMEZONE_MINUTE   = "TIMEZONE_MINUTE";
    public static final String T_TO                = "TO";
    static final String        T_TRAILING          = "TRAILING";
    static final String        T_TRANSLATE         = "TRANSLATE";
    static final String        T_TRANSLATE_REGEX   = "TRANSLATE_REGEX";
    static final String        T_TRANSLATION       = "TRANSLATION";
    static final String        T_TREAT             = "TREAT";
    public static final String T_TRIGGER           = "TRIGGER";
    static final String        T_TRIM              = "TRIM";
    static final String        T_TRIM_ARRAY        = "TRIM_ARRAY";
    static final String        T_TRUE              = "TRUE";
    static final String        T_TRUNCATE          = "TRUNCATE";
    static final String        T_UESCAPE           = "UESCAPE";
    static final String        T_UNION             = "UNION";
    // A VoltDB extension to support the assume unique index attribute
    static final String        T_ASSUMEUNIQUE      = "ASSUMEUNIQUE";     // For VoltDB
    // End of VoltDB extension
    public static final String T_UNIQUE            = "UNIQUE";
    static final String        T_UNKNOWN           = "UNKNOWN";
    static final String        T_UNNEST            = "UNNEST";
    static final String        T_UNTIL             = "UNTIL";
    public static final String T_UPDATE            = "UPDATE";
    static final String        T_UPPER             = "UPPER";
    public static final String T_USER              = "USER";
    static final String        T_USING             = "USING";
    static final String        T_VALUE             = "VALUE";
    static final String        T_VALUES            = "VALUES";
    static final String        T_VAR_POP           = "VAR_POP";
    static final String        T_VAR_SAMP          = "VAR_SAMP";
    public static final String T_VARBINARY         = "VARBINARY";
    public static final String T_VARCHAR           = "VARCHAR";
    static final String        T_VARYING           = "VARYING";
    static final String        T_WHEN              = "WHEN";
    static final String        T_WHENEVER          = "WHENEVER";
    static final String        T_WHERE             = "WHERE";
    public static final String T_WHILE             = "WHILE";
    static final String        T_WIDTH_BUCKET      = "WIDTH_BUCKET";
    static final String        T_WINDOW            = "WINDOW";
    public static final String T_WITH              = "WITH";
    static final String        T_WITHIN            = "WITHIN";
    static final String        T_WITHOUT           = "WITHOUT";
    public static final String T_YEAR              = "YEAR";

    // ops
    static final String        T_ASTERISK       = "*";
    static final String        T_COMMA          = ",";
    static final String        T_CIRCUMFLEX     = "^";
    static final String        T_CLOSEBRACKET   = ")";
    static final String        T_COLON          = ":";
    static final String        T_CONCAT         = "||";
    public static final String T_DIVIDE         = "/";
    static final String        T_EQUALS         = "=";
    static final String        T_GREATER        = ">";
    static final String        T_GREATER_EQUALS = ">=";
    static final String        T_LESS           = "<";
    static final String        T_LESS_EQUALS    = "<=";
    static final String        T_PERCENT        = "%";
    static final String        T_PLUS           = "+";
    static final String        T_MINUS          = "-";
    static final String        T_NOT_EQUALS     = "<>";
    static final String        T_NOT_EQUALS_ALT = "!=";
    static final String        T_OPENBRACKET    = "(";
    static final String        T_QUESTION       = "?";
    static final String        T_SEMICOLON      = ";";
    static final String        T_DOUBLE_COLON   = "::";

    // SQL:200n non-reserved word list
    static final String T_A                      = "A";
    static final String T_ABSOLUTE               = "ABSOLUTE";
    static final String T_ACTION                 = "ACTION";
    static final String T_ADA                    = "ADA";
    static final String T_ADMIN                  = "ADMIN";
    static final String T_AFTER                  = "AFTER";
    static final String T_ALWAYS                 = "ALWAYS";
    static final String T_ASC                    = "ASC";
    static final String T_ASSERTION              = "ASSERTION";
    static final String T_ASSIGNMENT             = "ASSIGNMENT";
    static final String T_ATTRIBUTE              = "ATTRIBUTE";
    static final String T_ATTRIBUTES             = "ATTRIBUTES";
    static final String T_BEFORE                 = "BEFORE";
    static final String T_BERNOULLI              = "BERNOULLI";
    static final String T_BREADTH                = "BREADTH";
    static final String T_C                      = "C";
    static final String T_CASCADE                = "CASCADE";
    static final String T_CATALOG                = "CATALOG";
    static final String T_CATALOG_NAME           = "CATALOG_NAME";
    static final String T_CHAIN                  = "CHAIN";
    static final String T_CHARACTER_SET_CATALOG  = "CHARACTER_SET_CATALOG";
    static final String T_CHARACTER_SET_NAME     = "CHARACTER_SET_NAME";
    static final String T_CHARACTER_SET_SCHEMA   = "CHARACTER_SET_SCHEMA";
    static final String T_CHARACTERISTICS        = "CHARACTERISTICS";
    static final String T_CHARACTERS             = "CHARACTERS";
    static final String T_CLASS_ORIGIN           = "CLASS_ORIGIN";
    static final String T_COBOL                  = "COBOL";
    static final String T_COLLATION              = "COLLATION";
    static final String T_COLLATION_CATALOG      = "COLLATION_CATALOG";
    static final String T_COLLATION_NAME         = "COLLATION_NAME";
    static final String T_COLLATION_SCHEMA       = "COLLATION_SCHEMA";
    static final String T_COLUMN_NAME            = "COLUMN_NAME";
    static final String T_COMMAND_FUNCTION       = "COMMAND_FUNCTION";
    static final String T_COMMAND_FUNCTION_CODE  = "COMMAND_FUNCTION_CODE";
    static final String T_COMMITTED              = "COMMITTED";
    static final String T_COMPARABLE             = "COMPARABLE";        // SQL/JRT
    static final String T_CONDITION_IDENTIFIER   = "CONDIITON_IDENTIFIER";
    static final String T_CONDITION_NUMBER       = "CONDITION_NUMBER";
    static final String T_CONNECTION_NAME        = "CONNECTION_NAME";
    static final String T_CONSTRAINT_CATALOG     = "CONSTRAINT_CATALOG";
    static final String T_CONSTRAINT_NAME        = "CONSTRAINT_NAME";
    static final String T_CONSTRAINT_SCHEMA      = "CONSTRAINT_SCHEMA";
    static final String T_CONSTRAINTS            = "CONSTRAINTS";
    static final String T_CONSTRUCTOR            = "CONSTRUCTOR";
    static final String T_CONTAINS               = "CONTAINS";
    static final String T_CONTINUE               = "CONTINUE";
    static final String T_CURRENT_COLLATION      = "CURRENT_COLLATION";
    static final String T_CURSOR_NAME            = "CURSOR_NAME";
    static final String T_DATA                   = "DATA";
    static final String T_DATETIME_INTERVAL_CODE = "DATETIME_INTERVAL_CODE";
    static final String T_DATETIME_INTERVAL_PRECISION =
        "DATETIME_INTERVAL_PRECISION";
    static final String        T_DEFAULTS             = "DEFAULTS";
    static final String        T_DEFERRABLE           = "DEFERRABLE";
    static final String        T_DEFERRED             = "DEFERRED";
    static final String        T_DEFINED              = "DEFINED";
    static final String        T_DEFINER              = "DEFINER";
    static final String        T_DEGREE               = "DEGREE";
    static final String        T_DEPTH                = "DEPTH";
    static final String        T_DERIVED              = "DERIVED";
    static final String        T_DESC                 = "DESC";
    static final String        T_DESCRIPTOR           = "DESCRIPTOR";
    static final String        T_DIAGNOSTICS          = "DIAGNOSTICS";
    static final String        T_DISPATCH             = "DISPATCH";
    public static final String T_DOMAIN               = "DOMAIN";
    static final String        T_DYNAMIC_FUNCTION     = "DYNAMIC_FUNCTION";
    static final String T_DYNAMIC_FUNCTION_CODE = "DYNAMIC_FUNCTION_CODE";
    static final String        T_EXCEPTION            = "EXCEPTION";
    static final String        T_EXCLUDE              = "EXCLUDE";
    static final String        T_EXCLUDING            = "EXCLUDING";
    static final String        T_EXIT                 = "EXIT";
    static final String        T_FINAL                = "FINAL";
    static final String        T_FIRST                = "FIRST";
    static final String        T_FOLLOWING            = "FOLLOWING";
    static final String        T_FORTRAN              = "FORTRAN";
    static final String        T_FOUND                = "FOUND";
    public static final String T_G_FACTOR             = "G";
    static final String        T_GENERAL              = "GENERAL";
    static final String        T_GO                   = "GO";
    static final String        T_GOTO                 = "GOTO";
    static final String        T_GRANTED              = "GRANTED";
    static final String        T_HIERARCHY            = "HIERARCHY";
    static final String        T_IMPLEMENTATION       = "IMPLEMENTATION";
    static final String        T_INCLUDING            = "INCLUDING";
    static final String        T_INCREMENT            = "INCREMENT";
    static final String        T_INITIALLY            = "INITIALLY";
    static final String        T_INPUT                = "INPUT";
    static final String        T_INSTANCE             = "INSTANCE";
    static final String        T_INSTANTIABLE         = "INSTANTIABLE";
    static final String        T_INSTEAD              = "INSTEAD";
    static final String        T_INTERFACE            = "INTERFACE";    // SQL/JRT
    static final String        T_INVOKER              = "INVOKER";
    static final String        T_ISOLATION            = "ISOLATION";
    static final String        T_JAVA                 = "JAVA";         // SQL/JRT
    public static final String T_K_FACTOR             = "K";
    static final String        T_KEY                  = "KEY";
    static final String        T_KEY_MEMBER           = "KEY_MEMBER";
    static final String        T_KEY_TYPE             = "KEY_TYPE";
    static final String        T_LAST                 = "LAST";
    static final String        T_LENGTH               = "LENGTH";
    static final String        T_LEVEL                = "LEVEL";
    static final String        T_LOCATOR              = "LOCATOR";
    public static final String T_M_FACTOR             = "M";
    static final String        T_MAP                  = "MAP";
    static final String        T_MATCHED              = "MATCHED";
    static final String        T_MAXVALUE             = "MAXVALUE";
    static final String        T_MESSAGE_LENGTH       = "MESSAGE_LENGTH";
    static final String        T_MESSAGE_OCTET_LENGTH = "MESSAGE_OCTET_LENGTH";
    static final String        T_MESSAGE_TEXT         = "MESSAGE_TEXT";
    static final String        T_MINVALUE             = "MINVALUE";
    static final String        T_MORE                 = "MORE";
    static final String        T_MUMPS                = "MUMPS";
    static final String        T_NAME                 = "NAME";
    static final String        T_NAMES                = "NAMES";
    static final String        T_NESTING              = "NESTING";
    static final String        T_NEXT                 = "NEXT";
    static final String        T_NORMALIZED           = "NORMALIZED";
    static final String        T_NULLABLE             = "NULLABLE";
    static final String        T_NULLS                = "NULLS";
    static final String        T_NUMBER               = "NUMBER";
    public static final String T_OBJECT               = "OBJECT";
    static final String        T_OCTETS               = "OCTETS";
    static final String        T_OPTION               = "OPTION";
    static final String        T_OPTIONS              = "OPTIONS";
    static final String        T_ORDERING             = "ORDERING";
    static final String        T_ORDINALITY           = "ORDINALITY";
    static final String        T_OTHERS               = "OTHERS";
    static final String        T_OVERRIDING           = "OVERRIDING";
    public static final String T_P_FACTOR             = "P";
    static final String        T_PAD                  = "PAD";
    static final String        T_PARAMETER_MODE       = "PARAMETER_MODE";
    static final String        T_PARAMETER_NAME       = "PARAMETER_NAME";
    static final String T_PARAMETER_ORDINAL_POSITION =
        "PARAMETER_ORDINAL_POSITION";
    static final String T_PARAMETER_SPECIFIC_CATALOG =
        "PARAMETER_SPECIFIC_CATALOG";
    static final String T_PARAMETER_SPEC_NAME = "PARAMETER_SPECIFIC_NAME";
    static final String T_PARAMETER_SPEC_SCHEMA = "PARAMETER_SPECIFIC_SCHEMA";
    static final String        T_PARTIAL              = "PARTIAL";
    static final String        T_PASCAL               = "PASCAL";
    static final String        T_PATH                 = "PATH";
    static final String        T_PLACING              = "PLACING";
    static final String        T_PLI                  = "PLI";
    static final String        T_PRECEDING            = "PRECEDING";
    static final String        T_PRESERVE             = "PRESERVE";
    static final String        T_PRIOR                = "PRIOR";
    static final String        T_PRIVILEGES           = "PRIVILEGES";
    static final String        T_PUBLIC               = "PUBLIC";
    static final String        T_READ                 = "READ";
    static final String        T_RELATIVE             = "RELATIVE";
    static final String        T_REPEATABLE           = "REPEATABLE";
    static final String        T_RESTART              = "RESTART";
    static final String        T_RETURNED_CARDINALITY = "RETURNED_CARDINALITY";
    static final String        T_RETURNED_LENGTH      = "RETURNED_LENGTH";
    static final String T_RETURNED_OCTET_LENGTH = "RETURNED_OCTET_LENGTH";
    static final String        T_RETURNED_SQLSTATE    = "RETURNED_SQLSTATE";
    public static final String T_ROLE                 = "ROLE";
    static final String        T_ROUTINE              = "ROUTINE";
    static final String        T_ROUTINE_CATALOG      = "ROUTINE_CATALOG";
    static final String        T_ROUTINE_NAME         = "ROUTINE_NAME";
    static final String        T_ROUTINE_SCHEMA       = "ROUTINE_SCHEMA";
    static final String        T_ROW_COUNT            = "ROW_COUNT";
    static final String        T_SCALE                = "SCALE";
    public static final String T_SCHEMA               = "SCHEMA";
    static final String        T_SCHEMA_NAME          = "SCHEMA_NAME";
    static final String        T_SCOPE_CATALOG        = "SCOPE_CATALOG";
    static final String        T_SCOPE_NAME           = "SCOPE_NAME";
    static final String        T_SCOPE_SCHEMA         = "SCOPE_SCHEMA";
    static final String        T_SECTION              = "SECTION";
    static final String        T_SECURITY             = "SECURITY";
    static final String        T_SELF                 = "SELF";
    static final String        T_SEQUENCE             = "SEQUENCE";
    static final String        T_SERIALIZABLE         = "SERIALIZABLE";
    static final String        T_SERVER_NAME          = "SERVER_NAME";
    public static final String T_SESSION              = "SESSION";
    static final String        T_SETS                 = "SETS";
    static final String        T_SIMPLE               = "SIMPLE";
    static final String        T_SIZE                 = "SIZE";
    static final String        T_SOURCE               = "SOURCE";
    static final String        T_SPACE                = "SPACE";
    static final String        T_SPECIFIC_NAME        = "SPECIFIC_NAME";
    static final String        T_SQLDATA              = "SQLDATA";      // SQL/JRT
    static final String        T_STACKED              = "STACKED";
    static final String        T_STATE                = "STATE";
    static final String        T_STATEMENT            = "STATEMENT";
    static final String        T_STRUCTURE            = "STRUCTURE";
    static final String        T_STYLE                = "STYLE";
    static final String        T_SUBCLASS_ORIGIN      = "SUBCLASS_ORIGIN";
    public static final String T_T_FACTOR             = "T";
    static final String        T_TABLE_NAME           = "TABLE_NAME";
    static final String        T_TEMPORARY            = "TEMPORARY";
    static final String        T_TIES                 = "TIES";
    static final String        T_TOP_LEVEL_COUNT      = "TOP_LEVEL_COUNT";
    static final String        T_TRANSACTION          = "TRANSACTION";
    static final String        T_TRANSACT_COMMITTED = "TRANSACTIONS_COMMITTED";
    static final String T_TRANSACTION_ROLLED_BACK = "TRANSACTIONS_ROLLED_BACK";
    static final String        T_TRANSACT_ACTIVE      = "TRANSACTION_ACTIVE";
    static final String        T_TRANSFORM            = "TRANSFORM";
    static final String        T_TRANSFORMS           = "TRANSFORMS";
    static final String        T_TRIGGER_CATALOG      = "TRIGGER_CATALOG";
    static final String        T_TRIGGER_NAME         = "TRIGGER_NAME";
    static final String        T_TRIGGER_SCHEMA       = "TRIGGER_SCHEMA";
    public static final String T_TYPE                 = "TYPE";
    static final String        T_UNBOUNDED            = "UNBOUNDED";
    static final String        T_UNCOMMITTED          = "UNCOMMITTED";
    static final String        T_UNDER                = "UNDER";
    static final String        T_UNDO                 = "UNDO";
    static final String        T_UNNAMED              = "UNNAMED";
    public static final String T_USAGE                = "USAGE";
    static final String T_USER_DEFINED_TYPE_CATALOG =
        "USER_DEFINED_TYPE_CATALOG";
    static final String T_USER_DEFINED_TYPE_CODE = "USER_DEFINED_TYPE_CODE";
    static final String T_USER_DEFINED_TYPE_NAME = "USER_DEFINED_TYPE_NAME";
    static final String T_USER_DEFINED_TYPE_SCHEMA =
        "USER_DEFINED_TYPE_SCHEMA";
    static final String        T_VIEW  = "VIEW";
    static final String        T_WORK  = "WORK";
    static final String        T_WRITE = "WRITE";
    public static final String T_ZONE  = "ZONE";

    // other tokens
    static final String        T_ADD                 = "ADD";
    static final String        T_ALIAS               = "ALIAS";
    static final String        T_AUTOCOMMIT          = "AUTOCOMMIT";
    static final String        T_BACKUP              = "BACKUP";
    public static final String T_BIT                 = "BIT";
    static final String        T_BITLENGTH           = "BITLENGTH";
    static final String        T_CACHE               = "CACHE";
    static final String        T_CACHED              = "CACHED";
    static final String        T_CASEWHEN            = "CASEWHEN";
    static final String        T_CHECKPOINT          = "CHECKPOINT";
    static final String        T_CLASS               = "CLASS";
    static final String        T_COMPACT             = "COMPACT";
    public static final String T_COMPRESSED          = "COMPRESSED";
    static final String        T_CONTROL             = "CONTROL";
    static final String        T_CURDATE             = "CURDATE";
    static final String        T_CURTIME             = "CURTIME";
    static final String        T_DATABASE            = "DATABASE";
    static final String        T_DEFRAG              = "DEFRAG";
    static final String        T_EXPLAIN             = "EXPLAIN";
    static final String        T_EVENT               = "EVENT";
    static final String        T_FILE                = "FILE";
    static final String        T_FILES               = "FILES";
    static final String        T_FOLD                = "FOLD";
    static final String        T_GENERATED           = "GENERATED";
    static final String        T_HEADER              = "HEADER";
    static final String        T_IFNULL              = "IFNULL";
    static final String        T_IGNORECASE          = "IGNORECASE";
    static final String        T_IMMEDIATELY         = "IMMEDIATELY";
    public static final String T_INDEX               = "INDEX";
    public static final String T_INITIAL             = "INITIAL";
    static final String        T_ISAUTOCOMMIT        = "ISAUTOCOMMIT";
    static final String        T_ISREADONLYDATABASE  = "ISREADONLYDATABASE";
    static final String T_ISREADONLYDATABASEFILES = "ISREADONLYDATABASEFILES";
    static final String        T_ISREADONLYSESSION   = "ISREADONLYSESSION";
    static final String        T_LIMIT               = "LIMIT";
    static final String        T_LOCK                = "LOCK";
    static final String        T_LOCKS               = "LOCKS";
    static final String        T_LOGSIZE             = "LOGSIZE";
    static final String        T_MAXROWS             = "MAXROWS";
    static final String        T_MEMORY              = "MEMORY";
    // A VoltDB extension to support more units for timestamp functions
    static final String        T_MICROS              = "MICROS";         // For VoltDB
    static final String        T_MICROSECOND         = "MICROSECOND";    // For VoltDB
    // End of VoltDB extension
    static final String        T_MILLIS              = "MILLIS";
    // A VoltDB extension to support more units for timestamp functions
    static final String        T_MILLISECOND         = "MILLISECOND";    // For VoltDB
    // End of VoltDB extension
    static final String        T_MINUS_EXCEPT        = "MINUS";
    static final String        T_MVCC                = "MVCC";
    static final String        T_NIO                 = "NIO";
    static final String        T_NOW                 = "NOW";
    static final String        T_NOWAIT              = "NOWAIT";
    static final String        T_NVL                 = "NVL";
    static final String        T_OCTETLENGTH         = "OCTETLENGTH";
    static final String        T_OFF                 = "OFF";
    public static final String T_OTHER               = "OTHER";
    public static final String T_PASSWORD            = "PASSWORD";
    static final String        T_PLAN                = "PLAN";
    static final String        T_PROPERTY            = "PROPERTY";
    static final String        T_QUEUE               = "QUEUE";
    static final String        T_READONLY            = "READONLY";
    static final String T_REFERENTIAL_INTEGRITY      = "REFERENTIAL_INTEGRITY";
    static final String        T_RENAME              = "RENAME";
    static final String        T_RESTRICT            = "RESTRICT";
    static final String        T_SCRIPT              = "SCRIPT";
    static final String        T_SCRIPTFORMAT        = "SCRIPTFORMAT";
    static final String        T_BLOCKING            = "BLOCKING";
    static final String        T_SHUTDOWN            = "SHUTDOWN";
    static final String        T_SQL_TSI_DAY         = "SQL_TSI_DAY";
    static final String        T_SQL_TSI_FRAC_SECOND = "SQL_TSI_FRAC_SECOND";
    static final String        T_SQL_TSI_HOUR        = "SQL_TSI_HOUR";
    static final String        T_SQL_TSI_MINUTE      = "SQL_TSI_MINUTE";
    static final String        T_SQL_TSI_MONTH       = "SQL_TSI_MONTH";
    static final String        T_SQL_TSI_QUARTER     = "SQL_TSI_QUARTER";
    static final String        T_SQL_TSI_SECOND      = "SQL_TSI_SECOND";
    static final String        T_SQL_TSI_WEEK        = "SQL_TSI_WEEK";
    static final String        T_SQL_TSI_YEAR        = "SQL_TSI_YEAR";
    static final String        T_SQL_BIGINT          = "SQL_BIGINT";
    static final String        T_SQL_BINARY          = "SQL_BINARY";
    static final String        T_SQL_BIT             = "SQL_BIT";
    static final String        T_SQL_BLOB            = "SQL_BLOB";
    static final String        T_SQL_BOOLEAN         = "SQL_BOOLEAN";
    static final String        T_SQL_CHAR            = "SQL_CHAR";
    static final String        T_SQL_CLOB            = "SQL_CLOB";
    static final String        T_SQL_DATE            = "SQL_DATE";
    static final String        T_SQL_DECIMAL         = "SQL_DECIMAL";
    static final String        T_SQL_DATALINK        = "SQL_DATALINK";
    static final String        T_SQL_DOUBLE          = "SQL_DOUBLE";
    static final String        T_SQL_FLOAT           = "SQL_FLOAT";
    static final String        T_SQL_INTEGER         = "SQL_INTEGER";
    static final String        T_SQL_LONGVARBINARY   = "SQL_LONGVARBINARY";
    static final String        T_SQL_LONGNVARCHAR    = "SQL_LONGNVARCHAR";
    static final String        T_SQL_LONGVARCHAR     = "SQL_LONGVARCHAR";
    static final String        T_SQL_NCHAR           = "SQL_NCHAR";
    static final String        T_SQL_NCLOB           = "SQL_NCLOB";
    static final String        T_SQL_NUMERIC         = "SQL_NUMERIC";
    static final String        T_SQL_NVARCHAR        = "SQL_NVARCHAR";
    static final String        T_SQL_REAL            = "SQL_REAL";
    static final String        T_SQL_ROWID           = "SQL_ROWID";
    static final String        T_SQL_SQLXML          = "SQL_SQLXML";
    static final String        T_SQL_SMALLINT        = "SQL_SMALLINT";
    static final String        T_SQL_TIME            = "SQL_TIME";
    static final String        T_SQL_TIMESTAMP       = "SQL_TIMESTAMP";
    static final String        T_SQL_TINYINT         = "SQL_TINYINT";
    static final String        T_SQL_VARBINARY       = "SQL_VARBINARY";
    static final String        T_SQL_VARCHAR         = "SQL_VARCHAR";
    static final String        T_SYSDATE             = "SYSDATE";
    static final String        T_TEMP                = "TEMP";
    public static final String T_TEXT                = "TEXT";
    static final String        T_TIMESTAMPADD        = "TIMESTAMPADD";
    static final String        T_TIMESTAMPDIFF       = "TIMESTAMPDIFF";
    public static final String T_TINYINT             = "TINYINT";
    static final String        T_TO_CHAR             = "TO_CHAR";
    static final String        T_TODAY               = "TODAY";
    static final String        T_TOP                 = "TOP";
    public static final String T_VARCHAR_IGNORECASE  = "VARCHAR_IGNORECASE";
    static final String        T_WRITE_DELAY         = "WRITE_DELAY";
    public static final String T_YES                 = "YES";
    public static final String T_DAY_NAME            = "DAY_NAME";
    public static final String T_MONTH_NAME          = "MONTH_NAME";
    public static final String T_QUARTER             = "QUARTER";
    public static final String T_DAY_OF_WEEK         = "DAY_OF_WEEK";
    public static final String T_DAY_OF_MONTH        = "DAY_OF_MONTH";
    public static final String T_DAY_OF_YEAR         = "DAY_OF_YEAR";
    public static final String T_WEEK_OF_YEAR        = "WEEK_OF_YEAR";
    static final String        T_DAYNAME             = "DAYNAME";
    static final String        T_NONTHNAME           = "NONTHNAME";
    static final String        T_DAYOFMONTH          = "DAYOFMONTH";
    static final String        T_DAYOFWEEK           = "DAYOFWEEK";
    static final String        T_DAYOFYEAR           = "DAYOFYEAR";
    static final String        T_WEEK                = "WEEK";
    // A VoltDB extension to support WEEKOFYEAR, WEEKDAY function
    static final String        T_WEEKOFYEAR          = "WEEKOFYEAR"; // for compliant with MySQL
    static final String        T_WEEKDAY             = "WEEKDAY";    // for compliant with MySQL
    // End of VoltDB extension

    //
    static final String        T_ACOS             = "ACOS";
    static final String        T_ASIN             = "ASIN";
    static final String        T_ATAN             = "ATAN";
    static final String        T_ATAN2            = "ATAN2";
    static final String        T_COS              = "COS";
    static final String        T_COT              = "COT";
    static final String        T_DEGREES          = "DEGREES";
    static final String        T_DMOD             = "DMOD";
    static final String        T_LOG              = "LOG";
    static final String        T_LOG10            = "LOG10";
    static final String        T_PI               = "PI";
    static final String        T_RADIANS          = "RADIANS";
    static final String        T_RAND             = "RAND";
    static final String        T_ROUND            = "ROUND";
    static final String        T_SIGN             = "SIGN";
    static final String        T_SIN              = "SIN";
    static final String        T_TAN              = "TAN";
    static final String        T_BITAND           = "BITAND";
    static final String        T_BITOR            = "BITOR";
    static final String        T_BITXOR           = "BITXOR";
    // CHERRY PICK -- mysterious
    static final String        T_CONCAT_WORD      = "CONCAT";
    // End of CHERRY PICK
    static final String        T_ROUNDMAGIC       = "ROUNDMAGIC";
    static final String        T_ASCII            = "ASCII";
    // CHERRY PICK -- mysterious
    /* disable 1 line ...
    static final String        T_CONCAT_WORD      = "CONCAT_WORD";
    ... disabled 1 line */
    // End of CHERRY PICK
    static final String        T_DIFFERENCE       = "DIFFERENCE";
    static final String        T_HEXTORAW         = "HEXTORAW";
    static final String        T_LCASE            = "LCASE";
    static final String        T_LOCATE           = "LOCATE";
    static final String        T_LTRIM            = "LTRIM";
    static final String        T_RAWTOHEX         = "RAWTOHEX";
    static final String        T_REPLACE          = "REPLACE";
    static final String        T_RTRIM            = "RTRIM";
    static final String        T_SOUNDEX          = "SOUNDEX";
    static final String        T_SPACE_WORD       = "SPACE_WORD";
    static final String        T_SUBSTR           = "SUBSTR";
    static final String        T_UCASE            = "UCASE";
    static final String        T_DATEDIFF         = "DATEDIFF";
    public static final String T_SECONDS_MIDNIGHT = "SECONDS_SINCE_MIDNIGHT";

    //
    //
    //SQL 200n Standard reserved keywords - full set
    public static final int ABS                              = 1;
    public static final int ALL                              = 2;
    public static final int ALLOCATE                         = 3;
    public static final int ALTER                            = 4;
    public static final int AND                              = 5;
    public static final int ANY                              = 6;
    public static final int ARE                              = 7;
    public static final int ARRAY                            = 8;
    public static final int AS                               = 9;
    public static final int ASENSITIVE                       = 10;
    public static final int ASYMMETRIC                       = 11;
    public static final int AT                               = 12;
    public static final int ATOMIC                           = 13;
    public static final int AUTHORIZATION                    = 14;
    public static final int AVG                              = 15;
    public static final int BEGIN                            = 16;
    public static final int BETWEEN                          = 17;
    public static final int BIGINT                           = 18;
    public static final int BINARY                           = 19;
    public static final int BLOB                             = 20;
    public static final int BOOLEAN                          = 21;
    public static final int BOTH                             = 22;
    public static final int BY                               = 23;
    public static final int CALL                             = 24;
    public static final int CALLED                           = 25;
    public static final int CARDINALITY                      = 26;
    public static final int CASCADED                         = 27;
    public static final int CASE                             = 28;
    public static final int CAST                             = 29;
    public static final int CEIL                             = 30;
    public static final int CEILING                          = 31;
    public static final int CHAR                             = 32;
    public static final int CHAR_LENGTH                      = 33;
    public static final int CHARACTER                        = 34;
    public static final int CHARACTER_LENGTH                 = 35;
    public static final int CHECK                            = 36;
    public static final int CLOB                             = 37;
    public static final int CLOSE                            = 38;
    public static final int COALESCE                         = 39;
    public static final int COLLATE                          = 40;
    public static final int COLLECT                          = 41;
    public static final int COLUMN                           = 42;
    public static final int COMMIT                           = 43;
    public static final int COMPARABLE                       = 44;
    public static final int CONDITION                        = 45;
    public static final int CONNECT                          = 46;
    public static final int CONSTRAINT                       = 47;
    public static final int CONVERT                          = 48;
    public static final int CORR                             = 49;
    public static final int CORRESPONDING                    = 50;
    public static final int COUNT                            = 51;
    public static final int COVAR_POP                        = 52;
    public static final int COVAR_SAMP                       = 53;
    public static final int CREATE                           = 54;
    public static final int CROSS                            = 55;
    public static final int CUBE                             = 56;
    public static final int CUME_DIST                        = 57;
    public static final int CURRENT                          = 58;
    public static final int CURRENT_CATALOG                  = 59;
    public static final int CURRENT_DATE                     = 60;
    public static final int CURRENT_DEFAULT_TRANSFORM_GROUP  = 61;
    public static final int CURRENT_PATH                     = 62;
    public static final int CURRENT_ROLE                     = 63;
    public static final int CURRENT_SCHEMA                   = 64;
    public static final int CURRENT_TIME                     = 65;
    public static final int CURRENT_TIMESTAMP                = 66;
    public static final int CURRENT_TRANSFORM_GROUP_FOR_TYPE = 67;
    public static final int CURRENT_USER                     = 68;
    public static final int CURSOR                           = 69;
    public static final int CYCLE                            = 70;
    public static final int DATE                             = 71;
    public static final int DAY                              = 72;
    public static final int DEALLOCATE                       = 73;
    public static final int DEC                              = 74;
    public static final int DECIMAL                          = 75;
    public static final int DECLARE                          = 76;
    public static final int DEFAULT                          = 77;
    public static final int DELETE                           = 78;
    public static final int DENSE_RANK                       = 79;
    public static final int DEREF                            = 80;
    public static final int DESCRIBE                         = 81;
    public static final int DETERMINISTIC                    = 82;
    public static final int DISCONNECT                       = 83;
    public static final int DISTINCT                         = 84;
    public static final int DO                               = 85;
    public static final int DOUBLE                           = 86;
    public static final int DROP                             = 87;
    public static final int DYNAMIC                          = 88;
    public static final int EACH                             = 89;
    public static final int ELEMENT                          = 90;
    public static final int ELSE                             = 91;
    public static final int ELSEIF                           = 92;
    public static final int END                              = 93;
    public static final int END_EXEC                         = 94;
    public static final int ESCAPE                           = 95;
    public static final int EVERY                            = 96;
    public static final int EXCEPT                           = 97;
    public static final int EXEC                             = 98;
    public static final int EXECUTE                          = 99;
    public static final int EXISTS                           = 100;
    public static final int EXIT                             = 101;
    public static final int EXP                              = 102;
    public static final int EXTERNAL                         = 103;
    public static final int EXTRACT                          = 104;
    public static final int FALSE                            = 105;
    public static final int FETCH                            = 106;
    public static final int FILTER                           = 107;
    public static final int FIRST_VALUE                      = 108;
    public static final int FLOAT                            = 109;
    public static final int FLOOR                            = 110;
    public static final int FOR                              = 111;
    public static final int FOREIGN                          = 112;
    public static final int FREE                             = 113;
    public static final int FROM                             = 114;
    public static final int FULL                             = 115;
    public static final int FUNCTION                         = 116;
    public static final int FUSION                           = 117;
    public static final int GET                              = 118;
    public static final int GLOBAL                           = 119;
    public static final int GRANT                            = 120;
    public static final int GROUP                            = 121;
    public static final int GROUPING                         = 122;
    public static final int HANDLER                          = 123;
    public static final int HAVING                           = 124;
    public static final int HOLD                             = 125;
    public static final int HOUR                             = 126;
    public static final int IDENTITY                         = 127;
    public static final int IN                               = 128;
    public static final int INDICATOR                        = 129;
    public static final int INNER                            = 130;
    public static final int INOUT                            = 131;
    public static final int INSENSITIVE                      = 132;
    public static final int INSERT                           = 133;
    public static final int INT                              = 134;
    public static final int INTEGER                          = 135;
    public static final int INTERSECT                        = 136;
    public static final int INTERSECTION                     = 137;
    public static final int INTERVAL                         = 138;
    public static final int INTO                             = 139;
    public static final int IS                               = 140;
    public static final int ITERATE                          = 141;
    public static final int JOIN                             = 142;
    public static final int LAG                              = 143;
    public static final int LANGUAGE                         = 144;
    public static final int LARGE                            = 145;
    public static final int LAST_VALUE                       = 146;
    public static final int LATERAL                          = 147;
    public static final int LEAD                             = 148;
    public static final int LEADING                          = 149;
    public static final int LEAVE                            = 150;
    public static final int LEFT                             = 151;
    public static final int LIKE                             = 152;
    public static final int LIKE_REGEX                       = 153;
    public static final int LN                               = 154;
    public static final int LOCAL                            = 155;
    public static final int LOCALTIME                        = 156;
    public static final int LOCALTIMESTAMP                   = 157;
    public static final int LOOP                             = 158;
    public static final int LOWER                            = 159;
    public static final int MATCH                            = 160;
    public static final int MAX                              = 161;
    public static final int MAX_CARDINALITY                  = 162;
    public static final int MEMBER                           = 163;
    public static final int MERGE                            = 164;
    public static final int METHOD                           = 165;
    public static final int MIN                              = 166;
    public static final int MINUTE                           = 167;
    public static final int MOD                              = 168;
    public static final int MODIFIES                         = 169;
    public static final int MODULE                           = 170;
    public static final int MONTH                            = 171;
    public static final int MULTISET                         = 172;
    public static final int NATIONAL                         = 173;
    public static final int NATURAL                          = 174;
    public static final int NCHAR                            = 175;
    public static final int NCLOB                            = 176;
    public static final int NEW                              = 177;
    public static final int NO                               = 178;
    public static final int NONE                             = 179;
    public static final int NORMALIZE                        = 180;
    public static final int NOT                              = 181;
    public static final int NTH_VALUE                        = 182;
    public static final int NTILE                            = 183;
    public static final int NULL                             = 184;
    public static final int NULLIF                           = 185;
    public static final int NUMERIC                          = 186;
    public static final int OCCURRENCES_REGEX                = 187;
    public static final int OCTET_LENGTH                     = 188;
    public static final int OF                               = 189;
    public static final int OFFSET                           = 190;
    public static final int OLD                              = 191;
    public static final int ON                               = 192;
    public static final int ONLY                             = 193;
    public static final int OPEN                             = 194;
    public static final int OR                               = 195;
    public static final int ORDER                            = 196;
    public static final int OUT                              = 197;
    public static final int OUTER                            = 198;
    public static final int OVER                             = 199;
    public static final int OVERLAPS                         = 200;
    public static final int OVERLAY                          = 201;
    public static final int PARAMETER                        = 202;
    public static final int PARTITION                        = 203;
    public static final int PERCENT_RANK                     = 204;
    public static final int PERCENTILE_CONT                  = 205;
    public static final int PERCENTILE_DISC                  = 206;
    public static final int POSITION                         = 207;
    public static final int POSITION_REGEX                   = 208;
    public static final int POWER                            = 209;
    public static final int PRECISION                        = 210;
    public static final int PREPARE                          = 211;
    public static final int PRIMARY                          = 212;
    public static final int PROCEDURE                        = 213;
    public static final int RANGE                            = 214;
    public static final int RANK                             = 215;
    public static final int READS                            = 216;
    public static final int REAL                             = 217;
    public static final int RECURSIVE                        = 218;
    public static final int REF                              = 219;
    public static final int REFERENCES                       = 220;
    public static final int REFERENCING                      = 221;
    public static final int REGR_AVGX                        = 222;
    public static final int REGR_AVGY                        = 223;
    public static final int REGR_COUNT                       = 224;
    public static final int REGR_INTERCEPT                   = 225;
    public static final int REGR_R2                          = 226;
    public static final int REGR_SLOPE                       = 227;
    public static final int REGR_SXX                         = 228;
    public static final int REGR_SXY                         = 229;
    public static final int REGR_SYY                         = 230;
    public static final int RELEASE                          = 231;
    public static final int REPEAT                           = 232;
    public static final int RESIGNAL                         = 233;
    public static final int RESULT                           = 234;
    public static final int RETURN                           = 235;
    public static final int RETURNS                          = 236;
    public static final int REVOKE                           = 237;
    public static final int RIGHT                            = 238;
    public static final int ROLLBACK                         = 239;
    public static final int ROLLUP                           = 240;
    public static final int ROW                              = 241;
    public static final int ROW_NUMBER                       = 242;
    public static final int ROWS                             = 243;
    public static final int SAVEPOINT                        = 244;
    public static final int SCOPE                            = 245;
    public static final int SCROLL                           = 246;
    public static final int SEARCH                           = 247;
    public static final int SECOND                           = 248;
    public static final int SELECT                           = 249;
    public static final int SENSITIVE                        = 250;
    public static final int SESSION_USER                     = 251;
    public static final int SET                              = 252;
    public static final int SIGNAL                           = 253;
    public static final int SIMILAR                          = 254;
    public static final int SMALLINT                         = 255;
    public static final int SOME                             = 256;
    public static final int SPECIFIC                         = 257;
    public static final int SPECIFICTYPE                     = 258;
    public static final int SQL                              = 259;
    public static final int SQLEXCEPTION                     = 260;
    public static final int SQLSTATE                         = 261;
    public static final int SQLWARNING                       = 262;
    public static final int SQRT                             = 263;
    public static final int STACKED                          = 264;
    public static final int START                            = 265;
    public static final int STATIC                           = 266;
    public static final int STDDEV_POP                       = 267;
    public static final int STDDEV_SAMP                      = 268;
    public static final int SUBMULTISET                      = 269;
    public static final int SUBSTRING                        = 270;
    public static final int SUBSTRING_REGEX                  = 271;
    public static final int SUM                              = 272;
    public static final int SYMMETRIC                        = 273;
    public static final int SYSTEM                           = 274;
    public static final int SYSTEM_USER                      = 275;
    public static final int TABLE                            = 276;
    public static final int TABLESAMPLE                      = 277;
    public static final int THEN                             = 278;
    public static final int TIME                             = 279;
    public static final int TIMESTAMP                        = 280;
    public static final int TIMEZONE_HOUR                    = 281;
    public static final int TIMEZONE_MINUTE                  = 282;
    public static final int TO                               = 283;
    public static final int TRAILING                         = 284;
    public static final int TRANSLATE                        = 285;
    public static final int TRANSLATE_REGEX                  = 286;
    public static final int TRANSLATION                      = 287;
    public static final int TREAT                            = 288;
    public static final int TRIGGER                          = 289;
    public static final int TRIM                             = 290;
    public static final int TRIM_ARRAY                       = 291;
    public static final int TRUE                             = 292;
    public static final int TRUNCATE                         = 293;
    public static final int UESCAPE                          = 294;
    public static final int UNDO                             = 295;
    public static final int UNION                            = 296;
    public static final int UNIQUE                           = 297;
    // A VoltDB extension to support the assume unique index attribute
    public static final int ASSUMEUNIQUE                     = 1303;    // For VoltDB
    // End of VoltDB extension
    public static final int UNKNOWN                          = 298;
    public static final int UNNEST                           = 299;
    public static final int UNTIL                            = 300;
    public static final int UPDATE                           = 301;
    public static final int UPPER                            = 302;
    public static final int USER                             = 303;
    public static final int USING                            = 304;
    public static final int VALUE                            = 305;
    public static final int VALUES                           = 306;
    public static final int VAR_POP                          = 307;
    public static final int VAR_SAMP                         = 308;
    public static final int VARBINARY                        = 309;
    public static final int VARCHAR                          = 310;
    public static final int VARYING                          = 311;
    public static final int WHEN                             = 312;
    public static final int WHENEVER                         = 313;
    public static final int WHERE                            = 314;
    public static final int WIDTH_BUCKET                     = 315;
    public static final int WINDOW                           = 316;
    public static final int WITH                             = 317;
    public static final int WITHIN                           = 318;
    public static final int WITHOUT                          = 319;
    public static final int WHILE                            = 320;
    public static final int YEAR                             = 321;

    //SQL 200n Standard non-reserved keywords - full set
    public static final int A                           = 330;
    public static final int ABSOLUTE                    = 331;
    public static final int ACTION                      = 332;
    public static final int ADA                         = 333;
    public static final int ADD                         = 334;
    public static final int ADMIN                       = 335;
    public static final int AFTER                       = 336;
    public static final int ALWAYS                      = 337;
    public static final int ASC                         = 338;
    public static final int ASSERTION                   = 339;
    public static final int ASSIGNMENT                  = 340;
    public static final int ATTRIBUTE                   = 341;
    public static final int ATTRIBUTES                  = 342;
    public static final int BEFORE                      = 343;
    public static final int BERNOULLI                   = 344;
    public static final int BREADTH                     = 345;
    public static final int C                           = 346;
    public static final int CASCADE                     = 347;
    public static final int CATALOG                     = 348;
    public static final int CATALOG_NAME                = 349;
    public static final int CHAIN                       = 350;
    public static final int CHARACTER_SET_CATALOG       = 351;
    public static final int CHARACTER_SET_NAME          = 352;
    public static final int CHARACTER_SET_SCHEMA        = 353;
    public static final int CHARACTERISTICS             = 354;
    public static final int CHARACTERS                  = 355;
    public static final int CLASS_ORIGIN                = 356;
    public static final int COBOL                       = 357;
    public static final int COLLATION                   = 358;
    public static final int COLLATION_CATALOG           = 359;
    public static final int COLLATION_NAME              = 360;
    public static final int COLLATION_SCHEMA            = 361;
    public static final int COLUMN_NAME                 = 362;
    public static final int COMMAND_FUNCTION            = 363;
    public static final int COMMAND_FUNCTION_CODE       = 364;
    public static final int COMMITTED                   = 365;
    public static final int CONDITION_IDENTIFIER        = 366;
    public static final int CONDITION_NUMBER            = 367;
    public static final int CONNECTION                  = 368;
    public static final int CONNECTION_NAME             = 369;
    public static final int CONSTRAINT_CATALOG          = 370;
    public static final int CONSTRAINT_NAME             = 371;
    public static final int CONSTRAINT_SCHEMA           = 372;
    public static final int CONSTRAINTS                 = 373;
    public static final int CONSTRUCTOR                 = 374;
    public static final int CONTAINS                    = 375;
    public static final int CONTINUE                    = 376;
    public static final int CURSOR_NAME                 = 377;
    public static final int DATA                        = 378;
    public static final int DATETIME_INTERVAL_CODE      = 379;
    public static final int DATETIME_INTERVAL_PRECISION = 380;
    public static final int DEFAULTS                    = 381;
    public static final int DEFERRABLE                  = 382;
    public static final int DEFERRED                    = 383;
    public static final int DEFINED                     = 384;
    public static final int DEFINER                     = 385;
    public static final int DEGREE                      = 386;
    public static final int DEPTH                       = 387;
    public static final int DERIVED                     = 388;
    public static final int DESC                        = 389;
    public static final int DESCRIPTOR                  = 390;
    public static final int DIAGNOSTICS                 = 391;
    public static final int DISPATCH                    = 392;
    public static final int DOMAIN                      = 393;
    public static final int DYNAMIC_FUNCTION            = 394;
    public static final int DYNAMIC_FUNCTION_CODE       = 395;
    public static final int EQUALS                      = 396;
    public static final int EXCEPTION                   = 397;
    public static final int EXCLUDE                     = 398;
    public static final int EXCLUDING                   = 399;
    public static final int FINAL                       = 400;
    public static final int FIRST                       = 401;
    public static final int FOLLOWING                   = 402;
    public static final int FORTRAN                     = 403;
    public static final int FOUND                       = 404;
    public static final int G                           = 405;
    public static final int GENERAL                     = 406;
    public static final int GENERATED                   = 407;
    public static final int GO                          = 408;
    public static final int GOTO                        = 409;
    public static final int GRANTED                     = 410;
    public static final int HIERARCHY                   = 411;
    public static final int IF                          = 412;
    public static final int IGNORE                      = 413;
    public static final int IMMEDIATE                   = 414;
    public static final int IMPLEMENTATION              = 415;
    public static final int INCLUDING                   = 416;
    public static final int INCREMENT                   = 417;
    public static final int INITIALLY                   = 418;
    public static final int INPUT                       = 419;
    public static final int INSTANCE                    = 420;
    public static final int INSTANTIABLE                = 421;
    public static final int INSTEAD                     = 422;
    public static final int INVOKER                     = 423;
    public static final int ISOLATION                   = 424;
    public static final int JAVA                        = 425;
    public static final int K                           = 426;
    public static final int KEY                         = 427;
    public static final int KEY_MEMBER                  = 428;
    public static final int KEY_TYPE                    = 429;
    public static final int LAST                        = 430;
    public static final int LENGTH                      = 431;
    public static final int LEVEL                       = 432;
    public static final int LOCATOR                     = 433;
    public static final int M                           = 434;
    public static final int MAP                         = 435;
    public static final int MATCHED                     = 436;
    public static final int MAXVALUE                    = 437;
    public static final int MESSAGE_LENGTH              = 438;
    public static final int MESSAGE_OCTET_LENGTH        = 439;
    public static final int MESSAGE_TEXT                = 440;
    public static final int MINVALUE                    = 441;
    public static final int MORE                        = 442;
    public static final int MUMPS                       = 443;
    public static final int NAME                        = 444;
    public static final int NAMES                       = 445;
    public static final int NESTING                     = 446;
    public static final int NEXT                        = 447;
    public static final int NORMALIZED                  = 448;
    public static final int NULLABLE                    = 449;
    public static final int NULLS                       = 450;
    public static final int NUMBER                      = 451;
    public static final int OBJECT                      = 452;
    public static final int OCTETS                      = 453;
    public static final int OPTION                      = 454;
    public static final int OPTIONS                     = 455;
    public static final int ORDERING                    = 456;
    public static final int ORDINALITY                  = 457;
    public static final int OTHERS                      = 458;
    public static final int OUTPUT                      = 459;
    public static final int OVERRIDING                  = 460;
    public static final int PAD                         = 461;
    public static final int PARAMETER_MODE              = 462;
    public static final int PARAMETER_NAME              = 463;
    public static final int PARAMETER_ORDINAL_POSITION  = 464;
    public static final int PARAMETER_SPECIFIC_CATALOG  = 465;
    public static final int PARAMETER_SPECIFIC_NAME     = 466;
    public static final int PARAMETER_SPECIFIC_SCHEMA   = 467;
    public static final int PARTIAL                     = 468;
    public static final int PASCAL                      = 469;
    public static final int PATH                        = 470;
    public static final int PLACING                     = 471;
    public static final int PLI                         = 472;
    public static final int PRECEDING                   = 473;
    public static final int PRESERVE                    = 474;
    public static final int PRIOR                       = 475;
    public static final int PRIVILEGES                  = 476;
    public static final int PUBLIC                      = 477;
    public static final int READ                        = 478;
    public static final int RELATIVE                    = 479;
    public static final int REPEATABLE                  = 480;
    public static final int RESPECT                     = 481;
    public static final int RESTART                     = 482;
    public static final int RESTRICT                    = 483;
    public static final int RETURNED_CARDINALITY        = 484;
    public static final int RETURNED_LENGTH             = 485;
    public static final int RETURNED_OCTET_LENGTH       = 486;
    public static final int RETURNED_SQLSTATE           = 487;
    public static final int ROLE                        = 488;
    public static final int ROUTINE                     = 489;
    public static final int ROUTINE_CATALOG             = 490;
    public static final int ROUTINE_NAME                = 491;
    public static final int ROUTINE_SCHEMA              = 492;
    public static final int ROW_COUNT                   = 493;
    public static final int SCALE                       = 494;
    public static final int SCHEMA                      = 495;
    public static final int SCHEMA_NAME                 = 496;
    public static final int SCOPE_CATALOG               = 497;
    public static final int SCOPE_NAME                  = 498;
    public static final int SCOPE_SCHEMA                = 499;
    public static final int SECTION                     = 500;
    public static final int SECURITY                    = 501;
    public static final int SELF                        = 502;
    public static final int SEQUENCE                    = 503;
    public static final int SERIALIZABLE                = 504;
    public static final int SERVER_NAME                 = 505;
    public static final int SESSION                     = 506;
    public static final int SETS                        = 507;
    public static final int SIMPLE                      = 508;
    public static final int SIZE                        = 509;
    public static final int SOURCE                      = 510;
    public static final int SPACE                       = 511;
    public static final int SPECIFIC_NAME               = 512;
    public static final int STATE                       = 513;
    public static final int STATEMENT                   = 514;
    public static final int STRUCTURE                   = 515;
    public static final int STYLE                       = 516;
    public static final int SUBCLASS_ORIGIN             = 517;
    public static final int TABLE_NAME                  = 518;
    public static final int TEMPORARY                   = 519;
    public static final int TIES                        = 520;
    public static final int TOP_LEVEL_COUNT             = 521;
    public static final int TRANSACTION                 = 522;
    public static final int TRANSACTION_ACTIVE          = 523;
    public static final int TRANSACTIONS_COMMITTED      = 524;
    public static final int TRANSACTIONS_ROLLED_BACK    = 525;
    public static final int TRANSFORM                   = 526;
    public static final int TRANSFORMS                  = 527;
    public static final int TRIGGER_CATALOG             = 528;
    public static final int TRIGGER_NAME                = 529;
    public static final int TRIGGER_SCHEMA              = 530;
    public static final int TYPE                        = 531;
    public static final int UNBOUNDED                   = 532;
    public static final int UNCOMMITTED                 = 533;
    public static final int UNDER                       = 534;
    public static final int UNNAMED                     = 535;
    public static final int USAGE                       = 536;
    public static final int USER_DEFINED_TYPE_CATALOG   = 537;
    public static final int USER_DEFINED_TYPE_CODE      = 538;
    public static final int USER_DEFINED_TYPE_NAME      = 539;
    public static final int USER_DEFINED_TYPE_SCHEMA    = 540;
    public static final int VIEW                        = 541;
    public static final int WORK                        = 542;
    public static final int WRITE                       = 543;
    public static final int ZONE                        = 544;

    //
    public static final int P = 545;
    public static final int T = 546;

    // other token values used as switch cases
    static final int        ALIAS                 = 551;
    static final int        AUTOCOMMIT            = 552;
    static final int        BIT                   = 553;
    static final int        BIT_LENGTH            = 554;
    // A VoltDB extension to support varchar column in bytes.
    static final int        BYTES                 = 1010; // For VoltDB
    // End of VoltDB extension
    static final int        CACHED                = 555;
    static final int        CASEWHEN              = 556;
    static final int        CHECKPOINT            = 557;
    static final int        COMPACT               = 558;
    static final int        DATABASE              = 559;
    public static final int DAY_OF_WEEK           = 560;
    static final int        DEFRAG                = 561;
    static final int        EXPLAIN               = 562;
    static final int        HEADER                = 563;
    static final int        IGNORECASE            = 564;
    static final int        IFNULL                = 565;
    static final int        INDEX                 = 566;
    static final int        IMMEDIATELY           = 567;
    static final int        INITIAL               = 568;
    static final int        LIMIT                 = 569;
    static final int        LOGSIZE               = 570;
    static final int        MAXROWS               = 571;
    static final int        MEMORY                = 572;
    // A VoltDB extension to support more units for timestamp functions
    static final int        MICROS                = 1000; // For VoltDB
    static final int        MICROSECOND           = 1001; // For VoltDB
    // End of VoltDB extension
    static final int        MILLIS                = 573;
    // A VoltDB extension to support more units for timestamp functions
    static final int        MILLISECOND           = 1002; // For VoltDB
    // End of VoltDB extension
    static final int        MINUS_EXCEPT          = 574;
    static final int        NOW                   = 575;
    static final int        OFF                   = 576;
    static final int        PASSWORD              = 577;
    static final int        PLAN                  = 578;
    static final int        PROPERTY              = 579;
    static final int        READONLY              = 580;
    static final int        REFERENTIAL_INTEGRITY = 581;
    static final int        RENAME                = 582;
    static final int        SCRIPT                = 583;
    static final int        SCRIPTFORMAT          = 584;
    static final int        SEMICOLON             = 585;
    static final int        SHUTDOWN              = 586;
    static final int        TEMP                  = 587;
    static final int        TEXT                  = 588;
    static final int        TO_CHAR               = 589;
    static final int        TODAY                 = 590;
    static final int        TOP                   = 591;
    public static final int WEEK_OF_YEAR          = 592;
    static final int        WRITE_DELAY           = 593;
    static final int        COMPRESSED            = 594;
    static final int        EVENT                 = 595;
    static final int        BACKUP                = 596;
    static final int        BLOCKING              = 597;

    //
    static final int        CURDATE                 = 598;
    static final int        CURTIME                 = 599;
    static final int        TIMESTAMPADD            = 600;
    static final int        TIMESTAMPDIFF           = 601;
    static final int        SYSDATE                 = 602;
    static final int        ISAUTOCOMMIT            = 603;
    static final int        ISREADONLYSESSION       = 604;
    static final int        ISREADONLYDATABASE      = 605;
    static final int        ISREADONLYDATABASEFILES = 606;
    public static final int DAY_NAME                = 607;
    public static final int MONTH_NAME              = 608;
    public static final int QUARTER                 = 609;
    public static final int DAY_OF_MONTH            = 610;
    public static final int DAY_OF_YEAR             = 611;
    static final int        DAYNAME                 = 612;
    static final int        NONTHNAME               = 613;
    static final int        DAYOFMONTH              = 614;
    static final int        DAYOFWEEK               = 615;
    static final int        DAYOFYEAR               = 616;
    // A VoltDB extension to make WEEK public
    public static final int WEEK                    = 617;
    /* disable 1 line ...
    static final int WEEK                           = 617;
    ... disabled 1 line */
    // End of VoltDB extension
    static final int        OCTETLENGTH             = 618;
    static final int        BITLENGTH               = 619;

    //
    static final int        ACOS             = 620;
    static final int        ASIN             = 621;
    static final int        ATAN             = 622;
    static final int        ATAN2            = 623;
    static final int        COS              = 624;
    static final int        COT              = 625;
    static final int        DEGREES          = 626;
    static final int        DMOD             = 627;
    static final int        LOG              = 628;
    static final int        LOG10            = 629;
    static final int        PI               = 630;
    static final int        RADIANS          = 631;
    static final int        RAND             = 632;
    static final int        ROUND            = 633;
    static final int        SIGN             = 634;
    static final int        SIN              = 635;
    static final int        TAN              = 636;
    static final int        BITAND           = 637;
    static final int        BITOR            = 638;
    static final int        BITXOR           = 639;
    static final int        ROUNDMAGIC       = 640;
    static final int        ASCII            = 641;
    static final int        CONCAT_WORD      = 642;
    static final int        DIFFERENCE       = 643;
    static final int        HEXTORAW         = 644;
    static final int        LCASE            = 645;
    static final int        LOCATE           = 646;
    static final int        LTRIM            = 647;
    static final int        RAWTOHEX         = 648;
    static final int        REPLACE          = 649;
    static final int        RTRIM            = 650;
    static final int        SOUNDEX          = 651;
    static final int        SPACE_WORD       = 652;
    static final int        SUBSTR           = 653;
    static final int        UCASE            = 654;
    static final int        DATEDIFF         = 655;
    public static final int SECONDS_MIDNIGHT = 656;

    //
    static final int CONTROL = 657;
    static final int LOCK    = 658;
    static final int LOCKS   = 659;
    static final int MVCC    = 660;

    //
    static final int        ASTERISK         = 661;
    static final int        CLOSEBRACKET     = 662;
    static final int        COLON            = 663;
    static final int        COMMA            = 664;
    static final int        CONCAT           = 665;
    static final int        DIVIDE           = 666;
    static final int        DOUBLE_COLON_OP  = 667;
    static final int        DOUBLE_PERIOD_OP = 668;
    static final int        DOUBLE_COLUMN_OP = 669;
    static final int        GREATER          = 670;
    static final int        GREATER_EQUALS   = 671;
    static final int        LESS             = 672;
    static final int        LESS_EQUALS      = 673;
    public static final int MINUS            = 674;
    static final int        NOT_EQUALS       = 675;
    static final int        OPENBRACKET      = 676;
    static final int        PLUS             = 677;
    static final int        QUESTION         = 678;
    static final int        RIGHT_ARROW_OP   = 679;
    static final int        DOUBLE_COLON     = 680;

    //
    static final int SQL_TSI_FRAC_SECOND = 681;
    static final int SQL_TSI_SECOND      = 682;
    static final int SQL_TSI_MINUTE      = 683;
    static final int SQL_TSI_HOUR        = 684;
    static final int SQL_TSI_DAY         = 685;
    static final int SQL_TSI_WEEK        = 686;
    static final int SQL_TSI_MONTH       = 687;
    static final int SQL_TSI_QUARTER     = 688;
    static final int SQL_TSI_YEAR        = 689;

    // todo - goes into general hsqldb list
    static final int FILE  = 691;
    static final int FILES = 692;
    static final int CACHE = 693;
    static final int NIO = 694;

    //
    static final int SQL_BIGINT        = 701;
    static final int SQL_BINARY        = 702;
    static final int SQL_BIT           = 703;
    static final int SQL_BLOB          = 704;
    static final int SQL_BOOLEAN       = 705;
    static final int SQL_CHAR          = 706;
    static final int SQL_CLOB          = 707;
    static final int SQL_DATE          = 708;
    static final int SQL_DECIMAL       = 709;
    static final int SQL_DATALINK      = 710;
    static final int SQL_DOUBLE        = 711;
    static final int SQL_FLOAT         = 712;
    static final int SQL_INTEGER       = 713;
    static final int SQL_LONGVARBINARY = 714;
    static final int SQL_LONGNVARCHAR  = 715;
    static final int SQL_LONGVARCHAR   = 716;
    static final int SQL_NCHAR         = 717;
    static final int SQL_NCLOB         = 718;
    static final int SQL_NUMERIC       = 719;
    static final int SQL_NVARCHAR      = 720;
    static final int SQL_REAL          = 721;
    static final int SQL_ROWID         = 722;
    static final int SQL_SQLXML        = 723;
    static final int SQL_SMALLINT      = 724;
    static final int SQL_TIME          = 725;
    static final int SQL_TIMESTAMP     = 726;
    static final int SQL_TINYINT       = 727;
    static final int SQL_VARBINARY     = 728;
    static final int SQL_VARCHAR       = 729;

    //
    static final int X_KEYSET      = 730;
    static final int X_OPTION      = 731;
    static final int X_REPEAT      = 732;
    static final int X_POS_INTEGER = 733;

    //
    public static final int X_VALUE                    = 734;
    public static final int X_IDENTIFIER               = 735;
    public static final int X_DELIMITED_IDENTIFIER     = 736;
    public static final int X_ENDPARSE                 = 737;
    public static final int X_STARTPARSE               = 738;
    public static final int X_REMARK                   = 739;
    public static final int X_NULL                     = 730;
    public static final int X_LOB_SIZE                 = 731;
    public static final int X_MALFORMED_STRING         = 732;
    public static final int X_MALFORMED_NUMERIC        = 733;
    public static final int X_MALFORMED_BIT_STRING     = 734;
    public static final int X_MALFORMED_BINARY_STRING  = 735;
    public static final int X_MALFORMED_UNICODE_STRING = 736;
    public static final int X_MALFORMED_COMMENT        = 737;
    public static final int X_MALFORMED_IDENTIFIER     = 738;
    public static final int X_MALFORMED_UNICODE_ESCAPE = 739;
    // A VoltDB extension to support WEEKOFYEAR, WEEKDAY
    public static final int WEEKOFYEAR                 = 740; // for compliant with MySQL
    public static final int WEEKDAY                    = 741; // for compliant with MySQL
    // End of VoltDB extension

    //
    public static final int X_UNKNOWN_TOKEN = -1;
    private static final IntValueHashMap reservedKeys =
        new IntValueHashMap(351);

    static {
        reservedKeys.put(Tokens.T_ABS, ABS);
        reservedKeys.put(Tokens.T_ALL, ALL);
        reservedKeys.put(Tokens.T_ALLOCATE, ALLOCATE);
        reservedKeys.put(Tokens.T_ALTER, ALTER);
        reservedKeys.put(Tokens.T_AND, AND);
        reservedKeys.put(Tokens.T_ANY, ANY);
        reservedKeys.put(Tokens.T_ARE, ARE);
        reservedKeys.put(Tokens.T_ARRAY, ARRAY);
        reservedKeys.put(Tokens.T_AS, AS);
        reservedKeys.put(Tokens.T_ASENSITIVE, ASENSITIVE);
        reservedKeys.put(Tokens.T_ASYMMETRIC, ASYMMETRIC);
        reservedKeys.put(Tokens.T_AT, AT);
        reservedKeys.put(Tokens.T_ATOMIC, ATOMIC);
        reservedKeys.put(Tokens.T_AUTHORIZATION, AUTHORIZATION);
        reservedKeys.put(Tokens.T_AVG, AVG);
        reservedKeys.put(Tokens.T_BEGIN, BEGIN);
        reservedKeys.put(Tokens.T_BETWEEN, BETWEEN);
        reservedKeys.put(Tokens.T_BIGINT, BIGINT);
        reservedKeys.put(Tokens.T_BINARY, BINARY);
        reservedKeys.put(Tokens.T_BIT_LENGTH, BIT_LENGTH);
        // A VoltDB extension to support varchar column in bytes.
        reservedKeys.put(Tokens.T_BYTES, BYTES); // For VoltDB
        // End of VoltDB extension
        reservedKeys.put(Tokens.T_BLOB, BLOB);
        reservedKeys.put(Tokens.T_BOOLEAN, BOOLEAN);
        reservedKeys.put(Tokens.T_BOTH, BOTH);
        reservedKeys.put(Tokens.T_BY, BY);
        reservedKeys.put(Tokens.T_CALL, CALL);
        reservedKeys.put(Tokens.T_CALLED, CALLED);
        reservedKeys.put(Tokens.T_CARDINALITY, CARDINALITY);
        reservedKeys.put(Tokens.T_CASCADED, CASCADED);
        reservedKeys.put(Tokens.T_CASE, CASE);
        reservedKeys.put(Tokens.T_CAST, CAST);
        reservedKeys.put(Tokens.T_CEIL, CEIL);
        reservedKeys.put(Tokens.T_CEILING, CEILING);
        reservedKeys.put(Tokens.T_CHAR, CHAR);
        reservedKeys.put(Tokens.T_CHAR_LENGTH, CHAR_LENGTH);
        reservedKeys.put(Tokens.T_CHARACTER, CHARACTER);
        reservedKeys.put(Tokens.T_CHARACTER_LENGTH, CHARACTER_LENGTH);
        reservedKeys.put(Tokens.T_CHECK, CHECK);
        reservedKeys.put(Tokens.T_CLOB, CLOB);
        reservedKeys.put(Tokens.T_CLOSE, CLOSE);
        reservedKeys.put(Tokens.T_COALESCE, COALESCE);
        reservedKeys.put(Tokens.T_COLLATE, COLLATE);
        reservedKeys.put(Tokens.T_COLLECT, COLLECT);
        reservedKeys.put(Tokens.T_COLUMN, COLUMN);
        reservedKeys.put(Tokens.T_COMMIT, COMMIT);
        reservedKeys.put(Tokens.T_COMPARABLE, COMPARABLE);
        // A VoltDB extension -- mysterious
        reservedKeys.put(Tokens.T_CONCAT, CONCAT);
        // End of VoltDB extension
        reservedKeys.put(Tokens.T_CONDITION, CONDITION);
        reservedKeys.put(Tokens.T_CONNECT, CONNECT);
        reservedKeys.put(Tokens.T_CONSTRAINT, CONSTRAINT);
        reservedKeys.put(Tokens.T_CONVERT, CONVERT);
        reservedKeys.put(Tokens.T_CORR, CORR);
        reservedKeys.put(Tokens.T_CORRESPONDING, CORRESPONDING);
        reservedKeys.put(Tokens.T_COUNT, COUNT);
        reservedKeys.put(Tokens.T_COVAR_POP, COVAR_POP);
        reservedKeys.put(Tokens.T_COVAR_SAMP, COVAR_SAMP);
        reservedKeys.put(Tokens.T_CREATE, CREATE);
        reservedKeys.put(Tokens.T_CROSS, CROSS);
        reservedKeys.put(Tokens.T_CUBE, CUBE);
        reservedKeys.put(Tokens.T_CUME_DIST, CUME_DIST);
        reservedKeys.put(Tokens.T_CURRENT, CURRENT);
        reservedKeys.put(Tokens.T_CURRENT_CATALOG, CURRENT_CATALOG);
        reservedKeys.put(Tokens.T_CURRENT_DATE, CURRENT_DATE);
        reservedKeys.put(Tokens.T_CURRENT_DEFAULT_TRANSFORM_GROUP,
                         CURRENT_DEFAULT_TRANSFORM_GROUP);
        reservedKeys.put(Tokens.T_CURRENT_PATH, CURRENT_PATH);
        reservedKeys.put(Tokens.T_CURRENT_ROLE, CURRENT_ROLE);
        reservedKeys.put(Tokens.T_CURRENT_SCHEMA, CURRENT_SCHEMA);
        reservedKeys.put(Tokens.T_CURRENT_TIME, CURRENT_TIME);
        reservedKeys.put(Tokens.T_CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        reservedKeys.put(Tokens.T_DO, DO);
        reservedKeys.put(Tokens.T_CURRENT_TRANSFORM_GROUP_FOR_TYPE,
                         CURRENT_TRANSFORM_GROUP_FOR_TYPE);
        reservedKeys.put(Tokens.T_CURRENT_USER, CURRENT_USER);
        reservedKeys.put(Tokens.T_CURSOR, CURSOR);
        reservedKeys.put(Tokens.T_CYCLE, CYCLE);
        reservedKeys.put(Tokens.T_DATE, DATE);
        reservedKeys.put(Tokens.T_DAY, DAY);
        reservedKeys.put(Tokens.T_DEALLOCATE, DEALLOCATE);
        reservedKeys.put(Tokens.T_DEC, DEC);
        reservedKeys.put(Tokens.T_DECIMAL, DECIMAL);
        reservedKeys.put(Tokens.T_DECLARE, DECLARE);
        reservedKeys.put(Tokens.T_DEFAULT, DEFAULT);
        reservedKeys.put(Tokens.T_DELETE, DELETE);
        reservedKeys.put(Tokens.T_DENSE_RANK, DENSE_RANK);
        reservedKeys.put(Tokens.T_DEREF, DEREF);
        reservedKeys.put(Tokens.T_DESCRIBE, DESCRIBE);
        reservedKeys.put(Tokens.T_DETERMINISTIC, DETERMINISTIC);
        reservedKeys.put(Tokens.T_DISCONNECT, DISCONNECT);
        reservedKeys.put(Tokens.T_DISTINCT, DISTINCT);
        reservedKeys.put(Tokens.T_DOUBLE, DOUBLE);
        reservedKeys.put(Tokens.T_DROP, DROP);
        reservedKeys.put(Tokens.T_DYNAMIC, DYNAMIC);
        reservedKeys.put(Tokens.T_EACH, EACH);
        reservedKeys.put(Tokens.T_ELEMENT, ELEMENT);
        reservedKeys.put(Tokens.T_ELSE, ELSE);
        reservedKeys.put(Tokens.T_ELSEIF, ELSEIF);
        reservedKeys.put(Tokens.T_END, END);
        reservedKeys.put(Tokens.T_END_EXEC, END_EXEC);
        reservedKeys.put(Tokens.T_ESCAPE, ESCAPE);
        reservedKeys.put(Tokens.T_EVERY, EVERY);
        reservedKeys.put(Tokens.T_EXCEPT, EXCEPT);
        reservedKeys.put(Tokens.T_EXEC, EXEC);
        reservedKeys.put(Tokens.T_EXECUTE, EXECUTE);
        reservedKeys.put(Tokens.T_EXISTS, EXISTS);
        reservedKeys.put(Tokens.T_EXIT, EXIT);
        reservedKeys.put(Tokens.T_EXP, EXP);
        reservedKeys.put(Tokens.T_EXTERNAL, EXTERNAL);
        reservedKeys.put(Tokens.T_EXTRACT, EXTRACT);
        reservedKeys.put(Tokens.T_FALSE, FALSE);
        reservedKeys.put(Tokens.T_FETCH, FETCH);
        reservedKeys.put(Tokens.T_FILTER, FILTER);
        reservedKeys.put(Tokens.T_FIRST_VALUE, FIRST_VALUE);
        reservedKeys.put(Tokens.T_FLOAT, FLOAT);
        reservedKeys.put(Tokens.T_FLOOR, FLOOR);
        reservedKeys.put(Tokens.T_FOR, FOR);
        reservedKeys.put(Tokens.T_FOREIGN, FOREIGN);
        reservedKeys.put(Tokens.T_FREE, FREE);
        reservedKeys.put(Tokens.T_FROM, FROM);
        reservedKeys.put(Tokens.T_FULL, FULL);
        reservedKeys.put(Tokens.T_FUNCTION, FUNCTION);
        reservedKeys.put(Tokens.T_FUSION, FUSION);
        reservedKeys.put(Tokens.T_GET, GET);
        reservedKeys.put(Tokens.T_GLOBAL, GLOBAL);
        reservedKeys.put(Tokens.T_GRANT, GRANT);
        reservedKeys.put(Tokens.T_GROUP, GROUP);
        reservedKeys.put(Tokens.T_GROUPING, GROUPING);
        reservedKeys.put(Tokens.T_HANDLER, HANDLER);
        reservedKeys.put(Tokens.T_HAVING, HAVING);
        reservedKeys.put(Tokens.T_HOLD, HOLD);
        reservedKeys.put(Tokens.T_HOUR, HOUR);
        reservedKeys.put(Tokens.T_IDENTITY, IDENTITY);
        reservedKeys.put(Tokens.T_IF, IF);
        reservedKeys.put(Tokens.T_IN, IN);
        reservedKeys.put(Tokens.T_INDICATOR, INDICATOR);
        reservedKeys.put(Tokens.T_INNER, INNER);
        reservedKeys.put(Tokens.T_INOUT, INOUT);
        reservedKeys.put(Tokens.T_INSENSITIVE, INSENSITIVE);
        reservedKeys.put(Tokens.T_INSERT, INSERT);
        reservedKeys.put(Tokens.T_INT, INT);
        reservedKeys.put(Tokens.T_INTEGER, INTEGER);
        reservedKeys.put(Tokens.T_INTERSECT, INTERSECT);
        reservedKeys.put(Tokens.T_INTERSECTION, INTERSECTION);
        reservedKeys.put(Tokens.T_INTERVAL, INTERVAL);
        reservedKeys.put(Tokens.T_INTO, INTO);
        reservedKeys.put(Tokens.T_IS, IS);
        reservedKeys.put(Tokens.T_ITERATE, ITERATE);
        reservedKeys.put(Tokens.T_JOIN, JOIN);
        reservedKeys.put(Tokens.T_LAG, LAG);
        reservedKeys.put(Tokens.T_LANGUAGE, LANGUAGE);
        reservedKeys.put(Tokens.T_LARGE, LARGE);
        reservedKeys.put(Tokens.T_LAST_VALUE, LAST_VALUE);
        reservedKeys.put(Tokens.T_LATERAL, LATERAL);
        reservedKeys.put(Tokens.T_LEAD, LEAD);
        reservedKeys.put(Tokens.T_LEADING, LEADING);
        reservedKeys.put(Tokens.T_LEAVE, LEAVE);
        reservedKeys.put(Tokens.T_LEFT, LEFT);
        reservedKeys.put(Tokens.T_LIKE, LIKE);
        reservedKeys.put(Tokens.T_LIKE_REGX, LIKE_REGEX);
        reservedKeys.put(Tokens.T_LN, LN);
        reservedKeys.put(Tokens.T_LOCAL, LOCAL);
        reservedKeys.put(Tokens.T_LOCALTIME, LOCALTIME);
        reservedKeys.put(Tokens.T_LOCALTIMESTAMP, LOCALTIMESTAMP);
        reservedKeys.put(Tokens.T_LOOP, LOOP);
        reservedKeys.put(Tokens.T_LOWER, LOWER);
        reservedKeys.put(Tokens.T_MATCH, MATCH);
        reservedKeys.put(Tokens.T_MAX, MAX);
        reservedKeys.put(Tokens.T_MAX_CARDINALITY, MAX_CARDINALITY);
        reservedKeys.put(Tokens.T_MEMBER, MEMBER);
        reservedKeys.put(Tokens.T_MERGE, MERGE);
        reservedKeys.put(Tokens.T_METHOD, METHOD);
        reservedKeys.put(Tokens.T_MIN, MIN);
        reservedKeys.put(Tokens.T_MINUTE, MINUTE);
        reservedKeys.put(Tokens.T_MOD, MOD);
        reservedKeys.put(Tokens.T_MODIFIES, MODIFIES);
        reservedKeys.put(Tokens.T_MODULE, MODULE);
        reservedKeys.put(Tokens.T_MONTH, MONTH);
        reservedKeys.put(Tokens.T_MULTISET, MULTISET);
        reservedKeys.put(Tokens.T_NATIONAL, NATIONAL);
        reservedKeys.put(Tokens.T_NATURAL, NATURAL);
        reservedKeys.put(Tokens.T_NCHAR, NCHAR);
        reservedKeys.put(Tokens.T_NCLOB, NCLOB);
        reservedKeys.put(Tokens.T_NEW, NEW);
        reservedKeys.put(Tokens.T_NO, NO);
        reservedKeys.put(Tokens.T_NONE, NONE);
        reservedKeys.put(Tokens.T_NORMALIZE, NORMALIZE);
        reservedKeys.put(Tokens.T_NOT, NOT);
        reservedKeys.put(Tokens.T_NTH_VALUE, NTH_VALUE);
        reservedKeys.put(Tokens.T_NTILE, NTILE);
        reservedKeys.put(Tokens.T_NULL, NULL);
        reservedKeys.put(Tokens.T_NULLIF, NULLIF);
        reservedKeys.put(Tokens.T_NUMERIC, NUMERIC);
        reservedKeys.put(Tokens.T_OCCURRENCES_REGEX, OCCURRENCES_REGEX);
        reservedKeys.put(Tokens.T_OCTET_LENGTH, OCTET_LENGTH);
        reservedKeys.put(Tokens.T_OF, OF);
        reservedKeys.put(Tokens.T_OFFSET, OFFSET);
        reservedKeys.put(Tokens.T_OLD, OLD);
        reservedKeys.put(Tokens.T_ON, ON);
        reservedKeys.put(Tokens.T_ONLY, ONLY);
        reservedKeys.put(Tokens.T_OPEN, OPEN);
        reservedKeys.put(Tokens.T_OR, OR);
        reservedKeys.put(Tokens.T_ORDER, ORDER);
        reservedKeys.put(Tokens.T_OUT, OUT);
        reservedKeys.put(Tokens.T_OUTER, OUTER);
        reservedKeys.put(Tokens.T_OVER, OVER);
        reservedKeys.put(Tokens.T_OVERLAPS, OVERLAPS);
        reservedKeys.put(Tokens.T_OVERLAY, OVERLAY);
        reservedKeys.put(Tokens.T_PARAMETER, PARAMETER);
        reservedKeys.put(Tokens.T_PARTITION, PARTITION);
        reservedKeys.put(Tokens.T_PERCENT_RANK, PERCENT_RANK);
        reservedKeys.put(Tokens.T_PERCENTILE_CONT, PERCENTILE_CONT);
        reservedKeys.put(Tokens.T_PERCENTILE_DISC, PERCENTILE_DISC);
        reservedKeys.put(Tokens.T_POSITION, POSITION);
        reservedKeys.put(Tokens.T_POSITION_REGEX, POSITION_REGEX);
        reservedKeys.put(Tokens.T_POWER, POWER);
        reservedKeys.put(Tokens.T_PRECISION, PRECISION);
        reservedKeys.put(Tokens.T_PREPARE, PREPARE);
        reservedKeys.put(Tokens.T_PRIMARY, PRIMARY);
        reservedKeys.put(Tokens.T_PROCEDURE, PROCEDURE);
        reservedKeys.put(Tokens.T_RANGE, RANGE);
        reservedKeys.put(Tokens.T_RANK, RANK);
        reservedKeys.put(Tokens.T_READS, READS);
        reservedKeys.put(Tokens.T_REAL, REAL);
        reservedKeys.put(Tokens.T_RECURSIVE, RECURSIVE);
        reservedKeys.put(Tokens.T_REF, REF);
        reservedKeys.put(Tokens.T_REFERENCES, REFERENCES);
        reservedKeys.put(Tokens.T_REFERENCING, REFERENCING);
        reservedKeys.put(Tokens.T_REGR_AVGX, REGR_AVGX);
        reservedKeys.put(Tokens.T_REGR_AVGY, REGR_AVGY);
        reservedKeys.put(Tokens.T_REGR_COUNT, REGR_COUNT);
        reservedKeys.put(Tokens.T_REGR_INTERCEPT, REGR_INTERCEPT);
        reservedKeys.put(Tokens.T_REGR_R2, REGR_R2);
        reservedKeys.put(Tokens.T_REGR_SLOPE, REGR_SLOPE);
        reservedKeys.put(Tokens.T_REGR_SXX, REGR_SXX);
        reservedKeys.put(Tokens.T_REGR_SXY, REGR_SXY);
        reservedKeys.put(Tokens.T_REGR_SYY, REGR_SYY);
        reservedKeys.put(Tokens.T_RELEASE, RELEASE);
        reservedKeys.put(Tokens.T_REPEAT, REPEAT);
        reservedKeys.put(Tokens.T_RESIGNAL, RESIGNAL);
        reservedKeys.put(Tokens.T_RETURN, RETURN);
        reservedKeys.put(Tokens.T_RETURNS, RETURNS);
        reservedKeys.put(Tokens.T_REVOKE, REVOKE);
        reservedKeys.put(Tokens.T_RIGHT, RIGHT);
        reservedKeys.put(Tokens.T_ROLLBACK, ROLLBACK);
        reservedKeys.put(Tokens.T_ROLLUP, ROLLUP);
        reservedKeys.put(Tokens.T_ROW, ROW);
        reservedKeys.put(Tokens.T_ROW_NUMBER, ROW_NUMBER);
        reservedKeys.put(Tokens.T_ROWS, ROWS);
        reservedKeys.put(Tokens.T_SAVEPOINT, SAVEPOINT);
        reservedKeys.put(Tokens.T_SCOPE, SCOPE);
        reservedKeys.put(Tokens.T_SCROLL, SCROLL);
        reservedKeys.put(Tokens.T_SEARCH, SEARCH);
        reservedKeys.put(Tokens.T_SECOND, SECOND);
        reservedKeys.put(Tokens.T_SELECT, SELECT);
        reservedKeys.put(Tokens.T_SENSITIVE, SENSITIVE);
        reservedKeys.put(Tokens.T_SESSION_USER, SESSION_USER);
        reservedKeys.put(Tokens.T_SET, SET);
        reservedKeys.put(Tokens.T_SIGNAL, SIGNAL);
        reservedKeys.put(Tokens.T_SIMILAR, SIMILAR);
        reservedKeys.put(Tokens.T_SMALLINT, SMALLINT);
        reservedKeys.put(Tokens.T_SOME, SOME);
        // A VoltDB extension to augment the set of SQL functions supported
        reservedKeys.put(Tokens.T_SPACE, SPACE);
        // End of VoltDB extension
        reservedKeys.put(Tokens.T_SPECIFIC, SPECIFIC);
        reservedKeys.put(Tokens.T_SPECIFICTYPE, SPECIFICTYPE);
        reservedKeys.put(Tokens.T_SQL, SQL);
        reservedKeys.put(Tokens.T_SQLEXCEPTION, SQLEXCEPTION);
        reservedKeys.put(Tokens.T_SQLSTATE, SQLSTATE);
        reservedKeys.put(Tokens.T_SQLWARNING, SQLWARNING);
        reservedKeys.put(Tokens.T_SQRT, SQRT);
        reservedKeys.put(Tokens.T_STACKED, STACKED);
        reservedKeys.put(Tokens.T_START, START);
        reservedKeys.put(Tokens.T_STATIC, STATIC);
        reservedKeys.put(Tokens.T_STDDEV_POP, STDDEV_POP);
        reservedKeys.put(Tokens.T_STDDEV_SAMP, STDDEV_SAMP);
        reservedKeys.put(Tokens.T_SUBMULTISET, SUBMULTISET);
        reservedKeys.put(Tokens.T_SUBSTRING, SUBSTRING);
        reservedKeys.put(Tokens.T_SUBSTRING_REGEX, SUBSTRING_REGEX);
        reservedKeys.put(Tokens.T_SUM, SUM);
        reservedKeys.put(Tokens.T_SYMMETRIC, SYMMETRIC);
        reservedKeys.put(Tokens.T_SYSTEM, SYSTEM);
        reservedKeys.put(Tokens.T_SYSTEM_USER, SYSTEM_USER);
        reservedKeys.put(Tokens.T_TABLE, TABLE);
        reservedKeys.put(Tokens.T_TABLESAMPLE, TABLESAMPLE);
        reservedKeys.put(Tokens.T_THEN, THEN);
        reservedKeys.put(Tokens.T_TIME, TIME);
        reservedKeys.put(Tokens.T_TIMESTAMP, TIMESTAMP);
        reservedKeys.put(Tokens.T_TIMEZONE_HOUR, TIMEZONE_HOUR);
        reservedKeys.put(Tokens.T_TIMEZONE_MINUTE, TIMEZONE_MINUTE);
        reservedKeys.put(Tokens.T_TO, TO);
        reservedKeys.put(Tokens.T_TRAILING, TRAILING);
        reservedKeys.put(Tokens.T_TRANSLATE, TRANSLATE);
        reservedKeys.put(Tokens.T_TRANSLATE_REGEX, TRANSLATE_REGEX);
        reservedKeys.put(Tokens.T_TRANSLATION, TRANSLATION);
        reservedKeys.put(Tokens.T_TREAT, TREAT);
        reservedKeys.put(Tokens.T_TRIGGER, TRIGGER);
        reservedKeys.put(Tokens.T_TRIM, TRIM);
        reservedKeys.put(Tokens.T_TRIM_ARRAY, TRIM_ARRAY);
        reservedKeys.put(Tokens.T_TRUE, TRUE);
        reservedKeys.put(Tokens.T_TRUNCATE, TRUNCATE);
        reservedKeys.put(Tokens.T_UESCAPE, UESCAPE);
        reservedKeys.put(Tokens.T_UNDO, UNDO);
        reservedKeys.put(Tokens.T_UNION, UNION);
        reservedKeys.put(Tokens.T_UNIQUE, UNIQUE);
        // A VoltDB extension to support the assume unique index attribute
        reservedKeys.put(Tokens.T_ASSUMEUNIQUE, ASSUMEUNIQUE);    // For VoltDB
        // End of VoltDB extension
        reservedKeys.put(Tokens.T_UNKNOWN, UNKNOWN);
        reservedKeys.put(Tokens.T_UNNEST, UNNEST);
        reservedKeys.put(Tokens.T_UNTIL, UNTIL);
        reservedKeys.put(Tokens.T_UPDATE, UPDATE);
        reservedKeys.put(Tokens.T_UPPER, UPPER);
        reservedKeys.put(Tokens.T_USER, USER);
        reservedKeys.put(Tokens.T_USING, USING);
        reservedKeys.put(Tokens.T_VALUE, VALUE);
        reservedKeys.put(Tokens.T_VALUES, VALUES);
        reservedKeys.put(Tokens.T_VAR_POP, VAR_POP);
        reservedKeys.put(Tokens.T_VAR_SAMP, VAR_SAMP);
        reservedKeys.put(Tokens.T_VARBINARY, VARBINARY);
        reservedKeys.put(Tokens.T_VARCHAR, VARCHAR);
        reservedKeys.put(Tokens.T_VARYING, VARYING);
        reservedKeys.put(Tokens.T_WHEN, WHEN);
        reservedKeys.put(Tokens.T_WHENEVER, WHENEVER);
        reservedKeys.put(Tokens.T_WHERE, WHERE);
        reservedKeys.put(Tokens.T_WIDTH_BUCKET, WIDTH_BUCKET);
        reservedKeys.put(Tokens.T_WINDOW, WINDOW);
        reservedKeys.put(Tokens.T_WITH, WITH);
        reservedKeys.put(Tokens.T_WITHIN, WITHIN);
        reservedKeys.put(Tokens.T_WITHOUT, WITHOUT);
        reservedKeys.put(Tokens.T_WHILE, WHILE);
        reservedKeys.put(Tokens.T_YEAR, YEAR);
        // A VoltDB extension to support WEEKOFYEAR and WEEKDAY function
        reservedKeys.put(Tokens.T_WEEKOFYEAR, WEEKOFYEAR);    // For compliant with MySQL
        reservedKeys.put(Tokens.T_WEEKDAY, WEEKDAY);          // For compliant with MySQL
        // End of VoltDB extension
    }

    private static final IntValueHashMap commandSet = new IntValueHashMap(251);

    static {
        commandSet.put(T_IF, Tokens.IF);
        commandSet.put(T_IFNULL, Tokens.IFNULL);
        commandSet.put(T_NVL, Tokens.IFNULL);
        commandSet.put(T_CASEWHEN, Tokens.CASEWHEN);

        //
        commandSet.put(T_ADD, ADD);
        commandSet.put(T_ADMIN, ADMIN);
        commandSet.put(T_ACTION, ACTION);
        commandSet.put(T_AFTER, AFTER);
        commandSet.put(T_ALIAS, ALIAS);
        commandSet.put(T_ALWAYS, ALWAYS);
        commandSet.put(T_ASC, ASC);
        commandSet.put(T_AUTOCOMMIT, AUTOCOMMIT);
        commandSet.put(T_BACKUP, BACKUP);
        commandSet.put(T_BEFORE, BEFORE);
        commandSet.put(T_BIT, BIT);
        commandSet.put(T_BLOCKING, BLOCKING);
        commandSet.put(T_CACHE, CACHE);
        commandSet.put(T_CACHED, CACHED);
        commandSet.put(T_CASCADE, CASCADE);
        commandSet.put(T_CATALOG, CATALOG);
        commandSet.put(T_CHARACTERISTICS, CHARACTERISTICS);
        commandSet.put(T_CHECKPOINT, CHECKPOINT);
        commandSet.put(T_COLLATE, COLLATE);
        commandSet.put(T_COLLATION, COLLATION);
        commandSet.put(T_COMMITTED, COMMITTED);
        commandSet.put(T_COMPACT, COMPACT);
        commandSet.put(T_COMPRESSED, COMPRESSED);
        commandSet.put(T_CONDITION_IDENTIFIER, Tokens.CONDITION_IDENTIFIER);
        commandSet.put(T_CONTAINS, CONTAINS);
        commandSet.put(T_CONTINUE, CONTINUE);
        commandSet.put(T_CONTROL, CONTROL);
        commandSet.put(T_CURDATE, CURDATE);
        commandSet.put(T_CURTIME, CURTIME);
        commandSet.put(T_DATA, DATA);
        commandSet.put(T_DATABASE, DATABASE);
        commandSet.put(T_DEFAULTS, DEFAULTS);
        commandSet.put(T_DEFRAG, DEFRAG);
        commandSet.put(T_DESC, DESC);
        commandSet.put(T_DOMAIN, DOMAIN);
        commandSet.put(T_EXCLUDING, EXCLUDING);
        commandSet.put(T_EXPLAIN, EXPLAIN);
        commandSet.put(T_EVENT, EVENT);
        commandSet.put(T_FILE, FILE);
        commandSet.put(T_FILES, FILES);
        commandSet.put(T_FINAL, FINAL);
        commandSet.put(T_FIRST, FIRST);
        commandSet.put(T_G_FACTOR, G);
        commandSet.put(T_GENERATED, GENERATED);
        commandSet.put(T_GRANTED, GRANTED);
        commandSet.put(T_HEADER, HEADER);
        commandSet.put(T_IGNORECASE, IGNORECASE);
        commandSet.put(T_IMMEDIATELY, IMMEDIATELY);
        commandSet.put(T_INCLUDING, INCLUDING);
        commandSet.put(T_INCREMENT, INCREMENT);
        commandSet.put(T_INDEX, INDEX);
        commandSet.put(T_INITIAL, INITIAL);
        commandSet.put(T_INPUT, INPUT);
        commandSet.put(T_INSTEAD, INSTEAD);
        commandSet.put(T_ISOLATION, ISOLATION);
        commandSet.put(T_ISAUTOCOMMIT, ISAUTOCOMMIT);
        commandSet.put(T_ISREADONLYDATABASE, ISREADONLYDATABASE);
        commandSet.put(T_ISREADONLYDATABASEFILES, ISREADONLYDATABASEFILES);
        commandSet.put(T_ISREADONLYSESSION, ISREADONLYSESSION);
        commandSet.put(T_JAVA, JAVA);
        commandSet.put(T_K_FACTOR, K);
        commandSet.put(T_KEY, KEY);
        commandSet.put(T_LAST, LAST);
        commandSet.put(T_LENGTH, LENGTH);
        commandSet.put(T_LEVEL, LEVEL);
        commandSet.put(T_LIMIT, LIMIT);
        commandSet.put(T_LOGSIZE, LOGSIZE);
        commandSet.put(T_LOCK, LOCK);
        commandSet.put(T_LOCKS, LOCKS);
        commandSet.put(T_M_FACTOR, M);
        commandSet.put(T_MATCHED, MATCHED);
        commandSet.put(T_MAXROWS, MAXROWS);
        commandSet.put(T_MAXVALUE, MAXVALUE);
        commandSet.put(T_MEMORY, MEMORY);
        // A VoltDB extension to support more units for timestamp functions
        commandSet.put(T_MICROS, MICROS);                // For VoltDB
        commandSet.put(T_MICROSECOND, MICROSECOND);      // For VoltDB
        // End of VoltDB extension
        commandSet.put(T_MILLIS, MILLIS);
        // A VoltDB extension to support more units for timestamp functions
        commandSet.put(T_MILLISECOND, MILLISECOND);      // For VoltDB
        // End of VoltDB extension
        commandSet.put(T_MINUS_EXCEPT, MINUS_EXCEPT);
        commandSet.put(T_MINVALUE, MINVALUE);
        commandSet.put(T_MVCC, MVCC);
        commandSet.put(T_NAME, NAME);
        commandSet.put(T_NEXT, NEXT);
        commandSet.put(T_NIO, NIO);
        commandSet.put(T_NOW, NOW);
        commandSet.put(T_NULLS, NULLS);
        commandSet.put(T_OFF, OFF);
        commandSet.put(T_OPTION, OPTION);
        commandSet.put(T_OVERRIDING, OVERRIDING);
        commandSet.put(T_P_FACTOR, P);
        commandSet.put(T_PARTIAL, PARTIAL);
        commandSet.put(T_PASSWORD, PASSWORD);
        commandSet.put(T_PLACING, PLACING);
        commandSet.put(T_PLAN, PLAN);
        commandSet.put(T_PRESERVE, PRESERVE);
        commandSet.put(T_PRIVILEGES, PRIVILEGES);
        commandSet.put(T_PROPERTY, PROPERTY);
        commandSet.put(T_READ, READ);
        commandSet.put(T_READONLY, READONLY);
        commandSet.put(T_REFERENTIAL_INTEGRITY, REFERENTIAL_INTEGRITY);
        commandSet.put(T_RENAME, RENAME);
        commandSet.put(T_REPEATABLE, REPEATABLE);
        commandSet.put(T_RESTART, RESTART);
        commandSet.put(T_RESTRICT, RESTRICT);
        commandSet.put(T_ROLE, ROLE);
        commandSet.put(T_SCHEMA, SCHEMA);
        commandSet.put(T_SCRIPT, SCRIPT);
        commandSet.put(T_SCRIPTFORMAT, SCRIPTFORMAT);
        commandSet.put(T_SEQUENCE, SEQUENCE);
        commandSet.put(T_SESSION, SESSION);
        commandSet.put(T_SERIALIZABLE, SERIALIZABLE);
        commandSet.put(T_SHUTDOWN, SHUTDOWN);
        commandSet.put(T_SIMPLE, SIMPLE);
        commandSet.put(T_SIZE, SIZE);
        commandSet.put(T_SOURCE, SOURCE);
        commandSet.put(T_SQL_BIGINT, SQL_BIGINT);
        commandSet.put(T_SQL_BINARY, SQL_BINARY);
        commandSet.put(T_SQL_BIT, SQL_BIT);
        commandSet.put(T_SQL_BLOB, SQL_BLOB);
        commandSet.put(T_SQL_BOOLEAN, SQL_BOOLEAN);
        commandSet.put(T_SQL_CHAR, SQL_CHAR);
        commandSet.put(T_SQL_CLOB, SQL_CLOB);
        commandSet.put(T_SQL_DATE, SQL_DATE);
        commandSet.put(T_SQL_DECIMAL, SQL_DECIMAL);
        commandSet.put(T_SQL_DATALINK, SQL_DATALINK);
        commandSet.put(T_SQL_DOUBLE, SQL_DOUBLE);
        commandSet.put(T_SQL_FLOAT, SQL_FLOAT);
        commandSet.put(T_SQL_INTEGER, SQL_INTEGER);
        commandSet.put(T_SQL_LONGVARBINARY, SQL_LONGVARBINARY);
        commandSet.put(T_SQL_LONGNVARCHAR, SQL_LONGNVARCHAR);
        commandSet.put(T_SQL_LONGVARCHAR, SQL_LONGVARCHAR);
        commandSet.put(T_SQL_NCHAR, SQL_NCHAR);
        commandSet.put(T_SQL_NCLOB, SQL_NCLOB);
        commandSet.put(T_SQL_NUMERIC, SQL_NUMERIC);
        commandSet.put(T_SQL_NVARCHAR, SQL_NVARCHAR);
        commandSet.put(T_SQL_REAL, SQL_REAL);
        commandSet.put(T_SQL_ROWID, SQL_ROWID);
        commandSet.put(T_SQL_SQLXML, SQL_SQLXML);
        commandSet.put(T_SQL_SMALLINT, SQL_SMALLINT);
        commandSet.put(T_SQL_TIME, SQL_TIME);
        commandSet.put(T_SQL_TIMESTAMP, SQL_TIMESTAMP);
        commandSet.put(T_SQL_TINYINT, SQL_TINYINT);
        commandSet.put(T_SQL_VARBINARY, SQL_VARBINARY);
        commandSet.put(T_SQL_VARCHAR, SQL_VARCHAR);
        commandSet.put(T_SQL_TSI_FRAC_SECOND, SQL_TSI_FRAC_SECOND);
        commandSet.put(T_SQL_TSI_SECOND, SQL_TSI_SECOND);
        commandSet.put(T_SQL_TSI_MINUTE, SQL_TSI_MINUTE);
        commandSet.put(T_SQL_TSI_HOUR, SQL_TSI_HOUR);
        commandSet.put(T_SQL_TSI_DAY, SQL_TSI_DAY);
        commandSet.put(T_SQL_TSI_WEEK, SQL_TSI_WEEK);
        commandSet.put(T_SQL_TSI_MONTH, SQL_TSI_MONTH);
        commandSet.put(T_SQL_TSI_QUARTER, SQL_TSI_QUARTER);
        commandSet.put(T_SQL_TSI_YEAR, SQL_TSI_YEAR);
        commandSet.put(T_STYLE, STYLE);
        commandSet.put(T_T_FACTOR, T);
        commandSet.put(T_TEMP, TEMP);
        commandSet.put(T_TEMPORARY, TEMPORARY);
        commandSet.put(T_TEXT, TEXT);
        commandSet.put(T_TIMESTAMPADD, TIMESTAMPADD);
        commandSet.put(T_TIMESTAMPDIFF, TIMESTAMPDIFF);
        commandSet.put(T_TO_CHAR, TO_CHAR);
        commandSet.put(T_TODAY, TODAY);
        commandSet.put(T_TOP, TOP);
        commandSet.put(T_TRANSACTION, TRANSACTION);
        commandSet.put(T_TYPE, TYPE);
        commandSet.put(T_UNCOMMITTED, UNCOMMITTED);
        commandSet.put(T_USAGE, USAGE);
        commandSet.put(T_VIEW, VIEW);
        commandSet.put(T_WRITE, WRITE);
        commandSet.put(T_WRITE_DELAY, WRITE_DELAY);
        commandSet.put(T_WORK, WORK);
        commandSet.put(T_ZONE, ZONE);

        //
        // A VoltDB extension to extract timestamp field function
        commandSet.put(T_DAYOFWEEK, DAYOFWEEK);
        commandSet.put(T_DAYOFYEAR, DAYOFYEAR);
        commandSet.put(T_WEEK, WEEK);
        commandSet.put(T_WEEKOFYEAR, WEEKOFYEAR);
        commandSet.put(T_WEEK_OF_YEAR, WEEK_OF_YEAR);
        commandSet.put(T_WEEKDAY, WEEKDAY);
        // End of VoltDB extension
        commandSet.put(T_DAY_NAME, DAY_NAME);
        commandSet.put(T_MONTH_NAME, MONTH_NAME);
        commandSet.put(T_QUARTER, QUARTER);
        commandSet.put(T_DAY_OF_WEEK, DAY_OF_WEEK);
        commandSet.put(T_DAY_OF_MONTH, DAY_OF_MONTH);
        commandSet.put(T_DAY_OF_YEAR, DAY_OF_YEAR);
        commandSet.put(T_DAYOFMONTH, DAYOFMONTH);
        commandSet.put(T_BITLENGTH, BITLENGTH);
        commandSet.put(T_OCTETLENGTH, OCTETLENGTH);
        commandSet.put(T_ACOS, ACOS);
        commandSet.put(T_ASIN, ASIN);
        commandSet.put(T_ATAN, ATAN);
        commandSet.put(T_ATAN2, ATAN2);
        commandSet.put(T_COS, COS);
        commandSet.put(T_COT, COT);
        commandSet.put(T_DEGREES, DEGREES);
        commandSet.put(T_DMOD, DMOD);
        commandSet.put(T_LOG, LOG);
        commandSet.put(T_LOG10, LOG10);
        commandSet.put(T_PI, PI);
        commandSet.put(T_RADIANS, RADIANS);
        commandSet.put(T_RAND, RAND);
        commandSet.put(T_ROUND, ROUND);
        commandSet.put(T_SIGN, SIGN);
        commandSet.put(T_SIN, SIN);
        commandSet.put(T_TAN, TAN);
        commandSet.put(T_BITAND, BITAND);
        commandSet.put(T_BITOR, BITOR);
        commandSet.put(T_BITXOR, BITXOR);
        commandSet.put(T_ROUNDMAGIC, ROUNDMAGIC);
        commandSet.put(T_ASCII, ASCII);
        commandSet.put(T_CONCAT_WORD, CONCAT_WORD);
        commandSet.put(T_DIFFERENCE, DIFFERENCE);
        commandSet.put(T_HEXTORAW, HEXTORAW);
        commandSet.put(T_LCASE, LCASE);
        commandSet.put(T_LOCATE, LOCATE);
        commandSet.put(T_LTRIM, LTRIM);
        commandSet.put(T_RAWTOHEX, RAWTOHEX);
        commandSet.put(T_REPLACE, REPLACE);
        commandSet.put(T_RTRIM, RTRIM);
        commandSet.put(T_SOUNDEX, SOUNDEX);
        commandSet.put(T_SPACE_WORD, SPACE_WORD);
        commandSet.put(T_SUBSTR, SUBSTR);
        commandSet.put(T_UCASE, UCASE);
        commandSet.put(T_DATEDIFF, DATEDIFF);
        commandSet.put(T_SECONDS_MIDNIGHT, SECONDS_MIDNIGHT);

        //
        commandSet.put(T_COLON, Tokens.COLON);
        commandSet.put(T_COMMA, Tokens.COMMA);
        commandSet.put(T_SEMICOLON, SEMICOLON);
        commandSet.put(T_EQUALS, Tokens.EQUALS);
        commandSet.put(T_NOT_EQUALS_ALT, Tokens.NOT_EQUALS);
        commandSet.put(T_NOT_EQUALS, Tokens.NOT_EQUALS);
        commandSet.put(T_LESS, Tokens.LESS);
        commandSet.put(T_GREATER, Tokens.GREATER);
        commandSet.put(T_LESS_EQUALS, Tokens.LESS_EQUALS);
        commandSet.put(T_GREATER_EQUALS, Tokens.GREATER_EQUALS);
        commandSet.put(T_PLUS, Tokens.PLUS);
        commandSet.put(T_MINUS, Tokens.MINUS);
        commandSet.put(T_ASTERISK, Tokens.ASTERISK);
        commandSet.put(T_DIVIDE, Tokens.DIVIDE);
        commandSet.put(T_CONCAT, Tokens.CONCAT);
        commandSet.put(T_QUESTION, Tokens.QUESTION);
        commandSet.put(T_OPENBRACKET, OPENBRACKET);
        commandSet.put(T_CLOSEBRACKET, CLOSEBRACKET);
    }

    static int get(String token) {

        int type = reservedKeys.get(token, -1);

        if (type == -1) {
            return commandSet.get(token, -1);
        }

        return type;
    }

    public static boolean isCoreKeyword(int token) {
        return coreReservedWords.contains(token);
    }

    public static boolean isKeyword(String token) {
        return reservedKeys.containsKey(token);
    }

    public static int getKeywordID(String token, int defaultValue) {
        return reservedKeys.get(token, defaultValue);
    }

    public static int getNonKeywordID(String token, int defaultValue) {
        return commandSet.get(token, defaultValue);
    }

    public static String getKeyword(int token) {

        String key = (String) reservedKeys.getKey(token);

        if (key != null) {
            return key;
        }

        key = (String) commandSet.getKey(token);

        return key;
    }

    private static final OrderedIntHashSet coreReservedWords;

    static {

        // minimal set of identifier not allowed as table / column / alias names
        // these are in effect interpreted as reserved words used by HSQLDB
        coreReservedWords = new OrderedIntHashSet(128);

        short[] keyword = {
            ADMIN, AS, AND, ALL, ANY, AT, AVG, BY, BETWEEN, BOTH, CALL, CASE,
            CAST, CORRESPONDING, CONVERT, COUNT, COALESCE, CREATE, CROSS,
            DISTINCT, DROP, ELSE, END, EVERY, EXISTS, EXCEPT, FOR, FROM, FULL,
            GRANT, GROUP, HAVING, INTO, IS, IN, INTERSECT, JOIN, INNER, LEFT,
            LEADING, LIKE, MAX, MIN, NATURAL, NULLIF, NOT, ON, ORDER, OR,
            OUTER, PRIMARY, REFERENCES, RIGHT, SELECT, SET, SOME, STDDEV_POP,
            STDDEV_SAMP, SUM, TABLE, THEN, TO, TRAILING, TRIGGER, UNION,
            UNIQUE, USING, VALUES, VAR_POP, VAR_SAMP, WHEN, WHERE, WITH,
            // A VoltDB extension to support the assume unique index attribute.
            ASSUMEUNIQUE, // For VoltDB
            // End of VoltDB extension
        };

        for (int i = 0; i < keyword.length; i++) {
            coreReservedWords.add(keyword[i]);
        }
    }

    public static final short[] SQL_INTERVAL_FIELD_CODES = new short[] {
        Tokens.YEAR, Tokens.MONTH, Tokens.DAY, Tokens.HOUR, Tokens.MINUTE,
        Tokens.SECOND
    };
    public static final String[] SQL_INTERVAL_FIELD_NAMES = new String[] {
        Tokens.T_YEAR, Tokens.T_MONTH, Tokens.T_DAY, Tokens.T_HOUR,
        Tokens.T_MINUTE, Tokens.T_SECOND
    };
}
