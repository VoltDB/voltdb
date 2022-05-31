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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;

/**
 * A wrapper around a PostgreSQL database server, and JDBC connection. This
 * class can be used to execute SQL statements instead of the C++ ExecutionEngine.
 * It is currently used only by the SQL Coverage tests (and perhaps, someday,
 * the JUnit regressionsuite tests).
 */
public class PostgreSQLBackend extends NonVoltDBBackend {
    /** java.util.logging logger. */
    @SuppressWarnings("unused")
    private static final VoltLogger log = new VoltLogger(PostgreSQLBackend.class.getName());

    protected static final String m_default_username = "test";
    protected static final String m_default_password = "test";
    protected static final String m_permanent_database_name = "postgres";
    protected static final String m_database_name = "sqlcoveragetest";
    protected static PostgreSQLBackend m_permanent_db_backend = null;
    // PostgreSQL column type names that are not found in VoltDB or HSQL,
    // mapped to their VoltDB/HSQL equivalents
    private static final Map<String,String> m_PostgreSQLTypeNames;
    static {
        m_PostgreSQLTypeNames = new HashMap<String,String>();
        m_PostgreSQLTypeNames.put("int2", "SMALLINT");
        m_PostgreSQLTypeNames.put("int4", "INTEGER");
        m_PostgreSQLTypeNames.put("int8", "BIGINT");
        m_PostgreSQLTypeNames.put("float8", "FLOAT");
        m_PostgreSQLTypeNames.put("numeric", "DECIMAL");
        m_PostgreSQLTypeNames.put("timestamptz", "TIMESTAMP");
        m_PostgreSQLTypeNames.put("bytea", "VARBINARY");
        m_PostgreSQLTypeNames.put("varbit", "VARBINARY");
        m_PostgreSQLTypeNames.put("char", "VARCHAR");
        m_PostgreSQLTypeNames.put("text", "VARCHAR");
        m_PostgreSQLTypeNames.put("unknown", "VARCHAR");  // this usually means a quoted string, e.g. 'abc'
        m_PostgreSQLTypeNames.put("geography", "GEOGRAPHY");
        // NOTE: what VoltDB calls "GEOGRAPHY_POINT" would also be called
        // "geography" by PostgreSQL, so this mapping is imperfect; however,
        // so far this has not been a problem, since the only query transformer
        // that specifies those two column types treats them the same
    }

    // Used below (twice), for an UPSERT INTO SELECT statement
    private static final String COLUMN_DEFN = "(\\w*\\s*\\(\\s*)*(\\w+\\.)?\\w"
            + "+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s*(\\+|\\-|\\*|\\/|\\|\\|)"
            + "\\s*(\\w|')+)*(\\s+(ASC|DESC))?";

    // Captures the use of ORDER BY, with up to 6 order-by columns; beyond
    // those will be ignored (similar to
    // voltdb/tests/scripts/examples/sql_coverage/StandardNormalizer.py)
    private static final Pattern orderByQuery = Pattern.compile(
            "ORDER BY\\s+(?<column1>"+COLUMN_DEFN+")"
            + "((?<column2>\\s*,\\s*"+COLUMN_DEFN+"))?"
            + "((?<column3>\\s*,\\s*"+COLUMN_DEFN+"))?"
            + "((?<column4>\\s*,\\s*"+COLUMN_DEFN+"))?"
            + "((?<column5>\\s*,\\s*"+COLUMN_DEFN+"))?"
            + "((?<column6>\\s*,\\s*"+COLUMN_DEFN+"))?",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an ORDER BY clause, by adding (for each
    // order-by column) either NULLS FIRST or (after "DESC") NULL LAST, so
    // that PostgreSQL results will match VoltDB results
    private static final QueryTransformer orderByQueryTransformer
            = new QueryTransformer(orderByQuery)
            .initialText("ORDER BY ").suffix(" NULLS FIRST")
            .alternateSuffix("DESC", " NULLS LAST")
            .groups("column1", "column2", "column3", "column4", "column5", "column6");

    // Regex patterns for a typical column, constant, or column expression
    // (including functions, operators, etc.); used, e.g., for modifying an AVG
    // query or a division (col1 / col2) query.
    // Note 1: WHERE and SELECT are specifically forbidden since they are not
    // function names, but they sometimes occur before parentheses; parentheses
    // without an actual function name do match here.
    // Note 2: an AS or FROM can occur here (rarely) if certain functions occur
    // within an otherwise matching expression, e.g., AVG(CAST(VCHAR AS INTEGER))
    // or AVG(EXTRACT(DAY FROM PAST))
    private static final String COLUMN_NAME   = "(\\w+\\.)?(?<column1>\\w+)";
    private static final String FUNCTION_NAME = "(?!WHERE|SELECT)((\\b\\w+\\s*)?\\(\\s*)*";
    private static final String FUNCTION_END  = "(\\s+(AS|FROM)\\s+\\w+\\s*\\))?(\\s*\\))*";
    private static final String ARITHMETIC_OP = "\\s*(\\+|\\-|\\*|\\/)\\s*";
    private static final String SIMPLE_COLUMN_EXPRESSION = FUNCTION_NAME + COLUMN_NAME + FUNCTION_END;
    private static final String COLUMN_EXPRESSION_PATTERN = SIMPLE_COLUMN_EXPRESSION
            + "(" + ARITHMETIC_OP + SIMPLE_COLUMN_EXPRESSION.replace("column1", "column2")
            + ")*";

    // Captures the use of AVG(columnExpression), which PostgreSQL handles
    // differently, when the columnExpression is of one of the integer types
    private static final Pattern avgQuery = Pattern.compile(
            "AVG\\s*\\(\\s*"+COLUMN_EXPRESSION_PATTERN+"\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an AVG(columnExpression) function, where
    // <i>columnName</i> is of an integer type, for which PostgreSQL returns
    // a numeric (non-integer) value, unlike VoltDB, which returns an integer;
    // so change it to: TRUNC ( AVG(columnExpression) )
    private static final QueryTransformer avgQueryTransformer
            = new QueryTransformer(avgQuery)
            .prefix("TRUNC ( ").suffix(" )").groups("column1", "column2")
            .useWholeMatch().columnType(ColumnType.INTEGER)
            .allColumnsShouldMatchType();

    // Constants used in the two QueryTransformer's below,
    // divisionQueryTransformer & bigintDivisionQueryTransformer
    private static final String DIVISION_QUERY_TRANSFORMER_PREFIX = "TRUNC( ";
    private static final String DIVISION_QUERY_TRANSFORMER_SUFFIX = " )";
    private static final String BIGINT_DIV_QUERY_TRANSFORMER_PREFIX = "CAST(TRUNC (";
    private static final String BIGINT_DIV_QUERY_TRANSFORMER_SUFFIX = ", 12) as BIGINT)";

    // Captures the use of expression1 / expression2, which PostgreSQL handles
    // differently, when the expressions are of one of the integer types
    private static final Pattern divisionQuery = Pattern.compile(
            SIMPLE_COLUMN_EXPRESSION + "\\s*\\/\\s*"
            + SIMPLE_COLUMN_EXPRESSION.replace("column1", "column2"),
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing expression1 / expression2, where the
    // expressions are of an integer type, for which PostgreSQL returns a
    // numeric (non-integer) value, unlike VoltDB, which returns an integer;
    // so change it to: TRUNC( expression1 / expression2 ).
    private static final QueryTransformer divisionQueryTransformer
            = new QueryTransformer(divisionQuery)
            .prefix(DIVISION_QUERY_TRANSFORMER_PREFIX)
            .suffix(DIVISION_QUERY_TRANSFORMER_SUFFIX)
            .exclude(BIGINT_DIV_QUERY_TRANSFORMER_PREFIX)
            .groups("column1", "column2").useWholeMatch()
            .columnType(ColumnType.INTEGER).allColumnsShouldMatchType();
    // Also modifies a query containing expression1 / expression2, but in this
    // case at least one of the expressions is a BIGINT, which can complicate
    // matters (for very large values), so here we change it to:
    // CAST(TRUNC (expression1 / expression2, 12) as BIGINT)
    // Note 1: the value of the second argument to TRUNC (', 12') does not
    // appear to matter, so I chose 12 to suggest 12 decimal places, which is
    // the default in sqlcoverage.
    // Note 2: this needs to be called before divisionQueryTransformer (and is,
    // in transformDML below), since the latter will exclude making changes
    // where this QueryTransformer has already made them.
    private static final QueryTransformer bigintDivisionQueryTransformer
            = new QueryTransformer(divisionQuery)
            .prefix(BIGINT_DIV_QUERY_TRANSFORMER_PREFIX)
            .suffix(BIGINT_DIV_QUERY_TRANSFORMER_SUFFIX)
            .groups("column1", "column2").useWholeMatch()
            .columnType(ColumnType.BIGINT);

    // Captures the use of CEILING(columnName) or FLOOR(columnName)
    private static final Pattern ceilingOrFloorQuery = Pattern.compile(
            "(CEILING|FLOOR)\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a CEILING(columnName) or FLOOR(columnName)
    // function, where <i>columnName</i> is of an integer type, for which
    // PostgreSQL returns a numeric (non-integer) value, unlike VoltDB, which
    // returns an integer; so change it to:
    // CAST ( CEILING(columnName) as BIGINT ), or
    // CAST ( FLOOR(columnName) as BIGINT ), respectively.
    private static final QueryTransformer ceilingOrFloorQueryTransformer
            = new QueryTransformer(ceilingOrFloorQuery)
            .prefix("CAST( ").suffix(" as BIGINT)").groups("column")
            .useWholeMatch().columnType(ColumnType.INTEGER);

    // Captures the use of CURRENT_TIMESTAMP(), with parentheses
    private static final Pattern currentTimestampQuery = Pattern.compile(
            "CURRENT_TIMESTAMP\\s*\\(\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing CURRENT_TIMESTAMP(), with parentheses,
    // which PostgreSQL does not support, and simply replaces it with
    // CURRENT_TIMESTAMP, without parentheses, which it does support.
    private static final QueryTransformer currentTimestampQueryTransformer
            = new QueryTransformer(currentTimestampQuery)
            .replacementText("CURRENT_TIMESTAMP").useWholeMatch();

    // Captures the use of NOW, without parentheses
    private static final Pattern nowQuery = Pattern.compile(
            "\\b(?<now>NOW)\\b(?!\\s*\\()", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing NOW, without parentheses, which PostgreSQL
    // does not support, and simply replaces it with NOW(), with parentheses,
    // which it does support.
    private static final QueryTransformer nowQueryTransformer
            = new QueryTransformer(nowQuery)
            .suffix("()").groups("now");

    // Captures the use of the SEC (secant) function
    private static final Pattern secantQuery = Pattern.compile(
            "SEC\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing SEC(...), which PostgreSQL does not support,
    // and replaces it with 1/COS(...), which is an equivalent that PostgreSQL
    // does support.
    // Note: this needs to be called after divisionQueryTransformer (and is,
    // in transformDML below), since the latter might try to further transform
    // the changes made here, potentially causing problems.
    private static final QueryTransformer secantQueryTransformer
            = new QueryTransformer(secantQuery)
            .replacementText("1/COS(").useWholeMatch();

    // Captures the use of the CSC (cosecant) function
    private static final Pattern cosecantQuery = Pattern.compile(
            "CSC\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing CSC(...), which PostgreSQL does not support,
    // and replaces it with 1/SIN(...), which is an equivalent that PostgreSQL
    // does support.
    // Note: this needs to be called after divisionQueryTransformer (and is,
    // in transformDML below), since the latter might try to further transform
    // the changes made here, potentially causing problems.
    private static final QueryTransformer cosecantQueryTransformer
            = new QueryTransformer(cosecantQuery)
            .replacementText("1/SIN(").useWholeMatch();

    // Captures the use of the LOG function
    private static final Pattern logQuery = Pattern.compile(
            "LOG\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing LOG(...), which PostgreSQL interprets to
    // mean LOG base 10 (whereas VoltDB interprets it to mean LOG base e),
    // and replaces it with LN(...), which PostgreSQL interprets to mean
    // LOG base e, as intended.
    // Note: this needs to be called before log10QueryTransformer (and is,
    // in transformDML below), since otherwise "LOG10" would get transformed
    // twice, to "LOG" and then to "LN", which would not be equivalent.
    private static final QueryTransformer logQueryTransformer
            = new QueryTransformer(logQuery)
            .replacementText("LN(").useWholeMatch();

    // Captures the use of the LOG10 function
    private static final Pattern log10Query = Pattern.compile(
            "LOG10\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing LOG10(...), which PostgreSQL does not
    // support, and replaces it with LOG(...), which is an equivalent that
    // PostgreSQL does support, and interprets to mean LOG base 10 (whereas
    // VoltDB interprets it to mean LOG base e).
    // Note: this needs to be called after logQueryTransformer (and is, in
    // transformDML below), since otherwise "LOG10" would get transformed
    // twice, to "LOG" and then to "LN", which would not be equivalent.
    private static final QueryTransformer log10QueryTransformer
            = new QueryTransformer(log10Query)
            .replacementText("LOG(").useWholeMatch();

    // Captures the use of the SECOND function
    private static final Pattern secondQuery = Pattern.compile(
            "SECOND\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing SECOND(time), where "time" is a timestamp
    // value, column, or expression, which PostgreSQL does not support, and
    // replaces it with EXTRACT(SECOND FROM time), which is an equivalent that
    // PostgreSQL does support.
    private static final QueryTransformer secondQueryTransformer
            = new QueryTransformer(secondQuery)
            .replacementText("EXTRACT(SECOND FROM ").useWholeMatch();

    // Captures the use of the WEEKDAY function
    private static final Pattern weekdayQuery = Pattern.compile(
            "WEEKDAY\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing WEEKDAY(time), where "time" is a timestamp
    // value, column, or expression, which PostgreSQL does not support, and
    // replaces it with EXTRACT(DAY_OF_WEEK FROM time), which PostgreSQL also
    // does not support, but that gets handled by the dayOfWeekQueryTransformer
    // below.
    // Note: this needs to be called before dayOfWeekQueryTransformer (and is,
    // in transformDML below), since this actually does only half the work.
    private static final QueryTransformer weekdayQueryTransformer
            = new QueryTransformer(weekdayQuery)
            .replacementText("EXTRACT(DAY_OF_WEEK FROM ").useWholeMatch();

    // Captures the use of EXTRACT(DAY_OF_WEEK FROM ...)
    private static final Pattern dayOfWeekQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_WEEK\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an EXTRACT(DAY_OF_WEEK FROM ...)
    // function, which PostgreSQL does not support, and replaces it with
    // EXTRACT(DOW FROM ...)+1, which is an equivalent that PostgreSQL does
    // support. (The '+1' is because PostgreSQL counts Sunday as 0 and Saturday
    // as 6, etc., whereas VoltDB counts Sunday as 1 and Saturday as 7, etc.)
    private static final QueryTransformer dayOfWeekQueryTransformer
            = new QueryTransformer(dayOfWeekQuery)
            .initialText("EXTRACT ( ").prefix("DOW FROM").suffix(")+1").groups("column");

    // Captures the use of EXTRACT(DAY_OF_YEAR FROM ...)
    private static final Pattern dayOfYearQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_YEAR\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an EXTRACT(DAY_OF_YEAR FROM ...)
    // function, which PostgreSQL does not support, and replaces it with
    // EXTRACT(DOY FROM ...), which is an equivalent that PostgreSQL does
    // support
    private static final QueryTransformer dayOfYearQueryTransformer
            = new QueryTransformer(dayOfYearQuery)
            .initialText("EXTRACT ( ").prefix("DOY FROM").suffix(")").groups("column");

    // Captures the use of the SPACE function
    private static final Pattern spaceQuery = Pattern.compile(
            "SPACE\\s*\\(", Pattern.CASE_INSENSITIVE);
    // Modifies a query containing SPACE(int), where "int" is an integer value,
    // column, or expression, which PostgreSQL does not support, and
    // replaces it with REPEAT(' ', int), which is an equivalent that
    // PostgreSQL does support.
    private static final QueryTransformer spaceQueryTransformer
            = new QueryTransformer(spaceQuery)
            .replacementText("REPEAT(' ', ").useWholeMatch();

    // Captures the use of the STARTS WITH operator, including on a string
    // (VARCHAR) constant containing up to 4 percent (%) signs
    private static final Pattern startsWithQuery = Pattern.compile(
            "(?<startswith>STARTS\\s+WITH)\\s+"
            + "(?<string>'[^']*'|" + COLUMN_EXPRESSION_PATTERN + ")",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing 'STARTS WITH FOO' (where FOO may be any
    // string in single-quotes or a column or column expression), which
    // PostgreSQL does not support, and replaces it with:
    //     LIKE REPLACE(REPLACE(FOO, '%', '\%'), '_', '\_') || '%',
    // which is an equivalent that PostgreSQL does support; the two calls to
    // PostgreSQL's REPLACE function are to escape any '%' or '_' characters,
    // since PostgreSQL's LIKE operator treats these characters specially, but
    // VoltDB's STARTS WITH does not.
    private static final QueryTransformer startsWithQueryTransformer
            = new QueryTransformer(startsWithQuery)
            .groups("startswith", "string")
            .groupReplacementText("LIKE",
                    "REPLACE(REPLACE({string}, '%', '\\%'), '_', '\\_') || '%'")
            .useWholeMatch();

    // Used in both versions, below, of an UPSERT statement: an
    // UPSERT INTO VALUES or an UPSERT INTO SELECT
    private static final String UPSERT_QUERY_START = "(?<upsert>UPSERT)\\s+INTO\\s+(?<table>\\w+)\\s+"
            + "(\\(\\s*(?<columns>\\w+\\s*(,\\s*\\w+\\s*)*)\\)\\s+)?";
    // Used below (twice), for an UPSERT INTO SELECT statement
    private static final String SORT_KEYWORDS = "GROUP\\s+BY|HAVING|ORDER\\s+BY|LIMIT|OFFSET";

    // Captures the use of an UPSERT INTO VALUES statement, for example:
    //     UPSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34)
    // where the column list, here "(C1, C2, C3)", is optional; the values
    // list, here "(1, 'abc', 12.34)", can include arbitrary values; and both
    // can include any number of items. (Though, for a valid UPSERT, the number
    // of values must match the number of columns, when included, or else the
    // number of columns defined in table T1; and the types must also match.)
    private static final Pattern upsertValuesQuery = Pattern.compile(
            UPSERT_QUERY_START + "VALUES\\s+\\(\\s*(?<values>.+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies an UPSERT INTO VALUES statement, as described above, such as:
    //     UPSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34)
    // which PostgreSQL does not support, and replaces it with an INSERT
    // statement using ON CONFLICT DO UPDATE, such as:
    //     INSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34) ON CONFLICT (C1)
    //         DO UPDATE SET (C2, C3) = ('abc', 12.34)
    // which is an equivalent that PostgreSQL does support. (This example
    // assumes that the C1 column is the primary key.)
    private static final QueryTransformer upsertValuesQueryTransformer
            = new QueryTransformer(upsertValuesQuery)
            .groups("upsert", "table", "columns", "values")
            .groupReplacementText("INSERT").useWholeMatch()
            .suffix(" ON CONFLICT ({columns:pk}) DO UPDATE SET ({columns:npk}) = ({values:npk})");

    // Captures the use of an UPSERT INTO SELECT statement, for example:
    //     UPSERT INTO T1 (C1, C2, C3) SELECT (C4, C5, C6) FROM T2
    // where the initial column list, here "(C1, C2, C3)", is optional; the
    // second column list, here "(C4, C5, C6)", can include arbitrary column
    // expressions; and both can include any number of items. (Though, for a
    // valid UPSERT, the number of column expressions must match the number
    // of columns, when included, or else the number of columns defined in
    // table T1; and the types must also match.) Also, the SELECT portion may
    // contain additional clauses, such as WHERE and ORDER BY clauses.
    private static final Pattern upsertSelectQuery = Pattern.compile(
            UPSERT_QUERY_START + "SELECT\\s+(?<values>[+\\-*\\/%|'\\s\\w]+(,\\s*[+\\-*\\/%|'\\s\\w]+)*)(?<!DISTINCT)\\s+"
                    + "FROM\\s+(?<selecttables>\\w+(\\s+AS\\s+\\w+)?((\\s*,\\s*|\\s+JOIN\\s+)\\w+(\\s+AS\\s+\\w+)?)*)\\s+"
                    + "(?<where>WHERE\\s+((?!"+SORT_KEYWORDS+").)+)?"
                    + "(?<sort>("+SORT_KEYWORDS+").+)?",
            Pattern.CASE_INSENSITIVE);
    // Modifies an UPSERT INTO SELECT statement, as described above, such as:
    //     UPSERT INTO T1 (C1, C2, C3) SELECT (C4, C5, C6) FROM T2
    // which PostgreSQL does not support, and replaces it with an INSERT
    // statement using ON CONFLICT DO UPDATE, such as:
    //     INSERT INTO T1 AS _TMP (C1, C2, C3) SELECT (C4, C5, C6) FROM T2 ON CONFLICT (C1)
    //         DO UPDATE SET (C2, C3) = (SELECT C5, C6 FROM T2 WHERE C4=_TMP.C1)
    // which is an equivalent that PostgreSQL does support. (This example
    // assumes that the C1 and C4 columns are the primary keys.)
    private static final QueryTransformer upsertSelectQueryTransformer
            = new QueryTransformer(upsertSelectQuery)
            .groups("upsert", "table", "columns", "values", "selecttables", "where", "sort")
            .groupReplacementText("INSERT", "{table} AS _TMP").useWholeMatch()
            .suffix(" ON CONFLICT ({columns:pk}) DO UPDATE SET ({columns:npk}) = "
                    + "(SELECT {values:npk} FROM {selecttables} {where:pk} {sort})");

    // Captures the use of string concatenation using a plus sign (+), e.g.:
    //     'str' + VCHAR
    // or, in the reverse order:
    //     VCHAR + 'str'
    // Note that this would not capture the concatenation of two VARCHAR
    // columns using a plus sign, e.g. VCHAR1 + VCHAR2, but there is no simple
    // way (without querying meta-data) to distinguish that from addition of
    // two numeric columns, which we would not want to change; so far, this
    // has not been a problem.
    private static final Pattern stringConcatQuery = Pattern.compile(
            "'\\w+'\\s*(?<plus>\\+)|(?<plus2>\\+)\\s*'\\w+'",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing 'FOO' + ..., which PostgreSQL does not
    // support, and replaces it with 'FOO' || ..., which is an equivalent
    // that PostgreSQL does support
    private static final QueryTransformer stringConcatQueryTransformer
            = new QueryTransformer(stringConcatQuery)
            .replacementText("||").useWholeMatch().groups("plus", "plus2");

    // Captures the use of a VARBINARY constant, e.g. x'12AF'
    private static final Pattern varbinaryConstant = Pattern.compile(
            "x'(?<bytes>[0-9A-Fa-f]+)'",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a VARBINARY constant, e.g. x'12AF', which
    // PostgreSQL does not support in that format, and replaces it with a
    // VARBINARY constant in the format it does support, e.g. E'\\x12AF'
    // (with extra backslashes, for escaping)
    private static final QueryTransformer varbinaryConstantTransformer
            = new QueryTransformer(varbinaryConstant)
            .prefix("E'\\\\x").suffix("'").groups("bytes");

    // Captures the use of CAST(FOO AS VARCHAR)
    private static final Pattern castVarchar = Pattern.compile(
            "CAST\\s*\\(\\s*(?<column>\\w*)\\s+AS\\s+VARCHAR\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a SQL statement containing CAST(FOO AS VARCHAR), where FOO is
    // a VARCHAR column of some size, which PostgreSQL treats differently from
    // VoltDB, and replaces it with CAST(FOO AS TEXT), which works better,
    // particularly in the context of a Recursive CTE statement
    private static final QueryTransformer castVarcharTransformer
            = new QueryTransformer(castVarchar)
            .prefix("CAST(").suffix(" AS TEXT)").groups("column")
            .columnType(ColumnType.VARCHAR);


    // Captures the use of DROP TABLE T1 IF EXISTS (in DDL)
    private static final Pattern dropTableIfExistsDdl = Pattern.compile(
            "DROP\\s+TABLE\\s+(?<table>\\w+)\\s+IF\\s+EXISTS",
            Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing DROP TABLE T1 IF EXISTS,
    // which PostgreSQL does not support, and replaces it with
    // DROP TABLE IF EXISTS T1, which it does support
    private static final QueryTransformer dropTableIfExistsDdlTransformer
            = new QueryTransformer(dropTableIfExistsDdl)
            .prefix("DROP TABLE IF EXISTS ").groups("table");

    // Captures the use of VARCHAR(n BYTES) (in DDL)
    private static final Pattern varcharBytesDdl = Pattern.compile(
            "VARCHAR\\s*\\(\\s*(?<numBytes>\\w+)\\s+BYTES\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing VARCHAR(n BYTES), which PostgreSQL
    // does not support, and replaces it with VARCHAR(m), where m = n / 4
    // (but m is always at least 14, since many SQLCoverage tests use strings
    // of that length), which it does support
    private static final QueryTransformer varcharBytesDdlTransformer
            = new QueryTransformer(varcharBytesDdl)
            .prefix("VARCHAR(").suffix(")").multiplier(0.50).minimum(14)
            .groups("numBytes");

    // Captures the use of VARBINARY(n); however, this does not capture the use
    // of just VARBINARY, without a number of bytes in parentheses, although
    // VoltDB supports that syntax, because that would also capture some DDL
    // that should not be changed, such as table names R_VARBINARY_TABLE and
    // P_VARBINARY_TABLE
    private static final Pattern varbinaryDdl = Pattern.compile(
            "VARBINARY\\s*\\(\\s*\\d+\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing VARBINARY(n), which PostgreSQL does
    // not support, and replaces it with BYTEA, which it does support
    private static final QueryTransformer varbinaryDdlTransformer
            = new QueryTransformer(varbinaryDdl)
            .replacementText("BYTEA").useWholeMatch();

    // Captures the use of TINYINT (in DDL)
    private static final Pattern tinyintDdl = Pattern.compile(
            "TINYINT", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing TINYINT, which PostgreSQL does not
    // support, and replaces it with SMALLINT, which is an equivalent that
    // PostgreSQL does support
    private static final QueryTransformer tinyintDdlTransformer
            = new QueryTransformer(tinyintDdl)
            .replacementText("SMALLINT").useWholeMatch();

    // Captures the use of ASSUMEUNIQUE (in DDL)
    private static final Pattern assumeUniqueDdl = Pattern.compile(
            "ASSUMEUNIQUE", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing ASSUMEUNIQUE, which PostgreSQL does
    // not support, and replaces it with UNIQUE, which is an equivalent that
    // PostgreSQL does support
    private static final QueryTransformer assumeUniqueDdlTransformer
            = new QueryTransformer(assumeUniqueDdl)
            .replacementText("UNIQUE").useWholeMatch();


    static public PostgreSQLBackend initializePostgreSQLBackend(CatalogContext context)
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
                    m_backend = new PostgreSQLBackend(m_database_name);
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
                hostLog.fatal("Unable to construct PostgreSQL backend");
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }
            return (PostgreSQLBackend) m_backend;
        }
    }

    /** Constructor specifying a (PostgreSQL) 'database', username and password. */
    public PostgreSQLBackend(String databaseName, String username, String password) {
        super("PostgreSQL", "org.postgresql.Driver",
                "jdbc:postgresql:" + databaseName,
                username, password);
    }

    /** Constructor specifying a username and password, with default (PostgreSQL) 'database'. */
    public PostgreSQLBackend(String username, String password) {
        this(m_permanent_database_name, username, password);
    }

    /** Constructor specifying a (PostgreSQL) 'database', with default username and password. */
    public PostgreSQLBackend(String databaseName) {
        this(databaseName, m_default_username, m_default_password);
    }

    /** Constructor using the default (PostgreSQL) 'database', username, and password. */
    public PostgreSQLBackend() {
        this(m_permanent_database_name);
    }

    /** Creates a new PostgreSQLBackend wrapping dbconn. This is (was?) used for testing only. */
    protected PostgreSQLBackend(Connection dbconn) {
        super(dbconn);
    }

    /** For a SQL DDL statement, replace (VoltDB) keywords not supported by
     *  PostgreSQL with other, similar terms. */
    public String transformDDL(String ddl) {
        return transformQuery(ddl, dropTableIfExistsDdlTransformer,
                varcharBytesDdlTransformer, varbinaryDdlTransformer,
                tinyintDdlTransformer, assumeUniqueDdlTransformer);
    }

    /** For a SQL query, replace (VoltDB) keywords not supported by PostgreSQL,
     *  or which behave differently in PostgreSQL than in VoltDB, with other,
     *  similar terms, so that the results will match. */
    public String transformDML(String dml) {
        return transformQuery(dml, orderByQueryTransformer,
                avgQueryTransformer, bigintDivisionQueryTransformer,
                divisionQueryTransformer, ceilingOrFloorQueryTransformer,
                stringConcatQueryTransformer, varbinaryConstantTransformer,
                currentTimestampQueryTransformer, nowQueryTransformer,
                secantQueryTransformer, cosecantQueryTransformer,
                logQueryTransformer, log10QueryTransformer,
                secondQueryTransformer, weekdayQueryTransformer,
                dayOfWeekQueryTransformer, dayOfYearQueryTransformer,
                spaceQueryTransformer, castVarcharTransformer,
                startsWithQueryTransformer,
                upsertValuesQueryTransformer, upsertSelectQueryTransformer);
    }

    /** Optionally, modifies DDL statements in such a way that PostgreSQL
     *  results will match VoltDB results; and then passes the remaining
     *  work to the base class version. */
    protected void runDDL(String ddl, boolean transformDdl) {
        String modifiedDdl = (transformDdl ? transformDDL(ddl) : ddl);
        printTransformedSql(ddl, modifiedDdl);
        super.runDDL(modifiedDdl);
    }

    /** Modifies DDL statements in such a way that PostgreSQL results will
     *  match VoltDB results, and then passes the remaining work to the base
     *  class version. */
    @Override
    public void runDDL(String ddl) {
        runDDL(ddl, true);
    }

    /** Returns the column type name, in VoltDB, corresponding to the specified
     *  column type name in PostgreSQL. */
    @Override
    protected String getVoltColumnTypeName(String columnTypeName) {
        String equivalentTypeName = m_PostgreSQLTypeNames.get(columnTypeName);
        return (equivalentTypeName == null) ? columnTypeName.toUpperCase() : equivalentTypeName;
    }

    /**
     * Returns a VoltTable.ColumnInfo of appropriate type, based on a
     * <i>columnTypeName</i> and <i>colName</i> (both Strings).
     * This version checks for column types used only by PostgreSQL,
     * and then passes the remaining work to the base class version.
     */
    @Override
    protected VoltTable.ColumnInfo getColumnInfo(String columnTypeName, String colName) {
        return super.getColumnInfo(getVoltColumnTypeName(columnTypeName), colName);
    }

    /**
     * Returns true of the specified String contains an odd number of single
     * quote (') characters; false otherwise. This is useful in determining
     * whether or not commas (or other characters) are enclosed in single quotes.
     */
    static private boolean hasOddNumberOfSingleQuotes(String str) {
        boolean result = false;
        for (int index = str.indexOf("'"); index > -1; index = str.indexOf("'", index+1)) {
            result = !result;
        }
        return result;
    }

    /** Returns the number of occurrences of the specified character in the
     *  specified String, but ignoring those contained in single quotes. */
    static private int numOccurencesOfCharIn(String str, char ch) {
        boolean inMiddleOfQuote = false;
        int num = 0, previousIndex = 0;
        for (int index = str.indexOf(ch); index >= 0 ; index = str.indexOf(ch, index+1)) {
            if (hasOddNumberOfSingleQuotes(str.substring(previousIndex, index))) {
                inMiddleOfQuote = !inMiddleOfQuote;
            }
            if (!inMiddleOfQuote) {
                num++;
            }
            previousIndex = index;
        }
        return num;
    }

    /** Returns the Nth occurrence of the specified character in the specified
     *  String, but ignoring those contained in single quotes. */
    static private int indexOfNthOccurrenceOfCharIn(String str, char ch, int n) {
        boolean inMiddleOfQuote = false;
        int index = -1, previousIndex = 0;
        for (int i=0; i < n; i++) {
            do {
                index = str.indexOf(ch, index+1);
                if (index < 0) {
                    return -1;
                }
                if (hasOddNumberOfSingleQuotes(str.substring(previousIndex, index))) {
                    inMiddleOfQuote = !inMiddleOfQuote;
                }
                previousIndex = index;
            } while (inMiddleOfQuote);
        }
        return index;
    }

    /** Normally, simply returns a String consisting of the <i>prefix</i>,
     *  <i>group</i>, and <i>suffix</i>, concatenated in that order; but also
     *  takes care not to cause mismatched parentheses by including more
     *  close-parentheses than open-parentheses before the <i>suffix</i>: if the
     *  group does contain more close-parens than open-parens, the <i>suffix</i>
     *  is inserted just before the first non-matching close-parens (i.e.,
     *  before the first close-parens that does not have a corresponding
     *  open-parens), instead of at the very end.<p>
     *  Similarly, if there are more open-parentheses than close-parentheses,
     *  then the <i>prefix</i> is inserted just after the last non-matching
     *  open-parens (i.e., after the last open-parens that does not have a
     *  corresponding close-parens).<p>
     *  Also, there is a special case when using the divisionQueryTransformer or
     *  bigintDivisionQueryTransformer (i.e., for 'expression1 / expression2'),
     *  in which case both the <i>prefix</i> and the <i>suffix</i> need to be
     *  placed so as to not cause mismatched parentheses on either side of the
     *  division operator ('/'). */
    @Override
    protected String handleParens(String group, String prefix, String suffix, boolean debugPrint) {
        // Default values, which indicate that the prefix simply goes before
        // the entire group, and the suffix simply follows the entire group
        int p_index = 0;
        int s_index = group.length();

        int numOpenParens  = numOccurencesOfCharIn(group, '(');
        int numCloseParens = numOccurencesOfCharIn(group, ')');

        if (debugPrint) {
            System.out.println("    In PostgreSQLBackend.handleParens:");
            System.out.println("      prefix: " + prefix);
            System.out.println("      group : " + group);
            System.out.println("      suffix: " + suffix);
            System.out.println("      numOpenParens, numCloseParens: "+numOpenParens+", "+numCloseParens);
        }

        // Case for a function (e.g., AVG, CEILING, FLOOR), i.e., the group is
        // something like: AVG(expression); check if the expression  has too
        // many close-parentheses in it, and if so, put the suffix before the
        // extra close-parentheses
        if (numOpenParens < numCloseParens && !suffix.isEmpty())  {
            s_index = indexOfNthOccurrenceOfCharIn(group, ')', numOpenParens + 1);

        // Similarly, check if the expression has too many open-parentheses in
        // it, and if so, put the prefix after the extra open-parentheses
        } else if (numOpenParens > numCloseParens && !prefix.isEmpty()) {
            p_index = indexOfNthOccurrenceOfCharIn(group, '(', numOpenParens-numCloseParens) + 1;
        }

        // Special case, for divisionQueryTransformer or bigintDivisionQueryTransformer,
        // i.e., the group is something like: expression1 / expression2; check
        // if there are too many open-parentheses
        if (!group.toUpperCase().startsWith("AVG") && (
                ( DIVISION_QUERY_TRANSFORMER_PREFIX.equals(prefix)
                        && DIVISION_QUERY_TRANSFORMER_SUFFIX.equals(suffix) )
                || (BIGINT_DIV_QUERY_TRANSFORMER_PREFIX.equals(prefix)
                        && BIGINT_DIV_QUERY_TRANSFORMER_SUFFIX.equals(suffix)) ) ) {
            int numDivOperators = numOccurencesOfCharIn(group, '/');
            int div_index = -2;
            if (numDivOperators != 1) {
                // Less than one should be impossible here; more than one means
                // a potentially ambiguous situation
                System.out.println("\nWARNING: in PostgreSQLBackend.handleParens, "
                        + "numDivOperators is not 1: " + numDivOperators);
                if (!debugPrint) {
                    System.out.println("In PostgreSQLBackend.handleParens:");
                    System.out.println("  prefix: " + prefix);
                    System.out.println("  group : " + group);
                    System.out.println("  suffix: " + suffix);
                    System.out.println("  numOpenParens, numCloseParens: "+numOpenParens+", "+numCloseParens);
                    debugPrint = true;
                }
            }
            if (numDivOperators > 0) {
                div_index = group.indexOf('/');
                String preDiv = group.substring(0, div_index);
                int numPreDivOpenParens  = numOccurencesOfCharIn(preDiv, '(');
                int numPreDivCloseParens = numOccurencesOfCharIn(preDiv, ')');
                String postDiv = group.substring(div_index);
                int numPostDivOpenParens  = numOccurencesOfCharIn(postDiv, '(');
                int numPostDivCloseParens = numOccurencesOfCharIn(postDiv, ')');
                if (numPreDivOpenParens > numPreDivCloseParens) {
                    // Put the prefix after the last unmatched open-parenthesis
                    p_index = indexOfNthOccurrenceOfCharIn(preDiv, '(', numPreDivOpenParens-numPreDivCloseParens) + 1;
                }
                if (numPreDivOpenParens >= numPreDivCloseParens && numPostDivOpenParens < numPostDivCloseParens) {
                    // Put the suffix before the first unmatched close-parenthesis
                    s_index = div_index + indexOfNthOccurrenceOfCharIn(postDiv, ')', numPostDivOpenParens+1);
                }

                if (debugPrint) {
                    System.out.println("    div_index: " + div_index);
                    System.out.println("    preDiv   : " + preDiv);
                    System.out.println("    postDiv  : " + postDiv);
                    System.out.println("    numPreDivOpenParens,  numPreDivCloseParens : "+numPreDivOpenParens+", "+numPreDivCloseParens);
                    System.out.println("    numPostDivOpenParens, numPostDivCloseParens: "+numPostDivOpenParens+", "+numPostDivCloseParens);
                }
            }
        }

        String result =    group.substring(0, p_index)
                + prefix + group.substring(p_index, s_index) + suffix
                         + group.substring(s_index);

        if (debugPrint) {
            System.out.println("    p_index, s_index: " + p_index+", "+s_index);
            System.out.println("    result: " + result);
        }

        return result;
    }

    /** Returns the specified String, after replacing certain simple "variables",
     *  if any, such as {foo}, where "foo" is in the List of groupNames, with
     *  the corresponding element in the List of groupValues. This handles the
     *  simple cases in replaceGroupNameVariables below, that do not have to
     *  do complicated substitutions of table and column names, or worry about
     *  primary keys (e.g., for an UPSERT statement). */
    protected String replaceSimpleGroupNameVariables(String str, List<String> groupNames,
            List<String> groupValues, boolean debugPrint) {
        String result = str;
        int length = Math.min(groupNames.size(), groupValues.size());
        for (int i=0; i < length; i++) {
            if (groupNames  != null && groupNames.get(i)  != null &&
                groupValues != null && groupValues.get(i) != null) {
                result = result.replace("{"+groupNames.get(i)+"}", groupValues.get(i));
            }
        }
        if (debugPrint) {
            System.out.println("    In PostgreSQLBackend.replaceSimpleGroupNameVariables:");
            System.out.println("      str        : " + str);
            System.out.println("      groupNames : " + groupNames);
            System.out.println("      groupValues: " + groupValues);
            System.out.println("      result     : " + result);
        }
        return result;
    }

    /** Returns the specified String, after replacing certain "variables", such
     *  as {table} or {column:pk} (the ":pk" means primary keys only; ":npk"
     *  means non-primary-keys), in a QueryTransformer's prefix, suffix, or
     *  (group) replacement text, for which a corresponding group value will
     *  be substituted. In particular, this version makes substitutions to
     *  enable the transformation of VoltDB's UPSERT statements (both UPSERT
     *  INTO ... VALUES and UPSERT INTO ... SELECT) into PostgreSQL's equivalent,
     *  INSERT statements with an ON CONFLICT ... DO UPDATE clause, via the use
     *  of these variables, used in <i>upsertValuesQueryTransformer</i> and
     *  <i>upsertSelectQueryTransformer</i> (see above):
     *    {columns:pk}   : the primary key columns for the (main) table
     *    {columns:npk}  : the non-primary-key columns for the (main) table
     *    {values:npk}   : the corresponding values, or (for UPSERT INTO SELECT)
     *                     column names or expressions, to which those columns
     *                     in {columns:npk} should be set
     *    {table}        : the main table, into which data should be "upserted"
     *    {selecttables} : the table(s) in the SELECT clause
     *    {where:pk}     : the WHERE clause in the SELECT, involving primary
     *                     keys, which must replace any existing WHERE clause,
     *                     in order to transform the query for PostgreSQL
     *    {sort}         : the GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET
     *                     clauses (if any), at the end of the SELECT
     *  The first 2 select from among the columns in the UPSERT (after the main
     *  table name), if they were specified; otherwise, they select from all
     *  the columns of the (main) table. The last 3 only apply to an UPSERT
     *  INTO ... SELECT statement. */
    @Override
    protected String replaceGroupNameVariables(String str, List<String> groupNames,
            List<String> groupValues, boolean debugPrint) {
        if (debugPrint) {
            System.out.println("    In PostgreSQLBackend.replaceGroupNameVariables:");
            System.out.println("      str        : " + str);
            System.out.println("      groupNames : " + groupNames);
            System.out.println("      groupValues: " + groupValues);
        }
        // If any of the inputs are null or empty, then never mind - just
        // return the original String (str)
        if (str == null || groupNames == null || groupValues == null ||
                str.isEmpty() || groupNames.isEmpty() || groupValues.isEmpty()) {
            return str;
        }

        // If a table was specified & found, use that; otherwise, just do a
        // simple text replacement, e.g., replace "{foo}" with "bar", where
        // "foo" is in the list of groupNames and "bar" is the corresponding
        // element in the list of groupValues.
        // (Note: this table represents the main table in an UPSERT statement,
        // e.g., "T1", in: UPSERT INTO T1 ...)
        String table = null;
        int index = groupNames.indexOf("table");
        if (index > -1 && index < groupValues.size()) {
            table = groupValues.get(index);
        } else {
            return replaceSimpleGroupNameVariables(str, groupNames, groupValues, debugPrint);
        }

        // If column values were specified & found, use those; otherwise, never
        // mind (Note: these column values may represent actual column values
        // specified in an UPSERT INTO VALUES statement; or column names or
        // expressions specified in an UPSERT INTO SELECT statement)
        List<String> columnValues = null;
        index = groupNames.indexOf("values");
        if (index > -1 && index < groupValues.size()) {
            String columnValuesString = groupValues.get(index);
            columnValues = new ArrayList<String>(Arrays.asList(columnValuesString.split("\\s*,\\s*")));

            // Handle the case where one or more of the commas were enclosed in
            // single quotes, so they should not have been used as separators
            // between the column values, meaning that some of the initially
            // defined column values need to be recombined
            if (columnValuesString.contains("'")) {
                List<Integer> indexesToDelete = new ArrayList<Integer>();
                for (int i=0; i < columnValues.size(); i++) {
                    if (hasOddNumberOfSingleQuotes(columnValues.get(i))) {
                        int j;
                        for (j=i+1; j < columnValues.size(); j++) {
                            columnValues.set(i, columnValues.get(i) + "," + columnValues.get(j));
                            indexesToDelete.add(j);
                            if (hasOddNumberOfSingleQuotes(columnValues.get(j))) {
                                break;
                            }
                        }
                        i = j;
                    }
                }
                for (int k=indexesToDelete.size() - 1; k >= 0; k--) {
                    columnValues.remove(indexesToDelete.get(k).intValue());
                }
            }
        } else {
            return str;
        }

        // Get the primary key column names, and the non-primary-key column
        // names, and all column names, for the specified table
        List<String> primaryKeyColumns = getPrimaryKeys(table);
        List<String> nonPrimaryKeyColumns = getNonPrimaryKeyColumns(table);

        // If one or more "select tables" was specified & found (that is, tables
        // used in the SELECT part of an UPSERT INTO T1 SELECT... statement),
        // use those; otherwise, this probably won't be needed, but use the main
        // (UPSERT INTO) table we already found, just in case
        List<String> selectTables = null;
        index = groupNames.indexOf("selecttables");
        if (index > -1 && index < groupValues.size()) {
            String selectTablesStr = groupValues.get(index).toUpperCase();
            if (selectTablesStr.contains(" JOIN ")) {
                selectTables = new ArrayList<String>(Arrays.asList(selectTablesStr.split(" JOIN ")));
            } else {
                selectTables = new ArrayList<String>(Arrays.asList(groupValues.get(index).split(",")));
            }
        } else {
            selectTables = Arrays.asList(table);
        }

        // If particular columns were specified, use only those; otherwise use
        // all columns (primary keys and non-primary-keys), as found above
        List<String> columns = null;
        index = groupNames.indexOf("columns");
        if (index > -1 && index < groupValues.size() && groupValues.get(index) != null) {
            columns = Arrays.asList(groupValues.get(index).split(","));
            // Lower-case table and column names are required for PostgreSQL;
            // we might need to alter this if we use another comparison
            // database (besides HSQL) someday
            for (int i=0; i < columns.size(); i++) {
                columns.set(i, columns.get(i).trim().toLowerCase());
            }
            // Retain only those primary key columns that are in the specified
            // list of columns - in the specified column list order
            List<String> temp = new ArrayList<String>(columns);
            temp.retainAll(primaryKeyColumns);
            primaryKeyColumns = temp;
            // Retain only those non-primary-key columns that are in the specified
            // list of columns - in the specified column list order
            temp = new ArrayList<String>(columns);
            temp.retainAll(nonPrimaryKeyColumns);
            nonPrimaryKeyColumns = temp;
        } else {
            columns = getAllColumns(table);
        }

        // Handle the special case of UPSERT INTO ... SELECT *, i.e., when one
        // (or more??) of the "columnValues" is equal to "*"; go in reverse
        // order, to avoid messing up the list
        for (int i = columnValues.size() - 1; i >= 0; i--) {
            if (columnValues.get(i).trim().equals("*")) {
                columnValues.remove(i);
                for (int t = selectTables.size() - 1; t >= 0; t--) {
                    columnValues.addAll(i, getAllColumns(selectTables.get(t).trim()));
                }
            }
        }

        // Remove elements from the "columnValues" list that correspond to
        // primary key columns, since those values do not need to be set in an
        // INSERT statement's ON CONFLICT ... DO UPDATE clause; but add them to
        // a separate list (which is needed only for an UPSERT INTO SELECT
        // statement); go in reverse order, to avoid messing up the list; the
        // "columnValues" and "columns" lists should have the same size here,
        // but just in case use the minimum
        List<String> pkColumnValues = new ArrayList<String>();
        for (int i = Math.min(columnValues.size(), columns.size()) - 1; i >= 0; i--) {
            if (primaryKeyColumns.contains(columns.get(i))) {
                pkColumnValues.add(0, columnValues.get(i));
                columnValues.remove(i);
            }
        }

        // If "where" was included in the "groupNames" (which it would be for an
        // UPSERT INTO SELECT statement), prepare a WHERE clause, setting primary
        // keys equal to their equivalent values in the main (_TMP) table, e.g.:
        //     "WHERE id=_TMP.id AND foo=_TMP.blah AND bar=_TMP.yada "
        String pkWhereClause = "EMPTY";
        if (groupNames.indexOf("where") > -1 && pkColumnValues.size() > 0
                && primaryKeyColumns.size() > 0) {
            pkWhereClause = "WHERE " + pkColumnValues.get(0)+"=_TMP."+primaryKeyColumns.get(0)+" ";
            for (int i=1; i < pkColumnValues.size(); i++) {
                pkWhereClause += "AND " + pkColumnValues.get(i)+"=_TMP."+primaryKeyColumns.get(i)+" ";
            }
        }

        // Replace the groupName "variables" with their corresponding
        // groupValues, processing special cases involving columnType
        // "pk" or "npk", as needed
        StringBuffer modified_str = new StringBuffer();
        Matcher matcher = groupNameVariables.matcher(str);
        while (matcher.find()) {
            String groupName = matcher.group("groupName");
            String columnType = matcher.group("columnType");
            // Filter this "variable" to only include primary key columns
            if ("pk".equalsIgnoreCase(columnType)) {
                if ("columns".equalsIgnoreCase(groupName)) {
                    matcher.appendReplacement(modified_str, String.join(", ", primaryKeyColumns));
                } else if ("where".equalsIgnoreCase(groupName)) {
                    matcher.appendReplacement(modified_str, pkWhereClause);
                } else {
                    // No match: give up on this "variable"
                    matcher.appendReplacement(modified_str, "{"+groupName+":pk}");
                }
            // Filter this "variable" to only include non-primary-key columns
            } else if ("npk".equalsIgnoreCase(columnType)) {
                if ("columns".equalsIgnoreCase(groupName)) {
                    matcher.appendReplacement(modified_str, String.join(", ", nonPrimaryKeyColumns));
                } else if ("values".equalsIgnoreCase(groupName)) {
                    // Extra escaping to make sure that "\\" remains as "\\" and
                    // "$" remains "$", despite appendReplacement's efforts to
                    // change them (not needed for column names, since they never
                    // contain those characters)
                    matcher.appendReplacement(modified_str,
                            String.join(", ", columnValues).replace("\\\\", "\\\\\\\\").replace("$", "\\$"));
                } else {
                    // No match: give up on this "variable"
                    matcher.appendReplacement(modified_str, "{"+groupName+":npk}");
                }
            // Simply return the value of this "variable"
            } else {
                index = groupNames.indexOf(groupName);
                if (index > -1 && index < groupValues.size()) {
                    String groupValue = groupValues.get(index);
                    matcher.appendReplacement(modified_str, (groupValue == null ? "" : groupValue));
                } else {
                    // No match: give up on this "variable"
                    matcher.appendReplacement(modified_str, "{"+groupName+"}");
                }
            }
        }
        matcher.appendTail(modified_str);
        if (debugPrint) {
            System.out.println("      table               : " + table);
            System.out.println("      columnValues        : " + columnValues);
            System.out.println("      primaryKeyColumns   : " + primaryKeyColumns);
            System.out.println("      nonPrimaryKeyColumns: " + nonPrimaryKeyColumns);
            System.out.println("      selectTables        : " + selectTables);
            System.out.println("      columns             : " + columns);
            System.out.println("      pkColumnValues      : " + pkColumnValues);
            System.out.println("      pkWhereClause       : " + pkWhereClause);
            System.out.println("      str                 : " + str);
            System.out.println("      modified_str        : " + modified_str);
        }
        return modified_str.toString();
    }

    /** Optionally, modifies queries in such a way that PostgreSQL results will
     *  match VoltDB results; and then passes the remaining work to the base
     *  class version. */
    protected VoltTable runDML(String dml, boolean transformDml) {
        String modifiedDml = (transformDml ? transformDML(dml) : dml);
        printTransformedSql(dml, modifiedDml);
        return super.runDML(modifiedDml);
    }

    /** Modifies queries in such a way that PostgreSQL results will match VoltDB
     *  results, and then passes the remaining work to the base class version. */
    @Override
    public VoltTable runDML(String dml) {
        return runDML(dml, true);
    }

    protected Connection getConnection() {
        return dbconn;
    }

    @Override
    protected void shutdown() {
        try {
            System.out.println("Closing and dropping database: " + dbconn_url
                    + "; and closing: "+m_permanent_db_backend.dbconn_url);
            dbconn.close();
            // Make sure the connection is closed, before proceeding
            for (int i=1; dbconn.isValid(i) && i<=10; i++) {
                Thread.sleep(1000*i);
            }
            dbconn = null;
            for (int i=1; i<=3; i++) {
                if (i > 1) {
                    System.err.println("In PostgreSQLBackend.shutdown(): attempt #"+i+" ...");
                }
                try {
                    Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                    stmt.execute("drop database if exists " + m_database_name + ";");
                    break;
                } catch (SQLException ex) {
                    System.err.println("In PostgreSQLBackend.shutdown(), for "+dbconn_url
                            +" on attempt #"+i+", caught exception:\n" + ex);
                    ex.printStackTrace();
                }
            }

            // Also close the connection to the "permanent" database
            try {
                m_permanent_db_backend.getConnection().close();
            } catch (SQLException ex) {
                System.err.println("In PostgreSQLBackend.shutdown(), for "+m_permanent_db_backend.dbconn_url
                        + ", caught exception:\n" + ex);
                ex.printStackTrace();
            }
            // Make sure the "permanent" connection is closed, before proceeding
            for (int i=1; m_permanent_db_backend.getConnection().isValid(i) && i<=10; i++) {
                Thread.sleep(1000*i);
            }
            m_permanent_db_backend.dbconn = null;
            m_permanent_db_backend = null;

            // And close the FileWriter (for printing modified SQL)
            if (transformedSqlFileWriter != null) {
                transformedSqlFileWriter.close();
                transformedSqlFileWriter = null;
            }
            System.gc();
        } catch (Exception e) {
            hostLog.error("Error shutting down backend", e);
        }
    }

}
