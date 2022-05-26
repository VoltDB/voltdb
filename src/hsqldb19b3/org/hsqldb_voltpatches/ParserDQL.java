/* Copyright (c) 2001-2009, The HSQL Development Group
 * Copyright (c) 2010-2022, Volt Active Data Inc.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.QueryExpression.WithList;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.store.BitMap;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.BlobType;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.DTIType;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.Type;

/**
 * Parser for DQL statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ParserDQL extends ParserBase {

    protected Database             database;
    protected Session              session;
    protected final CompileContext compileContext;
    HsqlException                  lastError;
    boolean                        strictSQLNames;
    boolean                        strictSQLIdentifierParts;
    // A VoltDB extension to reject quoted (delimited) names.
    // TODO: Set flag from property?
    boolean rejectQuotedSchemaObjectNames = true;
    int withStatementDepth = 0;
    // End of VoltDB extension

    //

    /**
     *  Constructs a new Parser object with the given context.
     *
     * @param  session the connected context
     * @param  t the token source from which to parse commands
     */
    ParserDQL(Session session, Scanner t) {

        super(t);

        this.session   = session;
        database       = session.getDatabase();
        compileContext = new CompileContext(session);
        strictSQLNames = database.getProperties().isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_keywords);
        strictSQLIdentifierParts = database.getProperties().isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_keywords);
    }

    /**
     *  Resets this parse context with the given SQL character sequence.
     *
     * @param sql a new SQL character sequence to replace the current one
     */
    @Override
    void reset(String sql) {

        super.reset(sql);
        compileContext.reset();

        lastError = null;
    }

    void checkIsSchemaObjectName() {

        if (strictSQLNames) {
            checkIsNonReservedIdentifier();
        } else {
            checkIsNonCoreReservedIdentifier();
        }
        // A VoltDB extension to reject quoted (delimited) names.
        if (rejectQuotedSchemaObjectNames && token.isDelimitedIdentifier) {
            throw unexpectedToken();
        }
        // End of VoltDB extension

        if (token.namePrePrefix != null) {
            throw tooManyIdentifiers();
        }
    }

    Type readTypeDefinition(boolean includeUserTypes) {

        int typeNumber = Integer.MIN_VALUE;

        checkIsIdentifier();

        if (token.namePrefix == null) {
            typeNumber = Type.getTypeNr(token.tokenString);
        }

        if (typeNumber == Integer.MIN_VALUE) {
            if (includeUserTypes) {
                checkIsSchemaObjectName();

                String schemaName = session.getSchemaName(token.namePrefix);
                Type type = database.schemaManager.getDomain(token.tokenString,
                    schemaName, false);

                if (type != null) {
                    read();

                    return type;
                }
            }

            throw Error.error(ErrorCode.X_42509, token.tokenString);
        }

        read();

        switch (typeNumber) {

            case Types.SQL_CHAR :
                if (token.tokenType == Tokens.VARYING) {
                    read();

                    typeNumber = Types.SQL_VARCHAR;
                } else if (token.tokenType == Tokens.LARGE) {
                    readThis(Tokens.OBJECT);
                    read();

                    typeNumber = Types.SQL_CLOB;
                }
                break;

            case Types.SQL_DOUBLE :
                if (token.tokenType == Tokens.PRECISION) {
                    read();
                }
                break;

            case Types.SQL_BINARY :
                if (token.tokenType == Tokens.VARYING) {
                    read();

                    typeNumber = Types.SQL_VARBINARY;
                } else if (token.tokenType == Tokens.LARGE) {
                    readThis(Tokens.OBJECT);
                    read();

                    typeNumber = Types.SQL_BLOB;
                }
                break;

            case Types.SQL_BIT :
                if (token.tokenType == Tokens.VARYING) {
                    read();

                    typeNumber = Types.SQL_BIT_VARYING;
                }
                break;

            case Types.SQL_INTERVAL :
                return readIntervalType();

            default :
        }

        long length = typeNumber == Types.SQL_TIMESTAMP
                      ? DTIType.defaultTimestampFractionPrecision
                      : 0;
        int scale = 0;

        if (Types.requiresPrecision(typeNumber)
                && token.tokenType != Tokens.OPENBRACKET
                && database.sqlEnforceStrictSize) {
            throw unexpectedTokenRequire(Tokens.T_OPENBRACKET);
        }

        // A VoltDB extension to support the character in bytes.
        boolean           inBytes = false;
        // End of VoltDB extension
        if (Types.acceptsPrecision(typeNumber)) {
            if (token.tokenType == Tokens.OPENBRACKET) {
                int multiplier = 1;

                read();

                switch (token.tokenType) {

                    case Tokens.X_VALUE :
                        if (token.dataType.typeCode != Types.SQL_INTEGER
                                && token.dataType.typeCode
                                   != Types.SQL_BIGINT) {
                            throw unexpectedToken();
                        }
                        break;

                    case Tokens.X_LOB_SIZE :
                        if (typeNumber == Types.SQL_BLOB
                                || typeNumber == Types.SQL_CLOB) {
                            switch (token.lobMultiplierType) {

                                case Tokens.K :
                                    multiplier = 1024;
                                    break;

                                case Tokens.M :
                                    multiplier = 1024 * 1024;
                                    break;

                                case Tokens.G :
                                    multiplier = 1024 * 1024 * 1024;
                                    break;

                                case Tokens.P :
                                case Tokens.T :
                                default :
                                    throw unexpectedToken();
                            }

                            break;
                        } else {
                            throw unexpectedToken(token.getFullString());
                        }
                    default :
                        throw unexpectedToken();
                }

                length = ((Number) token.tokenValue).longValue();

                if (length < 0
                        || (length == 0
                            && !Types.acceptsZeroPrecision(typeNumber))) {
                    throw Error.error(ErrorCode.X_42592);
                }

                length *= multiplier;

                read();

                if (typeNumber == Types.SQL_CHAR
                        || typeNumber == Types.SQL_VARCHAR
                        || typeNumber == Types.SQL_CLOB) {
                    if (token.tokenType == Tokens.CHARACTERS) {
                        read();
                    } else if (token.tokenType == Tokens.OCTETS) {
                        read();

                        length /= 2;
                    }
                }

                if (Types.acceptsScaleCreateParam(typeNumber)
                        && token.tokenType == Tokens.COMMA) {
                    read();

                    scale = readInteger();

                    if (scale < 0) {
                        throw Error.error(ErrorCode.X_42592);
                    }
                }

                // A VoltDB extension to support the character in bytes.
                if (typeNumber == Types.SQL_VARCHAR) {
                    inBytes = readIfThis(Tokens.BYTES);
                }
                // End of VoltDB extension

                readThis(Tokens.CLOSEBRACKET);
            } else if (typeNumber == Types.SQL_BIT) {
                length = 1;
            } else if (typeNumber == Types.SQL_BLOB
                       || typeNumber == Types.SQL_CLOB) {
                length = BlobType.defaultBlobSize;
            } else if (database.sqlEnforceStrictSize) {

                // BIT is always BIT(1), regardless of sqlEnforceStringSize
                if (typeNumber == Types.SQL_CHAR
                        || typeNumber == Types.SQL_BINARY) {
                    length = 1;
                }
            }

            if (typeNumber == Types.SQL_TIMESTAMP
                    || typeNumber == Types.SQL_TIME) {
                if (length > DTIType.maxFractionPrecision) {
                    throw Error.error(ErrorCode.X_42592);
                }

                scale  = (int) length;
                length = 0;

                if (token.tokenType == Tokens.WITH) {
                    read();
                    readThis(Tokens.TIME);
                    readThis(Tokens.ZONE);

                    if (typeNumber == Types.SQL_TIMESTAMP) {
                        typeNumber = Types.SQL_TIMESTAMP_WITH_TIME_ZONE;
                    } else {
                        typeNumber = Types.SQL_TIME_WITH_TIME_ZONE;
                    }
                } else if (token.tokenType == Tokens.WITHOUT) {
                    read();
                    readThis(Tokens.TIME);
                    readThis(Tokens.ZONE);
                }
            }
        }

        Type typeObject = Type.getType(typeNumber, 0, length, scale);

        if (typeObject.isCharacterType()) {
            // A VoltDB extension to support the character in bytes.
            if (inBytes) {
                ((CharacterType) typeObject).inBytes = true;
            }
            // End of VoltDB extension
            if (token.tokenType == Tokens.CHARACTER) {
                read();
                readThis(Tokens.SET);
                checkIsSchemaObjectName();

                String schemaName = session.getSchemaName(token.namePrefix);
                Charset charset =
                    (Charset) database.schemaManager.getSchemaObject(
                        token.tokenString, schemaName, SchemaObject.CHARSET);

                read();
            }
        }

        return typeObject;
    }

    void readSimpleColumnNames(OrderedHashSet columns,
                               RangeVariable rangeVar) {

        while (true) {
            ColumnSchema col = readSimpleColumnName(rangeVar);

            if (!columns.add(col.getName().name)) {
                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            if (token.tokenType == Tokens.CLOSEBRACKET) {
                break;
            }

            throw unexpectedToken();
        }
    }

    void readColumnNames(OrderedHashSet columns, RangeVariable[] rangeVars) {

        while (true) {
            ColumnSchema col = readColumnName(rangeVars);

            if (!columns.add(col.getName().name)) {
                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            if (token.tokenType == Tokens.CLOSEBRACKET) {
                break;
            }

            throw unexpectedToken();
        }
    }

    void readColumnNamesForSelectInto(OrderedHashSet columns,
                                      RangeVariable[] rangeVars) {

        while (true) {
            ColumnSchema col = readColumnName(rangeVars);

            if (!columns.add(col.getName().name)) {
                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            if (token.tokenType == Tokens.FROM) {
                break;
            }

            throw unexpectedToken();
        }
    }

    void readSimpleColumnNames(OrderedHashSet columns, Table table) {

        while (true) {
            ColumnSchema col = readSimpleColumnName(table);

            if (!columns.add(col.getName().name)) {
                throw Error.error(ErrorCode.X_42577, col.getName().name);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            if (token.tokenType == Tokens.CLOSEBRACKET) {
                break;
            }

            throw unexpectedToken();
        }
    }

    HsqlName[] readColumnNames(HsqlName tableName) {

        BitMap         quotedFlags = new BitMap(32);
        OrderedHashSet set         = readColumnNames(quotedFlags, false);
        HsqlName[]     colList     = new HsqlName[set.size()];

        for (int i = 0; i < colList.length; i++) {
            String  name   = (String) set.get(i);
            boolean quoted = quotedFlags.isSet(i);

            colList[i] = database.nameManager.newHsqlName(tableName.schema,
                    name, quoted, SchemaObject.COLUMN, tableName);
        }

        return colList;
    }

    OrderedHashSet readColumnNames(boolean readAscDesc) {
        return readColumnNames(null, readAscDesc);
    }

    OrderedHashSet readColumnNames(BitMap quotedFlags, boolean readAscDesc) {

        readThis(Tokens.OPENBRACKET);

        OrderedHashSet set = readColumnNameList(quotedFlags, readAscDesc);

        readThis(Tokens.CLOSEBRACKET);

        return set;
    }

    OrderedHashSet readColumnNameList(BitMap quotedFlags,
                                      boolean readAscDesc) {

        int            i   = 0;
        OrderedHashSet set = new OrderedHashSet();

        while (true) {
            if (session.isProcessingScript) {

                // for old scripts
                // A VoltDB extension to make reserved words more consistent.
                // Adding "strictSQLNames" as a param to the next two method calls.
                // SEE: ENG-912
                if (!isSimpleName(strictSQLNames)) {
                /* disable 1 line ...
                if (!isSimpleName()) {
                ... disabled 1 line */
                // End of VoltDB extension
                    token.isDelimitedIdentifier = true;
                }
            } else {
                // A VoltDB extension to make reserved words more consistent.
                checkIsSimpleName(strictSQLNames);
                /* disable 1 line ...
                checkIsSimpleName();
                ... disabled 1 line */
                // End of VoltDB extension
            }

            if (!set.add(token.tokenString)) {
                throw Error.error(ErrorCode.X_42577, token.tokenString);
            }

            if (quotedFlags != null && isDelimitedIdentifier()) {
                quotedFlags.set(i);
            }

            read();

            i++;

            if (readAscDesc) {
                if (token.tokenType == Tokens.ASC
                        || token.tokenType == Tokens.DESC) {
                    read();
                }
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            break;
        }

        return set;
    }

    SubQuery getViewSubquery(View v) {

        SubQuery sq = v.viewSubQuery;

        for (int i = 0; i < v.viewSubqueries.length; i++) {
            compileContext.subQueryList.add(v.viewSubqueries[i]);
        }

        return sq;
    }

    int XreadUnionType() {

        int unionType = QueryExpression.NOUNION;

        switch (token.tokenType) {

            case Tokens.UNION :
                read();

                unionType = QueryExpression.UNION;

                if (token.tokenType == Tokens.ALL) {
                    unionType = QueryExpression.UNION_ALL;

                    read();
                } else if (token.tokenType == Tokens.DISTINCT) {
                    read();
                }
                break;

            case Tokens.INTERSECT :
                read();

                unionType = QueryExpression.INTERSECT;

                if (token.tokenType == Tokens.ALL) {
                    unionType = QueryExpression.INTERSECT_ALL;

                    read();
                } else if (token.tokenType == Tokens.DISTINCT) {
                    read();
                }
                break;

            case Tokens.EXCEPT :
            case Tokens.MINUS_EXCEPT :
                read();

                unionType = QueryExpression.EXCEPT;

                if (token.tokenType == Tokens.ALL) {
                    unionType = QueryExpression.EXCEPT_ALL;

                    read();
                } else if (token.tokenType == Tokens.DISTINCT) {
                    read();
                }
                break;

            default :
                break;
        }

        return unionType;
    }

    void XreadUnionCorrespondingClause(QueryExpression queryExpression) {

        if (token.tokenType == Tokens.CORRESPONDING) {
            read();
            queryExpression.setUnionCorresponding();

            if (token.tokenType == Tokens.BY) {
                read();

                OrderedHashSet names = readColumnNames(false);

                queryExpression.setUnionCorrespondingColumns(names);
            }
        }
    }

    /*
     * <with clause> ::= WITH [ RECURSIVE ] <with list>
     *
     */
    private WithList XreadWithClause() {
        boolean recursive = false;
        if (withStatementDepth > 0) {
        	throw Error.error("With statements may not be nested.");
        }
        int oldStatementDepth = withStatementDepth;
        try {
        	session.clearLocalTables();
	        withStatementDepth += 1;
	        readThis(Tokens.WITH);
	        if (token.tokenType == Tokens.RECURSIVE) {
	            recursive = true;
	            read();
	        }
	        WithList withList = new WithList(recursive);
	        XreadWithList(withList);
	        return withList;
        } finally {
        	withStatementDepth = oldStatementDepth;
        }
    }

    /*
     * <with list> ::= <with list element> [ { <comma> <with list element> } ]
     */
    private void XreadWithList(WithList withList) {
        // <with list element> ::= <query name>
        //                         [ <left paren> <with column list> <right paren> ]
        //                         AS <left paren> <query expression> <right paren>
        //                         [ <search or cycle clause> ]
        //
        boolean recursive = withList.isRecursive();
        for (boolean done = false; ! done; done = ! readIfThis(Tokens.COMMA)) {
            HsqlName queryName = readNewSchemaObjectName(SchemaObject.TABLE);
            queryName.setSchemaIfNull(session.getCurrentSchemaHsqlName());
            List<HsqlName> columnNames = parseColumnNames();
            readThis(Tokens.AS);
            readThis(Tokens.OPENBRACKET);
            // Read a query.  If it's recursive it has to be of the form:
            //    Q1 union all Q2.
            // In Q1 we can't use an order by or limit.  It's ok
            // to use group by, aggregates and having, though.  The
            // query name will be visible in Q2 but not in Q1.
            //
            // If this with statement is not recursive then anything goes,
            // but the query name will not be visible in the query.
            QueryExpression baseQueryExpression = null;
            QueryExpression recursionQueryExpression = null;
            // This is not really standard behavior.  This should
            // really be just reading a query expression.  However,
            // This short circuits a great deal of complexity, and
            // will serve for prototype purposes.
            if (recursive) {
                baseQueryExpression = XreadQueryPrimary();
            } else {
                baseQueryExpression = XreadQueryExpressionBodyAndSortAndSlice();
            }
            // We have to resolve this here because we need
            // to fetch the types and perhaps the column aliases.
            // But we can't define the table now, because it is
            // not visible in the base query expression.
            baseQueryExpression.resolve(session);
            //
            // Calculate the schema of the common table.
            //
            HsqlName[] colNames = null;
            if (columnNames != null) {
                colNames = columnNames.toArray(new HsqlName[columnNames.size()]);
            } else {
                String[] queryNames = baseQueryExpression.getColumnNames();
                colNames = new HsqlName[queryNames.length];
                for (int idx = 0; idx < queryNames.length; idx += 1) {
                    String queryColumnName = queryNames[idx];
                    colNames[idx] = database.nameManager.newHsqlName(queryColumnName,
                                                                     false,
                                                                     SchemaObject.COLUMN);
                }
            }
            Type[] colTypes = baseQueryExpression.getColumnTypes();
            String cmp = null;
            if (colNames.length > colTypes.length) {
                cmp = "many";
            }
            else if (colNames.length < colTypes.length) {
                cmp = "few";
            }
            if (cmp != null) {
                throw Error.error("Too " + cmp + " column names in common table expression " + queryName.name);
            }
            // Now, define it.
            Table newTable = session.defineLocalTable(queryName, colNames, colTypes);
            if (recursive) {
                readThis(Tokens.UNION);
                readThis(Tokens.ALL);
                recursionQueryExpression = XreadQueryExpressionBody();
            }
            readThis(Tokens.CLOSEBRACKET);
            WithExpression withExpression = new WithExpression();
            withExpression.setQueryName(queryName);
            withExpression.setBaseQuery(baseQueryExpression);
            withExpression.setRecursiveQuery(recursionQueryExpression);
            withExpression.setTable(newTable);
            withList.add(withExpression);
        }
    }

    private List<HsqlName> parseColumnNames() {
        List<HsqlName> columnNames = null;
        if (token.tokenType == Tokens.OPENBRACKET) {
            columnNames = new ArrayList<>();
            read();
            while (true) {
                columnNames.add(readNewSchemaObjectName(SchemaObject.COLUMN));
                if (token.tokenType == Tokens.COMMA) {
                    read();
                } else if (token.tokenType == Tokens.CLOSEBRACKET) {
                    read();
                    break;
                } else {
                    throw unexpectedToken();
                }
            }
        }
        return columnNames;
    }

    QueryExpression XreadQueryExpression() {

        WithList withList = null;
        if (token.tokenType == Tokens.WITH) {
            withList = XreadWithClause();
        }

        QueryExpression queryExpression = XreadQueryExpressionBodyAndSortAndSlice();

        queryExpression.addWithList(withList);

        return queryExpression;
    }

    private QueryExpression XreadQueryExpressionBodyAndSortAndSlice() {
        QueryExpression queryExpression = XreadQueryExpressionBody();
        SortAndSlice    sortAndSlice    = XreadOrderByExpression();

        if (queryExpression.sortAndSlice == null) {
            queryExpression.addSortAndSlice(sortAndSlice);
        } else {
            if (queryExpression.sortAndSlice.hasLimit()) {
                if (sortAndSlice.hasLimit()) {
                    throw Error.error(ErrorCode.X_42549);
                }

                for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                    Expression e = (Expression) sortAndSlice.exprList.get(i);

                    queryExpression.sortAndSlice.addOrderExpression(e);
                }
            } else {
                queryExpression.addSortAndSlice(sortAndSlice);
            }
        }
        return queryExpression;
    }

    QueryExpression XreadQueryExpressionBody() {

        QueryExpression queryExpression = XreadQueryTerm();

        while (true) {
            switch (token.tokenType) {

                case Tokens.UNION :
                case Tokens.EXCEPT :
                case Tokens.MINUS_EXCEPT : {
                    queryExpression = XreadSetOperation(queryExpression);

                    break;
                }
                default : {
                    return queryExpression;
                }
            }
        }
    }

    QueryExpression XreadQueryTerm() {

        QueryExpression queryExpression = XreadQueryPrimary();

        while (true) {
            if (token.tokenType == Tokens.INTERSECT) {
                queryExpression = XreadSetOperation(queryExpression);
            } else {
                return queryExpression;
            }
        }
    }

    private QueryExpression XreadSetOperation(
            QueryExpression queryExpression) {

        queryExpression = new QueryExpression(compileContext, queryExpression);

        int unionType = XreadUnionType();

        XreadUnionCorrespondingClause(queryExpression);

        QueryExpression rightQueryExpression = XreadQueryTerm();

        queryExpression.addUnion(rightQueryExpression, unionType);

        return queryExpression;
    }

    QueryExpression XreadQueryPrimary() {

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT : {
                QuerySpecification select = XreadSimpleTable();

                return select;
            }
            case Tokens.OPENBRACKET : {
                read();

                QueryExpression queryExpression = XreadQueryExpressionBody();
                SortAndSlice    sortAndSlice    = XreadOrderByExpression();

                readThis(Tokens.CLOSEBRACKET);

                if (queryExpression.sortAndSlice == null) {
                    queryExpression.addSortAndSlice(sortAndSlice);
                } else {
                    if (queryExpression.sortAndSlice.hasLimit()) {
                        if (sortAndSlice.hasLimit()) {
                            throw Error.error(ErrorCode.X_42549);
                        }

                        for (int i = 0; i < sortAndSlice.exprList.size();
                                i++) {
                            Expression e =
                                (Expression) sortAndSlice.exprList.get(i);

                            queryExpression.sortAndSlice.addOrderExpression(e);
                        }
                    } else {
                        queryExpression.addSortAndSlice(sortAndSlice);
                    }
                }

                return queryExpression;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    QuerySpecification XreadSimpleTable() {

        QuerySpecification select;

        switch (token.tokenType) {

            case Tokens.TABLE : {
                read();

                Table table = readTableName();

                select = new QuerySpecification(session, table,
                                                compileContext);

                break;
            }
            case Tokens.VALUES : {
                read();

                SubQuery sq = XreadRowValueExpressionList();

                select = new QuerySpecification(session, sq.getTable(),
                                                compileContext);

                break;
            }
            case Tokens.SELECT : {
                select = XreadQuerySpecification();

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }

        return select;
    }

    QuerySpecification XreadQuerySpecification() {

        QuerySpecification select = XreadSelect();

        XreadTableExpression(select);

        return select;
    }

    void XreadTableExpression(QuerySpecification select) {
        XreadFromClause(select);
        readWhereGroupHaving(select);
    }

    QuerySpecification XreadSelect() {

        QuerySpecification select = new QuerySpecification(compileContext);

        readThis(Tokens.SELECT);

        if (token.tokenType == Tokens.TOP || token.tokenType == Tokens.LIMIT) {
            SortAndSlice sortAndSlice = XreadTopOrLimit();

            if (sortAndSlice != null) {
                select.addSortAndSlice(sortAndSlice);
            }
        }

        if (token.tokenType == Tokens.DISTINCT) {
            select.isDistinctSelect = true;

            read();
        } else if (token.tokenType == Tokens.ALL) {
            read();
        }

        while (true) {
            Expression e = XreadValueExpression();

            if (token.tokenType == Tokens.AS) {
                read();
                checkIsNonCoreReservedIdentifier();
            }

            if (isNonCoreReservedIdentifier()) {
                e.setAlias(HsqlNameManager.getSimpleName(token.tokenString,
                        isDelimitedIdentifier()));
                read();
            }

            select.addSelectColumnExpression(e);

            if (token.tokenType == Tokens.FROM) {
                break;
            }

            if (token.tokenType == Tokens.INTO) {
                break;
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            throw unexpectedToken();
        }

        return select;
    }

    void XreadFromClause(QuerySpecification select) {

        readThis(Tokens.FROM);

        while (true) {
            XreadTableReference(select);

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            break;
        }
    }

    void XreadTableReference(QuerySpecification select) {

        boolean       natural = false;
        RangeVariable range   = readTableOrSubquery();

        select.addRangeVariable(range);

        while (true) {
            int     type  = token.tokenType;
            boolean left  = false;
            boolean right = false;
            boolean end   = false;

            type = token.tokenType;

            switch (token.tokenType) {

                case Tokens.INNER :
                    read();
                    readThis(Tokens.JOIN);
                    break;

                case Tokens.CROSS :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.JOIN);
                    break;

                case Tokens.UNION :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    int position = getPosition();

                    read();

                    if (token.tokenType == Tokens.JOIN) {
                        read();

                        break;
                    } else {
                        rewind(position);

                        end = true;

                        break;
                    }
                case Tokens.NATURAL :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();

                    natural = true;

                    continue;
                case Tokens.LEFT :
                    read();
                    readIfThis(Tokens.OUTER);
                    readThis(Tokens.JOIN);

                    left = true;
                    break;

                case Tokens.RIGHT :
                    read();
                    readIfThis(Tokens.OUTER);
                    readThis(Tokens.JOIN);

                    right = true;
                    break;

                case Tokens.FULL :
                    read();
                    readIfThis(Tokens.OUTER);
                    readThis(Tokens.JOIN);

                    left  = true;
                    right = true;
                    break;

                case Tokens.JOIN :
                    read();

                    type = Tokens.INNER;
                    break;

                case Tokens.COMMA :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();

                    type = Tokens.CROSS;
                    break;

                default :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    end = true;
                    break;
            }

            if (end) {
                break;
            }

            range = readTableOrSubquery();

            Expression condition = null;

            switch (type) {

                case Tokens.CROSS :
                    select.addRangeVariable(range);
                    break;

                case Tokens.UNION :
                    select.addRangeVariable(range);

                    condition = Expression.EXPR_FALSE;

                    range.setJoinType(true, true);
                    break;

                case Tokens.LEFT :
                case Tokens.RIGHT :
                case Tokens.INNER :
                case Tokens.FULL : {
                    if (natural) {
                        OrderedHashSet columns =
                            range.getUniqueColumnNameSet();

                        condition = select.getEquiJoinExpressions(columns,
                                range, false);

                        select.addRangeVariable(range);
                    } else if (token.tokenType == Tokens.USING) {
                        read();

                        OrderedHashSet columns = new OrderedHashSet();

                        readThis(Tokens.OPENBRACKET);
                        readSimpleColumnNames(columns, range);
                        readThis(Tokens.CLOSEBRACKET);

                        condition = select.getEquiJoinExpressions(columns,
                                range, true);

                        select.addRangeVariable(range);
                    } else if (token.tokenType == Tokens.ON) {
                        read();

                        condition = XreadBooleanValueExpression();

                        select.addRangeVariable(range);

                        // must ensure references are limited to the current table
//                        select.finaliseRangeVariables();
//                        select.resolveColumnReferencesAndAllocate(condition);
                    } else {
                        throw Error.error(ErrorCode.X_42581);
                    }

                    range.setJoinType(left, right);

                    break;
                }
            }

            range.addJoinCondition(condition);

            natural = false;
        }
    }

    Expression getRowExpression(OrderedHashSet columnNames) {

        Expression[] elements = new Expression[columnNames.size()];

        for (int i = 0; i < elements.length; i++) {
            String name = (String) columnNames.get(i);

            elements[i] = new ExpressionColumn(null, null, name);
        }

        return new Expression(OpTypes.ROW, elements);
    }

    void readWhereGroupHaving(QuerySpecification select) {

        // where
        if (token.tokenType == Tokens.WHERE) {
            read();

            Expression e = XreadBooleanValueExpression();

            select.addQueryCondition(e);
        }

        // group by
        if (token.tokenType == Tokens.GROUP) {
            read();
            readThis(Tokens.BY);

            while (true) {
                Expression e = XreadValueExpression();

                select.addGroupByColumnExpression(e);

                if (token.tokenType == Tokens.COMMA) {
                    read();

                    continue;
                }

                break;
            }
        }

        // having
        if (token.tokenType == Tokens.HAVING) {
            read();

            Expression e = XreadBooleanValueExpression();

            select.addHavingExpression(e);
        }
    }

    SortAndSlice XreadOrderByExpression() {

        SortAndSlice sortAndSlice = null;

        if (token.tokenType == Tokens.ORDER) {
            read();
            readThis(Tokens.BY);

            sortAndSlice = XreadOrderBy();
        }

        if (token.tokenType == Tokens.LIMIT || token.tokenType == Tokens.FETCH
                || token.tokenType == Tokens.OFFSET) {
            if (sortAndSlice == null) {
                sortAndSlice = new SortAndSlice();
            }

            XreadLimit(sortAndSlice);
        }

        return sortAndSlice == null ? SortAndSlice.noSort
                                    : sortAndSlice;
    }

    private SortAndSlice XreadTopOrLimit() {

        Expression e1 = null;
        Expression e2 = null;

        if (token.tokenType == Tokens.LIMIT) {
            int position = getPosition();

            read();

            e1 = XreadSimpleValueSpecificationOrNull();

            if (e1 == null) {
                super.rewind(position);

                return null;
            }

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                throw Error.error(ErrorCode.X_42565, ErrorCode.INVALID_LIMIT);
            }
        } else if (token.tokenType == Tokens.TOP) {
            int position = getPosition();

            read();

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                super.rewind(position);

                return null;
            }

            e1 = new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER);
        }

        boolean valid = true;

        if (e1.isParam()) {
            e1.setDataType(session, Type.SQL_INTEGER);
        } else {
            valid = (e1.getDataType().typeCode == Types.SQL_INTEGER
                     && ((Integer) e1.getValue(null)).intValue() >= 0);
        }

        if (e2.isParam()) {
            e2.setDataType(session, Type.SQL_INTEGER);
        } else {
            valid &= (e2.getDataType().typeCode == Types.SQL_INTEGER
                      && ((Integer) e2.getValue(null)).intValue() >= 0);
        }

        if (valid) {
            SortAndSlice sortAndSlice = new SortAndSlice();

            sortAndSlice.addLimitCondition(new ExpressionOp(OpTypes.LIMIT, e1,
                    e2));

            return sortAndSlice;
        }

        throw Error.error(ErrorCode.X_42565, ErrorCode.INVALID_LIMIT);
    }

    private void XreadLimit(SortAndSlice sortAndSlice) {

        Expression e1 = null;
        Expression e2 = null;

        if (token.tokenType == Tokens.OFFSET) {
            read();

            e1 = XreadSimpleValueSpecificationOrNull();

            if (e1 == null) {
                throw Error.error(ErrorCode.X_42565, ErrorCode.INVALID_LIMIT);
            }
        }

        if (token.tokenType == Tokens.LIMIT) {
            read();

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                throw Error.error(ErrorCode.X_42565, ErrorCode.INVALID_LIMIT);
            }

            if (e1 == null && token.tokenType == Tokens.OFFSET) {
                read();

                e1 = XreadSimpleValueSpecificationOrNull();
            }
        } else if (token.tokenType == Tokens.FETCH) {
            read();

            if (token.tokenType == Tokens.FIRST
                    || token.tokenType == Tokens.NEXT) {
                read();
            }

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                e2 = new ExpressionValue(ValuePool.INTEGER_1,
                                         Type.SQL_INTEGER);
            }

            if (token.tokenType == Tokens.ROW
                    || token.tokenType == Tokens.ROWS) {
                read();
            }

            readThis(Tokens.ONLY);
        }

        if (e1 == null) {
            e1 = new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER);
        }

        boolean valid = true;

        if (e1.isParam()) {
            e1.setDataType(session, Type.SQL_INTEGER);
        } else {
            valid = (e1.getDataType().typeCode == Types.SQL_INTEGER
                     && ((Integer) e1.getValue(null)).intValue() >= 0);
        }

        // A VoltDB extension to allow OFFSET without LIMIT
        if (e2 != null) {
            if (e2.isParam()) {
                e2.setDataType(session, Type.SQL_INTEGER);
            } else {
                valid &= (e2.getDataType().typeCode == Types.SQL_INTEGER
                        && ((Integer) e2.getValue(null)).intValue() >= 0);
            }
        }
        // End of VoltDB extension

        /* disable 6 lines ...
        if (e2.isParam()) {
            e2.setDataType(session, Type.SQL_INTEGER);
        } else {
            valid &= (e2.getDataType().typeCode == Types.SQL_INTEGER
                      && ((Integer) e2.getValue(null)).intValue() >= 0);
        }
        ... disabled 6 lines */

        if (valid) {
            sortAndSlice.addLimitCondition(new ExpressionOp(OpTypes.LIMIT, e1,
                    e2));

            return;
        }

        throw Error.error(ErrorCode.X_42565, ErrorCode.INVALID_LIMIT);
    }

    private SortAndSlice XreadOrderBy() {

        SortAndSlice sortAndSlice = new SortAndSlice();

        while (true) {
            Expression        e = XreadValueExpression();
            ExpressionOrderBy o = new ExpressionOrderBy(e);

            if (token.tokenType == Tokens.DESC) {
                o.setDescending();
                read();
            } else if (token.tokenType == Tokens.ASC) {
                read();
            }

            if (token.tokenType == Tokens.NULLS) {
                read();

                if (token.tokenType == Tokens.FIRST) {
                    read();
                } else if (token.tokenType == Tokens.LAST) {
                    read();
                    o.setNullsLast();
                } else {
                    throw unexpectedToken();
                }
            }

            sortAndSlice.addOrderExpression(o);

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            break;
        }

        return sortAndSlice;
    }

    protected RangeVariable readSimpleRangeVariable(int operation) {

        Table      table = readTableName();
        SimpleName alias = null;

        if (token.tokenType == Tokens.AS) {
            read();
            checkIsNonCoreReservedIdentifier();
        }

        if (isNonCoreReservedIdentifier()) {
            alias = HsqlNameManager.getSimpleName(token.tokenString,
                                                  isDelimitedIdentifier());

            read();
        }

        if (table.isView) {
            switch (operation) {

                case StatementTypes.MERGE :
                    if (!table.isUpdatable() || !table.isInsertable()) {
                        throw Error.error(ErrorCode.X_42545);
                    }
                    break;

                case StatementTypes.UPDATE_WHERE :
                case StatementTypes.DELETE_WHERE :
                    /* A VoltDB Extension.
                     * Views from Streams are now updatable.
                     * Comment out this guard and check if it is a view
                     * from Stream or PersistentTable in planner.
                    if (!table.isUpdatable()) {
                        throw Error.error(ErrorCode.X_42545);
                    }
                    A VoltDB Extension */
                    break;
            }

            SubQuery sq = getViewSubquery((View) table);

            table = sq.getTable();
        }

        RangeVariable range = new RangeVariable(table, alias, null, null,
            compileContext);

        return range;
    }

    /**
     * Creates a RangeVariable from the parse context. <p>
     */
    protected RangeVariable readTableOrSubquery() {

        Table          table            = null;
        SimpleName     alias            = null;
        OrderedHashSet columnList       = null;
        BitMap         columnNameQuoted = null;
        SimpleName[]   columnNameList   = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            Expression e = XreadTableSubqueryOrJoinedTable();

            table = e.subQuery.getTable();
            if (table instanceof TableDerived) {
                ((TableDerived)table).dataExpression = e;
            }
        } else {
            table = readTableName();

            if (table.isView()) {
                SubQuery sq = getViewSubquery((View) table);

//                sq.queryExpression = ((View) table).queryExpression;
                table = sq.getTable();
            }
        }

        boolean hasAs = false;

        if (token.tokenType == Tokens.AS) {
            read();
            checkIsNonCoreReservedIdentifier();

            hasAs = true;
        }

        if (isNonCoreReservedIdentifier()) {
            boolean limit = token.tokenType == Tokens.LIMIT
                            || token.tokenType == Tokens.OFFSET;
            int position = getPosition();

            alias = HsqlNameManager.getSimpleName(token.tokenString,
                                                  isDelimitedIdentifier());

            read();

            if (token.tokenType == Tokens.OPENBRACKET) {
                columnNameQuoted = new BitMap(32);
                columnList       = readColumnNames(columnNameQuoted, false);
            } else if (!hasAs && limit) {
                if (token.tokenType == Tokens.QUESTION
                        || token.tokenType == Tokens.X_VALUE) {
                    alias = null;

                    rewind(position);
                }
            }
        }

        if (columnList != null) {
            if (table.getColumnCount() != columnList.size()) {
                throw Error.error(ErrorCode.X_42593);
            }

            columnNameList = new SimpleName[columnList.size()];

            for (int i = 0; i < columnList.size(); i++) {
                SimpleName name =
                    HsqlNameManager.getSimpleName((String) columnList.get(i),
                                                  columnNameQuoted.isSet(i));

                columnNameList[i] = name;
            }
        }

        RangeVariable range = new RangeVariable(table, alias, columnList,
            columnNameList, compileContext);

        return range;
    }

    private Expression readAggregate() {

        int        tokenT = token.tokenType;
        String     funcName = token.tokenString;
        Expression aggExpr;

        read();
        readThis(Tokens.OPENBRACKET);

        aggExpr = readAggregateExpression(tokenT, funcName);

        readThis(Tokens.CLOSEBRACKET);
        if (token.tokenType == Tokens.OVER) {
            read();
            aggExpr = readWindowSpecification(tokenT, aggExpr);
            if (aggExpr instanceof ExpressionWindowed) {
            	ExpressionWindowed aggWinExpr = (ExpressionWindowed)aggExpr;
            	if (aggWinExpr.isDistinct()) {
            		throw Error.error("DISTINCT is not allowed in window functions.", "", -1);
            	}
            }
        }

        return aggExpr;
    }

    private ExpressionAggregate readAggregateExpression(int tokenT, String funcName) {

        int     type     = ParserDQL.getExpressionType(tokenT);
        boolean distinct = false;
        boolean all      = false;

        // If this is one of the RANK family, it takes no
        // parameters.  So, just return null here.  The caller
        // will know what to do with this.
        switch (tokenT) {
        case Tokens.RANK:
        case Tokens.DENSE_RANK:
        case Tokens.ROW_NUMBER:
        // No current support for WINDOWED_PERCENT_RANK or WINDOWED_CUME_DIST.
        // case Tokens.PERCENT_RANK:
        // case Tokens.CUME_DIST:
            if (token.tokenType != Tokens.CLOSEBRACKET) {
                throw Error.error("Expected a right parenthesis (')') here.", "", -1);
            }
            return null;
        }
        if (token.tokenType == Tokens.DISTINCT) {
            distinct = true;

            read();
        } else if (token.tokenType == Tokens.ALL) {
            all = true;

            read();
        }
        if (token.tokenType == Tokens.CLOSEBRACKET) {
            throw Error.error("Expected an expression here.", "", -1);
        }
        Expression e = XreadValueExpression();

        switch (type) {

            case OpTypes.COUNT :
                if (e.getType() == OpTypes.MULTICOLUMN) {
                    if (((ExpressionColumn) e).tableName != null) {
                        throw unexpectedToken();
                    }

                    e.opType = OpTypes.ASTERISK;

                    break;
                } else {
                    break;
                }
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
            // A VoltDB extension APPROX_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT :
            // End of VoltDB extension
                if (all || distinct) {
                    throw Error.error(ErrorCode.X_42582, all ? Tokens.T_ALL
                                                             : Tokens
                                                             .T_DISTINCT);
                }
                break;

            case OpTypes.USER_DEFINED_AGGREGATE :
                int functionid = FunctionForVoltDB.newVoltDBFunctionID(funcName);
                ExpressionAggregate aggregateExp = new ExpressionAggregate(type, distinct, e, functionid, funcName);
                return aggregateExp;

            default :
                if (e.getType() == OpTypes.ASTERISK) {
                    throw unexpectedToken();
                }
        }

        ExpressionAggregate aggregateExp = new ExpressionAggregate(type, distinct, e);

        return aggregateExp;
    }

    /**
     * This is a minimal parsing of the Window Specification.  We only use
     * partition by and order by lists.  There is a lot of complexity in the
     * full SQL specification which we don't parse at all.
     *
     * @param tokenT
     * @param aggExpr
     * @return
     */
    private Expression readWindowSpecification(int tokenT, Expression aggExpr) {
        SortAndSlice sortAndSlice = null;

        readThis(Tokens.OPENBRACKET);

        List<Expression> partitionByList = new ArrayList<>();
        if (token.tokenType == Tokens.PARTITION) {
            read();
            readThis(Tokens.BY);

            while (true) {
                Expression partitionExpr = XreadValueExpression();
                partitionByList.add(partitionExpr);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                    continue;
                }
                break;
            }
        }

        if (token.tokenType == Tokens.ORDER) {
            // order by clause
            read();
            readThis(Tokens.BY);
            sortAndSlice = XreadOrderBy();
        }
        readThis(Tokens.CLOSEBRACKET);

        // We don't really care about aggExpr any more.  It has the
        // aggregate expression as a non-windowed expression.  We do
        // care about its parameters and whether it's specified as
        // unique though.
        assert(aggExpr == null || aggExpr instanceof ExpressionAggregate);
        Expression nodes[];
        boolean isDistinct;
        if (aggExpr != null) {
            ExpressionAggregate winAggExpr = (ExpressionAggregate)aggExpr;
            nodes = winAggExpr.nodes;
            isDistinct = winAggExpr.isDistinctAggregate;
        } else {
            nodes = Expression.emptyExpressionArray;
            isDistinct = false;
        }
        ExpressionWindowed windowedExpr = new ExpressionWindowed(tokenT,
                                                                 nodes,
                                                                 isDistinct,
                                                                 sortAndSlice,
                                                                 partitionByList);

        return windowedExpr;
    }

    //--------------------------------------
    // returns null
    // := <unsigned literal> | <general value specification>
    Expression XreadValueSpecificationOrNull() {

        Expression e     = null;
        boolean    minus = false;

        switch (token.tokenType) {

            case Tokens.PLUS :
                read();
                break;

            case Tokens.MINUS :
                read();

                minus = true;
                break;
        }

        e = XreadUnsignedValueSpecificationOrNull();

        if (e == null) {
            return null;
        }

        if (minus) {
            e = new ExpressionArithmetic(OpTypes.NEGATE, e);
        }

        return e;
    }

    // returns null
    // <unsigned literl> | <general value specification>
    Expression XreadUnsignedValueSpecificationOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.TRUE :
                read();

                return Expression.EXPR_TRUE;

            case Tokens.FALSE :
                read();

                return Expression.EXPR_FALSE;

            case Tokens.DEFAULT :
                if (compileContext.contextuallyTypedExpression) {
                    read();

                    e = new ExpressionColumn(OpTypes.DEFAULT);

                    return e;
                }
                break;

            case Tokens.NULL :
                e = new ExpressionValue(null, null);

                read();

                return e;

            case Tokens.X_VALUE :
                e = new ExpressionValue(token.tokenValue, token.dataType);

                read();

                return e;

            case Tokens.X_DELIMITED_IDENTIFIER :
            case Tokens.X_IDENTIFIER :
                if (!token.isHostParameter) {
                    return null;
                }

            // $FALL-THROUGH$
            case Tokens.QUESTION :
                e = new ExpressionColumn(OpTypes.DYNAMIC_PARAM);

                compileContext.parameters.add(e);
                read();

                return e;

            case Tokens.COLLATION :
                return XreadCurrentCollationSpec();

            case Tokens.VALUE :
            case Tokens.CURRENT_CATALOG :
            case Tokens.CURRENT_DEFAULT_TRANSFORM_GROUP :
            case Tokens.CURRENT_PATH :
            case Tokens.CURRENT_ROLE :
            case Tokens.CURRENT_SCHEMA :
            case Tokens.CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            case Tokens.CURRENT_USER :
            case Tokens.SESSION_USER :
            case Tokens.SYSTEM_USER :
            case Tokens.USER :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    return null;
                }

                return readSQLFunction(function);

            // read SQL parameter reference
        }

        return null;
    }

    // <unsigned literl> | <parameter>
    Expression XreadSimpleValueSpecificationOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.X_VALUE :
                e = new ExpressionValue(token.tokenValue, token.dataType);

                read();

                return e;

            case Tokens.QUESTION :
                e = new ExpressionColumn(OpTypes.DYNAMIC_PARAM);

                compileContext.parameters.add(e);
                read();

                return e;

            default :
                return null;
        }
    }

    // combined <value expression primary> and <predicate>
    // exclusively called
    // <explicit row value constructor> needed for predicate
    Expression XreadAllTypesValueExpressionPrimary(boolean boole) {

        Expression e = null;

        switch (token.tokenType) {

            case Tokens.EXISTS :
            case Tokens.UNIQUE :
                if (boole) {
                    return XreadPredicate();
                }
                break;

            case Tokens.ROW :
                if (boole) {
                    break;
                }

                read();
                readThis(Tokens.OPENBRACKET);

                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
                break;

            default :
                e = XreadSimpleValueExpressionPrimary();
        }

        if (e == null && token.tokenType == Tokens.OPENBRACKET) {
            read();

            e = XreadRowElementList(true);

            readThis(Tokens.CLOSEBRACKET);
        }

        if (boole && e != null) {
            e = XreadPredicateRightPart(e);
        }

        return e;
    }

    // doesn't return null
    // <value expression primary> ::= <parenthesized value expression>
    // | <nonparenthesized value expression primary>
    Expression XreadValueExpressionPrimary() {

        Expression e;

        e = XreadSimpleValueExpressionPrimary();

        if (e != null) {
            return e;
        }

        if (token.tokenType == Tokens.OPENBRACKET) {
            read();

            e = XreadValueExpression();

            readThis(Tokens.CLOSEBRACKET);
        } else {
            return null;
        }

        return e;
    }

    // returns null
    //  <row value special case> :== this
    // <boolean predicand> :== this | <parenthesized boolean value expression>
    Expression XreadSimpleValueExpressionPrimary() {

        Expression e;

        e = XreadUnsignedValueSpecificationOrNull();

        if (e != null) {
            return e;
        }

        switch (token.tokenType) {

            case Tokens.OPENBRACKET :
                int position = getPosition();

                read();

                int subqueryPosition = getPosition();

                readOpenBrackets();

                switch (token.tokenType) {

                    case Tokens.TABLE :
                    case Tokens.VALUES :
                    case Tokens.SELECT :
                        SubQuery sq = null;

                        rewind(subqueryPosition);

                        try {
                            sq = XreadSubqueryBody(false,
                                                   OpTypes.SCALAR_SUBQUERY);

                            readThis(Tokens.CLOSEBRACKET);
                        } catch (HsqlException ex) {
                            compileContext.resetSubQueryLevel();
                            ex.setLevel(compileContext.subQueryDepth);

                            if (lastError == null
                                    || lastError.getLevel() < ex.getLevel()) {
                                lastError = ex;
                            }

                            rewind(position);

                            return null;
                        }

                        // A VoltDB extension to adapt to the hsqldb 2.3.2 change for fuller subquery support.
                        SubQuery td = sq;
                        // End of VoltDB extension
                        // BEGIN Cherry-picked code change from hsqldb-2.3.2
                        if (td.queryExpression.isSingleColumn()) {
                            e = new Expression(OpTypes.SCALAR_SUBQUERY, td);
                        } else {
                            e = new Expression(OpTypes.ROW_SUBQUERY, td);
                        }

                        return e;
                        /* Disable 5 lines ...
                        if (!sq.queryExpression.isSingleColumn()) {
                            throw Error.error(ErrorCode.W_01000);
                        }

                        return new Expression(OpTypes.SCALAR_SUBQUERY, sq);
                        ... disabled 5 lines. */
                        // END Cherry-picked code change from hsqldb-2.3.2

                    default :
                        rewind(position);

                        return null;
                }
            case Tokens.ASTERISK :
                e = new ExpressionColumn(token.namePrePrefix,
                                         token.namePrefix);

                recordExpressionForToken((ExpressionColumn) e);
                read();

                return e;

            case Tokens.CASEWHEN :
                return readCaseWhenExpression();

            case Tokens.CASE :
                return readCaseExpression();

            case Tokens.NULLIF :
                return readNullIfExpression();

            case Tokens.COALESCE :
            case Tokens.IFNULL :
                return readCoalesceExpression();

            case Tokens.CAST :
            case Tokens.CONVERT :
                return readCastExpression();

            case Tokens.DATE :
            case Tokens.TIME :
            case Tokens.TIMESTAMP :
            case Tokens.INTERVAL :
                e = readDateTimeIntervalLiteral();

                if (e != null) {
                    return e;
                }
                break;

            case Tokens.ANY :
            case Tokens.SOME :
            case Tokens.EVERY :
            case Tokens.COUNT :
            case Tokens.APPROX_COUNT_DISTINCT :
            case Tokens.MAX :
            case Tokens.MIN :
            case Tokens.SUM :
            case Tokens.AVG :
            case Tokens.STDDEV_POP :
            case Tokens.STDDEV_SAMP :
            case Tokens.VAR_POP :
            case Tokens.VAR_SAMP :
            case Tokens.RANK :
            case Tokens.DENSE_RANK:
            case Tokens.ROW_NUMBER:
                return readAggregate();

            case Tokens.NEXT :
                return readSequenceExpression();

            case Tokens.LEFT :
            case Tokens.RIGHT :
                // CLI function names
                break;


            default :
                if (isCoreReservedKey()) {
                    throw unexpectedToken();
                } else if (FunctionForVoltDB.newVoltDBFunction(token.tokenString) != null && FunctionForVoltDB.isUserDefineAggregate(token.tokenString)) {
                    return readAggregate();
                }
        }

        return readColumnOrFunctionExpression();
    }

    // OK - composite production -
    // <numeric primary> <charactr primary> <binary primary> <datetime primary> <interval primary>
    Expression XreadAllTypesPrimary(boolean boole) {

        Expression e = null;

        switch (token.tokenType) {

            case Tokens.SUBSTRING :
            case Tokens.SUBSTRING_REGEX :
            case Tokens.LOWER :
            case Tokens.UPPER :
            case Tokens.TRANSLATE_REGEX :
            case Tokens.TRIM :
            case Tokens.OVERLAY :
            case Tokens.NORMALIZE :

            //
            case Tokens.POSITION :
            case Tokens.OCCURRENCES_REGEX :
            case Tokens.POSITION_REGEX :
            case Tokens.EXTRACT :
            case Tokens.CHAR_LENGTH :
            case Tokens.CHARACTER_LENGTH :
            case Tokens.OCTET_LENGTH :
            case Tokens.CARDINALITY :
            case Tokens.ABS :
            case Tokens.MOD :
            case Tokens.LN :
            case Tokens.EXP :
            case Tokens.POWER :
            case Tokens.SQRT :
            case Tokens.FLOOR :
            case Tokens.CEILING :
            case Tokens.CEIL :
            case Tokens.WIDTH_BUCKET :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    throw unsupportedFeature();
                }

                e = readSQLFunction(function);
                break;

            default :
                e = XreadAllTypesValueExpressionPrimary(boole);
        }

        e = XreadModifier(e);

        return e;
    }

    Expression XreadModifier(Expression e) {

        switch (token.tokenType) {

            case Tokens.AT : {
                read();

                Expression e1 = null;

                if (token.tokenType == Tokens.LOCAL) {
                    read();
                } else {
                    readThis(Tokens.TIME);
                    readThis(Tokens.ZONE);

                    e1 = XreadValueExpressionPrimary();

                    switch (token.tokenType) {

                        case Tokens.YEAR :
                        case Tokens.MONTH :
                        case Tokens.DAY :
                        case Tokens.HOUR :
                        case Tokens.MINUTE :
                        case Tokens.SECOND : {
                            IntervalType type = readIntervalType();

                            if (e1.getType() == OpTypes.SUBTRACT) {
                                e1.dataType = type;
                            } else {
                                e1 = new ExpressionOp(e1, type);
                            }
                        }
                    }
                }

                e = new ExpressionOp(OpTypes.ZONE_MODIFIER, e, e1);

                break;
            }
            case Tokens.YEAR :
            case Tokens.MONTH :
            case Tokens.DAY :
            case Tokens.HOUR :
            case Tokens.MINUTE :
            case Tokens.SECOND : {
                IntervalType type = readIntervalType();

                if (e.getType() == OpTypes.SUBTRACT) {
                    e.dataType = type;
                } else {
                    e = new ExpressionOp(e, type);
                }

                break;
            }
            case Tokens.COLLATE : {
                read();

                SchemaObject collation =
                    database.schemaManager.getSchemaObject(token.namePrefix,
                        token.tokenString, SchemaObject.COLLATION);
            }
        }

        return e;
    }

    Expression XreadValueExpressionWithContext() {

        Expression e;

        compileContext.contextuallyTypedExpression = true;
        e = XreadValueExpressionOrNull();
        compileContext.contextuallyTypedExpression = false;

        return e;
    }

    Expression XreadValueExpressionOrNull() {
        return XreadAllTypesCommonValueExpression(true);
    }

    /**
     *     <value expression> ::=
     *   <common value expression>
     *   | <boolean value expression>
     *   | <row value expression>
     *
     */
    Expression XreadValueExpression() {
        return XreadAllTypesCommonValueExpression(true);
    }

    // union of <numeric | datetime | string | interval value expression>
    Expression XreadRowOrCommonValueExpression() {
        return XreadAllTypesCommonValueExpression(false);
    }

    // union of <numeric | datetime | string | interval | boolean value expression>
    // no <row value expression> and no <predicate>
    Expression XreadAllTypesCommonValueExpression(boolean boole) {

        Expression e    = XreadAllTypesTerm(boole);
        int        type = 0;
        boolean    end  = false;

        while (true) {
            switch (token.tokenType) {

                case Tokens.PLUS :
                    type  = OpTypes.ADD;
                    boole = false;
                    break;

                case Tokens.MINUS :
                    type  = OpTypes.SUBTRACT;
                    boole = false;
                    break;

                case Tokens.CONCAT :
                    type  = OpTypes.CONCAT;
                    boole = false;
                    break;

                case Tokens.OR :
                    if (boole) {
                        type = OpTypes.OR;

                        break;
                    }

                // $FALL-THROUGH$
                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }

            read();

            Expression a = e;

            e = XreadAllTypesTerm(boole);
            e = boole ? new ExpressionLogical(type, a, e)
                      : new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadAllTypesTerm(boolean boole) {

        Expression e    = XreadAllTypesFactor(boole);
        int        type = 0;
        boolean    end  = false;

        while (true) {
            switch (token.tokenType) {

                case Tokens.ASTERISK :
                    type  = OpTypes.MULTIPLY;
                    boole = false;
                    break;

                case Tokens.DIVIDE :
                    type  = OpTypes.DIVIDE;
                    boole = false;
                    break;

                case Tokens.AND :
                    if (boole) {
                        type = OpTypes.AND;

                        break;
                    }

                // $FALL-THROUGH$
                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }

            read();

            Expression a = e;

            e = XreadAllTypesFactor(boole);

            if (e == null) {
                throw unexpectedToken();
            }

            e = boole ? new ExpressionLogical(type, a, e)
                      : new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadAllTypesFactor(boolean boole) {

        Expression e;
        boolean    minus   = false;
        boolean    not     = false;
        boolean    unknown = false;

        switch (token.tokenType) {

            case Tokens.PLUS :
                read();

                boole = false;
                break;

            case Tokens.MINUS :
                read();

                boole = false;
                minus = true;
                break;

            case Tokens.NOT :
                if (boole) {
                    read();

                    not = true;
                }
                break;
        }

        e = XreadAllTypesPrimary(boole);

        if (boole && token.tokenType == Tokens.IS) {
            read();

            if (token.tokenType == Tokens.NOT) {
                read();

                not = !not;
            }

            if (token.tokenType == Tokens.TRUE) {
                read();
            } else if (token.tokenType == Tokens.FALSE) {
                read();

                not = !not;
            } else if (token.tokenType == Tokens.UNKNOWN) {
                read();

                unknown = true;
            } else {
                throw unexpectedToken();
            }
        }

        if (unknown) {
            e = new ExpressionLogical(OpTypes.IS_NULL, e);
        } else if (minus) {
            e = new ExpressionArithmetic(OpTypes.NEGATE, e);
        } else if (not) {
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    Expression XreadStringValueExpression() {

        return XreadCharacterValueExpression();

//        XreadBinaryValueExpression();
    }

    Expression XreadCharacterValueExpression() {

        Expression   e         = XreadCharacterPrimary();
        SchemaObject collation = readCollateClauseOrNull();

        while (token.tokenType == Tokens.CONCAT) {
            read();

            Expression a = e;

            e         = XreadCharacterPrimary();
            collation = readCollateClauseOrNull();
            e         = new ExpressionArithmetic(OpTypes.CONCAT, a, e);
        }

        return e;
    }

    Expression XreadCharacterPrimary() {

        switch (token.tokenType) {

            case Tokens.SUBSTRING :

//            case Token.SUBSTRING_REGEX :
            case Tokens.LOWER :
            case Tokens.UPPER :

//            case Token.TRANSLATE_REGEX :
            case Tokens.TRIM :
            case Tokens.OVERLAY :

//            case Token.NORMALIZE :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                return readSQLFunction(function);

            default :
        }

        return XreadValueExpressionPrimary();
    }

    Expression XreadNumericPrimary() {

        switch (token.tokenType) {

            case Tokens.POSITION :

//            case Token.OCCURRENCES_REGEX :
//            case Token.POSITION_REGEX :
            case Tokens.EXTRACT :
            case Tokens.CHAR_LENGTH :
            case Tokens.CHARACTER_LENGTH :
            case Tokens.OCTET_LENGTH :
            case Tokens.CARDINALITY :
            case Tokens.ABS :
            case Tokens.MOD :
            case Tokens.LN :
            case Tokens.EXP :
            case Tokens.POWER :
            case Tokens.SQRT :
            case Tokens.FLOOR :
            case Tokens.CEILING :
            case Tokens.CEIL :

//            case Token.WIDTH_BUCKET :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    throw super.unexpectedToken();
                }

                return readSQLFunction(function);

            default :
        }

        return XreadValueExpressionPrimary();
    }

    Expression XreadNumericValueExpression() {

        Expression e = XreadTerm();

        while (true) {
            int type;

            if (token.tokenType == Tokens.PLUS) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS) {
                type = OpTypes.SUBTRACT;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadTerm();
            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadTerm() {

        Expression e = XreadFactor();
        int        type;

        while (true) {
            if (token.tokenType == Tokens.ASTERISK) {
                type = OpTypes.MULTIPLY;
            } else if (token.tokenType == Tokens.DIVIDE) {
                type = OpTypes.DIVIDE;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadFactor();

            if (e == null) {
                throw unexpectedToken();
            }

            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadFactor() {

        Expression e;
        boolean    minus = false;

        if (token.tokenType == Tokens.PLUS) {
            read();
        } else if (token.tokenType == Tokens.MINUS) {
            read();

            minus = true;
        }

        e = XreadNumericPrimary();

        if (e == null) {
            return null;
        }

        if (minus) {
            e = new ExpressionArithmetic(OpTypes.NEGATE, e);
        }

        return e;
    }

    Expression XreadDatetimeValueExpression() {

        Expression e = XreadDateTimeIntervalTerm();

        while (true) {
            int type;

            if (token.tokenType == Tokens.PLUS) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS) {
                type = OpTypes.SUBTRACT;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadDateTimeIntervalTerm();
            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadIntervalValueExpression() {

        Expression e = XreadDateTimeIntervalTerm();

        while (true) {
            int type;

            if (token.tokenType == Tokens.PLUS) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS) {
                type = OpTypes.SUBTRACT;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadDateTimeIntervalTerm();
            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadDateTimeIntervalTerm() {

        switch (token.tokenType) {

            case Tokens.CURRENT_DATE :
            case Tokens.CURRENT_TIME :
            case Tokens.CURRENT_TIMESTAMP :
            case Tokens.LOCALTIME :
            case Tokens.LOCALTIMESTAMP :

            //
            case Tokens.ABS :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    throw super.unexpectedToken();
                }

                return readSQLFunction(function);

            default :
        }

        return XreadValueExpressionPrimary();
    }

    // returns null
    Expression XreadDateTimeValueFunctionOrNull() {

        FunctionSQL function = null;

        switch (token.tokenType) {

            case Tokens.CURRENT_DATE :
            case Tokens.CURRENT_TIME :
            case Tokens.CURRENT_TIMESTAMP :
            case Tokens.LOCALTIME :
            case Tokens.LOCALTIMESTAMP :
                function = FunctionSQL.newSQLFunction(token.tokenString,
                                                      compileContext);
                break;

            case Tokens.NOW :
            case Tokens.TODAY :
                function = FunctionCustom.newCustomFunction(token.tokenString,
                        token.tokenType);
                break;

            default :
                return null;
        }

        if (function == null) {
            throw super.unexpectedToken();
        }

        return readSQLFunction(function);
    }

    Expression XreadBooleanValueExpression() {

        try {
            Expression e = XreadBooleanTermOrNull();

            if (e == null) {
                throw Error.error(ErrorCode.X_42568);
            }

            while (true) {
                int type;

                if (token.tokenType == Tokens.OR) {
                    type = OpTypes.OR;
                } else {
                    break;
                }

                read();

                Expression a = e;

                e = XreadBooleanTermOrNull();
                if (e == null) {
                    throw Error.error(ErrorCode.X_42568);
                }
                e = new ExpressionLogical(type, a, e);
            }

            if (e == null) {
                throw Error.error(ErrorCode.X_42568);
            }

            return e;
        } catch (HsqlException ex) {
            ex.setLevel(compileContext.subQueryDepth);

            if (lastError == null || lastError.getLevel() < ex.getLevel()) {
                lastError = ex;
            }

            throw lastError;
        }
    }

    Expression XreadBooleanTermOrNull() {

        Expression e = XreadBooleanFactorOrNull();
        int        type;

        while (true) {
            if (token.tokenType == Tokens.AND) {
                type = OpTypes.AND;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadBooleanFactorOrNull();
            if (e == null) {
                throw Error.error(ErrorCode.X_42568);
            }
            e = new ExpressionLogical(type, a, e);
        }

        return e;
    }

    Expression XreadBooleanFactorOrNull() {

        Expression e;
        boolean    not     = false;
        boolean    unknown = false;

        if (token.tokenType == Tokens.NOT) {
            read();

            not = true;
        }

        e = XreadBooleanPrimaryOrNull();

        if (e == null) {
            return null;
        }

        if (token.tokenType == Tokens.IS) {
            read();

            if (token.tokenType == Tokens.NOT) {
                read();

                not = !not;
            }

            if (token.tokenType == Tokens.TRUE) {
                read();
            } else if (token.tokenType == Tokens.FALSE) {
                not = !not;

                read();
            } else if (token.tokenType == Tokens.UNKNOWN) {
                unknown = true;

                read();
            } else {
                throw unexpectedToken();
            }
        }

        if (unknown) {
            e = new ExpressionLogical(OpTypes.IS_NULL, e);
        }

        if (not) {
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    // <boolean primary> ::= <predicate> | <boolean predicand>
    Expression XreadBooleanPrimaryOrNull() {

        Expression e = null;
        int        position;

        switch (token.tokenType) {

            case Tokens.EXISTS :
            case Tokens.UNIQUE :
                return XreadPredicate();

            case Tokens.ROW :
                read();
                readThis(Tokens.OPENBRACKET);

                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
                break;

            default :
                position = getPosition();

                try {
                    e = XreadAllTypesCommonValueExpression(false);
                } catch (HsqlException ex) {
                    ex.setLevel(compileContext.subQueryDepth);

                    if (lastError == null
                            || lastError.getLevel() < ex.getLevel()) {
                        lastError = ex;
                    }

                    rewind(position);
                }
        }

        if (e == null && token.tokenType == Tokens.OPENBRACKET) {
            read();

            position = getPosition();

            try {
                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
            } catch (HsqlException ex) {
                ex.setLevel(compileContext.subQueryDepth);

                if (lastError == null
                        || lastError.getLevel() < ex.getLevel()) {
                    lastError = ex;
                }

                rewind(position);

                e = XreadBooleanValueExpression();

                readThis(Tokens.CLOSEBRACKET);
            }
        }

        if (e != null) {
            e = XreadPredicateRightPart(e);
        }

        return e;
    }

    // similar to <value expression primary>
    Expression XreadBooleanPredicand() {

        Expression e;

        if (token.tokenType == Tokens.OPENBRACKET) {
            read();

            e = XreadBooleanValueExpression();

            readThis(Tokens.CLOSEBRACKET);

            return e;
        } else {
            return XreadSimpleValueExpressionPrimary();
        }
    }

    Expression XreadPredicate() {

        switch (token.tokenType) {

            case Tokens.EXISTS : {
                read();

                Expression s = XreadTableSubqueryForPredicate(OpTypes.EXISTS);

                return new ExpressionLogical(OpTypes.EXISTS, s);
            }
            case Tokens.UNIQUE : {
                read();

                Expression s = XreadTableSubqueryForPredicate(OpTypes.UNIQUE);

                return new ExpressionLogical(OpTypes.UNIQUE, s);
            }
            default : {
                Expression a = XreadRowValuePredicand();

                return XreadPredicateRightPart(a);
            }
        }
    }

    Expression XreadPredicateRightPart(final Expression l) {

        boolean           hasNot = false;
        ExpressionLogical e      = null;
        Expression        r;

        if (token.tokenType == Tokens.NOT) {
            read();

            hasNot = true;
        }

        switch (token.tokenType) {

            case Tokens.IS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                read();

                if (token.tokenType == Tokens.NOT) {
                    hasNot = true;

                    read();
                }

                if (token.tokenType == Tokens.DISTINCT) {
                    read();
                    readThis(Tokens.FROM);

                    r      = XreadRowValuePredicand();
                    e      = new ExpressionLogical(OpTypes.NOT_DISTINCT, l, r);
                    hasNot = !hasNot;

                    break;
                }

                if (token.tokenType == Tokens.NULL
                        || token.tokenType == Tokens.UNKNOWN) {
                    read();

                    e = new ExpressionLogical(OpTypes.IS_NULL, l);

                    break;
                }

                throw unexpectedToken();
            }
            case Tokens.LIKE : {
                e                = XreadLikePredicateRightPart(l);
                e.noOptimisation = isCheckOrTriggerCondition;

                break;
            }
            case Tokens.STARTS : {
                read();

                e                = XStartsWithPredicateRightPart(l);
                e.noOptimisation = isCheckOrTriggerCondition;

                break;
            }
            case Tokens.BETWEEN : {
                e = XreadBetweenPredicateRightPart(l);

                break;
            }
            case Tokens.IN : {
                e                = XreadInPredicateRightPart(l);
                e.noOptimisation = isCheckOrTriggerCondition;

                break;
            }
            case Tokens.OVERLAPS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                e = XreadOverlapsPredicateRightPart(l);

                break;
            }
            case Tokens.EQUALS :
            case Tokens.GREATER_EQUALS :
            case Tokens.GREATER :
            case Tokens.LESS :
            case Tokens.LESS_EQUALS :
            case Tokens.NOT_EQUALS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                int type = getExpressionType(token.tokenType);

                read();

                switch (token.tokenType) {

                    case Tokens.ANY :
                    case Tokens.SOME :
                    case Tokens.ALL :
                        e = XreadQuantifiedComparisonRightPart(type, l);
                        break;

                    default : {
                        Expression row = XreadRowValuePredicand();

                        e = new ExpressionLogical(type, l, row);

                        break;
                    }
                }

                break;
            }
            case Tokens.MATCH : {
                e = XreadMatchPredicateRightPart(l);

                break;
            }
            default : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                return l;
            }
        }

        if (hasNot) {
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    private ExpressionLogical XreadBetweenPredicateRightPart(
            final Expression a) {

        boolean symmetric = false;

        read();

        if (token.tokenType == Tokens.ASYMMETRIC) {
            read();
        } else if (token.tokenType == Tokens.SYMMETRIC) {
            symmetric = true;

            read();
        }

        Expression left = XreadRowValuePredicand();

        readThis(Tokens.AND);

        Expression right = XreadRowValuePredicand();

        if (a.isParam() && left.isParam()) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (a.isParam() && right.isParam()) {
            throw Error.error(ErrorCode.X_42567);
        }

        Expression l = new ExpressionLogical(OpTypes.GREATER_EQUAL, a, left);
        Expression r = new ExpressionLogical(OpTypes.SMALLER_EQUAL, a, right);
        ExpressionLogical leftToRight = new ExpressionLogical(OpTypes.AND, l,
            r);

        if (symmetric) {
            l = new ExpressionLogical(OpTypes.SMALLER_EQUAL, a, left);
            r = new ExpressionLogical(OpTypes.GREATER_EQUAL, a, right);

            Expression rightToLeft = new ExpressionLogical(OpTypes.AND, l, r);

            return new ExpressionLogical(OpTypes.OR, leftToRight, rightToLeft);
        } else {
            return leftToRight;
        }
    }

    private ExpressionLogical XreadQuantifiedComparisonRightPart(int exprType,
            Expression l) {

        int        tokenT      = token.tokenType;
        int        exprSubType = 0;
        Expression e;

        switch (token.tokenType) {

            case Tokens.ANY :
            case Tokens.SOME :
                exprSubType = OpTypes.ANY_QUANTIFIED;
                break;

            case Tokens.ALL :
                exprSubType = OpTypes.ALL_QUANTIFIED;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Parser");
        }

        read();
        readThis(Tokens.OPENBRACKET);

        int position = getPosition();

        readOpenBrackets();

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT :
                rewind(position);

                SubQuery sq = XreadSubqueryBody(false, OpTypes.IN);

                e = new Expression(OpTypes.TABLE_SUBQUERY, sq);

                readThis(Tokens.CLOSEBRACKET);
                break;

            default :
                rewind(position);

                e = readAggregateExpression(tokenT, "");

                if (e == null) {
                    throw Error.error("Unsupported aggregate expression " + Tokens.getKeyword(tokenT), "", 0);
                }

                readThis(Tokens.CLOSEBRACKET);
        }

        ExpressionLogical r = new ExpressionLogical(exprType, l, e);

        r.exprSubType = exprSubType;

        return r;
    }

    private ExpressionLogical XreadInPredicateRightPart(Expression l) {

        int        degree = l.getDegree();
        Expression e      = null;

        read();
        // A VoltDB extension to add support for x IN ?
        if (token.tokenType == Tokens.QUESTION &&
            ! isCheckOrTriggerCondition) {
            read();
            e = new ExpressionColumn(OpTypes.DYNAMIC_PARAM);
            compileContext.parameters.add(e);
            e.nodeDataTypes = new Type[degree];
            ExpressionLogical r = new ExpressionLogical(OpTypes.EQUAL, l, e);
            r.exprSubType = OpTypes.ANY_QUANTIFIED;
            return r;
        }
        // End of VoltDB extension
        readThis(Tokens.OPENBRACKET);

        int position = getPosition();

        readOpenBrackets();

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT : {
                rewind(position);

                SubQuery sq = XreadSubqueryBody(false, OpTypes.IN);

                e = new Expression(OpTypes.TABLE_SUBQUERY, sq);

                readThis(Tokens.CLOSEBRACKET);

                break;
            }
            default : {
                rewind(position);

                e = XreadInValueListConstructor(degree);

                readThis(Tokens.CLOSEBRACKET);

                break;
            }
        }

        ExpressionLogical r;

        if (isCheckOrTriggerCondition) {
            r = new ExpressionLogical(OpTypes.IN, l, e);
        } else {
            r             = new ExpressionLogical(OpTypes.EQUAL, l, e);
            r.exprSubType = OpTypes.ANY_QUANTIFIED;
        }

        return r;
    }

    Expression XreadInValueList(int degree) {
        List<Expression> rowList = new ArrayList<>();
        while (true) {
            Expression row = XreadValueExpression();
            if (row.getType() == OpTypes.ROW) {
                // Disallow rows of the wrong length or rows of rows.
                if (row.nodes.length != degree) {
                    // SQL error message
                    throw unexpectedToken();
                }
                for (Expression value : row.nodes) {
                    if (value.getType() == OpTypes.ROW) {
                        // SQL error message
                        throw unexpectedToken();
                    }
                }
            }
            // Degree > 1 can only be satisfied by a list of row expressions.
            else if (degree != 1) {
                throw unexpectedToken();
            }
            // Make a degree 1 row from a non-row value expression.
            else {
                row = new Expression(OpTypes.ROW, new Expression[]{ row });
            }
            rowList.add(row);

            if (token.tokenType != Tokens.COMMA) {
                break;
            }
            read(); // consume the comma
        }

        Expression[] rowArray = new Expression[rowList.size()];
        rowList.toArray(rowArray);

        return new Expression(OpTypes.TABLE, rowArray);
    }

    private ExpressionLogical XreadLikePredicateRightPart(Expression a) {

        read();

        Expression b      = XreadStringValueExpression();
        Expression escape = null;

        if (token.tokenString.equals(Tokens.T_ESCAPE)) {
            read();

            escape = XreadStringValueExpression();
        }

        return new ExpressionLike(a, b, escape,
                                  this.isCheckOrTriggerCondition);
    }

    /**
     *  Scan the right-side string value, return a STARTS WITH Expression for generating XML
     *
     * @param a ExpressionColumn
     */
    private ExpressionLogical XStartsWithPredicateRightPart(Expression left) {

        readThis(Tokens.WITH);

        if (token.tokenType == Tokens.QUESTION) {    // handle user parameter case
            Expression right = XreadRowValuePredicand();
            if (left.isParam() && right.isParam()) {  // again make sure the left side is valid
                throw Error.error(ErrorCode.X_42567);
            }
            /** In this case, we make the right parameter as the lower bound,
             *  and the right parameter concatenating a special char (greater than any other chars) as the upper bound.
             *  It now becomes a range scan for all the strings with right parameter as its prefix.
             */
            Expression l = new ExpressionLogical(OpTypes.GREATER_EQUAL, left, right);
            Expression r = new ExpressionLogical(OpTypes.SMALLER_EQUAL, left,
                    new ExpressionArithmetic(OpTypes.CONCAT, right, new ExpressionValue("\uffff", Type.SQL_CHAR)));
            return new ExpressionLogical(OpTypes.AND, l, r);
        } else {          // handle plain string value and the column
            Expression right      = XreadStringValueExpression();
            return new ExpressionStartsWith(left, right, this.isCheckOrTriggerCondition);
        }
    }

    private ExpressionLogical XreadMatchPredicateRightPart(Expression a) {

        boolean isUnique  = false;
        int     matchType = OpTypes.MATCH_SIMPLE;

        read();

        if (token.tokenType == Tokens.UNIQUE) {
            read();

            isUnique = true;
        }

        if (token.tokenType == Tokens.SIMPLE) {
            read();

            matchType = isUnique ? OpTypes.MATCH_UNIQUE_SIMPLE
                                 : OpTypes.MATCH_SIMPLE;
        } else if (token.tokenType == Tokens.PARTIAL) {
            read();

            matchType = isUnique ? OpTypes.MATCH_UNIQUE_PARTIAL
                                 : OpTypes.MATCH_PARTIAL;
        } else if (token.tokenType == Tokens.FULL) {
            read();

            matchType = isUnique ? OpTypes.MATCH_UNIQUE_FULL
                                 : OpTypes.MATCH_FULL;
        }

        int        mode = isUnique ? OpTypes.TABLE_SUBQUERY
                                   : OpTypes.IN;
        Expression s    = XreadTableSubqueryForPredicate(mode);

        return new ExpressionLogical(matchType, a, s);
    }

    private ExpressionLogical XreadOverlapsPredicateRightPart(Expression l) {

        if (l.getType() != OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        if (l.nodes.length != 2) {
            throw Error.error(ErrorCode.X_42564);
        }

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            throw unexpectedToken();
        }

        Expression r = XreadRowValuePredicand();

        if (r.nodes.length != 2) {
            throw Error.error(ErrorCode.X_42564);
        }

        return new ExpressionLogical(OpTypes.OVERLAPS, l, r);
    }

    Expression XreadRowValueExpression() {

        Expression e = XreadExplicitRowValueConstructorOrNull();

        if (e != null) {
            return e;
        }

        return XreadRowValueSpecialCase();
    }

    Expression XreadTableRowValueConstructor() {

        Expression e = XreadExplicitRowValueConstructorOrNull();

        if (e != null) {
            return e;
        }

        return XreadRowValueSpecialCase();
    }

    //  union of <row value expression> |
    // <boolean predicand> | <non parenthesized value expression primary> |
    //  translated to <explicit row value constructor>
    // <value expression primary> | <non parenthesized value expression primary> |
    Expression XreadRowValuePredicand() {
        return XreadRowOrCommonValueExpression();
    }

    Expression XreadRowValueSpecialCase() {
        return XreadSimpleValueExpressionPrimary();
    }

    // <row value constructor>
    // ISSUE - XreadCommonValueExpression and XreadBooleanValueExpression should merge
    Expression XreadRowValueConstructor() {

        Expression e;

        e = XreadExplicitRowValueConstructorOrNull();

        if (e != null) {
            return e;
        }

        e = XreadRowOrCommonValueExpression();

        if (e != null) {
            return e;
        }

        return XreadBooleanValueExpression();
    }

    // returns null
    // must be called in conjusnction with <parenthesized ..
    Expression XreadExplicitRowValueConstructorOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.OPENBRACKET : {
                read();

                int position = getPosition();
                int brackets = readOpenBrackets();

                switch (token.tokenType) {

                    case Tokens.TABLE :
                    case Tokens.VALUES :
                    case Tokens.SELECT :
                        rewind(position);

                        SubQuery sq = XreadSubqueryBody(false,
                                                        OpTypes.ROW_SUBQUERY);

                        readThis(Tokens.CLOSEBRACKET);

                        return new Expression(OpTypes.ROW_SUBQUERY, sq);

                    default :
                        rewind(position);

                        e = XreadRowElementList(true);

                        readThis(Tokens.CLOSEBRACKET);

                        return e;
                }
            }
            case Tokens.ROW : {
                read();
                readThis(Tokens.OPENBRACKET);

                e = XreadRowElementList(false);

                readThis(Tokens.CLOSEBRACKET);

                return e;
            }
        }

        return null;
    }

    Expression XreadRowElementList(boolean multiple) {

        Expression    e;
        HsqlArrayList list = new HsqlArrayList();

        while (true) {
            e = XreadValueExpression();

            list.add(e);

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            if (multiple && list.size() == 1) {
                return e;
            }

            break;
        }

        Expression[] array = new Expression[list.size()];

        list.toArray(array);

        return new Expression(OpTypes.ROW, array);
    }

    Expression XreadCurrentCollationSpec() {
        throw Error.error(ErrorCode.X_0A000);
    }

    Expression XreadRowSubquery() {

        readThis(Tokens.OPENBRACKET);

        SubQuery sq = XreadSubqueryBody(false, OpTypes.ROW_SUBQUERY);

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.ROW_SUBQUERY, sq);
    }

    Expression XreadTableSubqueryForPredicate(int mode) {

        readThis(Tokens.OPENBRACKET);

        SubQuery sq = XreadSubqueryBody(false, mode);

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.TABLE_SUBQUERY, sq);
    }

    Expression XreadTableSubqueryOrJoinedTable() {

        boolean joinedTable = false;
        int     position;

        readThis(Tokens.OPENBRACKET);

        position = getPosition();

        readOpenBrackets();

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT :
            case Tokens.WITH :
                break;

            default :
                joinedTable = true;
        }

        rewind(position);

        if (joinedTable) {
            SubQuery sq = XreadJoinedTableAsSubquery();

            readThis(Tokens.CLOSEBRACKET);

            return new Expression(OpTypes.TABLE_SUBQUERY, sq);
        } else {
            SubQuery sq = XreadTableSubqueryBody();

            readThis(Tokens.CLOSEBRACKET);

            return new Expression(OpTypes.TABLE_SUBQUERY, sq);
        }
    }

    SubQuery XreadJoinedTableAsSubquery() {

        compileContext.subQueryDepth++;

        QueryExpression queryExpression = XreadJoinedTable();

        queryExpression.resolve(session);

        if (((QuerySpecification) queryExpression).rangeVariables.length < 2) {
            throw unexpectedTokenRequire(Tokens.T_JOIN);
        }

        SubQuery sq = new SubQuery(database, compileContext.subQueryDepth,
                                   queryExpression, OpTypes.TABLE_SUBQUERY);

        sq.prepareTable(session);

        compileContext.subQueryDepth--;

        compileContext.subQueryList.add(sq);

        return sq;
    }

    QueryExpression XreadJoinedTable() {

        QuerySpecification select = new QuerySpecification(compileContext);
        Expression         e      = new ExpressionColumn(OpTypes.MULTICOLUMN);

        select.addSelectColumnExpression(e);
        XreadTableReference(select);

        return select;
    }

    SubQuery XreadTableSubqueryBody() {

        SubQuery sq = XreadSubqueryBody(true, OpTypes.TABLE_SUBQUERY);

/*
        Select   select = sq.queryExpression.getMainSelect();

        for (int i = 0; i < select.indexLimitVisible; i++) {
            String colname = select.exprColumns[i].getAlias();

            if (colname == null || colname.length() == 0) {

                // fredt - this does not guarantee the uniqueness of column
                // names but addColumns() will throw later if names are not unique.
                colname = HsqlNameManager.getAutoColumnNameString(i);

                select.exprColumns[i].setAlias(colname, false);
            }
        }
*/
        sq.prepareTable(session);

        return sq;
    }

    SubQuery XreadSubqueryBody(boolean resolve, int mode) {

        compileContext.subQueryDepth++;

        QueryExpression queryExpression = XreadQueryExpression();

        if (resolve) {
            queryExpression.resolve(session);
        } else {
            queryExpression.resolveReferences(session);
        }

        SubQuery sq = new SubQuery(database, compileContext.subQueryDepth,
                                   queryExpression, mode);

        compileContext.subQueryList.add(sq);

        compileContext.subQueryDepth--;

        return sq;
    }

    SubQuery XreadViewSubquery(View view) {

        compileContext.subQueryDepth++;

        QueryExpression queryExpression;

        try {
            queryExpression = XreadQueryExpression();
        } catch (HsqlException e) {
            queryExpression = XreadJoinedTable();
        }

        queryExpression.setAsTopLevel();
        queryExpression.setView(view);
        queryExpression.resolve(session);

        SubQuery sq = new SubQuery(database, compileContext.subQueryDepth,
                                   queryExpression, view);

        compileContext.subQueryList.add(sq);

        compileContext.subQueryDepth--;

        return sq;
    }

// Additional Common Elements
// returns null
    SchemaObject readCollateClauseOrNull() {

        if (token.tokenType == Tokens.COLLATE) {
            read();

            SchemaObject collation =
                database.schemaManager.getSchemaObject(token.namePrefix,
                    token.tokenString, SchemaObject.COLLATION);

            return collation;
        }

        return null;
    }

    Expression readRow() {

        Expression r = null;

        while (true) {
            Expression e = XreadValueExpressionWithContext();

            if (r == null) {
                r = e;
            } else if (r.getType() == OpTypes.ROW) {
                if (e.getType() == OpTypes.ROW
                        && r.nodes[0].getType() != OpTypes.ROW) {
                    r = new Expression(OpTypes.ROW, new Expression[] {
                        r, e
                    });
                } else {
                    r.nodes = (Expression[]) ArrayUtil.resizeArray(r.nodes,
                            r.nodes.length + 1);
                    r.nodes[r.nodes.length - 1] = e;
                }
            } else {
                r = new Expression(OpTypes.ROW, new Expression[] {
                    r, e
                });
            }

            if (token.tokenType != Tokens.COMMA) {
                break;
            }

            read();
        }

        return r;
    }

    Expression XreadContextuallyTypedTable(int degree) {

        Expression   e       = readRow();
        Expression[] list    = e.nodes;
        boolean      isTable = false;

        if (degree == 1) {
            if (e.getType() == OpTypes.ROW) {
                e.opType = OpTypes.TABLE;

                for (int i = 0; i < list.length; i++) {
                    if (list[i].getType() != OpTypes.ROW) {
                        list[i] = new Expression(OpTypes.ROW,
                                                 new Expression[]{ list[i] });
                    } else if (list[i].nodes.length != degree) {
                        throw Error.error(ErrorCode.X_42564);
                    }
                }

                return e;
            } else {
                e = new Expression(OpTypes.ROW, new Expression[]{ e });
                e = new Expression(OpTypes.TABLE, new Expression[]{ e });

                return e;
            }
        }

        if (e.getType() != OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        for (int i = 0; i < list.length; i++) {
            if (list[i].getType() == OpTypes.ROW) {
                isTable = true;

                break;
            }
        }

        if (isTable) {
            e.opType = OpTypes.TABLE;

            for (int i = 0; i < list.length; i++) {
                if (list[i].getType() != OpTypes.ROW) {
                    throw Error.error(ErrorCode.X_42564);
                }

                Expression[] args = list[i].nodes;

                if (args.length != degree) {
                    throw Error.error(ErrorCode.X_42564);
                }

                for (int j = 0; j < degree; j++) {
                    if (args[j].getType() == OpTypes.ROW) {
                        throw Error.error(ErrorCode.X_42564);
                    }
                }
            }
        } else {
            if (list.length != degree) {
                throw Error.error(ErrorCode.X_42564);
            }

            e = new Expression(OpTypes.TABLE, new Expression[]{ e });
        }

        return e;
    }

    private Expression XreadInValueListConstructor(int degree) {

        compileContext.subQueryDepth++;

        Expression e = XreadInValueList(degree);
        SubQuery sq = new SubQuery(database, compileContext.subQueryDepth, e,
                                   OpTypes.IN);

        compileContext.subQueryList.add(sq);

        compileContext.subQueryDepth--;

        return e;
    }

    private SubQuery XreadRowValueExpressionList() {

        compileContext.subQueryDepth++;

        Expression e = XreadRowValueExpressionListBody();
        HsqlList unresolved =
            e.resolveColumnReferences(RangeVariable.emptyArray, null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);
        e.prepareTable(session, null, e.nodes[0].nodes.length);

        SubQuery sq = new SubQuery(database, compileContext.subQueryDepth, e,
                                   OpTypes.TABLE);

        sq.prepareTable(session);
        compileContext.subQueryList.add(sq);

        compileContext.subQueryDepth--;

        return sq;
    }

    Expression XreadRowValueExpressionListBody() {

        Expression r = null;

        while (true) {
            int        brackets = readOpenBrackets();
            Expression e        = readRow();

            readCloseBrackets(brackets);

            if (r == null) {
                r = new Expression(OpTypes.ROW, new Expression[]{ e });
            } else {
                r.nodes = (Expression[]) ArrayUtil.resizeArray(r.nodes,
                        r.nodes.length + 1);
                r.nodes[r.nodes.length - 1] = e;
            }

            if (token.tokenType != Tokens.COMMA) {
                break;
            }

            read();
        }

        Expression[] list   = r.nodes;
        int          degree = 1;

        if (list[0].getType() == OpTypes.ROW) {
            degree = list[0].nodes.length;
        }

        r.opType = OpTypes.TABLE;

        for (int i = 0; i < list.length; i++) {
            if (list[i].getType() == OpTypes.ROW) {
                if (list[i].nodes.length != degree) {
                    throw Error.error(ErrorCode.X_42564);
                }
            } else {
                if (degree != 1) {
                    throw Error.error(ErrorCode.X_42564);
                }

                list[i] = new Expression(OpTypes.ROW,
                                         new Expression[]{ list[i] });
            }
        }

        return r;
    }

    Expression readCaseExpression() {

        Expression predicand = null;

        read();

        if (token.tokenType != Tokens.WHEN) {
            predicand = XreadRowValuePredicand();
        }

        return readCaseWhen(predicand);
    }

    /**
     * Reads part of a CASE .. WHEN  expression
     */
    private Expression readCaseWhen(final Expression l) {

        readThis(Tokens.WHEN);

        Expression condition = null;

        if (l == null) {
            condition = XreadBooleanValueExpression();
        } else {
            while (true) {
                Expression newCondition = XreadPredicateRightPart(l);

                if (l == newCondition) {
                    newCondition =
                        new ExpressionLogical(l, XreadRowValuePredicand());
                }

                if (condition == null) {
                    condition = newCondition;
                } else {
                    condition = new ExpressionLogical(OpTypes.OR, condition,
                                                      newCondition);
                }

                if (token.tokenType == Tokens.COMMA) {
                    read();
                } else {
                    break;
                }
            }
        }

        readThis(Tokens.THEN);

        Expression current  = XreadValueExpression();
        Expression elseExpr = null;

        if (token.tokenType == Tokens.WHEN) {
            elseExpr = readCaseWhen(l);
        } else if (token.tokenType == Tokens.ELSE) {
            read();

            elseExpr = XreadValueExpression();

            readThis(Tokens.END);
            readIfThis(Tokens.CASE);
        } else {
            elseExpr = new ExpressionValue((Object) null, Type.SQL_ALL_TYPES);

            readThis(Tokens.END);
            readIfThis(Tokens.CASE);
        }

        Expression alternatives = new ExpressionOp(OpTypes.ALTERNATIVE,
            current, elseExpr);
        Expression casewhen = new ExpressionOp(OpTypes.CASEWHEN, condition,
                                               alternatives);

        return casewhen;
    }

    /**
     * reads a CASEWHEN expression
     */
    private Expression readCaseWhenExpression() {

        Expression l = null;

        read();
        readThis(Tokens.OPENBRACKET);

        l = XreadBooleanValueExpression();

        readThis(Tokens.COMMA);

        Expression thenelse = XreadRowValueExpression();

        readThis(Tokens.COMMA);

        thenelse = new ExpressionOp(OpTypes.ALTERNATIVE, thenelse,
                                    XreadValueExpression());
        l = new ExpressionOp(OpTypes.CASEWHEN, l, thenelse);

        readThis(Tokens.CLOSEBRACKET);

        return l;
    }

    /**
     * Reads a CAST or CONVERT expression
     */
    private Expression readCastExpression() {

        boolean isConvert = token.tokenType == Tokens.CONVERT;

        read();
        readThis(Tokens.OPENBRACKET);

        Expression l = this.XreadValueExpressionOrNull();

        if (isConvert) {
            readThis(Tokens.COMMA);
        } else {
            readThis(Tokens.AS);
        }

        Type typeObject = readTypeDefinition(true);

        if (l.isParam()) {
            l.setDataType(session, typeObject);
        }

        l = new ExpressionOp(l, typeObject);

        readThis(Tokens.CLOSEBRACKET);

        return l;
    }

    /**
     * reads a Column or Function expression
     */
    private Expression readColumnOrFunctionExpression() {

        String  name           = token.tokenString;
        boolean isSimpleQuoted = isDelimitedSimpleName();
        String  prefix         = token.namePrefix;
        String  prePrefix      = token.namePrePrefix;

        if (isUndelimitedSimpleName()) {
            // A VoltDB extension to augment the standard sql function set.
            FunctionSQL function;
            function = FunctionForVoltDB.newVoltDBFunction(name);
            if (function == null) {
                // These seem to be JDBC ("Open Group"?) aliases and extensions to the standard sql functions.
                function = FunctionCustom.newCustomFunction(token.tokenString, token.tokenType);
            }
            /* disable 3 lines ...
            FunctionSQL function =
                FunctionCustom.newCustomFunction(token.tokenString,
                                                 token.tokenType);
            ... disabled 3 lines */
            // End of VoltDB extension

            if (function != null) {
                int pos = getPosition();

                // A VoltDB extension to avoid abusing the exception handling mechanism
                // for normal flow control.
                HsqlException ex = null;
                try {
                    ExpressionOrException result = readSQLFunction(function, false);
                    ex = result.exception();
                    if (ex == null) {
                        return result.knownGood();
                    }
                } catch (HsqlException caught) {
                    ex = caught;
                }
                if (ex != null) {
                /* disable 3 lines ...
                try {
                    return readSQLFunction(function);
                } catch (HsqlException ex) {
                ... disabled 3 lines */
                // End of VoltDB extension
                    ex.setLevel(compileContext.subQueryDepth);

                    if (lastError == null
                            || lastError.getLevel() < ex.getLevel()) {
                        lastError = ex;
                    }

                    rewind(pos);
                }
            } else if (isReservedKey()) {
                function = FunctionSQL.newSQLFunction(name, compileContext);

                if (function != null) {
                    // It's not really clear why readSQLFunction exceptions just get thrown through here
                    // but get post-processed in the similar call above for FunctionForVoltDB and FunctionCustom
                    return readSQLFunction(function);
                }
            }
        }

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            Expression column = new ExpressionColumn(prePrefix, prefix, name);

            return column;
        }

        checkValidCatalogName(prePrefix);

        prefix = session.getSchemaName(prefix);

        RoutineSchema routineSchema =
            (RoutineSchema) database.schemaManager.findSchemaObject(name,
                prefix, SchemaObject.FUNCTION);

        if (routineSchema == null && isSimpleQuoted) {
            HsqlName schema =
                database.schemaManager.getDefaultSchemaHsqlName();

            routineSchema =
                (RoutineSchema) database.schemaManager.findSchemaObject(name,
                    schema.name, SchemaObject.FUNCTION);

            if (routineSchema == null) {
                Method[]  methods  = Routine.getMethods(name);
                Routine[] routines = Routine.newRoutines(methods);
                HsqlName routineName = database.nameManager.newHsqlName(schema,
                    name, true, SchemaObject.FUNCTION);

                for (int i = 0; i < routines.length; i++) {
                    routines[i].setName(routineName);
                    session.database.schemaManager.addSchemaObject(
                        routines[i]);
                }

                routineSchema =
                    (RoutineSchema) database.schemaManager.findSchemaObject(
                        name, schema.name, SchemaObject.FUNCTION);
            }
        }

        if (routineSchema == null) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        HsqlArrayList list = new HsqlArrayList();

        readThis(Tokens.OPENBRACKET);

        if (token.tokenType == Tokens.CLOSEBRACKET) {
            read();
        } else {
            while (true) {
                Expression e = XreadValueExpression();

                list.add(e);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                } else {
                    readThis(Tokens.CLOSEBRACKET);

                    break;
                }
            }
        }

        FunctionSQLInvoked function  = new FunctionSQLInvoked(routineSchema);
        Expression[]       arguments = new Expression[list.size()];

        list.toArray(arguments);
        function.setArguments(arguments);
        compileContext.addRoutine(function);

        return function;
    }

    /**
     * Reads a NULLIF expression
     */
    private Expression readNullIfExpression() {

        // turn into a CASEWHEN
        read();
        readThis(Tokens.OPENBRACKET);

        Expression c = XreadValueExpression();

        readThis(Tokens.COMMA);

        Expression thenelse =
            new ExpressionOp(OpTypes.ALTERNATIVE,
                             new ExpressionValue((Object) null, (Type) null),
                             c);

        c = new ExpressionLogical(c, XreadValueExpression());
        c = new ExpressionOp(OpTypes.CASEWHEN, c, thenelse);

        readThis(Tokens.CLOSEBRACKET);

        return c;
    }

    /**
     * Reads a COALESE or IFNULL expression
     */
    private Expression readCoalesceExpression() {

        Expression c = null;

        // turn into a CASEWHEN
        read();
        readThis(Tokens.OPENBRACKET);

        Expression leaf = null;

        while (true) {
            Expression current = XreadValueExpression();

            if (leaf != null && token.tokenType == Tokens.CLOSEBRACKET) {
                readThis(Tokens.CLOSEBRACKET);
                leaf.setLeftNode(current);

                break;
            }

            Expression condition = new ExpressionLogical(OpTypes.IS_NULL,
                current);
            Expression alternatives = new ExpressionOp(OpTypes.ALTERNATIVE,
                new ExpressionValue((Object) null, (Type) null), current);
            Expression casewhen = new ExpressionOp(OpTypes.CASEWHEN,
                                                   condition, alternatives);

            if (c == null) {
                c = casewhen;
            } else {
                leaf.setLeftNode(casewhen);
            }

            leaf = alternatives;

            readThis(Tokens.COMMA);
        }

        return c;
    }

    Expression readSQLFunction(FunctionSQL function) {
        // A VoltDB extension to avoid using exceptions for flow control.
        // Throwing exceptions as they are detected ensures that the normal code path
        // returns a valid expression.
        return readSQLFunction(function, true).knownGood();
    }

    ExpressionOrException readSQLFunction(FunctionSQL function, boolean preferToThrow) {
        // End of VoltDB extension

        read();

        int     position  = getPosition();
        short[] parseList = function.parseList;

        if (parseList.length == 0) {
            // A VoltDB extension to avoid using exceptions for flow control.
            return new ExpressionOrException(function);
            /* disable 1 line ...
            return function;
            ... disabled 1 line */
            // End of VoltDB extension
        }

        HsqlArrayList exprList = new HsqlArrayList();

        // A VoltDB extension to avoid using exceptions for flow control.
        HsqlException e = null;
        int prevParamCount = compileContext.parameters.size();
        try {
            e = readExpression(exprList, parseList, 0, parseList.length, false, false);
        } catch (HsqlException caught) {
            e = caught;
        }
        if (e != null) {
            if (function.parseListAlt == null) {
                if ( ! preferToThrow) {
                    return new ExpressionOrException(e);
                }
        /* disable 4 lines ...
        try {
            readExpression(exprList, parseList, 0, parseList.length, false);
        } catch (HsqlException e) {
            if (function.parseListAlt == null) {
        ... disabled 4 lines */
        // End of VoltDB extension
                throw e;
            }

            rewind(position);
            // Discard parsed parameters along with the token rewinding.
            for (int i = compileContext.parameters.size()-1; i >= prevParamCount; i--) {
                compileContext.parameters.remove(i);
            }

            parseList = function.parseListAlt;
            exprList  = new HsqlArrayList();

            // A VoltDB extension to avoid using exceptions for flow control.
            HsqlException e2 = readExpression(exprList, parseList, 0, parseList.length, false, false);
            if (e2 != null) {
                // Return or throw the original exception (e) thrown from the
                // mismatch with the preferred standard argument syntax,
                // rather than (e2) from the mismatch with the (not as
                // standard) alternative syntax that also failed.
                if ( ! preferToThrow ) {
                    return new ExpressionOrException(e);
                }
                throw e;
            }
            /* disable 1 line ...
            readExpression(exprList, parseList, 0, parseList.length, false);
            ... disabled 1 line */
            // End of VoltDB extension
        }

        Expression[] expr = new Expression[exprList.size()];

        exprList.toArray(expr);
        function.setArguments(expr);

        // A VoltDB extension to avoid using exceptions for flow control.
        return new ExpressionOrException(function.getFunctionExpression());
        /* disable 1 line ...
        return function.getFunctionExpression();
        ... disabled 1 line */
        // End of VoltDB extension
    }

    // A VoltDB extension to avoid using exceptions for flow control.
    /***
     *
     * @param exprList
     * @param parseList
     * @param start
     * @param count
     * @param isOption
     * @param preferToThrow - if false, exceptions are quietly passed up the stack rather than thrown.
     *                        making it possible to distinguish (breakpoint at) serious exceptions
     *                        vs. false alarms that would merely indicate that the parser wandered
     *                        down an unfruitful path and needs to backtrack.
     *                        Exceptions should not be used for normal control flow.
     * @return a non-thrown HsqlException that can be thrown later if/when the alternatives have run out.
     */
    private HsqlException readExpression(HsqlArrayList exprList, short[] parseList, int start,
                                          int count, boolean isOption, boolean preferToThrow) {
    /* disable 2 lines ...
    void readExpression(HsqlArrayList exprList, short[] parseList, int start,
                        int count, boolean isOption) {
    ... disabled 2 lines */
    // End of VoltDB extension

        for (int i = start; i < start + count; i++) {
            int exprType = parseList[i];

            switch (exprType) {

                case Tokens.QUESTION : {
                    Expression e = null;

                    e = XreadAllTypesCommonValueExpression(false);

                    exprList.add(e);

                    continue;
                }
                case Tokens.X_POS_INTEGER : {
                    Expression e       = null;
                    int        integer = readInteger();

                    if (integer < 0) {
                        throw Error.error(ErrorCode.X_42592);
                    }

                    e = new ExpressionValue(ValuePool.getInt(integer),
                                            Type.SQL_INTEGER);

                    exprList.add(e);

                    continue;
                }
                case Tokens.X_OPTION : {
                    i++;

                    int expressionCount  = exprList.size();
                    int position         = getPosition();
                    int elementCount     = parseList[i++];
                    int initialExprIndex = exprList.size();

                    // A VoltDB extension to avoid using exceptions for flow control.
                    HsqlException ex = null;
                    try {
                        ex = readExpression(exprList, parseList, i, elementCount, true, false);
                    } catch (HsqlException caught) {
                        ex = caught;
                    }
                    if (ex != null) {
                    /* disable 4 lines ...
                    try {
                        readExpression(exprList, parseList, i, elementCount,
                                       true);
                    } catch (HsqlException ex) {
                    ... disabled 4 lines */
                    // End of VoltDB extension
                        ex.setLevel(compileContext.subQueryDepth);

                        if (lastError == null
                                || lastError.getLevel() < ex.getLevel()) {
                            lastError = ex;
                        }

                        rewind(position);
                        exprList.setSize(expressionCount);

                        for (int j = i; j < i + elementCount; j++) {
                            if (parseList[j] == Tokens.QUESTION
                                    || parseList[j] == Tokens.X_KEYSET
                                    || parseList[j] == Tokens.X_POS_INTEGER) {
                                exprList.add(null);
                            }
                        }

                        i += elementCount - 1;

                        continue;
                    }

                    if (initialExprIndex == exprList.size()) {
                        exprList.add(null);
                    }

                    i += elementCount - 1;

                    continue;
                }
                case Tokens.X_REPEAT : {
                    i++;

                    int elementCount = parseList[i++];
                    int parseIndex   = i;

                    while (true) {
                        int initialExprIndex = exprList.size();

                        // A VoltDB extension to avoid using exceptions for flow control.
                        if (preferToThrow) {
                            readExpression(exprList, parseList, parseIndex,
                                           elementCount, true, true);
                        } else {
                            HsqlException ex = null;
                            try {
                                ex = readExpression(exprList, parseList, parseIndex,
                                                           elementCount, true, false);
                            } catch (HsqlException caught) {
                                ex = caught;
                            }
                            if (ex != null) {
                                // TODO: There is likely a more elegant pre-emptive way of handling
                                // the inevitable close paren that properly terminates a repeating group.
                                // This filtering probably masks/ignores some syntax errors such as
                                // a trailing comma right before the paren.
                                if (ex.getMessage().equalsIgnoreCase("unexpected token: )")) {
                                    break;
                                }
                                return ex;
                            }
                        }
                        /* disable 2 lines ...
                        readExpression(exprList, parseList, parseIndex,
                                       elementCount, true);
                        ... disabled 2 lines */
                        // End of VoltDB extension

                        if (exprList.size() == initialExprIndex) {
                            break;
                        }
                    }

                    i += elementCount - 1;

                    continue;
                }
                case Tokens.X_KEYSET : {
                    int        elementCount = parseList[++i];
                    Expression e            = null;

                    if (ArrayUtil.find(parseList, token.tokenType, i
                                       + 1, elementCount) == -1) {
                        if (!isOption) {
                            // A VoltDB extension to avoid using exceptions for flow control.
                            if ( ! preferToThrow) {
                                return unexpectedToken();
                            }
                            // End of VoltDB extension
                            throw unexpectedToken();
                        }
                    } else {
                        e = new ExpressionValue(
                            ValuePool.getInt(token.tokenType),
                            Type.SQL_INTEGER);

                        read();
                    }

                    exprList.add(e);

                    i += elementCount;

                    continue;
                }
                case Tokens.OPENBRACKET :
                case Tokens.CLOSEBRACKET :
                case Tokens.COMMA :
                default :
                    if (token.tokenType != exprType) {
                        // A VoltDB extension to avoid using exceptions for flow control.
                        if ( ! preferToThrow) {
                            return unexpectedToken();
                        }
                        // End of VoltDB extension
                        throw unexpectedToken();
                    }

                    read();

                    continue;
            }
        }
        // A VoltDB extension to avoid using exceptions for flow control.
        return null; // Successful return -- no exception to pass back.
        // End of VoltDB extension
    }

    private Expression readSequenceExpression() {

        read();
        readThis(Tokens.VALUE);
        readThis(Tokens.FOR);
        checkIsSchemaObjectName();

        String schema = session.getSchemaName(token.namePrefix);
        NumberSequence sequence =
            database.schemaManager.getSequence(token.tokenString, schema,
                                               true);

        read();

        Expression e = new ExpressionColumn(sequence);

        compileContext.addSequence(sequence);

        return e;
    }

    HsqlName readNewSchemaName() {

        checkIsSchemaObjectName();
        checkValidCatalogName(token.namePrefix);
        SqlInvariants.checkSchemaNameNotSystem(token.tokenString);

        HsqlName name = database.nameManager.newHsqlName(token.tokenString,
            isDelimitedIdentifier(), SchemaObject.SCHEMA);

        read();

        return name;
    }

    HsqlName readNewSchemaObjectNameNoCheck(int type) {

        checkIsSchemaObjectName();

        HsqlName hsqlName = database.nameManager.newHsqlName(token.tokenString,
            isDelimitedIdentifier(), type);

        if (token.namePrefix != null) {
            if (type == SchemaObject.GRANTEE) {
                throw unexpectedToken();
            }

            HsqlName schemaName =
                session.database.schemaManager.findSchemaHsqlName(
                    token.namePrefix);

            if (schemaName == null) {
                schemaName = database.nameManager.newHsqlName(token.namePrefix,
                        isDelimitedIdentifier(), SchemaObject.SCHEMA);
            }

            hsqlName.setSchemaIfNull(schemaName);
        }

        read();

        return hsqlName;
    }

    HsqlName readNewSchemaObjectName(int type) {

        checkIsSchemaObjectName();

        HsqlName hsqlName = database.nameManager.newHsqlName(token.tokenString,
            isDelimitedIdentifier(), type);

        if (token.namePrefix != null) {
            if (type == SchemaObject.GRANTEE || type == SchemaObject.VARIABLE
                    || type == SchemaObject.CATALOG) {
                throw unexpectedToken();
            }

            HsqlName schemaName = session.getSchemaHsqlName(token.namePrefix);

            hsqlName.setSchemaIfNull(schemaName);
        }

        read();

        return hsqlName;
    }

    HsqlName readNewDependentSchemaObjectName(HsqlName parentName, int type) {

        HsqlName name = readNewSchemaObjectName(type);

        name.parent = parentName;

        name.setSchemaIfNull(parentName.schema);

        if (name.schema != null && parentName.schema != null
                && name.schema != parentName.schema) {
            throw Error.error(ErrorCode.X_42505, token.namePrefix);
        }

        return name;
    }

    HsqlName readSchemaName() {

        checkIsSchemaObjectName();
        checkValidCatalogName(token.namePrefix);

        HsqlName schema = session.getSchemaHsqlName(token.tokenString);

        read();

        return schema;
    }

    SchemaObject readSchemaObjectName(int type) {

        checkIsSchemaObjectName();

        SchemaObject object =
            database.schemaManager.getSchemaObject(token.tokenString,
                token.namePrefix, type);

        if (token.namePrePrefix != null) {
            if (!token.namePrePrefix.equals(database.getCatalogName().name)) {
                throw Error.error(ErrorCode.X_42505, token.namePrefix);
            }
        }

        read();

        return object;
    }

    SchemaObject readDependentSchemaObjectName(HsqlName parentName, int type) {

        checkIsSchemaObjectName();

        SchemaObject object =
            database.schemaManager.getSchemaObject(token.tokenString,
                parentName.schema.name, type);

        if (token.namePrefix != null) {
            if (!token.namePrefix.equals(parentName.name)) {

                // todo - better error message
                throw Error.error(ErrorCode.X_42505, token.namePrefix);
            }

            if (token.namePrePrefix != null) {
                if (!token.namePrePrefix.equals(parentName.schema.name)) {

                    // todo - better error message
                    throw Error.error(ErrorCode.X_42505, token.namePrefix);
                }
            }
        }

        read();

        return object;
    }

    Table readTableName() {

        checkIsIdentifier();

        if (token.namePrePrefix != null) {
            throw Error.error(ErrorCode.X_42551, token.tokenString);
        }

        Table table = database.schemaManager.getTable(session,
            token.tokenString, token.namePrefix);

        read();

        return table;
    }

    ColumnSchema readSimpleColumnName(RangeVariable rangeVar) {

        ColumnSchema column = null;

        checkIsIdentifier();

        if (token.namePrefix != null) {
            throw Error.error(ErrorCode.X_42551, token.tokenString);
        }

        int index = rangeVar.findColumn(token.tokenString);

        if (index > -1 && rangeVar.resolvesTableName(token.namePrefix)
                && rangeVar.resolvesSchemaName(token.namePrePrefix)) {
            column = rangeVar.getTable().getColumn(index);

            read();

            return column;
        }

        throw Error.error(ErrorCode.X_42501, token.tokenString);
    }

    ColumnSchema readSimpleColumnName(Table table) {

        checkIsIdentifier();

        if (token.namePrefix != null) {
            throw Error.error(ErrorCode.X_42551, token.tokenString);
        }

        int index = table.findColumn(token.tokenString);

        if (index == -1) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        ColumnSchema column = table.getColumn(index);

        read();

        return column;
    }

    ColumnSchema readColumnName(RangeVariable[] rangeVars) {

        ColumnSchema column = null;

        checkIsIdentifier();

        if (strictSQLIdentifierParts && token.namePrefix != null) {
            throw Error.error(ErrorCode.X_42551, token.tokenString);
        }

        for (int i = 0; i < rangeVars.length; i++) {
            int index = rangeVars[i].findColumn(token.tokenString);

            if (index > -1 && rangeVars[i].resolvesTableName(token.namePrefix)
                    && rangeVars[i].resolvesSchemaName(token.namePrePrefix)) {
                column = rangeVars[i].getColumn(index);

                read();

                return column;
            }
        }

        throw Error.error(ErrorCode.X_42501, token.tokenString);
    }

    StatementDMQL compileDeclareCursor() {

        int sensitivity   = 0;    // ASENSITIVE
        int scrollability = 0;    // NO_SCROLL
        int holdability   = 0;    // WITHOUT_HOLD
        int returnability = 0;    // WITHOUT_RETURN

        readThis(Tokens.DECLARE);
        readNewSchemaObjectName(SchemaObject.CURSOR);

        switch (token.tokenType) {

            case Tokens.SENSITIVE :
                read();

                // sensitivity = SENSITIVE
                break;

            case Tokens.INSENSITIVE :
                read();

                // sensitivity = INSENSITIVE
                break;

            case Tokens.ASENSITIVE :
                read();
                break;
        }

        if (token.tokenType == Tokens.NO) {
            readThis(Tokens.SCROLL);
        } else {
            if (token.tokenType == Tokens.SCROLL) {
                read();

                // scrollability = SCROLL
            }
        }

        readThis(Tokens.CURSOR);

        for (int round = 0; round < 2; round++) {
            if (token.tokenType == Tokens.WITH) {
                read();

                if (round == 0 && token.tokenType == Tokens.HOLD) {
                    read();

                    // holdability = HOLD
                } else {
                    readThis(Tokens.RETURN);

                    round++;

                    // returnability = WITH_RETURN
                }
            } else if (token.tokenType == Tokens.WITHOUT) {
                read();

                if (round == 0 && token.tokenType == Tokens.HOLD) {
                    read();
                } else {
                    readThis(Tokens.RETURN);

                    round++;
                }
            }
        }

        StatementDMQL cs = compileCursorSpecification();

        return cs;
    }

    /**
     * Retrieves a SELECT or other query expression Statement from this parse context.
     */
    StatementDMQL compileCursorSpecification() {

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setAsTopLevel();
        queryExpression.resolve(session);
        if (token.tokenType == Tokens.FOR) {
            read();

            if (token.tokenType == Tokens.READ) {
                read();
                readThis(Tokens.ONLY);
            } else {
                readThis(Tokens.UPDATE);

                if (token.tokenType == Tokens.OF) {
                    readThis(Tokens.OF);

                    OrderedHashSet colNames = readColumnNameList(null, false);
                }
            }
        }

        StatementDMQL cs = new StatementQuery(session, queryExpression,
                                              compileContext);

        return cs;
    }

    int readCloseBrackets(int limit) {

        int count = 0;

        while (count < limit && token.tokenType == Tokens.CLOSEBRACKET) {
            read();

            count++;
        }

        return count;
    }

    int readOpenBrackets() {

        int count = 0;

        while (token.tokenType == Tokens.OPENBRACKET) {
            count++;

            read();
        }

        return count;
    }

    void checkValidCatalogName(String name) {

        if (name != null && !name.equals(database.getCatalogName().name)) {
            throw Error.error(ErrorCode.X_42501, name);
        }
    }

    public static final class CompileContext {

        //
        private int           subQueryDepth;
        private HsqlArrayList subQueryList   = new HsqlArrayList(true);
        HsqlArrayList         parameters     = new HsqlArrayList(true);
        private HsqlArrayList usedSequences  = new HsqlArrayList(true);
        private HsqlArrayList usedRoutines   = new HsqlArrayList(true);
        private HsqlArrayList rangeVariables = new HsqlArrayList(true);
        Type                  currentDomain;
        boolean               contextuallyTypedExpression;
        final Session         session;

        //
        private int rangeVarIndex = 0;

        public CompileContext(Session session) {

            this.session = session;

            reset();
        }

        public void reset() {

            rangeVarIndex = 0;

            rangeVariables.clear();
            subQueryList.clear();

            subQueryDepth = 0;

            parameters.clear();
            usedSequences.clear();
            usedRoutines.clear();

            //
            currentDomain               = null;
            contextuallyTypedExpression = false;
        }

        public void registerRangeVariable(RangeVariable range) {

            range.rangePosition = getNextRangeVarIndex();
            range.level         = subQueryDepth;

            rangeVariables.add(range);
        }

        public int getNextRangeVarIndex() {
            return rangeVarIndex++;
        }

        public int getRangeVarCount() {
            return rangeVarIndex;
        }

        public RangeVariable[] getRangeVariables() {

            RangeVariable[] array = new RangeVariable[rangeVariables.size()];

            rangeVariables.toArray(array);

            return array;
        }

        public NumberSequence[] getSequences() {

            if (usedSequences.size() == 0) {
                return NumberSequence.emptyArray;
            }

            NumberSequence[] array = new NumberSequence[usedSequences.size()];

            usedSequences.toArray(array);

            return array;
        }

        public Routine[] getRoutines() {

            if (usedRoutines.size() == 0) {
                return Routine.emptyArray;
            }

            for (int i = 0; i < usedRoutines.size(); i++) {
                FunctionSQLInvoked function =
                    (FunctionSQLInvoked) usedRoutines.get(i);

                usedRoutines.set(i, function.routine);
            }

            Routine[] array = new Routine[usedRoutines.size()];

            usedRoutines.toArray(array);

            return array;
        }

        private void addSequence(NumberSequence sequence) {
            usedSequences.add(sequence);
        }

        void addRoutine(FunctionSQLInvoked function) {
            usedRoutines.add(function);
        }

        void resetSubQueryLevel() {

            for (int i = rangeVariables.size() - 1; i >= 0; i--) {
                RangeVariable range = (RangeVariable) rangeVariables.get(i);

                if (range.level > subQueryDepth) {
                    rangeVariables.remove(i);
                } else {
                    rangeVarIndex = rangeVariables.size();

                    break;
                }
            }

            for (int i = subQueryList.size() - 1; i >= 0; i--) {
                SubQuery subQuery = (SubQuery) subQueryList.get(i);

                if (subQuery.level > subQueryDepth) {
                    subQueryList.remove(i);
                } else {
                    break;
                }
            }
        }

        /**
         * Return the list of subqueries as an array sorted according to the
         * order of materialization
         */
        SubQuery[] getSubqueries() {

            if (subQueryList.size() == 0) {
                return SubQuery.emptySubqueryArray;
            }

            subQueryList.sort((SubQuery) subQueryList.get(0));

            SubQuery[] subqueries = new SubQuery[subQueryList.size()];

            subQueryList.toArray(subqueries);
            subQueryList.clear();

            for (int i = 0; i < subqueries.length; i++) {
                subqueries[i].prepareTable(session);
            }

            return subqueries;
        }

        ExpressionColumn[] getParameters() {

            if (parameters.size() == 0) {
                return ExpressionColumn.emptyArray;
            }

            ExpressionColumn[] result =
                (ExpressionColumn[]) parameters.toArray(
                    new ExpressionColumn[parameters.size()]);

            parameters.clear();

            return result;
        }

        void clearParameters() {
            parameters.clear();
        }

        public OrderedHashSet getSchemaObjectNames() {

            OrderedHashSet set = new OrderedHashSet();

            for (int i = 0; i < usedSequences.size(); i++) {
                NumberSequence sequence =
                    (NumberSequence) usedSequences.get(i);

                set.add(sequence.getName());
            }

            for (int i = 0; i < rangeVariables.size(); i++) {
                RangeVariable range = (RangeVariable) rangeVariables.get(i);
                HsqlName      name  = range.rangeTable.getName();

                if (name.schema != SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                    set.add(range.rangeTable.getName());
                    set.addAll(range.getColumnNames());
                } else if (name.type == SchemaObject.TRANSITION) {
                    set.addAll(range.getColumnNames());
                }
            }

            Routine[] routines = getRoutines();

            for (int i = 0; i < routines.length; i++) {
                set.add(routines[i].getName());
            }

            return set;
        }
    }

    /************************* Volt DB Extensions *************************/

    /** This wraps either an Expression result of readSQLFunction or a throwable Exception discovered
     *  in the process of reading that Expression.
     *  It allows different parser paths to be tried without losing exception
     *  information OR needlessly throwing, catching, and recovering from those exceptions.
     *  The main purpose of this class is to avoid using Exception for "normal case" flow control,
     *  so that more significant exception throws can be tracked down more easily.
     *  This helped a lot in the development of VoltDB-specific SQL functions.
     *  It might have been slightly more traditional to collect the HsqlException 's initializers and
     *  only construct the HsqlException when ready to throw it.
     *  But having an HsqlException handy allowed better integration with the existing logic for
     *  conditional re-throws.
     */
    static class ExpressionOrException {
        private Expression m_good;
        private HsqlException m_bad;

        ExpressionOrException(Expression good) { m_good = good; }
        ExpressionOrException(HsqlException bad) { m_bad = bad; }

        // It MAY be safe to extract the Expression,
        // but if there was an exception, throw that now, instead.
        Expression orThrow() throws HsqlException {
            if (m_bad != null)
                throw m_bad;
            return m_good;
        }

        // Allow checking, to ensure whether the Expression value is valid.
        public HsqlException exception() {
            return m_bad;
        }

        // It is safe to extract the Expression when there is known not to be an Exception.
        public Expression knownGood() {
            // TODO Auto-generated method stub
            assert(m_bad == null);
            return m_good;
        }
    }
    /**********************************************************************/
}
