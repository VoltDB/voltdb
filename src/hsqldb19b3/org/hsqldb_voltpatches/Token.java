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

import org.hsqldb_voltpatches.types.Type;

public class Token {

    String  tokenString = "";
    int     tokenType   = Tokens.X_UNKNOWN_TOKEN;
    Type    dataType;
    Object  tokenValue;
    String  namePrefix;
    String  namePrePrefix;
    String  charsetSchema;
    String  charsetName;
    String  fullString;
    int     lobMultiplierType = Tokens.X_UNKNOWN_TOKEN;
    boolean isDelimiter;
    boolean isDelimitedIdentifier;
    boolean isDelimitedPrefix;
    boolean isDelimitedPrePrefix;
    boolean isUndelimitedIdentifier;
    boolean isReservedIdentifier;
    boolean isCoreReservedIdentifier;
    boolean isHostParameter;
    boolean isMalformed;

    //
    int              position;
    ExpressionColumn columnExpression;

    void reset() {

        tokenString              = "";
        tokenType                = Tokens.X_UNKNOWN_TOKEN;
        dataType                 = null;
        tokenValue               = null;
        namePrefix               = null;
        namePrePrefix            = null;
        charsetSchema            = null;
        charsetName              = null;
         fullString              = null;
        lobMultiplierType        = Tokens.X_UNKNOWN_TOKEN;
        isDelimiter              = false;
        isDelimitedIdentifier    = false;
        isDelimitedPrefix        = false;
        isDelimitedPrePrefix     = false;
        isUndelimitedIdentifier  = false;
        isReservedIdentifier     = false;
        isCoreReservedIdentifier = false;
        isHostParameter          = false;
        isMalformed              = false;
    }

    Token duplicate() {

        Token token = new Token();

        token.tokenString              = tokenString;
        token.tokenType                = tokenType;
        token.dataType                 = dataType;
        token.tokenValue               = tokenValue;
        token.namePrefix               = namePrefix;
        token.namePrePrefix            = namePrePrefix;
        token.charsetSchema            = charsetSchema;
        token.charsetName              = charsetName;
        token.fullString               = fullString;
        token.lobMultiplierType        = lobMultiplierType;
        token.isDelimiter              = isDelimiter;
        token.isDelimitedIdentifier    = isDelimitedIdentifier;
        token.isDelimitedPrefix        = isDelimitedPrefix;
        token.isDelimitedPrePrefix     = isDelimitedPrePrefix;
        token.isUndelimitedIdentifier  = isUndelimitedIdentifier;
        token.isReservedIdentifier     = isReservedIdentifier;
        token.isCoreReservedIdentifier = isCoreReservedIdentifier;
        token.isHostParameter          = isHostParameter;
        token.isMalformed              = isMalformed;

        return token;
    }

    public String getFullString() {
        return fullString;
    }


    String getSQL() {

        if (namePrefix == null && isUndelimitedIdentifier) {
            return tokenString;
        }

        if (tokenType == Tokens.X_VALUE) {
            return dataType.convertToSQLString(tokenValue);
        }

        StringBuffer sb = new StringBuffer();

        if (tokenType == Tokens.ASTERISK) {
            if (columnExpression != null
                    && columnExpression.opType == OpTypes.MULTICOLUMN
                    && columnExpression.nodes.length > 0) {
                sb.append(' ');

                for (int i = 0; i < columnExpression.nodes.length; i++) {
                    Expression   e = columnExpression.nodes[i];
                    ColumnSchema c = e.getColumn();
                    String       name;

                    if (e.opType == OpTypes.COALESCE) {
                        sb.append(e.getColumnName());
                        continue;
                    }

                    if (e.getRangeVariable().tableAlias == null) {
                        name = c.getName().getSchemaQualifiedStatementName();
                    } else {
                        RangeVariable range = e.getRangeVariable();

                        name = range.tableAlias.getStatementName()
                               + '.' + c.getName().statementName;
                    }

                    if (i > 0) {
                        sb.append(',');
                    }

                    sb.append(name);
                }

                sb.append(' ');
            } else {
                return tokenString;
            }

            return sb.toString();
        }

        if (namePrePrefix != null) {
            if (isDelimitedPrePrefix) {
                sb.append('"');
                sb.append(namePrePrefix);
                sb.append('"');
            } else {
                sb.append(namePrePrefix);
            }

            sb.append('.');
        }

        if (namePrefix != null) {
            if (isDelimitedPrefix) {
                sb.append('"');
                sb.append(namePrefix);
                sb.append('"');
            } else {
                sb.append(namePrefix);
            }

            sb.append('.');
        }

        if (isDelimitedIdentifier) {
            sb.append('"');
            sb.append(tokenString);
            sb.append('"');
        } else {
            sb.append(tokenString);
        }

        return sb.toString();
    }

/*
    for (int i = 0; i < tokens.length; i++) {
        if (tokens[i].schemaObjectIdentifier instanceof Expression) {
            ColumnSchema column =
                ((Expression) tokens[i].schemaObjectIdentifier)
                    .getColumn();

            tokens[i].schemaObjectIdentifier = column.getName();
        }
    }
*/
    static String getSQL(Token[] statement) {

        boolean      wasDelimiter = true;
        StringBuffer sb           = new StringBuffer();

        for (int i = 0; i < statement.length; i++) {
            // when the previous statement is ')', there should be a blank before the non-delimiter statement.
            if (!statement[i].isDelimiter &&
                    (!wasDelimiter
                            || (i > 0 && ")".equals(statement[i - 1].getSQL()))))
            {
                sb.append(' ');
            }

            sb.append(statement[i].getSQL());

            wasDelimiter = statement[i].isDelimiter;
        }

        return sb.toString();
    }
}
