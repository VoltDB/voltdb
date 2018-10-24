/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.newplanner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.test.SqlTests;
import org.voltdb.parser.SqlParserFactory;
import org.voltdb.planner.PlannerTestCase;

import java.util.Objects;

public class VoltSqlValidatorTestCase extends PlannerTestCase {
    private VoltSqlValidator m_validator;

    protected void setupValidator(SchemaPlus schemaPlus) {
        m_validator = new VoltSqlValidator(schemaPlus);
    }

    protected VoltSqlValidator getValidator() {
        return m_validator;
    }

    private SqlNode parseQuery(String sql) throws SqlParseException {
        SqlParser parser = SqlParserFactory.create(sql);
        return parser.parseStmt();
    }

    protected SqlNode parseAndValidate(String sql) {
        Objects.requireNonNull(m_validator, "m_validator");
        SqlNode sqlNode;
        try {
            sqlNode = parseQuery(sql);
        } catch (Throwable e) {
            throw new RuntimeException("Error while parsing query: " + sql, e);
        }
        return m_validator.validate(sqlNode);
    }

    protected RelDataType getResultType(String sql) {
        SqlNode node = parseAndValidate(sql);

        return m_validator.getValidatedNodeType(node);
    }

    private void checkParseEx(Throwable e, String expectedMsgPattern, String sql) {
        try {
            throw e;
        } catch (SqlParseException spe) {
            String errMessage = spe.getMessage();
            if (expectedMsgPattern == null) {
                throw new RuntimeException("Error while parsing query:" + sql, spe);
            } else if (errMessage == null
                    || !errMessage.matches(expectedMsgPattern)) {
                throw new RuntimeException("Error did not match expected ["
                        + expectedMsgPattern + "] while parsing query ["
                        + sql + "]", spe);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Error while parsing query: " + sql, t);
        }
    }

    protected void assertExceptionIsThrown(String sql, String expectedMsgPattern) {
        final SqlNode sqlNode;
        final SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
        try {
            sqlNode = parseQuery(sap.sql);
        } catch (Throwable e) {
            checkParseEx(e, expectedMsgPattern, sap.sql);
            return;
        }

        Throwable thrown = null;
        try {
            m_validator.validate(sqlNode);
        } catch (Throwable ex) {
            thrown = ex;
        }

        SqlTests.checkEx(thrown, expectedMsgPattern, sap, SqlTests.Stage.VALIDATE);
    }

    public void assertValid(String sql) {
        assertExceptionIsThrown(sql, null);
    }
}
