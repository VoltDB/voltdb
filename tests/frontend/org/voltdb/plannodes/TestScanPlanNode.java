/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.plannodes;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltType;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;

public class TestScanPlanNode extends TestCase
{
    static final String TABLE1 = "TABLE1";
    static final String[] COLS = { "COL0", "COL1", "COL2", "COL3", "COL4" };
    static final VoltType[] COLTYPES = { VoltType.INTEGER, VoltType.TINYINT,
                                         VoltType.TIMESTAMP, VoltType.FLOAT,
                                         VoltType.BIGINT };

    MockVoltDB m_voltdb;

    @Override
    protected void setUp() {
        m_voltdb = new MockVoltDB();
        m_voltdb.addTable(TABLE1, false);
        for (int i = 0; i < COLS.length; ++i)
        {
            m_voltdb.addColumnToTable(TABLE1, COLS[i], COLTYPES[i], false, "",
                                      COLTYPES[i]);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        m_voltdb.shutdown(null);
    }

    // test that if no scan columns are specified, the output schema of
    // a scan node is the schema of the table
    public void testOutputSchemaNoScanColumns()
    {
        AbstractScanPlanNode dut = new SeqScanPlanNode(TABLE1, TABLE1);

        dut.generateOutputSchema(m_voltdb.getDatabase());
        NodeSchema dut_schema = dut.getOutputSchema();
        System.out.println(dut_schema.toString());
        assertEquals(COLS.length, dut_schema.size());
        for (int i = 0; i < COLS.length; ++i)
        {
            SchemaColumn col = dut_schema.find(TABLE1, TABLE1, COLS[i], COLS[i]);
            assertNotNull(col);
            assertEquals(col.getExpression().getExpressionType(),
                         ExpressionType.VALUE_TUPLE);
            assertEquals(col.getExpression().getValueType(),
                         COLTYPES[i]);
        }
    }

    // test that if scan columns are specified the output schema of
    // a scan node consists of those columns
    public void testOutputSchemaSomeScanColumns()
    {
        int[] scan_col_indexes = { 1, 3 };
        ArrayList<SchemaColumn> scanColumns = new ArrayList<SchemaColumn>();
        for (int index : scan_col_indexes) {
            TupleValueExpression tve = new TupleValueExpression(TABLE1, TABLE1,
                    COLS[index], COLS[index]);
            tve.setValueType(COLTYPES[index]);
            tve.setValueSize(COLTYPES[index].getLengthInBytesForFixedTypes());
            SchemaColumn col = new SchemaColumn(TABLE1, TABLE1,
                    COLS[index], COLS[index],
                    tve);
            scanColumns.add(col);
        }
        AbstractScanPlanNode dut = SeqScanPlanNode.createDummyForTest(TABLE1, scanColumns);

        // Should be able to do this safely and repeatably multiple times
        for (int i = 0; i < 3; i++) {
            dut.generateOutputSchema(m_voltdb.getDatabase());
            NodeSchema dut_schema = dut.getOutputSchema();
            System.out.println(dut_schema.toString());
            assertEquals(scan_col_indexes.length, dut_schema.size());
            for (int index : scan_col_indexes) {
                SchemaColumn col = dut_schema.find(TABLE1, TABLE1, COLS[index], "");
                assertNotNull(col);
                assertEquals(col.getExpression().getExpressionType(), ExpressionType.VALUE_TUPLE);
                assertEquals(col.getExpression().getValueType(), COLTYPES[index]);
            }
        }
    }

    // test that if someone provides their own inline projection
    // that the output schema of the scan node consists of the output
    // schema of the projection.  Updates will do this so that the
    // inlined projection fills the values of the output tuples correctly
    // before it attempts to update them
    public void testOutputSchemaOverriddenProjection()
    {
        AbstractScanPlanNode dut = new SeqScanPlanNode(TABLE1, TABLE1);

        // Create an output schema like we might see for an inlined projection
        // generated for update.  We'll have 4 output columns, the first will
        // be the tuple address, the second one a parameter expression, next
        // will be a constant, and the other will be a more complex expression
        // that uses some TVEs.
        NodeSchema proj_schema = new NodeSchema();
        String[] cols = new String[4];

        TupleAddressExpression col1_exp = new TupleAddressExpression();
        proj_schema.addColumn("", "",
                "tuple_address", "tuple_address",
                col1_exp);
        cols[0] = "tuple_address";

        // update column 1 with a parameter value
        ParameterValueExpression col2_exp = new ParameterValueExpression();
        col2_exp.setParameterIndex(0);
        col2_exp.setValueType(COLTYPES[1]);
        col2_exp.setValueSize(COLTYPES[1].getLengthInBytesForFixedTypes());
        // XXX I'm not sure what to do with the name for the updated column yet.
        // I think it should be an alias and not the original table name/col name
        proj_schema.addColumn(TABLE1, TABLE1, COLS[1], COLS[1], col2_exp);
        cols[1] = COLS[1];

        // Update column 3 with a constant value
        ConstantValueExpression col3_exp = new ConstantValueExpression();
        col3_exp.setValueType(COLTYPES[3]);
        col3_exp.setValueSize(COLTYPES[3].getLengthInBytesForFixedTypes());
        col3_exp.setValue("3.14159");
        proj_schema.addColumn(TABLE1, TABLE1, COLS[3], COLS[3], col3_exp);
        cols[2] = COLS[3];

        // update column 4 with a sum of columns 0 and 2
        OperatorExpression col4_exp = new OperatorExpression();
        col4_exp.setValueType(COLTYPES[4]);
        col4_exp.setValueSize(COLTYPES[4].getLengthInBytesForFixedTypes());
        col4_exp.setExpressionType(ExpressionType.OPERATOR_PLUS);
        TupleValueExpression left = new TupleValueExpression(TABLE1, TABLE1, COLS[0], COLS[0], 0);
        left.setValueType(COLTYPES[0]);
        left.setValueSize(COLTYPES[0].getLengthInBytesForFixedTypes());
        TupleValueExpression right = new TupleValueExpression(TABLE1, TABLE1, COLS[2], COLS[2], 0);
        right.setValueType(COLTYPES[2]);
        right.setValueSize(COLTYPES[2].getLengthInBytesForFixedTypes());
        col4_exp.setLeft(left);
        col4_exp.setRight(right);
        proj_schema.addColumn(TABLE1, TABLE1, COLS[4], "C1", col4_exp);
        cols[3] = COLS[4];

        ProjectionPlanNode proj_node = new ProjectionPlanNode(proj_schema);
        dut.addInlinePlanNode(proj_node);

        System.out.println("ProjSchema: " + proj_schema.toString());

        dut.generateOutputSchema(m_voltdb.getDatabase());
        NodeSchema dut_schema = dut.getOutputSchema();
        System.out.println(dut_schema.toString());
        for (int i = 0; i < cols.length; i++)
        {
            SchemaColumn col = null;
            if (i == 0)
            {
                col = dut_schema.find("", "", cols[i], cols[i]);
            }
            else
            {
                col = dut_schema.find(TABLE1, TABLE1, cols[i], cols[i]);
            }
            assertNotNull(col);
            assertEquals(col.getExpression().getExpressionType(),
                         ExpressionType.VALUE_TUPLE);
        }
    }
}
