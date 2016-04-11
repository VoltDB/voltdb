package org.voltdb.plannodes;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltType;
import org.voltdb.expressions.TupleValueExpression;

import junit.framework.TestCase;

public class TestPartitionByNode extends TestCase {
    static final String TABLE1 = "TABLE1";
    static final String[] COLS = { "COL0", "COL1", "COL2", "COL3", "COL4" };
    static final VoltType[] COLTYPES = { VoltType.INTEGER, VoltType.TINYINT,
                                         VoltType.TIMESTAMP, VoltType.FLOAT,
                                         VoltType.BIGINT };

    MockVoltDB m_voltdb;

    @Override
    protected void setUp()
    {
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

    public void testOutputSchema() throws Exception {
        PartitionByPlanNode dut = new PartitionByPlanNode();
        NodeSchema outputSchema = new NodeSchema();
        TupleValueExpression tve = new TupleValueExpression();
        outputSchema.addColumn(new SchemaColumn(TABLE1, TABLE1, "C1", "C1", tve));
        dut.setOutputSchema(outputSchema);
        dut.generateOutputSchema(m_voltdb.getDatabase());
        NodeSchema dut_schema = dut.getOutputSchema();
        System.out.println(dut_schema.toString());
        assertEquals(2, dut_schema.size());
    }
}
