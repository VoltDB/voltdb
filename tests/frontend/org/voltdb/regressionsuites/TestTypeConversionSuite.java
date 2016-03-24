/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;

import org.voltdb.BackendTarget;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcToTestTypeConversion;

public class TestTypeConversionSuite extends RegressionSuite {

    // stores columns types of table T in order they are defined
    private VoltType[] m_tableColTypeVal = {
            VoltType.TINYINT,
            VoltType.SMALLINT,
            VoltType.INTEGER,
            VoltType.BIGINT,
            VoltType.FLOAT,
            VoltType.DECIMAL,
            VoltType.TIMESTAMP,
            VoltType.STRING,
            VoltType.VARBINARY,
            VoltType.GEOGRAPHY_POINT,
            VoltType.GEOGRAPHY
    };

    // row index provides the type converting "from"
    // column index provides the type converting "to"
    // to see type for column (to) or row (from), map it's index m_tableColTypeVal to get it's type
    boolean [][] m_typeConversionMatrix =
        {
            {true, true, true, true, true, true, true, true, false, false, false},          // TinyInt
            {true, true, true, true, true, true, true, true, false, false, false},          // SmallInt
            {true, true, true, true, true, true, true, true, false, false, false},          // Integer
            {true, true, true, true, true, true, true, true, false, false, false},          // BigInt
            {true, true, true, true, true, true, true, true, false, false, false},          // Float
            {true, true, true, true, true, true, true, true, false, false, false},          // Decimal
            {true, true, true, true, true, false, true, true, false, false, false},         // TIMESTAMP
            {true, true, true, true, true, true, true, true, false, false, false},          // VARCHAR/STRING
            {false, false, false, false, false, false, false, true, true, false, false},    // VARBINARY
            {false, false, false, false, false, false, false, false, false, true, false},   // POINT
            {false, false, false, false, false, false, false, false, false, false, true},   // POLYGON
        };

    // row index provides the type converting "from" (user supplied data of certain type)
    // column index provides the type converting "to" (the actual column type in table)
    // to see type for column (to) or row (from), map it's index m_tableColTypeVal to get it's type
    // this is based on allowed type conversion for comparison which in list param arguments uses
    boolean [][] m_typeConversionMatrixInList =
        {
            {true, true, true, true, true, true, true, false, false, false, false},         // TinyInt
            {true, true, true, true, true, true, true, false, false, false, false},         // SmallInt
            {true, true, true, true, true, true, true, false, false, false, false},         // Integer
            {true, true, true, true, true, true, true, false, false, false, false},         // BigInt
            {true, true, true, true, true, true, true, false, false, false, false},         // Float
            {true, true, true, true, true, true, true, false, false, false, false},         // Decimal
            {true, true, true, true, true, true, true, false, false, false, false},         // TIMESTAMP
            {false, false, false, false, false, false, false, true, false, false, false},   // VARCHAR/STRING
            {false, false, false, false, false, false, false, false, true, false, false},   // VARBINARY
            {false, false, false, false, false, false, false, false, false, true, false},   // POINT
            {false, false, false, false, false, false, false, false, false, false, true},   // POLYGON
        };

    public TestTypeConversionSuite(String name) {
        super(name);
    }

    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                "CREATE TABLE T (\n"
                + "  ti TINYINT,\n"
                + "  si SMALLINT ,\n"
                + "  int INTEGER NOT NULL ,\n"
                + "  bi BIGINT ,\n"
                + "  flt FLOAT ,\n"
                + "  dec DECIMAL ,\n"
                + "  ts TIMESTAMP ,\n"
                + "  str VARCHAR(64),\n"
                + "  bin VARBINARY(64),\n"
                + "  pt GEOGRAPHY_POINT,\n"
                + "  pol GEOGRAPHY \n"
                + ");\n"
                ;
        project.addLiteralSchema(literalSchema);
        project.setUseDDLSchema(true);
    }
    public void testTypeConversion() throws IOException, ProcCallException {
        Client client = getClient();
        client.callProcedure("T.Insert", 1, 1, 1, 1, 1, new BigDecimal(1),
                "2012-12-01", "hi", "10", null, null);

        // test different cases of type conversion that are allowed
        client.callProcedure("ProcToTestTypeConversion",
                             ProcToTestTypeConversion.TestAllAllowedTypeConv,
                             0, 0);


        // Use the conversion matrix to test type conversion cases that are allowed and blocked.
        // This uses non-array argument list as supplied arguments for params (except varbinary
        // that is byte[])
        int rowId = 0;
        VoltType typeToTest = VoltType.INVALID;
        String errorMsg = null;
        for(boolean[] from: m_typeConversionMatrix) {
            int colInd = 0;
            typeToTest = m_tableColTypeVal[rowId];
            for (boolean to: from) {
                if (to) {
                    // type conversion feasible
                    client.callProcedure("ProcToTestTypeConversion",
                                         ProcToTestTypeConversion.TestTypeConvWithInsertProc,
                                         colInd, typeToTest.getValue());

                }
                else {
                    errorMsg = "Incompatible parameter type: can not convert type '"
                            + typeToTest.getName() +
                            "' to '" + m_tableColTypeVal[colInd].getName() +
                            "' for arg " + colInd + " for SQL stmt";

                    // type conversion not allowed
                    if (typeToTest == VoltType.TIMESTAMP &&
                            m_tableColTypeVal[colInd] == VoltType.DECIMAL) {
                        // Conversion from Timestamp -> decimal is allowed in voltqueue
                        // sql as the Timestamp -> decimal is supported in comparison
                        // expression for select and insert statement with select statement with
                        // where predicate. So for update queries this works and hence is allowed
                        // in voltqueue sql. But in case of insert this conversion is not allowed
                        // as the conversion is flagged in EE with different error message So
                        // test for that
                        errorMsg = "Type "+ typeToTest.getName() +" can't be cast as "
                                    + m_tableColTypeVal[colInd].getName();
                    } else if (typeToTest == VoltType.VARBINARY) {
                        switch (m_tableColTypeVal[colInd]) {
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case TIMESTAMP:
                        case FLOAT:
                        case DECIMAL:
                            errorMsg = "Type "+ typeToTest.getName() +" can't be cast as "
                                    + m_tableColTypeVal[colInd].getName();
                            break;
                        default:
                            break;
                        }
                    }
                    verifyProcFails(client, errorMsg, "ProcToTestTypeConversion",
                                    ProcToTestTypeConversion.TestTypeConvWithInsertProc,
                                    colInd, typeToTest.getValue());
                }
                colInd++;
            }
            rowId++;
        }

        // test cases where supplied is an array of arguments. This internally uses select statements
        // with IN predicate to test broadest conversion allowed in comparison of in list arguments
        rowId = 0;
        for(boolean[] from: m_typeConversionMatrixInList) {
            int colInd = 0;
            typeToTest = m_tableColTypeVal[rowId];
            for (boolean to: from) {
                if (rowId == 0) {
                    // currently there is known issue in EE where array of tinyInt get's interpreted as
                    // varbinary. so don't test those cases- ENG-10107. Once the ticket is fixed, enable
                    // testing of conversion array of tinyint params for different types
                    colInd++;
                    continue;
                }
                if (to) {
                    // type conversion feasible
                    client.callProcedure("ProcToTestTypeConversion",
                                         ProcToTestTypeConversion.TestTypesInList,
                                         colInd, typeToTest.getValue());
                }
                else {
                    errorMsg = "Incompatible parameter type: can not convert type '"
                            + typeToTest.getName() +
                            "' to '" + m_tableColTypeVal[colInd].getName() +
                            "' for arg 0 for SQL stmt";

                    verifyProcFails(client, errorMsg, "ProcToTestTypeConversion",
                                    ProcToTestTypeConversion.TestTypesInList,
                                    colInd, typeToTest.getValue());
                }
                colInd++;
            }
            rowId++;
        }
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestTypeConversionSuite.class);
        boolean success = false;


        try {
            VoltProjectBuilder project = new VoltProjectBuilder();
            config = new LocalCluster("geography-value-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            project.addProcedures(ProcToTestTypeConversion.class);
            success = config.compile(project);
        }
        catch (IOException excp) {
            fail();
        }

        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
