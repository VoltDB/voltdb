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

    // column types of table T in order they are defined
    private static final VoltType[] m_tableColTypeVal = {
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
            VoltType.GEOGRAPHY,
    };
    private static final String m_javaTypeNamePatternForInsertTest[] = {
            "Byte",
            "Short",
            "Integer",
            "Long",
            "Double",
            "BigDecimal",
            "TimestampType",
            "String",
            "byte\\[\\]",
            "GeographyPointValue",
            "GeographyValue",
    };
    private static final VoltType[] m_tableColInListTypeVal = {
            VoltType.INLIST_OF_BIGINT,
            VoltType.INLIST_OF_BIGINT,
            VoltType.INLIST_OF_BIGINT,
            VoltType.INLIST_OF_BIGINT,
            null, // IN LIST of FLOAT not supported
            null, // IN LIST of DECIMAL not supported
            VoltType.INLIST_OF_BIGINT,
            VoltType.INLIST_OF_STRING,
            null, // IN LIST of VARBINARY not supported
            null, // IN LIST of GEOGRAPHY_POINT not supported
            null, // IN LIST of GEOGRAPHY not supported
    };
    private static final String m_javaTypeNamePatternForInListTest[] = {
            // Interpretation of byte[] as a list of tiny int is prevented
            // by an overriding interpretation of byte[] as a VARBINARY value.
            // So, it can not be supported,
            // unless/until we find a way around that interpretation.
            "byte\\[\\]",
            "short\\[\\]",
            "int\\[\\]",
            "long\\[\\]",
            "double\\[\\]",
            "BigDecimal\\[\\]",
            "TimestampType\\[\\]",
            "String\\[\\]",
            "byte\\[\\]\\[\\]",
            "GeographyPointValue\\[\\]",
            "GeographyValue\\[\\]",
    };
    // A list of non-array types that should all fail to type-check when passed
    // to "IN ?" parameters that expect some kind of array argument.
    private static final String m_javaTypeNamePatternForInListFailureTest[] = {
            "Byte",
            "Short",
            "Integer",
            "Long",
            "Double",
            "BigDecimal",
            "TimestampType",
            "String",
            // Interpretation of byte[] as a list of tiny int is prevented
            // by an overriding interpretation of byte[] as a VARBINARY value.
            // So, it can not be supported,
            // unless/until we find a way around that interpretation.
            // Expect the error message for this case to look a little different.
            "byte\\[\\]",
            "GeographyPointValue",
            "GeographyValue",
    };

    // Row index provides the type converting "from"
    // Column index provides the type converting "to"
    // To see type for column (to) or row (from), map it's
    // index m_tableColTypeVal to get it's type.
    private static final boolean [][] m_typeConversionMatrix = {
            /* To:
             tiny   small  int    bigint float  decimal ts    string binary point  polygon  // From: */
            {true,  true,  true,  true,  true,  true,  true,  true,  false, false, false},  // TinyInt
            {true,  true,  true,  true,  true,  true,  true,  true,  false, false, false},  // SmallInt
            {true,  true,  true,  true,  true,  true,  true,  true,  false, false, false},  // Integer
            {true,  true,  true,  true,  true,  true,  true,  true,  false, false, false},  // BigInt
            {true,  true,  true,  true,  true,  true,  true,  true,  false, false, false},  // Float
            {true,  true,  true,  true,  true,  true,  true,  true,  false, false, false},  // Decimal
            {true,  true,  true,  true,  true,  false, true,  true,  false, false, false},  // TIMESTAMP
            {true,  true,  true,  true,  true,  true,  true,  true,  false, false, false},  // VARCHAR/STRING
            {false, false, false, false, false, false, false, true,  true,  false, false},  // VARBINARY
            {false, false, false, false, false, false, false, false, false, true,  false},  // POINT
            {false, false, false, false, false, false, false, false, false, false, true},   // POLYGON
    };

    // Row index provides the type converting "from" (user supplied data of certain type).
    // Column index provides the type converting "to" (the actual column type in table).
    // To see type for column (to) or row (from), map it's index m_tableColTypeVal to get it's
    // type this is based on allowed type conversion for comparison which in list param arguments uses
    private static final boolean [][] m_typeConversionMatrixInList = {
            /* To:
             tiny   small  int    bigint float  decimal ts    string binary point  polygon  // From: */
            {true,  true,  true,  true,  false, false, true,  false, false, false, false},  // TinyInt
            {true,  true,  true,  true,  false, false, true,  false, false, false, false},  // SmallInt
            {true,  true,  true,  true,  false, false, true,  false, false, false, false},  // Integer
            {true,  true,  true,  true,  false, false, true,  false, false, false, false},  // BigInt
            {false, false, false, false, false, false, false, false, false, false, false},  // Float
            {false, false, false, false, false, false, false, false, false, false, false},  // Decimal
            {false, false, false, false, false, false, false, false, false, false, false},  // TIMESTAMP
            {false, false, false, false, false, false, false, true,  false, false, false},  // VARCHAR/STRING
            {false, false, false, false, false, false, false, false, true,  false, false},  // VARBINARY
            {false, false, false, false, false, false, false, false, false, true,  false},  // POINT
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
        VoltType typeToTest = VoltType.INVALID;
        String errorMsg = null;
        for (int fromType = 0; fromType < m_tableColTypeVal.length; ++fromType) {
            boolean[] supportedFromType = m_typeConversionMatrix[fromType];
            typeToTest = m_tableColTypeVal[fromType];
            for (int toType = 0; toType < m_tableColTypeVal.length; ++toType) {
                if (supportedFromType[toType]) {
                    // type conversion feasible
                    client.callProcedure("ProcToTestTypeConversion",
                                         ProcToTestTypeConversion.TestTypeConvWithInsertProc,
                                         toType, typeToTest.getValue());
                }
                else {
                    String typeTriedByInsert = m_javaTypeNamePatternForInsertTest[fromType];

                    // type conversion not allowed
                    if (typeToTest == VoltType.TIMESTAMP &&
                            m_tableColTypeVal[toType] == VoltType.DECIMAL) {
                        // Conversion from Timestamp -> decimal is allowed in voltqueue
                        // sql as the Timestamp -> decimal is supported in comparison
                        // expression for select and insert statement with select statement with
                        // where predicate. So for update queries this works and hence is allowed
                        // in voltqueue sql. But in case of insert this conversion is not allowed
                        // as the conversion is flagged in EE with different error message So
                        // test for that
                        errorMsg = "Type "+ typeToTest.getName() +" can't be cast as "
                                    + m_tableColTypeVal[toType].getName();
                    }
                    else {
                        errorMsg = "Incompatible parameter type: can not convert type '"
                                + typeTriedByInsert +
                                "' to '" + m_tableColTypeVal[toType].getName() +
                                "' for arg " + toType + " for SQL stmt";
                    }
                    verifyProcFails(client, errorMsg, "ProcToTestTypeConversion",
                                    ProcToTestTypeConversion.TestTypeConvWithInsertProc,
                                    toType, typeToTest.getValue());
                }

                if (typeToTest == VoltType.TINYINT &&
                        m_tableColTypeVal[fromType] == VoltType.TINYINT) {
                    continue;
                }

                // Test that array arguments are not typically compatible
                // with parameters passed into column comparisons.
                String typeTriedByCompare = m_javaTypeNamePatternForInListTest[fromType];
                errorMsg = "Incompatible parameter type: can not convert type '"
                        + typeTriedByCompare +
                        "' to '" + m_tableColTypeVal[toType].getName() +
                        "' for arg 0 for SQL stmt";
                verifyProcFails(client, errorMsg, "ProcToTestTypeConversion",
                                ProcToTestTypeConversion.TestFailingArrayTypeCompare,
                                toType, typeToTest.getValue());
            }
        }

        // Test cases an array argument is supplied to a select statement with
        // an IN ? predicate to test the conversions allowed in comparison of in
        // list arguments,
        // Purposely starting fromType at index 1 instead of 0 because:
        // Interpretation of byte[] as a list of tiny int is prevented
        // by an overriding interpretation of byte[] as a VARBINARY value.
        // So, it can not be supported,
        // unless/until we find a way around that interpretation.
        for (int fromType = 1; fromType < m_tableColTypeVal.length; ++fromType) {
            boolean[] supportedForFromType = m_typeConversionMatrixInList[fromType];
            String typeTriedByInListQuery = m_javaTypeNamePatternForInListTest[fromType];
            typeToTest = m_tableColTypeVal[fromType];
            for (int toType = 0; toType < m_tableColTypeVal.length; ++toType) {
                VoltType inListType = m_tableColInListTypeVal[toType];
                if (supportedForFromType[toType]) {
                    // type conversion feasible
                    client.callProcedure("ProcToTestTypeConversion",
                                         ProcToTestTypeConversion.TestTypesInList,
                                         toType, typeToTest.getValue());
                    if (inListType == null) {
                        // Some column types (non-integer, non-string) do not support
                        // IN LIST matching. these should be caught in the statement
                        // compiler as tested in a statement compiler test, not here.
                        continue;
                    }
                }
                else {
                    if (inListType == null) {
                        // Some column types (non-integer, non-string) do not support
                        // IN LIST matching. these should be caught in the statement
                        // compiler as tested in a statement compiler test, not here.
                        continue;
                    }
                    errorMsg = "Incompatible parameter type: can not convert type '"
                            + typeTriedByInListQuery +
                            "' to '" + inListType.getName() +
                            "' for arg 0 for SQL stmt";

                    verifyProcFails(client, errorMsg, "ProcToTestTypeConversion",
                                    ProcToTestTypeConversion.TestTypesInList,
                                    toType, typeToTest.getValue());
                }

                // Test that non-array arguments are not allowed for IN LIST params.
                String typeExpectedToFailInListQuery =
                        m_javaTypeNamePatternForInListFailureTest[fromType];
                if (typeExpectedToFailInListQuery.endsWith("]") &&
                        inListType == VoltType.INLIST_OF_BIGINT) {
                    errorMsg = "rhs of IN expression is of a non-list type VARBINARY";
                }
                else {
                    errorMsg = "Incompatible parameter type: can not convert type '"
                            + typeExpectedToFailInListQuery +
                            "' to '" + inListType.getName() +
                            "' for arg 0 for SQL stmt";
                }
                verifyProcFails(client, errorMsg, "ProcToTestTypeConversion",
                                ProcToTestTypeConversion.TestFailingTypesInList,
                                toType, typeToTest.getValue());
            }
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
            project.addProcedure(ProcToTestTypeConversion.class);
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
