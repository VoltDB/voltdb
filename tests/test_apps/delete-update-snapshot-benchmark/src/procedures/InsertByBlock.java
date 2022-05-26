/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

import java.math.BigDecimal;


public class InsertByBlock extends VoltProcedure {

    final static SQLStmt INSERT_SQL = new SQLStmt("INSERT INTO PARTITIONED (ID, BLOCK_ID, MOD_ID, TINY, SMALL, INTEG, BIG, FLOT, DECML, TIMESTMP,"
            + " VCHAR_INLINE,  VCHAR_INLINE_MAX,  VCHAR_OUTLINE_MIN,  VCHAR_OUTLINE,  VCHAR_DEFAULT, "
            + "VARBIN_INLINE, VARBIN_INLINE_MAX, VARBIN_OUTLINE_MIN, VARBIN_OUTLINE, VARBIN_DEFAULT, "
            + "POINT, POLYGON) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(long blockId, long id, String[] columnNames, String[] columnValues)
            throws VoltAbortException
    {
        // Get the query args, as an Object array
        Object[] args = getInsertArgs(id, blockId, columnNames, columnValues);

        voltQueueSQL(INSERT_SQL, args);
        return voltExecuteSQL(true);
    }

    Object[] getInsertArgs(long id, long blockId,
            String[] columnNames, String[] columnValues) {

        int numColumns = columnNames.length;
        Object[] argsArray = new Object[22];

        // Initialize all column values as null, by default
        for (int i=0; i < numColumns; i++) {
            argsArray[i] = null;
        }

        argsArray[0] = id;
        argsArray[2] = id;
        argsArray[1] = blockId;

        // Numerical columns whose types have a special null value in Volt
        argsArray[3] = VoltType.TINYINT.getNullValue();
        argsArray[4] = VoltType.SMALLINT.getNullValue();
        argsArray[5] = VoltType.INTEGER.getNullValue();
        argsArray[6] = VoltType.BIGINT.getNullValue();
        argsArray[7] = VoltType.FLOAT.getNullValue();

        // Determine which columns are non-null
        for (int i=0; i < numColumns; i++) {
            try {
                switch (columnNames[i].toUpperCase()) {
                case "TINY":
                case "TINYINT":
                    argsArray[3] = Byte.parseByte(columnValues[i]);
                    break;
                case "SMALL":
                case "SMALLINT":
                    argsArray[4] = Short.parseShort(columnValues[i]);
                    break;
                case "INT":
                case "INTEG":
                case "INTEGER":
                    argsArray[5] = Integer.parseInt(columnValues[i]);
                    break;
                case "BIG":
                case "BIGINT":
                    argsArray[6] = Long.parseLong(columnValues[i]);
                    break;
                case "FLOT":
                case "FLOAT":
                    argsArray[7] = Double.parseDouble(columnValues[i]);
                    break;
                case "DECML":
                case "DECIMAL":
                    argsArray[8] = new BigDecimal(columnValues[i]);
                    break;
                case "TIME":
                case "TIMESTMP":
                case "TIMESTAMP":
                    argsArray[9] = columnValues[i];
                    break;
                case "VCHAR_INLINE":
                    argsArray[10] = columnValues[i];
                    break;
                case "VCHAR_INLINE_MAX":
                    argsArray[11] = columnValues[i];
                    break;
                case "VCHAR_OUTLINE_MIN":
                    argsArray[12] = columnValues[i];
                    break;
                case "VCHAR_OUTLINE":
                    argsArray[13] = columnValues[i];
                    break;
                case "VCHAR_DEFAULT":
                case "VARCHAR":
                    argsArray[14] = columnValues[i];
                    break;
                case "VARBIN_INLINE":
                    argsArray[15] = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_INLINE_MAX":
                    argsArray[16] = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_OUTLINE_MIN":
                    argsArray[17] = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_OUTLINE":
                    argsArray[18] = columnValues[i].getBytes().toString();
                    break;
                case "VARBIN_DEFAULT":
                case "VARBINARY":
                    argsArray[19] = columnValues[i].getBytes().toString();
                    break;
                case "POINT":
                case "GEOGRAPHY_POINT":
                    argsArray[20] = GeographyPointValue.fromWKT(columnValues[i]);
                    break;
                case "POLYGON":
                case "GEOGRAPHY":
                    argsArray[21] = new GeographyValue(columnValues[i]);
                    break;
                default:
                    throw new VoltTypeException("Unknown column name: '"+columnNames[i]+"'.");
                }
            } catch (IllegalArgumentException e) {
                throw new VoltTypeException("Unable to convert value '"+columnValues[i]
                        +"', for column name '"+columnNames[i]+"'.", e);
            }
        }
        return argsArray;
    }
}
