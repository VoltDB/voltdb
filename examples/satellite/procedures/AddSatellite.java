/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.*;

@ProcInfo(
    singlePartition = false
)
public class AddSatellite extends VoltProcedure {

    public final SQLStmt getLastId =
        new SQLStmt("SELECT ID FROM SATELLITE " +
                    " ORDER BY ID DESC LIMIT 1;");
    public final SQLStmt insertSatItem =
        new SQLStmt("INSERT INTO SATELLITE (ID,MODEL_NUMBER,COUNTRY) " +
                    "VALUES (?,?,?);");
    public final SQLStmt insertLocItem =
        new SQLStmt("INSERT INTO LOCATION (ID,LATITUDE,LONGITUDE) " +
                    "VALUES (?,?,?);");

    public long run( long id, String model, String country,
                     double latitude, double longitude) {
        long newid;

        // First, get the last ID, add 1, then use this as the new id.
        voltQueueSQL( getLastId);
        VoltTable[] queryresults = voltExecuteSQL();
        VoltTable result = queryresults[0];
        if (result.getRowCount() == 1) {
            newid = result.fetchRow(0).getLong(0);
            newid++;
        }
        else {
            newid = 0;
        }

        // Add a SQL statement to the execution queue. Queries
        // and DMLs may not be mixed in one batch.
        voltQueueSQL( insertSatItem, newid, model, country );
        voltQueueSQL( insertLocItem, newid, latitude, longitude );

        // Run all queued queries.
        queryresults = voltExecuteSQL();

        return newid;
    }
}
