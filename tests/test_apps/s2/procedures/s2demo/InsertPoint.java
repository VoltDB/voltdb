/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package s2demo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2LatLng;

public class InsertPoint extends VoltProcedure {

    final SQLStmt insStmt = new SQLStmt("insert into points values ("
            + "?, "                // id
            + "?, "                // name
            + "pointfromtext(?), " // point
            + "?"                  // cell id
            + ")");

    public long run(long id, String name, double lat, double lng) {
        S2LatLng ll = S2LatLng.fromDegrees(lat, lng);
        S2CellId cell = S2CellId.fromLatLng(ll);

        String wkt = "point(" + lat + " " + lng + ")";
        voltQueueSQL(insStmt, id, name, wkt, cell.id());
        VoltTable vt = voltExecuteSQL(true)[0];
        vt.advanceRow();
        return vt.getLong(0);
    }
}
