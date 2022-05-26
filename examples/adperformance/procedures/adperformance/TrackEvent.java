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

package adperformance;

import java.math.BigDecimal;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class TrackEvent extends VoltProcedure {

    public final SQLStmt selectCreative = new SQLStmt(
        "SELECT campaign_id, advertiser_id FROM creatives WHERE creative_id = ?;");

    public final SQLStmt selectInventory = new SQLStmt(
        "SELECT site_id, page_id FROM inventory WHERE inventory_id = ?;");

    public final SQLStmt insertEvent = new SQLStmt(
        "INSERT INTO event_data VALUES (" +
        "?,?,?,?,?,?,?," +
        "?,?,?,?,?,?,?" +
        ");");

    public long run( TimestampType utc_time,
                     long ip_address,
                     long cookie_uid,
                     int creative_id,
                     int inventory_id,
                     int type_id,
                     BigDecimal cost
             ) throws VoltAbortException {

        // derive counter fields from type_id
        //   0 = impression
        //   1 = clickthrough
        //   2 = conversion
        int is_impression = (type_id == 0) ? 1 : 0;
        int is_clickthrough = (type_id == 1) ? 1 : 0;
        int is_conversion = (type_id == 2) ? 1 : 0;

        // lookup creative_id and inventory_id
        voltQueueSQL(selectCreative, creative_id);
        voltQueueSQL(selectInventory, inventory_id);
        VoltTable lookups[] = voltExecuteSQL();

        VoltTableRow creative = lookups[0].fetchRow(0);
        int campaign_id = (int)creative.getLong(0);
        int advertiser_id = (int)creative.getLong(1);

        VoltTableRow inventory = lookups[1].fetchRow(0);
        int site_id = (int)inventory.getLong(0);
        int page_id = (int)inventory.getLong(1);

        voltQueueSQL( insertEvent,
                      utc_time,
                      ip_address,
                      cookie_uid,
                      creative_id,
                      inventory_id,
                      type_id,
                      cost,
                      campaign_id,
                      advertiser_id,
                      site_id,
                      page_id,
                      is_impression,
                      is_clickthrough,
                      is_conversion
                      );

        voltExecuteSQL();

        return ClientResponse.SUCCESS;
    }
}
