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

package bankoffers;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class CheckForOffers extends VoltProcedure {

    public final SQLStmt insertActivity = new SQLStmt(
        "INSERT INTO activity VALUES ( ?,?,?,?,?,?,?);");

    public final SQLStmt checkCustState = new SQLStmt(
        "SELECT c.cust_state "+
        "FROM account a "+
        "INNER JOIN customer c ON a.cust_id = c.cust_id "+
        "WHERE a.acc_no = ?;");

    // check for vendor offers
    public final SQLStmt getVendorOffers = new SQLStmt(
        "SELECT vo.offer_text "+
        "FROM acct_vendor_totals avt "+
        "INNER JOIN vendor_offers vo ON avt.vendor_id = vo.vendor_id "+
        "WHERE "+
        "  avt.acc_no = ? AND"+
        "  avt.vendor_id = ? AND"+
        "  avt.total_visits > vo.min_visits AND"+
        "  avt.total_spend > vo.min_spend "+
        "ORDER BY vo.offer_priority, vo.offer_text;");

    public final SQLStmt insertOffer = new SQLStmt(
        "INSERT INTO offers_given VALUES (?,?,NOW,?);");
    public final SQLStmt insertOffer_export = new SQLStmt(
            "INSERT INTO offers_given_export VALUES (?,?,NOW,?);");

    public long run(long txnId,
                    long acctNo,
                    double txnAmt,
                    String txnState,
                    String txnCity,
                    TimestampType txnTimestamp,
                    int vendorId) throws VoltAbortException
    {
        // insert activity
        voltQueueSQL(insertActivity, txnId, acctNo, txnAmt, txnState, txnCity, txnTimestamp, vendorId);

        // get vendor offers
        voltQueueSQL(getVendorOffers, acctNo, vendorId);

        VoltTable[] results0 = voltExecuteSQL();

        // if offers found
        if (results0[1].getRowCount() > 0) {
            String offerText = results0[1].fetchRow(0).getString(0);
            voltQueueSQL(insertOffer, acctNo, vendorId, offerText);
            voltQueueSQL(insertOffer_export, acctNo, vendorId, offerText);
            voltExecuteSQL();
        }

        return ClientResponse.SUCCESS;
    }
}
