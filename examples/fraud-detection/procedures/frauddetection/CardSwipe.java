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

package frauddetection;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class CardSwipe extends VoltProcedure {
    private static byte ACTIVITY_INVALID = 0;
    private static byte ACTIVITY_EXIT = -1;
    private static byte ACTIVITY_ENTER = 1;
    private static byte ACTIVITY_PURCHASE = 2;

    public static byte ACTIVITY_ACCEPTED = 1;
    public static byte ACTIVITY_REJECTED = 0;

    public final SQLStmt checkCard = new SQLStmt(
        "SELECT enabled, card_type, balance, expires, name, phone, email, notify FROM cards WHERE card_id = ?;");

    public final SQLStmt chargeCard = new SQLStmt(
        "UPDATE cards SET balance = ? WHERE card_id = ?;");

    public final SQLStmt checkStationFare = new SQLStmt(
        "SELECT fare, name FROM stations WHERE station_id = ?;");

    public final SQLStmt insertActivity = new SQLStmt(
        "INSERT INTO activity (card_id, date_time, station_id, activity_code, amount, accept) VALUES (?,?,?,?,?,?);");

    public final SQLStmt exportActivity = new SQLStmt(
        "INSERT INTO card_alert_export (card_id, export_time, station_name, name, phone, email, notify, alert_message) VALUES (?,?,?,?,?,?,?,?);");
    public final SQLStmt replenishCard = new SQLStmt("UPDATE cards SET balance = balance + ? WHERE card_id = ? AND card_type = 0");

    // for returning results as a VoltTable
    final VoltTable resultTemplate = new VoltTable(
            new VoltTable.ColumnInfo("card_accepted",VoltType.TINYINT),
            new VoltTable.ColumnInfo("message",VoltType.STRING));

    public VoltTable buildResult(int accepted, String msg) {
        VoltTable r = resultTemplate.clone(64);
        r.addRow(accepted, msg);
        return r;
    }

    public static String intToCurrency(int i) {
        return String.format("%d.%02d", i/100, i%100);
    }

    public VoltTable run(int cardId, long tsl, int stationId, byte activity_code, int amt) throws VoltAbortException {

        // check station fare, card status, get card owner's particulars
        voltQueueSQL(checkCard, EXPECT_ZERO_OR_ONE_ROW, cardId);
        voltQueueSQL(checkStationFare, EXPECT_ONE_ROW, stationId);
        VoltTable[] checks = voltExecuteSQL();
        VoltTable cardInfo = checks[0];
        VoltTable stationInfo = checks[1];
        byte accepted = 0;

        // check that card exists
        if (cardInfo.getRowCount() == 0) {
            return buildResult(accepted,"Card Invalid");
        }

        // Exit
        if (activity_code == ACTIVITY_EXIT) {
            voltQueueSQL(insertActivity, cardId, tsl, stationId, activity_code, 0, ACTIVITY_ACCEPTED);
            voltExecuteSQL(true);
            return buildResult(1, "");
        }
        //Replenish card.
        if (activity_code == ACTIVITY_PURCHASE) {
            voltQueueSQL(replenishCard, amt, cardId);
            voltExecuteSQL(true);
            return buildResult(1, "Replinished");
        }

        // card exists, so advanceRow to read the record
        cardInfo.advanceRow();
        int enabled = (int)cardInfo.getLong(0);
        int cardType = (int)cardInfo.getLong(1);
        int balance = (int)cardInfo.getLong(2);
        TimestampType expires = cardInfo.getTimestampAsTimestamp(3);
        String owner = cardInfo.getString(4);
        String phone = cardInfo.getString(5);
        String email = cardInfo.getString(6);
        int notify = (int)cardInfo.getLong(7);

        // read station fare
        stationInfo.advanceRow();
        int fare = (int)stationInfo.getLong(0);
        String stationName = stationInfo.getString(1);
        TimestampType ts = new TimestampType(tsl);

        // if card is disabled
        if (enabled == 0) {
                return buildResult(accepted,"Card Disabled");
        }

        // check balance or expiration for valid cards
        if (cardType == 0) { // pay per ride
                if (balance > fare) {
                    if (isFraud(cardId, ts, stationId)) {
                        // Fraud
                        voltQueueSQL(insertActivity, cardId, ts, stationId, ACTIVITY_ENTER, fare, ACTIVITY_REJECTED);
                        voltExecuteSQL(true);
                        return buildResult(0, "Fraudulent transaction");
                    } else {
                        // charge the fare
                        voltQueueSQL(chargeCard, balance - fare, cardId);
                        voltQueueSQL(insertActivity, cardId, ts, stationId, ACTIVITY_ENTER, fare, ACTIVITY_ACCEPTED);
                        voltExecuteSQL(true);
                        return buildResult(1, "Remaining Balance: " + intToCurrency(balance - fare));
                    }
                } else {
                        // insufficient balance
                        voltQueueSQL(insertActivity, cardId, ts, stationId, ACTIVITY_ENTER, 0, ACTIVITY_REJECTED);
                        if (notify != 0) {  // only export if notify is 1 or 2 -- email or text
                            voltQueueSQL(exportActivity, cardId, getTransactionTime().getTime(), stationName, owner, phone, email, notify, "Insufficient Balance");
                        }
                        voltExecuteSQL(true);
                        return buildResult(0,"Card has insufficient balance: "+intToCurrency(balance));
                }
        }
        else { // unlimited card (e.g. monthly or weekly pass)
                if (expires.compareTo(ts) > 0) {
                    if (isFraud(cardId, ts, stationId)) {
                        // Fraud
                        voltQueueSQL(insertActivity, cardId, ts, stationId, ACTIVITY_ENTER, 0, ACTIVITY_REJECTED);
                        voltExecuteSQL(true);
                        return buildResult(0, "Fraudulent transaction");
                    } else {
                        voltQueueSQL(insertActivity, cardId, ts, stationId, ACTIVITY_ENTER, 0, ACTIVITY_ACCEPTED);
                        voltExecuteSQL(true);
                        return buildResult(1, "Card Expires: " + expires.toString());
                    }
                } else {
                        voltQueueSQL(insertActivity, cardId, ts, stationId, ACTIVITY_ENTER, 0, ACTIVITY_REJECTED);
                        voltExecuteSQL(true);
                        return buildResult(0,"Card Expired");
                }
        }
    }

    public final SQLStmt cardHistoryAtStations = new SQLStmt(
        "SELECT activity_code, COUNT(DISTINCT station_id) AS stations " +
        "FROM activity " +
        "WHERE card_id = ? AND date_time >= DATEADD(HOUR, -1, ?) " +
        "GROUP BY activity_code;"
    );

    public final SQLStmt cardEntries = new SQLStmt(
    "SELECT activity_code " +
    "FROM activity " +
    "WHERE card_id = ? AND station_id = ? AND date_time >= DATEADD(HOUR, -1, ?) " +
    "ORDER BY date_time;"
    );

    public boolean isFraud(int cardId, TimestampType ts, int stationId) {
        voltQueueSQL(cardHistoryAtStations, cardId, ts);
        voltQueueSQL(cardEntries, cardId, stationId, ts);
        final VoltTable[] results = voltExecuteSQL();
        final VoltTable cardHistoryAtStationisTable = results[0];
        final VoltTable cardEntriesTable = results[1];

        while (cardHistoryAtStationisTable.advanceRow()) {
            final byte activity_code = (byte) cardHistoryAtStationisTable.getLong("activity_code");
            final long stations = cardHistoryAtStationisTable.getLong("stations");

            if (activity_code == ACTIVITY_ENTER) {
                // Is more than 5 entries at different stations in past hour?
                if (stations >= 5) {
                    return true;
                }
            }
        }

        byte prevActivity = ACTIVITY_INVALID;
        int entranceCount = 0;
        while (cardEntriesTable.advanceRow()) {
            final byte activity_code = (byte) cardHistoryAtStationisTable.getLong("activity_code");

            if (prevActivity == ACTIVITY_INVALID || prevActivity == activity_code) {
                if (activity_code == ACTIVITY_ENTER) {
                    prevActivity = activity_code;
                    entranceCount++;
                } else {
                    prevActivity = ACTIVITY_INVALID;
                }
            }
        }

        // Is more than 10 consecutive entries in past hour?
        if (entranceCount >= 10) {
            return true;
        }

        return false;
    }
}
