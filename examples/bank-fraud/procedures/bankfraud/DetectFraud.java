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

package bankfraud;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

//import procedures.PerformanceTimer;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class DetectFraud extends VoltProcedure {

    // performance timing
    //public final SQLStmt insertTiming = new SQLStmt("INSERT INTO procedure_timings VALUES (?,?,?,?);");

    //SQL to check if account exists
    public final SQLStmt CheckAcctExists = new SQLStmt(
        "SELECT ACC_NO FROM ACCOUNT WHERE ACC_NO=?;");

    //SQL to check if account is already identified as fraudulent
    public final SQLStmt CheckAlreadyFraud = new SQLStmt(
        "SELECT ACC_NO"
        + " FROM ACCOUNT_FRAUD WHERE ACC_NO=?;");

    //SQL to check if any transaction exists for account in transaction view
    public final SQLStmt TxnAlreadyStatus = new SQLStmt(
        "SELECT THRESHOLD,TOTAL_TXN_AMT"
        + " FROM TRANSACTION_VW WHERE ACC_NO=? AND TXN_DT=TRUNCATE(DAY,?);");

    //SQL to insert values in transaction table
    public final SQLStmt TxnInsert = new SQLStmt( "INSERT INTO TRANSACTION VALUES ( ?,?,?,?,?,?,?);");

    //SQL to retrieve rules from rule table
    public final SQLStmt GetRule = new SQLStmt( "SELECT * FROM RULES ORDER BY RULE_ID;");

    public final SQLStmt checkCustState = new SQLStmt(
        "SELECT DISTINCT c.cust_state "+
        "FROM account a "+
        "INNER JOIN customer c ON a.cust_id = c.cust_id "+
        "WHERE a.acc_no = ?;");

    public final SQLStmt AcctFraudInsert = new SQLStmt ("INSERT INTO ACCOUNT_FRAUD VALUES (?,?,?,?,?);");

    public final SQLStmt getTransactions = new SQLStmt("SELECT * FROM transaction WHERE acc_no = ? and TRUNCATE(DAY,txn_ts) = TRUNCATE(DAY,?) ORDER BY txn_ts;");

    public ScriptEngineManager manager = new ScriptEngineManager();
    public ScriptEngine engine = manager.getEngineByName("js");


    // public static boolean evalBoolExpression(String expr) {
    //     ScriptEngineManager manager = new ScriptEngineManager();
    //     ScriptEngine engine = manager.getEngineByName("js");
    //     Object result = null;
    //     try {
    //         result = engine.eval(expr);
    //         //System.out.println(result);
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    //     return ((Boolean)result).booleanValue();
    // }

    public VoltTable[] run( long txnId,
                            long acctNo,
                            double txnAmt,
                            String txnState,
                            String txnCity,
                            TimestampType txnTimestamp)
        throws VoltAbortException {

        //PerformanceTimer pt = new PerformanceTimer();
        //pt.start("validate inputs");

        String txnStatus = "Approved";
        long threshold = 1;
        double totalAmt = txnAmt;
        Long ruleId = null;
        boolean includeDetail = false;

        // run all the information-gathering queries up front
        // if no fraud, will need to run them all anyway, and better to do it in one batch (one voltExecuteSQL() call)

        //pt.start("run queries");

        // Check if account exists
        voltQueueSQL(CheckAcctExists,EXPECT_ONE_ROW,acctNo);
        // Check if account is already in the fraud table
        voltQueueSQL(CheckAlreadyFraud,EXPECT_ZERO_OR_ONE_ROW,acctNo);
        // Check customer state for the account
        voltQueueSQL(checkCustState,EXPECT_ONE_ROW,acctNo);
        // get count and sum of transactions so far
        voltQueueSQL(TxnAlreadyStatus,EXPECT_ZERO_OR_ONE_ROW,acctNo,txnTimestamp);
        // get the rules
        voltQueueSQL(GetRule,EXPECT_NON_EMPTY);
        VoltTable[] results1 = voltExecuteSQL();

        //pt.start("check if already fraud");
        // if account was found in fraud table, status is denied
        if (results1[1].getRowCount() == 1 ) {
            txnStatus="Denied";
        }

        //pt.start("check for card chameleon");
        // check for card chameleon
        if (!txnStatus.equals("Denied")) {
            //Cust_State=getCustomerState(acctNo);
            VoltTable t = results1[2];
            t.advanceRow();
            String Cust_State=t.getString(0);
            if(!Cust_State.equals(txnState)) {
                txnStatus="Denied";
                ruleId = new Long(99);
                voltQueueSQL(AcctFraudInsert,acctNo,ruleId,"Y","N",getTransactionTime());
            }
        }

        //pt.stop();

        // evaluate other rules
        if (!txnStatus.equals("Denied")) {

            // if there were previous transactions for the day
            if (results1[3].getRowCount() == 1) {
                VoltTable t = results1[3];
                t.advanceRow();
                threshold += t.getLong(0);
                totalAmt += t.getDouble(1);
            }

            // evaluate each rule, stop if a rule results in Denied
            VoltTable rules = results1[4];
            Object result = null;
            while (rules.advanceRow() && !txnStatus.equals("Denied")) {
                String cond = rules.getString(1);
                ruleId = rules.getLong(0);
                //pt.start("rule "+ruleId);
                cond = cond.replaceAll("Threshold",String.valueOf(threshold));
                cond = cond.replaceAll("Total_Txn_Amt",String.valueOf(totalAmt));
                // evaluate expression
                boolean denied = false;
                try {
                    denied = ((Boolean)engine.eval(cond)).booleanValue();
                } catch (ScriptException e) {
                    throw new VoltAbortException("ScriptEngine Exception: " + e.getMessage());
                }
                if (denied) {
                    txnStatus="Denied";
                    includeDetail=true;
                    voltQueueSQL(getTransactions,acctNo,txnTimestamp);
                    voltQueueSQL(AcctFraudInsert,acctNo,ruleId,"Y","N",txnTimestamp);
                }
            }
        }

        //pt.start("get details");
        // (optionally get details, insert fraud record) and insert Transaction record
        voltQueueSQL(TxnInsert,txnId,acctNo,txnAmt,txnState,txnCity,txnTimestamp,txnStatus);
        VoltTable[] results2 = voltExecuteSQL();
        VoltTable detail = results2[0];
        //pt.stop();

        // insert timing results
        // for (Map.Entry<String, Long> entry : pt.getResults().entrySet()) {
        //     voltQueueSQL(insertTiming,acctNo,"DetectFraud",entry.getKey(),entry.getValue());
        //     voltExecuteSQL();
        // }

        // status results
        VoltTable t = new VoltTable(
                new VoltTable.ColumnInfo("txn_status",VoltType.STRING),
                new VoltTable.ColumnInfo("rule_id",VoltType.BIGINT),
                new VoltTable.ColumnInfo("threshold",VoltType.BIGINT),
                new VoltTable.ColumnInfo("total_amt",VoltType.BIGINT));
        t.addRow(new Object[] {
                txnStatus,
                ruleId,
                threshold,
                totalAmt});

        // array of final results
        if (includeDetail) {
            return new VoltTable[]{ t, detail };
        } else {
            return new VoltTable[]{ t };
        }

    }
}
