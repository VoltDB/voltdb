package debitcredit;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class Redeem extends VoltProcedure {

    public final SQLStmt getAcct = new SQLStmt("SELECT * FROM card_account WHERE pan = ?;");

    public final SQLStmt updateAcct = new SQLStmt("UPDATE card_account SET " +
                                                  " balance = ?," +
                                                  " available_balance = ?," +
                                                  " last_activity = ?" +
                                                  " WHERE pan = ?;");

    public final SQLStmt insertActivity = new SQLStmt("INSERT INTO card_activity VALUES (?,?,?,?,?);");


    public long run( String        pan,
		     double        amount,
                     String        currency,
                     int           preAuth
		     ) throws VoltAbortException {

	long result = 0;

	voltQueueSQL(getAcct, EXPECT_ZERO_OR_ONE_ROW, pan);
	VoltTable accts[] = voltExecuteSQL();
	

	if (accts[0].getRowCount() == 0) {
	    // card was not found
	    return 0;
	}

	VoltTableRow acct = accts[0].fetchRow(0);
	int available = (int)acct.getLong(1);
	String status = acct.getString(2);
        double balance = acct.getDouble(3);
        double availableBalance = acct.getDouble(4);
	String acctCurrency = acct.getString(5);

        if (available == 0) {
            // card is not available for authorization or redemption
            return 0;
        }

        double new_balance = balance;
        double new_available_balance = availableBalance;

        if (preAuth == 1) {
            // pre-authorized (we could verify this with an ID, but for simplicity, assume it is)
            // subtract from balance, not available_balance (which was already subtracted)
            new_balance = balance - amount;

            if (balance < amount) {
                // no balance available, so this will be declined
                return 0;
            }

        } else if (preAuth == 0) {
            // not pre-authorized
            // subtract from balance AND available_balance
            new_balance = balance - amount;
            new_available_balance = availableBalance - amount;

            if (balance < amount) {
                // no balance available, so this will be declined
                return 0;
            }
        }

        // account is available with sufficient balance, so execute the transaction
        voltQueueSQL(updateAcct,
                     new_balance,
                     new_available_balance,
                     getTransactionTime(),
                     pan
                     );

        voltQueueSQL(insertActivity,
                     pan,
                     getTransactionTime(),
                     "REDEEM",
                     "D",
                     -amount
                     );

        voltExecuteSQL(true);
        return 1;
    }
}
