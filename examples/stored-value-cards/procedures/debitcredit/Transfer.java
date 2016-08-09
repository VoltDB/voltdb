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


public class Transfer extends VoltProcedure {

    public final SQLStmt getAcct = new SQLStmt("SELECT * FROM card_account WHERE pan = ?;");

    public final SQLStmt updateAcct = new SQLStmt("UPDATE card_account SET " +
                                                  " balance = balance + ?," +
                                                  " available_balance = available_balance + ?," +
                                                  " last_activity = ?" +
                                                  " WHERE pan = ?;");

    public final SQLStmt insertActivity = new SQLStmt("INSERT INTO card_activity VALUES (?,?,?,?,?);");


    public long run( String        from_pan,
                     String        to_pan,
		     double        amount,
                     String        currency
		     ) throws VoltAbortException {

	long result = 0;

	voltQueueSQL(getAcct, EXPECT_ZERO_OR_ONE_ROW, from_pan);
	voltQueueSQL(getAcct, EXPECT_ZERO_OR_ONE_ROW, to_pan);

        // assume everything is good, eliminates a round-trip to the partitions
        voltQueueSQL(updateAcct,
                     -amount,
                     -amount,
                     getTransactionTime(),
                     from_pan
                     );

        voltQueueSQL(updateAcct,
                     amount,
                     amount,
                     getTransactionTime(),
                     to_pan
                     );

        voltQueueSQL(insertActivity,
                     from_pan,
                     getTransactionTime(),
                     "TRANSFER",
                     "D",
                     -amount
                     );

        voltQueueSQL(insertActivity,
                     to_pan,
                     getTransactionTime(),
                     "TRANSFER",
                     "C",
                     amount
                     );


	VoltTable accts[] = voltExecuteSQL(true);
	
	VoltTableRow from_acct = accts[0].fetchRow(0);
	int from_available = (int)from_acct.getLong(1);
        double from_balance = from_acct.getDouble(3);

	VoltTableRow to_acct = accts[1].fetchRow(0);
	int to_available = (int)to_acct.getLong(1);

        if (from_available == 0) {
            // card is not available for authorization or redemption
            throw new VoltAbortException("The transfer from card PAN " + from_pan + " was not available");
        }
        if (to_available == 0) {
            // card is not available for authorization or redemption
            throw new VoltAbortException("The transfer to card PAN " + to_pan + " was not available");
        }

        if (from_balance < amount) {
            // no balance available, so this will be declined
            throw new VoltAbortException("The transfer from card PAN " + from_pan + " rejected for insufficient balance");
        }

        return 1;
    }
}
