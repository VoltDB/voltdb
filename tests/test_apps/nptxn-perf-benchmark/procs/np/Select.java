package np;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class Select extends VoltProcedure {

    public final SQLStmt selAcct = new SQLStmt("SELECT * FROM card_account WHERE pan >= ? and pan < ?;");

    public final SQLStmt selActv = new SQLStmt("SELECT * FROM card_activity WHERE pan >= ? and pan < ?;");

    public long run (String start, String end) {
        voltQueueSQL(selAcct, EXPECT_NON_EMPTY, start, end);
        voltQueueSQL(selActv, start, end);
        VoltTable results[] = voltExecuteSQL(true);
        return 1;
    }
}