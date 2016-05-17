package procedures;

import org.voltdb.*;

public class InsertDelete extends VoltProcedure {

    public final SQLStmt insert0 = new SQLStmt("INSERT INTO tmp_0 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert1 = new SQLStmt("INSERT INTO tmp_1 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert2 = new SQLStmt("INSERT INTO tmp_2 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert3 = new SQLStmt("INSERT INTO tmp_3 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert4 = new SQLStmt("INSERT INTO tmp_4 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert5 = new SQLStmt("INSERT INTO tmp_5 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert6 = new SQLStmt("INSERT INTO tmp_6 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert7 = new SQLStmt("INSERT INTO tmp_7 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert8 = new SQLStmt("INSERT INTO tmp_8 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert9 = new SQLStmt("INSERT INTO tmp_9 (appid,deviceid) VALUES (?,?);");

    public final SQLStmt delete0 = new SQLStmt("DELETE FROM tmp_0 WHERE deviceid = ?;");
    public final SQLStmt delete1 = new SQLStmt("DELETE FROM tmp_1 WHERE deviceid = ?;");
    public final SQLStmt delete2 = new SQLStmt("DELETE FROM tmp_2 WHERE deviceid = ?;");
    public final SQLStmt delete3 = new SQLStmt("DELETE FROM tmp_3 WHERE deviceid = ?;");
    public final SQLStmt delete4 = new SQLStmt("DELETE FROM tmp_4 WHERE deviceid = ?;");
    public final SQLStmt delete5 = new SQLStmt("DELETE FROM tmp_5 WHERE deviceid = ?;");
    public final SQLStmt delete6 = new SQLStmt("DELETE FROM tmp_6 WHERE deviceid = ?;");
    public final SQLStmt delete7 = new SQLStmt("DELETE FROM tmp_7 WHERE deviceid = ?;");
    public final SQLStmt delete8 = new SQLStmt("DELETE FROM tmp_8 WHERE deviceid = ?;");
    public final SQLStmt delete9 = new SQLStmt("DELETE FROM tmp_9 WHERE deviceid = ?;");


    public VoltTable[] run(int appid, long deviceid)
        throws VoltAbortException {

        voltQueueSQL(insert0,appid,deviceid);
        voltQueueSQL(insert1,appid,deviceid);
        voltQueueSQL(insert2,appid,deviceid);
        voltQueueSQL(insert3,appid,deviceid);
        voltQueueSQL(insert4,appid,deviceid);
        voltQueueSQL(insert5,appid,deviceid);
        voltQueueSQL(insert6,appid,deviceid);
        voltQueueSQL(insert7,appid,deviceid);
        voltQueueSQL(insert8,appid,deviceid);
        voltQueueSQL(insert9,appid,deviceid);

        voltQueueSQL(delete0,deviceid);
        voltQueueSQL(delete1,deviceid);
        voltQueueSQL(delete2,deviceid);
        voltQueueSQL(delete3,deviceid);
        voltQueueSQL(delete4,deviceid);
        voltQueueSQL(delete5,deviceid);
        voltQueueSQL(delete6,deviceid);
        voltQueueSQL(delete7,deviceid);
        voltQueueSQL(delete8,deviceid);
        voltQueueSQL(delete9,deviceid);

        return voltExecuteSQL();
    }
}
