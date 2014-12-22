package org.voltdb.tomcat;

import org.voltdb.*;

public class VoltdbInsert extends VoltProcedure {
	
	final String insertLog_sql = "INSERT INTO Logs (?,?);";
	public final SQLStmt insertLog = new SQLStmt(insertLog_sql);
	
	public VoltTable[] run (String message) throws VoltAbortException {
		voltQueueSQL(insertLog, message);
		return voltExecuteSQL();
	}
}