package org.voltdb.benchmark.workloads.procedures;

import org.voltdb.*;

/** A VoltDB stored procedure is a Java class defining one or
 * more SQL statements and implementing a <code>public
 * VoltTable[] run</code> method. VoltDB requires a
 * <code>ProcInfo</code> annotation providing metadata for the
 * procedure.  The <code>run</code> method is
 * defined to accept one or more parameters. These parameters take the
 * values the client passes via the
 * <code>VoltClient.callProcedure</code> invocation.
 *
 * <a
 * href="https://hzproject.com/svn/repos/doc/trunk/Stored%20Procedure%20API.docx">Stored
 * Procedure API</a> specifies valid stored procedure definitions,
 * including valid run method parameter types, required annotation
 * metadata, and correct use the Volt query interface.
*/
@ProcInfo(
    partitionInfo = "MINIBENCHMARK.MINIBENCHMARK_ID: 0",
    singlePartition = true
)
public class Select extends VoltProcedure {

    public final SQLStmt selectItem =
      new SQLStmt("SELECT MINIBENCHMARK_ID,  MINIBENCHMARK_ITEM " +
                  "FROM MINIBENCHMARK WHERE  MINIBENCHMARK_ID = ?");

    public VoltTable[] run( int MINIBENCHMARK_ID ) throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL( selectItem, MINIBENCHMARK_ID );

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
