package org.voltdb.benchmark.workloads.procedures;

import org.voltdb.*;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo
(
    singlePartition = false
)
public class SelectAll extends VoltProcedure
{
      public final SQLStmt selectItem = new SQLStmt
      (
          "SELECT MINIBENCHMARK_ID,  MINIBENCHMARK_ITEM " +
          "FROM MINIBENCHMARK;"
      );

      public VoltTable[] run()
          throws VoltAbortException
      {
          voltQueueSQL(selectItem);

          return voltExecuteSQL(true);
      }
}
