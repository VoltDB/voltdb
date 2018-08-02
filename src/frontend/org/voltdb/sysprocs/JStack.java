package org.voltdb.sysprocs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

public class JStack extends VoltSystemProcedure {
    private static final VoltLogger JSTACK_LOG = new VoltLogger("JSTACK");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command)
    {
        Process process = null;
        List<String> processList = new ArrayList<String>();
        try {
            process = Runtime.getRuntime().exec("jps");
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                processList.add(line);
            }
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        List<String> stackTrace = new ArrayList<>();
        for (String line : processList) {
            String[] ss = line.split(" ");
            if(ss.length > 1 && ss[1].equals("VoltDB")) {
                int pid = Integer.parseInt(ss[0]);
                try {
                    Process pcsStackTrace = Runtime.getRuntime().exec("jstack " + pid);
                    BufferedReader input = new BufferedReader(new InputStreamReader(pcsStackTrace.getInputStream()));
                    stackTrace.add("--------------Stack trace for PID " + pid + "--------------");
                    String s = "";
                    while ((s = input.readLine()) != null) {
                        stackTrace.add(s);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        for(String s : stackTrace) {
            JSTACK_LOG.info(s);
        }
        
        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {t});
    }
}
