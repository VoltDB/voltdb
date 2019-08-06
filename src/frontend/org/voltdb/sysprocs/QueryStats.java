package org.voltdb.sysprocs;

import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.Pair;
import org.voltdb.*;
import org.voltdb.volttableutil.VoltTableUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.volttableutil.VoltTableUtil;

public class QueryStats extends VoltSystemProcedure {
    private final static String tempTableAlias = "TT";
    Pattern stats_proc = Pattern.compile("(\\(\\s*exec\\s*@Statistics\\s*[a-zA-Z]+\\s*,\\s*\\d+\\s*\\))");

   public VoltTable run(String sql) throws Exception {
       StatsSelector[] proc_types = StatsSelector.getAllStatsCollector();
       List<Pair<String, VoltTable>> tables = new LinkedList<>();
       StringBuffer buf = new StringBuffer();
       Matcher m = this.stats_proc.matcher(sql);

       while(m.find()) {
           String proc = m.group(1);
           m.appendReplacement(buf, Matcher.quoteReplacement(tempTableAlias + tables.size()));
           JSONObject obj = new JSONObject();
           Pattern subselector = Pattern.compile("[a-zA-Z]+");
           Matcher s = subselector.matcher(proc);
           obj.put("selector", "STATISTICS");

           for (StatsSelector proc_type : proc_types) {
               if(proc.toUpperCase().contains(proc_type.name())) {
                   obj.put("subselector", proc_type.name());
               }
           }

           obj.put("interval", false);
           tables.add(new Pair<String, VoltTable>(tempTableAlias + tables.size(), VoltDB.instance().getStatsAgent().collectDistributedStats(obj)[0], false));
       }

       m.appendTail(buf);
       return VoltTableUtil.executeSql(buf.toString().replaceAll(";", ""), tables);
   }

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(
    Map<Integer, List<VoltTable>> dependencies, long fragmentId,
    ParameterSet params, SystemProcedureExecutionContext context)
    {
        throw new RuntimeException("Pause was given an " +
        "invalid fragment id: " + String.valueOf(fragmentId));
    }
}
