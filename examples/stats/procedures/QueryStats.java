import java.util.List;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.volttableutil.VoltTableUtil;

public class QueryStats extends VoltProcedure {
    Pattern stats_proc = Pattern.compile("(\\(\\s*exec\\s*@Statistics\\s*[a-zA-Z]+\\s*,\\s*\\d+\\s*\\))");
    String[] proc_types = {"TABLE", "DR", "DRPRODUCER", "DRPRODUCERNODE", "DRPRODUCERPARTITION", "SNAPSHOTSTATUS",
             "MEMORY", "CPU", "IOSTATS", "INITIATOR", "INDEX", "PROCEDURE", "PROCEDUREINPUT", "PROCEDUREOUTPUT",
             "PROCEDUREPROFILE", "PROCEDUREDETAIL", "STARVATION", "IDLETIME", "QUEUE", "PLANNER", "LIVECLIENTS", "LATENCY",
             "LATENCY_COMPRESSED", "LATENCY_HISTOGRAM", "MANAGEMENT", "REBALANCE", "KSAFETY", "DRCONSUMER",
             "DRCONSUMERNODE", "DRCONSUMERPARTITION", "COMMANDLOG", "IMPORTER", "IMPORT", "DRROLE", "GC", "TTL", "EXPORT"};

    public QueryStats() {
    }

    public VoltTable run(String sql) throws Exception {
        List<Pair<String, VoltTable>> tables = new LinkedList<>();
        StringBuffer buf = new StringBuffer();
        Matcher m = this.stats_proc.matcher(sql);

        while(m.find()) {
            String proc = m.group(1);
            m.appendReplacement(buf, Matcher.quoteReplacement("TT" + tables.size()));
            JSONObject obj = new JSONObject();
            Pattern subselector = Pattern.compile("[a-zA-Z]+");
            Matcher s = subselector.matcher(proc);
            obj.put("selector", "STATISTICS");

            for (String proc_type : proc_types) {
                if(proc.toUpperCase().contains(proc_type)) {
                    obj.put("subselector", proc_type);
                }
            }

            obj.put("interval", false);
            tables.add(new Pair<String, VoltTable>("TT" + tables.size(), VoltDB.instance().getStatsAgent().collectDistributedStats(obj)[0], false));
        }

        m.appendTail(buf);
        return VoltTableUtil.executeSql(buf.toString().replaceAll(";", ""), tables);
    }
}
