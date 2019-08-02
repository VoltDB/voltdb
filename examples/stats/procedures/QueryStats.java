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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.ProtectionDomain;

public class QueryStats extends VoltProcedure {
    Pattern stats_proc = Pattern.compile("(\\(\\s*exec\\s*@Statistics\\s*[a-zA-Z]+\\s*,\\s*\\d+\\s*\\))");

    public QueryStats() {
    }

    public VoltTable run(String sql) throws Exception {
        List<Pair<String, VoltTable>> tables = new LinkedList<>();
        StringBuffer buf = new StringBuffer();
        Matcher m = this.stats_proc.matcher(sql);

        while(m.find()) {
            String proc = m.group(1);
            proc.substring(1, proc.length() - 1);
            m.appendReplacement(buf, Matcher.quoteReplacement("tt" + tables.size()));
            JSONObject obj = new JSONObject();
            obj.put("selector", "STATISTICS");
            obj.put("subselector", "TABLE");
            obj.put("interval", false);
            tables.add(new Pair<String, VoltTable>("tt" + tables.size(), VoltDB.instance().getStatsAgent().collectDistributedStats(obj)[0], false));
        }

        m.appendTail(buf);
        return VoltTableUtil.executeSql(buf.toString(), tables);
    }
}
