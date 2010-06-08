package org.voltdb.twitter.drivers;

import java.util.LinkedList;
import java.util.List;

import org.voltdb.twitter.database.DB;

public class Cull {

    private long retentionRangeMicros;
    private long runFrequencyMillis;
    private DB db;
    
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: [retention range in hours] [run frequency in hours] " +
              "[server list (comma seperated)]");
            System.exit(-1);
        }
        
        int retentionRangeHours = Integer.parseInt(args[0]);
        int runFrequencyHours = Integer.parseInt(args[1]);
        String commaSeparatedServers = args[2];
        
        // parse the server list
        List<String> servers = new LinkedList<String>();
        String[] commaSeparatedServersParts = commaSeparatedServers.split(",");
        for (String server : commaSeparatedServersParts) {
            servers.add(server.trim());
        }
        
        // start the cull process
        new Cull(retentionRangeHours, runFrequencyHours, servers).start();
    }
    
    public Cull(int retentionRangeHours, int runFrequencyHours, List<String> servers) {
        retentionRangeMicros = (long) retentionRangeHours * 60L * 60L * 1000L * 1000L;
        runFrequencyMillis = (long) runFrequencyHours * 60L * 60L * 1000L;
        db = new DB(servers);
    }
    
    public void start() {
        // main loop
        while (true) {
            // wait the specified time period
            try {
                Thread.sleep(runFrequencyMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            // delete hashtags
            long deleteCount = db.deleteHashTags(retentionRangeMicros);
            System.out.println("Deleted " + deleteCount + " old hashtags");
        }
    }
    
}
