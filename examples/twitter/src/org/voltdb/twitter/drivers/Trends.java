package org.voltdb.twitter.drivers;

import java.util.LinkedList;
import java.util.List;

import org.voltdb.twitter.server.WebServer;
import org.voltdb.twitter.util.TrendsListener;

import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStreamFactory;

public class Trends {
    
    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("usage: [username] [password] [web server port] [hashtag count] " +
                "[server list (comma seperated)]");
            System.exit(-1);
        }
        
        String username = args[0];
        String password = args[1];
        int port = Integer.parseInt(args[2]);
        int limit = Integer.parseInt(args[3]);
        String commaSeparatedServers = args[4];
        
        // parse the server list
        List<String> servers = new LinkedList<String>();
        String[] commaSeparatedServersParts = commaSeparatedServers.split(",");
        for (String server : commaSeparatedServersParts) {
            servers.add(server.trim());
        }
        
        // verify twitter credentials (if valid, no exception is thrown)
        try {
            new TwitterFactory().getInstance(username, password).verifyCredentials();
        } catch (TwitterException e) {
            System.err.println("Could not verify Twitter credentials");
            System.exit(-1);
        }
        
        // start collecting tweets
        new TwitterStreamFactory(new TrendsListener(servers)).getInstance(username, password).sample();
        
        // start the web server
        new WebServer(servers, port, limit).start();
    }
    
}
