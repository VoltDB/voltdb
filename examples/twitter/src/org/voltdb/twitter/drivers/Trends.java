/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.twitter.drivers;

import java.util.LinkedList;
import java.util.List;

import org.voltdb.twitter.server.WebServer;
import org.voltdb.twitter.util.TrendsListener;

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

        // start collecting tweets
        new TwitterStreamFactory(new TrendsListener(servers)).getInstance(username, password).sample();

        // start the web server
        new WebServer(servers, port, limit).start();
    }

}
