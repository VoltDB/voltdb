/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
package org.voltdb.twitter.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import org.voltdb.twitter.database.DB;
import org.voltdb.twitter.util.HTMLUtils;

public class WebServer {

    private static final long SECOND = 1000L * 1000L;
    private static final long MINUTE = 60L * SECOND;
    private static final long HOUR = 60L * MINUTE;

    private DB db;
    private int port;
    private int limit;
    private ServerSocket server;

    public WebServer(List<String> servers, int port, int limit) {
        this.db = new DB(servers);
        this.port = port;
        this.limit = limit;
        try {
            this.server = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // main server loop
    public void start() {
        System.out.println("Web server online, go to http://localhost:" + port + "/ in a browser");
        while (true) {
            try {
                Socket client = server.accept();
                BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String header = reader.readLine();
                String resource = header.split("\\s+")[1].substring(1);

                OutputStream out = client.getOutputStream();

                render(out, resource);
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void render(OutputStream outputStream, String resource) {
        if (resource.equals("")) {
            HTMLUtils util = new HTMLUtils();
            List<String> html = new LinkedList<String>();
            html.addAll(util.header());
            html.add("<div id=\"top\">");
            html.add("<p>");
            html.addAll(util.image("twitter.png", 224, 55, "Twitter"));
            html.add("</p>");
            html.add("</div>");

            html.add("<div id=\"middle\">");
            html.add("<h1>Twitter Trends</h1>");
            html.add("</div>");

            html.add("<div id=\"bottom\">");
            html.add("<table><tbody><tr>");

            // past 15 seconds
            html.add("<td>");
            html.add("<h2>15 seconds</h2>");
            html.addAll(util.table(db.selectHashTags(15L * SECOND, limit)));
            html.add("</td>");

            // past 1 minute
            html.add("<td>");
            html.add("<h2>1 minute</h2>");
            html.addAll(util.table(db.selectHashTags(1L * MINUTE, limit)));
            html.add("</td>");

            // past 5 minutes
            html.add("<td>");
            html.add("<h2>5 minutes</h2>");
            html.addAll(util.table(db.selectHashTags(5L * MINUTE, limit)));
            html.add("</td>");

            // past 30 minutes
            html.add("<td>");
            html.add("<h2>30 minutes</h2>");
            html.addAll(util.table(db.selectHashTags(30L * MINUTE, limit)));
            html.add("</td>");

            // past 2 hours
            html.add("<td>");
            html.add("<h2>2 hours</h2>");
            html.addAll(util.table(db.selectHashTags(2L * HOUR, limit)));
            html.add("</td>");

            html.add("</tr></tbody></table>");
            html.add("</div>");
            html.addAll(util.footer());
            new HttpResponse(html, "html").send(outputStream);
        } else if (resource.equals("style.css")) {
            new HttpResponse(new File("www/" + resource), "css").send(outputStream);
        } else if (resource.equals("twitter.png")) {
            new HttpResponse(new File("www/" + resource), "png").send(outputStream);
        } else {
            System.err.println("Unknown resource request: " + resource);
        }
    }

}
