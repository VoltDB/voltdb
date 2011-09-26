/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.AsyncContinuation;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.voltdb.CatalogContext;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Cluster;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.HTTPAdminListener.RequestHandler;
import org.voltdb.utils.HTTPAdminListener.StudioHander;

public class HTTPAdminListener {

    Server m_server = new Server();
    HTTPClientInterface httpClientInterface = new HTTPClientInterface();
    final boolean m_jsonEnabled;
    Map<String, String> m_htmlTemplates = new HashMap<String, String>();

    class StudioHander extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {

            // redirect the base dir
            if (target.equals("/")) target = "/index.htm";

            // check if a file exists
            URL url = VoltDB.class.getResource("studio" + target);
            if (url == null) {
                // write 404
                String msg = "404: Resource not found.\n";
                response.setContentType("text/plain;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                baseRequest.setHandled(true);
                response.getWriter().print(msg);
            }

            // read the template
            InputStream is = VoltDB.class.getResourceAsStream("studio" + target);

            if (target.endsWith("/index.htm")) {
                // load the file as text
                BufferedReader r = new BufferedReader(new InputStreamReader(is));
                StringBuilder sb = new StringBuilder();
                String line = null;
                while ((line = r.readLine()) != null) {
                    sb.append(line + "\n");
                }
                r.close(); is.close();
                String template = sb.toString();

                // fill in missing values in the template
                Cluster cluster = VoltDB.instance().getCatalogContext().cluster;
                template = template.replace("${hostname}", "localhost");
                template = template.replace("${portnumber}", String.valueOf(cluster.getHttpdportno()));
                template = template.replace("${requires-authentication}", cluster.getSecurityenabled() ? "true" : "false");

                // set the headers
                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);

                // write the response
                assert(template != null);
                response.getWriter().print(template);
            }
            else {
                // set the mime type in a giant hack
                String mime = "text/html;charset=utf-8";
                if (target.endsWith(".js"))
                    mime = "application/x-javascript;charset=utf-8";
                if (target.endsWith(".css"))
                    mime = "text/css;charset=utf-8";
                if (target.endsWith(".gif"))
                    mime = "image/gif";
                if (target.endsWith(".png"))
                    mime = "image/png";
                if ((target.endsWith(".jpg")) || (target.endsWith(".jpeg")))
                    mime = "image/jpeg";

                // set the headers
                response.setContentType(mime);
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);

                // write the file out
                BufferedInputStream bis = new BufferedInputStream(is);
                OutputStream os = response.getOutputStream();
                int c = -1;
                while ((c = bis.read()) != -1) {
                    os.write(c);
                }
            }
        }
    }

    class RequestHandler extends AbstractHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {

            // if this is an internal jetty retry, then just tell
            // jetty we're still working on it. There is a risk of
            // masking other errors in doing this, but it's probably
            // low compared with the default policy of retrys.
            AsyncContinuation cont = baseRequest.getAsyncContinuation();
            // this is set to false on internal jetty retrys
            if (!cont.isInitial()) {
                // The continuation object has been woken up by the
                // retry. Tell it to go back to sleep.
                cont.suspend();
                return;
            }

            // kick over to the HTTP/JSON interface
            if (baseRequest.getRequestURI().contains("/api/1.0/")) {
                // http://www.ietf.org/rfc/rfc4627.txt dictates this mime type
                response.setContentType("application/json;charset=utf-8");
                if (m_jsonEnabled) {
                    httpClientInterface.process(baseRequest, response);

                    // used for perf testing of the http interface
                    /*String msg = "{\"status\":1,\"appstatus\":-128,\"statusstring\":null,\"appstatusstring\":null,\"exception\":null,\"results\":[{\"status\":-128,\"schema\":[{\"name\":\"SVAL1\",\"type\":9},{\"name\":\"SVAL2\",\"type\":9},{\"name\":\"SVAL3\",\"type\":9}],\"data\":[[\"FOO\",\"BAR\",\"BOO\"]]}]}";
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);
                    response.getWriter().print(msg);*/
                }
                else {
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    baseRequest.setHandled(true);
                    response.getWriter().println("JSON API IS CURRENTLY DISABLED");
                }
                return;
            }

            // handle the CSV request for memory stats
            if (baseRequest.getRequestURI().contains("/memorycsv/")) {
                String msg = SystemStatsCollector.getCSV();
                response.setContentType("text/plain;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
                response.getWriter().print(msg);
                return;
            }

            try {
                // handle the basic info page below this

                CatalogContext context = VoltDB.instance().getCatalogContext();

                // get the cluster info
                String clusterinfo = context.numberOfNodes + " hosts ";
                clusterinfo += " with " + context.numberOfExecSites + " sites ";
                clusterinfo += " (" + context.numberOfExecSites / context.numberOfNodes;
                clusterinfo += " per host)";

                // get the start time
                long t = SystemStatsCollector.getStartTime();
                Date date = new Date(t);
                long duration = System.currentTimeMillis() - t;
                long minutes = duration / 60000;
                long hours = minutes / 60; minutes -= hours * 60;
                long days = hours / 24; hours -= days * 24;
                String starttime = String.format("%s (%dd %dh %dm)",
                        date.toString(), days, hours, minutes);

                // get the hostname, but fail gracefully
                String hostname = "&lt;unknownhost&gt;";
                try {
                    InetAddress addr = InetAddress.getLocalHost();
                    hostname = addr.getHostName();
                }
                catch (Exception e) {
                    try {
                        InetAddress addr = InetAddress.getLocalHost();
                        hostname = addr.getHostAddress();
                    }
                    catch (Exception e2) {}
                }

                // get memory usage
                SystemStatsCollector.Datum d = SystemStatsCollector.getRecentSample();
                double totalmemory = SystemStatsCollector.memorysize;
                double used = d.rss / (double) (SystemStatsCollector.memorysize * 1024 * 1024);
                double javaclaimed = d.javatotalheapmem + d.javatotalsysmem;
                double javaused = d.javausedheapmem + d.javausedsysmem;
                double javaunused = SystemStatsCollector.javamaxheapmem - d.javatotalheapmem;
                double risk = (d.rss + javaunused) / (SystemStatsCollector.memorysize * 1024 * 1024);

                // get csvfilename
                String csvFilename = String.format("memstats-%s-%s.csv", hostname, new Date(System.currentTimeMillis()).toString());

                // just print voltdb version for now
                Map<String,String> params = new HashMap<String,String>();

                params.put("hostname", hostname);
                params.put("version", VoltDB.instance().getVersionString());
                params.put("buildstring", VoltDB.instance().getBuildString());
                params.put("cluster", clusterinfo);
                params.put("starttime", starttime);
                params.put("csvfilename", csvFilename);

                params.put("2mincharturl", SystemStatsCollector.getGoogleChartURL(2, 320, 240, "-2min"));
                params.put("30mincharturl", SystemStatsCollector.getGoogleChartURL(30, 640, 240, "-30min"));
                params.put("24hrcharturl", SystemStatsCollector.getGoogleChartURL(1440, 640, 240, "-24hrs"));

                params.put("totalmemory", String.format("%.1f", totalmemory));
                params.put("used", String.format("%.1f", used * 100.0));
                params.put("risk", String.format("%.1f", risk * 100.0));
                params.put("rss", String.format("%.1f", d.rss / 1024.0 / 1024.0));
                params.put("usedjava", String.format("%.1f", javaused / 1024.0 / 1024.0));
                params.put("claimedjava", String.format("%.1f", javaclaimed / 1024.0 / 1024.0));
                params.put("javamaxheap", String.format("%.1f", SystemStatsCollector.javamaxheapmem / 1024.0 / 1024.0));

                String msg = getHTMLForAdminPage(params);

                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
                response.getWriter().print(msg);
            }
            catch (IOException e) {
                e.printStackTrace();
                throw e;
            }
        }

    }

    /**
     * Load a template for the admin page, fill it out and return the value.
     * @param params The key-value set of variables to replace in the template.
     * @return The completed template.
     */
    String getHTMLForAdminPage(Map<String,String> params) {
        try {
            String template = m_htmlTemplates.get("admintemplate.html");
            for (Entry<String, String> e : params.entrySet()) {
                String key = e.getKey().toUpperCase();
                String value = e.getValue();
                if (key == null) continue;
                if (value == null) value = "NULL";
                template = template.replace("#" + key + "#", value);
            }
            return template;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return "<html><body>An unrecoverable error was encountered while generating this page.</body></html>";
    }

    private void loadTemplate(String name) throws Exception {
        // 8 lines or so just to read the file
        InputStream is = HTTPAdminListener.class.getResourceAsStream(name);
        BufferedReader r = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line = null;
        while ((line = r.readLine()) != null) {
            sb.append(line);
        }
        r.close(); is.close();
        String template = sb.toString();
        m_htmlTemplates.put(name, template);
    }

    public HTTPAdminListener(boolean jsonEnabled, int port) throws Exception {
        // PRE-LOAD ALL HTML TEMPLATES (one for now)
        try {
            loadTemplate("admintemplate.html");
        }
        catch (Exception e) {
            VoltLogger logger = new VoltLogger("HOST");
            logger.error("Unable to load HTML templates from jar for admin pages.", e);
            throw e;
        }

        // NOW START JETTY SERVER
        try {
            // The socket channel connector seems to be faster for our use
            //SelectChannelConnector connector = new SelectChannelConnector();
            SocketConnector connector = new SocketConnector();

            connector.setPort(port);
            connector.setName("VoltDB-HTTPD");
            m_server.addConnector(connector);

            ContextHandler studioHander = new ContextHandler("/studio");
            studioHander.setHandler(new StudioHander());

            ContextHandler baseHander = new ContextHandler("/");
            baseHander.setHandler(new RequestHandler());

            ContextHandlerCollection handlers = new ContextHandlerCollection();
            handlers.setHandlers(new Handler[] {
                    studioHander,
                    baseHander
            });

            m_server.setHandler(handlers);
            m_server.start();
            m_jsonEnabled = jsonEnabled;
        }
        catch (Exception e) {
            // double try to make sure the port doesn't get eaten
            try { m_server.stop(); } catch (Exception e2) {}
            try { m_server.destroy(); } catch (Exception e2) {}
            throw e;
        }
    }

    public void stop() {
        try {
            m_server.stop();
            m_server.join();
        }
        catch (Exception e) {}
        try { m_server.destroy(); } catch (Exception e2) {}
        m_server = null;
    }

    public void notifyOfCatalogUpdate()
    {
        httpClientInterface.notifyOfCatalogUpdate();
    }
}
