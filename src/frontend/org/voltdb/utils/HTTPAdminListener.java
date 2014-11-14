/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
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
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.voltcore.logging.VoltLogger;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.compilereport.ReportMaker;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;

public class HTTPAdminListener {

    Server m_server = new Server();
    HTTPClientInterface httpClientInterface = new HTTPClientInterface();
    final boolean m_jsonEnabled;
    Map<String, String> m_htmlTemplates = new HashMap<String, String>();
    final boolean m_mustListen;

    class DBMonitorHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest,
                           HttpServletRequest request, HttpServletResponse response)
                            throws IOException, ServletException {
            VoltLogger logger = new VoltLogger("HOST");
            try{

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

                //Special handling for API as they continue and setHandled differently
                if (baseRequest.getRequestURI().contains("/api/1.0/")) {
                    baseRequest.setHandled(false);
                    return;
                }
                //Send old /studio back to "/"
                if (baseRequest.getRequestURI().contains("/studio")) {
                    response.sendRedirect("/");
                    baseRequest.setHandled(true);
                    return;
                }
                // redirect the base dir
                if (target.equals("/")) target = "/index.htm";
                // check if a file exists
                URL url = VoltDB.class.getResource("dbmonitor" + target);
                if (url == null) {
                    // write 404
                    String msg = "404: Resource not found.\n"+url.toString();
                    response.setContentType("text/plain;charset=utf-8");
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    baseRequest.setHandled(true);
                    response.getWriter().print(msg);
                    return;
                }

                // read the template
                InputStream is = VoltDB.class.getResourceAsStream("dbmonitor" + target);

                if (target.endsWith("/index.htm")) {

                    // set the headers
                    response.setContentType("text/html;charset=utf-8");
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);

                    OutputStream os = response.getOutputStream();
                    BufferedInputStream bis = new BufferedInputStream(is);

                    int c = -1;
                    while ((c = bis.read()) != -1) {
                        os.write(c);
                    }
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
            }catch(Exception ex){
                logger.error("Error servicing url: " + baseRequest.getRequestURI() + " Details: "+ ex.getMessage());
            }
        }
    }

    class CatalogRequestHandler extends AbstractHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {

            handleReportPage(baseRequest, response);
        }

    }

    class DDLRequestHandler extends AbstractHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {

            byte[] reportbytes = VoltDB.instance().getCatalogContext().getFileInJar("autogen-ddl.sql");
            String ddl = new String(reportbytes, Charsets.UTF_8);
            response.setContentType("text/plain;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            response.getWriter().print(ddl);
        }

    }

    class APIRequestHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest,
                           HttpServletRequest request, HttpServletResponse response)
                            throws IOException, ServletException {
            VoltLogger logger = new VoltLogger("HOST");
            try {
                // http://www.ietf.org/rfc/rfc4627.txt dictates this mime type
                response.setContentType("application/json;charset=utf-8");
                if (m_jsonEnabled) {
                    httpClientInterface.process(baseRequest, response);

                    // used for perf testing of the http interface
                    /*String msg = "{\"status\":1,\"appstatus\":-128,\"statusstring\":null,\"appstatusstring\":null,\"exception\":null,\"results\":[{\"status\":-128,\"schema\":[{\"name\":\"SVAL1\",\"type\":9},{\"name\":\"SVAL2\",\"type\":9},{\"name\":\"SVAL3\",\"type\":9}],\"data\":[[\"FOO\",\"BAR\",\"BOO\"]]}]}";
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);
                    response.getWriter().print(msg);*/
                } else {
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    baseRequest.setHandled(true);
                    response.getWriter().println("JSON API IS CURRENTLY DISABLED");
                }

            } catch(Exception ex){
                logger.error("Error servicing url: " + baseRequest.getRequestURI() + " Details: "+ ex.getMessage());
            }
        }
    }

    /**
     * Draw the catalog report page, mostly by pulling it from the JAR.
     */
    void handleReportPage(Request baseRequest, HttpServletResponse response) {
        try {
            String report = ReportMaker.liveReport();

            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);

            response.getWriter().print(report);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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

    private void loadTemplate(Class<?> clz, String name) throws Exception {
        URL url = Resources.getResource(clz, name);
        String contents = Resources.toString(url, Charsets.UTF_8);
        m_htmlTemplates.put(name, contents);
    }

    public HTTPAdminListener(boolean jsonEnabled, String intf, int port, boolean mustListen) throws Exception {
        m_mustListen = mustListen;
        // PRE-LOAD ALL HTML TEMPLATES (one for now)
        try {
            loadTemplate(HTTPAdminListener.class, "admintemplate.html");
        }
        catch (Exception e) {
            VoltLogger logger = new VoltLogger("HOST");
            logger.error("Unable to load HTML templates from jar for admin pages.", e);
            throw e;
        }

        // NOW START SocketConnector and create Jetty server but dont start.
        SocketConnector connector = null;
        try {
            // The socket channel connector seems to be faster for our use
            //SelectChannelConnector connector = new SelectChannelConnector();
            connector = new SocketConnector();

            if (intf != null && intf.length() > 0) {
                connector.setHost(intf);
            }
            connector.setPort(port);
            connector.statsReset();
            connector.setName("VoltDB-HTTPD");
            //open the connector here so we know if port is available and Init work can retry with next port.
            connector.open();
            m_server.addConnector(connector);

            //"/"
            ContextHandler dbMonitorHandler = new ContextHandler("/");
            dbMonitorHandler.setHandler(new DBMonitorHandler());

            ///api/1.0/
            ContextHandler apiRequestHandler = new ContextHandler("/api/1.0");
            apiRequestHandler.setHandler(new APIRequestHandler());

            ///catalog
            ContextHandler catalogRequestHandler = new ContextHandler("/catalog");
            catalogRequestHandler.setHandler(new CatalogRequestHandler());

            ///catalog
            ContextHandler ddlRequestHandler = new ContextHandler("/ddl");
            ddlRequestHandler.setHandler(new DDLRequestHandler());

            ContextHandlerCollection handlers = new ContextHandlerCollection();
            handlers.setHandlers(new Handler[] {
                    apiRequestHandler,
                    catalogRequestHandler,
                    ddlRequestHandler,
                    dbMonitorHandler
            });

            m_server.setHandler(handlers);

            /*
             * Don't force us to look at a huge pile of threads
             */
            final QueuedThreadPool qtp = new QueuedThreadPool();
            qtp.setMaxIdleTimeMs(15000);
            qtp.setMinThreads(1);
            m_server.setThreadPool(qtp);

            m_jsonEnabled = jsonEnabled;
        } catch (Exception e) {
            // double try to make sure the port doesn't get eaten
            try { connector.close(); } catch (Exception e2) {}
            try { m_server.destroy(); } catch (Exception e2) {}
            throw new Exception(e);
        }
    }

    public void start() throws Exception {
        try {
            m_server.start();
        } catch (Exception e) {
            // double try to make sure the port doesn't get eaten
            try { m_server.stop(); } catch (Exception e2) {}
            try { m_server.destroy(); } catch (Exception e2) {}
            //We only throw exception to halt and we expect to mustListen;
            if (m_mustListen) {
                throw new Exception(e);
            }
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
