/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

import java.io.IOException;

import javax.servlet_voltpatches.ServletException;
import javax.servlet_voltpatches.http.HttpServletRequest;
import javax.servlet_voltpatches.http.HttpServletResponse;

import org.eclipse.jetty_voltpatches.server.Request;
import org.eclipse.jetty_voltpatches.server.Server;
import org.eclipse.jetty_voltpatches.server.bio.SocketConnector;
import org.eclipse.jetty_voltpatches.server.handler.AbstractHandler;
import org.voltdb.CatalogContext;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;

public class HTTPAdminListener {

    Server m_server = new Server();
    HTTPClientInterface httpClientInterface = new HTTPClientInterface();
    final boolean m_jsonEnabled;

    class RequestHandler extends AbstractHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {

            // kick over to the HTTP/JSON interface
            if (baseRequest.getRequestURI().contains("/api/1.0/")) {
                response.setContentType("text/plain;charset=utf-8");
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

            // code for debugging
            //System.out.println( method + " '" + uri + "' " );

            // example form parsing from nanohttpd website
            /*String msg = "<html><body><h1>Hello server</h1>\n";
            if (parms.getProperty("username") == null)
                msg +=
                    "<form action='?' method='get'>\n" +
                    "  <p>Your name: <input type='text' name='username'></p>\n" +
                    "</form>\n";
            else
                msg += "<p>Hello, " + parms.getProperty("username") + "!</p>";

            msg += "</body></html>\n";*/

            CatalogContext context = VoltDB.instance().getCatalogContext();

            // just print voltdb version for now
            String msg = "<html><body>\n";
            msg += "<h2>VoltDB Version " + VoltDB.instance().getVersionString() + "</h2>\n";
            msg += "<p><b>Buildstring:</b> " + VoltDB.instance().getBuildString() + "</p>\n";
            msg += "<p>Running on a cluster of " + context.numberOfNodes + " hosts ";
            msg += " with " + context.numberOfExecSites + " sites ";
            msg += " (" + context.numberOfExecSites / context.numberOfNodes + " per host).</p>\n";
            msg += "</body></html>\n";

            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            response.getWriter().print(msg);
        }

    }

    public HTTPAdminListener(boolean jsonEnabled, int port) throws Exception {
        try {
            // The socket channel connector seems to be faster for our use
            //SelectChannelConnector connector = new SelectChannelConnector();
            SocketConnector connector = new SocketConnector();

            connector.setPort(port);
            connector.setName("VoltDB-HTTPD");
            m_server.addConnector(connector);

            m_server.setHandler(new RequestHandler());
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
}
