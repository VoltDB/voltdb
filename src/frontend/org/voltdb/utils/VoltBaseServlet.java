/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import static org.voltdb.utils.HTTPAdminListener.HTML_CONTENT_TYPE;

import java.io.IOException;
import java.net.InetAddress;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.AuthenticationResult;
import org.voltdb.ClientResponseImpl;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.compilereport.ReportMaker;

/**
 *
 * @author akhanzode
 */
public class VoltBaseServlet extends HttpServlet {

    private static final long serialVersionUID = -7435813850243095149L;
    protected VoltLogger m_log = new VoltLogger("HOST");
    private String m_hostHeader = null;
    protected HTTPAdminListener httpAdminListener = VoltDB.instance().getHttpAdminListener();
    protected HTTPClientInterface httpClientInterface = httpAdminListener.httpClientInterface;

    public VoltBaseServlet() {

    }

    @Override
    public void init() {

    }

    protected String buildClientResponse(String jsonp, byte code, String msg) {
        ClientResponseImpl rimpl = new ClientResponseImpl(code, new VoltTable[0], msg);
        return HTTPClientInterface.asJsonp(jsonp, rimpl.toJSONString());
    }

    protected String getHostHeader() {
        if (m_hostHeader != null) {
            return m_hostHeader;
        }

        if (!httpAdminListener.m_publicIntf.isEmpty()) {
            m_hostHeader = httpAdminListener.m_publicIntf;
            return m_hostHeader;
        }

        InetAddress addr = null;
        int httpPort = VoltDB.DEFAULT_HTTP_PORT;
        try {
            String localMetadata = VoltDB.instance().getLocalMetadata();
            JSONObject jsObj = new JSONObject(localMetadata);
            JSONArray interfaces = jsObj.getJSONArray("interfaces");
            //The first interface is external interface if specified.
            String iface = interfaces.getString(0);
            addr = InetAddress.getByName(iface);
            httpPort = jsObj.getInt("httpPort");
        } catch (Exception e) {
            m_log.warn("Failed to get HTTP interface information.", e);
        }
        if (addr == null) {
            addr = org.voltcore.utils.CoreUtils.getLocalAddress();
        }
        //Make the header string.
        m_hostHeader = addr.getHostAddress() + ":" + httpPort;
        return m_hostHeader;
    }

    public AuthenticationResult authenticate(HttpServletRequest request) {
        return httpClientInterface.authenticate(request);
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        doGet(request, response);
    }

    @Override
    protected void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
        response.setHeader("Host", getHostHeader());
    }

    /**
     * Draw the catalog report page, mostly by pulling it from the JAR.
     */
    void handleReportPage(HttpServletRequest request, HttpServletResponse response) {
        try {
            String report = ReportMaker.liveReport();

            response.setContentType(HTML_CONTENT_TYPE);
            response.setStatus(HttpServletResponse.SC_OK);

            response.getWriter().print(report);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
