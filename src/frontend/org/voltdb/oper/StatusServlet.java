/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

package org.voltdb.oper;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json_voltpatches.JSONObject;

import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;

/**
 * Servlet to report status of the instance in which
 * it is executing.
 */
public class StatusServlet extends HttpServlet {

    private static final String HDR_HOST = "Host";
    private static final String HDR_CACHECTRL = "Cache-Control";
    private static final String NO_CACHE = "no-cache";
    private static final String JSON_CONTENT = "application/json";
    private static final String UTF8_ENCODING = "utf-8";
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        response.setHeader(HDR_HOST, StatusListener.instance().getHostHeader());
        response.setHeader(HDR_CACHECTRL, NO_CACHE);
        response.setContentType(JSON_CONTENT);
        response.setCharacterEncoding(UTF8_ENCODING);
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().print(theStatus());
    }

    private String theStatus() throws ServletException {
        try {
            VoltDBInterface instance = VoltDB.instance();
            JSONObject json = new JSONObject();
            json.put("pid", instance.getVoltPid());
            json.put("hostId", instance.getMyHostId());
            json.put("nodeState", instance.getNodeState());
            json.put("clusterState", instance.getMode());
            json.put("startAction", instance.getStartAction());
            json.put("startupProgress", progress(instance));
            json.put("shutdownPending", instance.isPreparingShuttingdown());
            return json.toString();
        }
        catch (Exception ex) {
            throw new ServletException(ex);
        }
    }

    private String progress(VoltDBInterface instance) {
        String p = "complete";
        if (!instance.getNodeStartupComplete()) {
            int[] n = instance.getNodeStartupProgress();
            if (n[1] > 0) {
                if (n[0] < 0) { n[0] = 0; }
                if (n[0] > n[1]) { n[0] = n[1]; }
                p = String.format("%d/%d (%d%%)", n[0], n[1], (n[0] * 100) / n[1]);
            }
            else {
                p = "pending";
            }
        }
        return p;
    }
}
