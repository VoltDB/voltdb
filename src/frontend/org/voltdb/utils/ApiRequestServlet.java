/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import com.google_voltpatches.common.base.Throwables;
import static org.voltdb.utils.HTTPAdminListener.JSON_CONTENT_TYPE;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;

import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;

/**
 *
 * The servlet that handles all API request for HTTP-JSON interface.
 * This servlet calls procedures system or otherwise.
 */
public class ApiRequestServlet extends VoltBaseServlet {

    private static final long serialVersionUID = -6240161897983329796L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        super.doGet(request, response);
        String target = request.getPathInfo();
        if (target == null) target = "/";
        try {
            // http://www.ietf.org/rfc/rfc4627.txt dictates this mime type
            response.setContentType(JSON_CONTENT_TYPE);
            if (VoltDB.instance().getHttpAdminListener().m_jsonEnabled) {
                if (target.equals("/")) {
                    httpClientInterface.process((Request )request, response);
                } else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.getWriter().println("Resource not found");
                }

                // used for perf testing of the http interface
                /*String msg = "{\"status\":1,\"appstatus\":-128,\"statusstring\":null,\"appstatusstring\":null,\"exception\":null,\"results\":[{\"status\":-128,\"schema\":[{\"name\":\"SVAL1\",\"type\":9},{\"name\":\"SVAL2\",\"type\":9},{\"name\":\"SVAL3\",\"type\":9}],\"data\":[[\"FOO\",\"BAR\",\"BOO\"]]}]}";
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);
                    response.getWriter().print(msg);*/
            } else {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.getWriter().println("JSON API IS CURRENTLY DISABLED");
            }

        } catch (Exception ex) {
            rateLimitedLogWarn("Not servicing url: %s Details: ", target, ex.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(null, ClientResponse.UNEXPECTED_FAILURE, Throwables.getStackTraceAsString(ex)));
        }
    }
}
