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
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.voltdb.client.ClientResponse;
import static org.voltdb.utils.HTTPAdminListener.HTML_CONTENT_TYPE;

/**
 *
 * Logout the current user by invalidating session.
 */
public class LogoutServlet extends VoltBaseServlet {

    // GET on /logout resources.
    @Override
    public void doGet(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {
        super.doGet(request, response);
        String target = request.getPathInfo();
        if (target == null) target = "/";
        try {
            unauthenticate(request);
            response.setContentType(HTML_CONTENT_TYPE);
            response.sendRedirect("/");
        } catch (Exception ex) {
            rateLimitedLogWarn("Not servicing url: %s Details: ", target, ex.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(null, ClientResponse.UNEXPECTED_FAILURE, Throwables.getStackTraceAsString(ex)));
        }
    }
}
