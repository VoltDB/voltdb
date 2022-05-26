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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.server.Request;
import org.voltdb.AuthenticationResult;
import org.voltdb.HTTPClientInterface;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.DeploymentRequestServlet.MapperHolder;

/**
 *
 * Servers /profile endpoint which returns existing users configured and their roles.
 */
public class UserProfileServlet extends VoltBaseServlet {

    private static final long serialVersionUID = 8909068708587475420L;
    private final ObjectMapper m_mapper = MapperHolder.mapper;

    //This is a wrapper to generate JSON for profile of authenticated user.
    private final class Profile {
        private final String user;
        private final String permissions[];
        public Profile(String u, String[] p) {
            user = u;
            permissions = p;
        }
        //These methods are not really unused but used by ObjectMapper
        @SuppressWarnings("unused")
        public String getUser() {
            return user;
        }
        //These methods are not really unused but used by ObjectMapper
        @SuppressWarnings("unused")
        public String[] getPermissions() {
            return permissions;
        }
    }

    // GET on /profile resources.
    @Override
    public void doGet(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {
        super.doGet(request, response);
        //jsonp is specified when response is expected to go to javascript function.
        String jsonp = request.getParameter(HTTPClientInterface.JSONP);
        AuthenticationResult authResult = null;
        String target = request.getPathInfo();
        if (target == null) target = "/";
        try {
            response.setContentType(HTTPAdminListener.JSON_CONTENT_TYPE);
            if (!HTTPClientInterface.validateJSONP(jsonp, (Request )request, response)) {
                return;
            }
            response.setStatus(HttpServletResponse.SC_OK);
            authResult = authenticate(request);
            if (!authResult.isAuthenticated()) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, authResult.m_message));
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }

            if (!target.equals("/")) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Resource not found"));
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            if (jsonp != null) {
                response.getWriter().write(jsonp + "(");
            }
            m_mapper.writeValue(response.getWriter(), new Profile(authResult.m_user, authResult.m_perms));
            if (jsonp != null) {
                response.getWriter().write(")");
            }
        } catch (Exception ex) {
            rateLimitedLogWarn("Not servicing url: %s Details: ", target, ex.getMessage());
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(null, ClientResponse.UNEXPECTED_FAILURE, Throwables.getStackTraceAsString(ex)));
        }
    }
}
