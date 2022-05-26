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

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.voltdb.AuthenticationResult;
import org.voltdb.HTTPClientInterface;
import org.voltdb.client.ClientResponse;

/**
 *
 * This servlet is used for getting catalog report html.
 */
public class CatalogRequestServlet extends VoltBaseServlet {

    private static final long serialVersionUID = 8267233695774734052L;

    @Override
    public void doGet(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        super.doGet(request, response);
        //jsonp is specified when response is expected to go to javascript function.
        String jsonp = request.getParameter(HTTPClientInterface.JSONP);
        AuthenticationResult authResult = authenticate(request);
        if (!authResult.isAuthenticated()) {
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, authResult.m_message));
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        handleReportPage(request, response);
    }

}
