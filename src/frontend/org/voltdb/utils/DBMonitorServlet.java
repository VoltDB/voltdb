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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.voltdb.VoltDB;
import static org.voltdb.utils.HTTPAdminListener.HTML_CONTENT_TYPE;

/**
 *
 * This servlet serves index and help html.
 */
public class DBMonitorServlet extends VoltBaseServlet {

    private static final long serialVersionUID = -1053844839053182391L;
    private static final String INDEX_HTM = "/index.htm";
    private static final String HELP_HTM = "/help.htm";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        super.doGet(request, response);
        String target = request.getPathInfo();
        final String uri = request.getRequestURI();
        final String msg = "404: Resource not found.\n";
        try {
            if (uri != null && uri.endsWith("help.htm")) {
                target = HELP_HTM;
            } else  if (uri != null && "/".equals(uri)){
                target = INDEX_HTM;
            }
            // check if a file exists
            URL url = VoltDB.class.getResource("dbmonitor" + target);
            if (url == null) {
                // write 404
                response.setContentType("text/plain;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print(msg);
                return;
            }

            // read the template
            InputStream is = VoltDB.class.getResourceAsStream("dbmonitor" + target);

            if (target.endsWith(INDEX_HTM) || target.endsWith(HELP_HTM)) {
                // set the headers
                response.setContentType(HTML_CONTENT_TYPE);
                response.setStatus(HttpServletResponse.SC_OK);

                OutputStream os = response.getOutputStream();
                BufferedInputStream bis = new BufferedInputStream(is);

                int c = -1;
                while ((c = bis.read()) != -1) {
                    os.write(c);
                }
            } else {
                response.setContentType("text/plain;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print(msg);
            }
        } catch (Exception ex) {
            rateLimitedLogWarn("Not servicing url: %s Details: ", target, ex.getMessage());
            response.setContentType("text/plain;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().print(msg);
        }
    }
}
