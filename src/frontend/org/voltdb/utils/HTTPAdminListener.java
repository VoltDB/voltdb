/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import java.util.Properties;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;

public class HTTPAdminListener extends NanoHTTPD {

    public HTTPAdminListener(int port) throws IOException {
        super(port);
    }

    @Override
    public Response serve(String uri, String method, Properties header, Properties parms) {
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
        msg += "<p>Running on a cluser of " + context.numberOfNodes + " hosts ";
        msg += " with " + context.numberOfExecSites + " sites ";
        msg += " (" + context.numberOfExecSites / context.numberOfNodes + " per host).</p>\n";
        msg += "</body></html>\n";

        return new NanoHTTPD.Response(HTTP_OK, MIME_HTML, msg);
    }
}
