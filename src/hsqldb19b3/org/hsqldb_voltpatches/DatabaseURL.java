/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import java.util.Locale;

import org.hsqldb_voltpatches.persist.HsqlProperties;
// A VoltDB extension to disable a subpackage dependency
/* disable 1 line ...
import org.hsqldb_voltpatches.server.ServerConstants;
... disabled 1 line */
// End of VoltDB extension

/*
 * Parses a connection URL into parts.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.8.0
 */

// patch 1.9.0 by Blaine Simpson - IPv6 support
public class DatabaseURL {

    static final String        S_DOT               = ".";
    public static final String S_MEM               = "mem:";
    public static final String S_FILE              = "file:";
    public static final String S_RES               = "res:";
    public static final String S_ALIAS             = "alias:";
    public static final String S_HSQL              = "hsql://";
    public static final String S_HSQLS             = "hsqls://";
    public static final String S_HTTP              = "http://";
    public static final String S_HTTPS             = "https://";
    public static final String S_URL_PREFIX        = "jdbc:hsqldb:";
    public static final String url_connection_type = "connection_type";
    public static final String url_database        = "database";

    /**
     * Returns true if type represents an in-process connection to a file backed
     * database.
     */
    public static boolean isFileBasedDatabaseType(String url) {

        if (url == S_FILE || url == S_RES) {
            return true;
        }

        return false;
    }

    /**
     * Returns true if type represents an in-process connection to database.
     */
    public static boolean isInProcessDatabaseType(String url) {

        if (url == S_FILE || url == S_RES || url == S_MEM) {
            return true;
        }

        return false;
    }

    /**
     * Parses the url into components that are returned in a properties object.
     *
     * <p> The following components are isolated:
     *
     * <p>
     * <ul> url: the original url
     *
     * <p> connection_type: a static string that indicate the protocol. If the
     * url does not begin with a valid protocol, null is returned by this method
     * instead of the properties object.
     *
     * <p> host: name of host in networked modes in lowercase
     *
     * <p> port: port number in networked mode, or 0 if not present
     *
     * <p> path: path of the resource on server in networked modes, minimum
     * (slash) with path elements appended apart from servlet path which is
     * (slash) plus the name of the servlet
     *
     * <p> database: database name. For memory, resource and networked modes,
     * this is returned in lowercase, for file databases the original case of
     * characters is preserved. Returns empty string if name is not present in
     * the url.
     *
     * <p> for each protocol if port number is not in the url
     *
     * <p> Additional connection properties specified as key/value pairs.
     * </ul>
     *
     * @return null returned if the part that should represent the port is not
     *   an integer or the part for database name is empty. Empty
     *   HsqlProperties returned if if url does not begin with valid protocol
     *   and could refer to another JDBC driver.
     * @param url String
     * @param hasPrefix indicates URL prefix is present
     * @param noPath indicates empty path and verbatim use of path elements as
     * database
     */
    public static HsqlProperties parseURL(String url, boolean hasPrefix,
                                          boolean noPath) {

        String         urlImage   = url.toLowerCase(Locale.ENGLISH);
        HsqlProperties props      = new HsqlProperties();
        HsqlProperties extraProps = null;
        String         arguments  = null;
        int            pos        = 0;

        if (hasPrefix) {
            if (urlImage.startsWith(S_URL_PREFIX)) {
                pos = S_URL_PREFIX.length();
            } else {
                return props;
            }
        }

        String  type = null;
        int     port = 0;
        String  database;
        String  path;
        boolean isNetwork = false;

        props.setProperty("url", url);

        int postUrlPos = url.length();

        // postUrlPos is the END position in url String,
        // wrt what remains to be processed.
        // I.e., if postUrlPos is 100, url no longer needs to examined at
        // index 100 or later.
        int semiPos = url.indexOf(';', pos);

        if (semiPos > -1) {
            arguments  = urlImage.substring(semiPos + 1, urlImage.length());
            postUrlPos = semiPos;
            extraProps = HsqlProperties.delimitedArgPairsToProps(arguments,
                    "=", ";", null);

            /** @todo 1.9.0 - check if properties have valid names / values */
            props.addProperties(extraProps);
        }

        if (postUrlPos == pos + 1 && urlImage.startsWith(S_DOT, pos)) {
            type = S_DOT;
        } else if (urlImage.startsWith(S_MEM, pos)) {
            type = S_MEM;
        } else if (urlImage.startsWith(S_FILE, pos)) {
            type = S_FILE;
        } else if (urlImage.startsWith(S_RES, pos)) {
            type = S_RES;
        } else if (urlImage.startsWith(S_ALIAS, pos)) {
            type = S_ALIAS;
        } else if (urlImage.startsWith(S_HSQL, pos)) {
            type      = S_HSQL;
            port      = ServerConstants.SC_DEFAULT_HSQL_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HSQLS, pos)) {
            type      = S_HSQLS;
            port      = ServerConstants.SC_DEFAULT_HSQLS_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HTTP, pos)) {
            type      = S_HTTP;
            port      = ServerConstants.SC_DEFAULT_HTTP_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HTTPS, pos)) {
            type      = S_HTTPS;
            port      = ServerConstants.SC_DEFAULT_HTTPS_SERVER_PORT;
            isNetwork = true;
        }

        if (type == null) {
            type = S_FILE;
        } else if (type == S_DOT) {
            type = S_MEM;

            // keep pos
        } else {
            pos += type.length();
        }

        props.setProperty("connection_type", type);

        if (isNetwork) {

            // First capture 3 segments:  host + port + path
            String pathSeg  = null;
            String hostSeg  = null;
            String portSeg  = null;
            int    slashPos = url.indexOf('/', pos);

            if (slashPos > 0 && slashPos < postUrlPos) {
                pathSeg = url.substring(slashPos, postUrlPos);

                // N.b. pathSeg necessarily begins with /.
                postUrlPos = slashPos;
            }

            // Assertion
            if (postUrlPos <= pos) {
                return null;
            }

            // Processing different for ipv6 host address and all others:
            if (url.charAt(pos) == '[') {

                // ipv6
                int endIpv6 = url.indexOf(']', pos + 2);

                // Notice 2 instead of 1 to require non-empty addr segment
                if (endIpv6 < 0 || endIpv6 >= postUrlPos) {
                    return null;

                    // Wish could throw something with a useful message for user
                    // here.
                }

                hostSeg = urlImage.substring(pos + 1, endIpv6);

                if (postUrlPos > endIpv6 + 1) {
                    portSeg = url.substring(endIpv6 + 1, postUrlPos);
                }
            } else {

                // non-ipv6
                int colPos = url.indexOf(':', pos + 1);

                // Notice + 1 to require non-empty addr segment
                hostSeg = urlImage.substring(pos, (colPos > 0) ? colPos
                                                               : postUrlPos);

                if (colPos > -1 && postUrlPos > colPos + 1) {

                    // portSeg will be non-empty, but could contain just ":"
                    portSeg = url.substring(colPos, postUrlPos);
                }
            }

            // At this point, the entire url has been parsed into
            // hostSeg + portSeg + pathSeg.
            if (portSeg != null) {
                if (portSeg.length() < 2 || portSeg.charAt(0) != ':') {

                    // Wish could throw something with a useful message for user
                    // here.
                    return null;
                }

                try {
                    port = Integer.parseInt(portSeg.substring(1));
                } catch (NumberFormatException e) {

                    // System.err.println("NFE for (" + portSeg + ')'); debug
                    return null;
                }
            }

            if (noPath) {
                path     = "";
                database = pathSeg;
            } else if (pathSeg == null) {
                path     = "/";
                database = "";
            } else {
                int lastSlashPos = pathSeg.lastIndexOf('/');

                if (lastSlashPos < 1) {
                    path = "/";
                    database =
                        pathSeg.substring(1).toLowerCase(Locale.ENGLISH);
                } else {
                    path     = pathSeg.substring(0, lastSlashPos);
                    database = pathSeg.substring(lastSlashPos + 1);
                }
            }

            /* Just for debug.  Remove once stable:
            System.err.println("Host seg (" + hostSeg + "), Port val (" + port
                    + "), Path val (" + pathSeg + "), path (" + path
                    + "), db (" + database + ')');
             */
            props.setProperty("port", port);
            props.setProperty("host", hostSeg);
            props.setProperty("path", path);

            if (!noPath && extraProps != null) {
                String filePath = extraProps.getProperty("filepath");

                if (filePath != null && database.length() != 0) {
                    database += ";" + filePath;
                }
            }
        } else {
            if (type == S_MEM || type == S_RES) {
                database = urlImage.substring(pos, postUrlPos).toLowerCase();

                if (type == S_RES) {
                    if (database.indexOf('/') != 0) {
                        database = '/' + database;
                    }
                }
            } else {
                database = url.substring(pos, postUrlPos);
            }

            if (database.length() == 0) {
                return null;
            }
        }

        props.setProperty("database", database);

        return props;
    }

    /************************* Volt DB Extensions *************************/
    /**
     * Stub to allow disabling of server package dependency
     * -- these values probably don't even matter.
     */
    private static interface ServerConstants {
        // default port for each protocol
        final int SC_DEFAULT_HSQL_SERVER_PORT  = 9001;
        final int SC_DEFAULT_HSQLS_SERVER_PORT = 554;
        final int SC_DEFAULT_HTTP_SERVER_PORT  = 80;
        final int SC_DEFAULT_HTTPS_SERVER_PORT = 443;
    }
    /**********************************************************************/
}
