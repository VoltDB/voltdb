/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.voltdb.client.ClientConfig;

public class JDBCTestCommons {

    final static String SSL_URL_SUFFIX = "ssl=true&trustStore=tests/frontend/org/voltdb/keystore&trustStorePassword=password";

    static Connection getJdbcConnection(String url, Properties props) throws Exception
    {
        Class.forName("org.voltdb.jdbc.Driver");
        if (ClientConfig.ENABLE_SSL_FOR_TEST) {
            if (url.contains("?")) {
                url = url + "&" + SSL_URL_SUFFIX;
            }
            else {
                url = url + "?" + SSL_URL_SUFFIX;
            }
        }
        return DriverManager.getConnection(url, props);
    }

}
