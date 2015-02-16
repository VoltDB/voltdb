/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches.jdbc;

import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.RefAddr;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;

/**
 * A JNDI ObjectFactory for creating data sources supported by HyperSQL
 *  {@link org.hsqldb_voltpatches.jdbc.JDBCDataSource JDBCDataSource} for plain
 *  connections for the end user.
 *  {@link org.hsqldb_voltpatches.jdbc.JDBCPool JDBCPool} for pooled plain
 *   connections for the end user.
 *  {@link org.hsqldb_voltpatches.jdbc.pool.JDBCPooledDataSource JDBCPooledDataSource} for
 *  PooleConnection objects used
 *  by external connection pooling software.
 *  {@link org.hsqldb_voltpatches.jdbc.pool.JDBCXADataSource JDBCXADataSource} for
 *  XAConnection objects used by external connection pooling software.
 *
 * @author Darin DeForest (deforest@users dot sourceforge.net) original version
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @version 2.0.0
 */
public class JDBCDataSourceFactory implements ObjectFactory {

    /**
     * Static method to create a JDBCDataSource instance using the
     * given properties for url, user, password, etc.
     */
    public static DataSource createDataSource(Properties props)
    throws Exception {

        JDBCDataSource ds =
            (JDBCDataSource) Class.forName(bdsClassName).newInstance();
        String value = props.getProperty(databaseName);

        if (value == null) {
            value = props.getProperty(urlName);
        }

        ds.setDatabase(value);

        value = props.getProperty(userName);

        if (value == null) {
            value = props.getProperty(userNameName);
        }

        ds.setUser(value);

        value = props.getProperty(passwordName);

        ds.setPassword(value);

        value = props.getProperty(loginTimeoutName);

        if (value != null) {
            value = value.trim();

            if (value.length() > 0) {
                try {
                    ds.setLoginTimeout(Integer.parseInt(value));
                } catch (NumberFormatException nfe) {}
            }
        }

        return ds;
    }

    /**
     * Creates a DataSource object using the javax.naming.Reference object
     * specified.<p>
     *
     * The Reference object's class name should be one of the four supported
     * data source class names and it must support the properties, database,
     * user and password. It may optionally support the logingTimeout property.
     *
     * HyperSQL's JDBCPooledDataSource and JDBCXADataSource object are intended
     * as factories used by a connection pooling DataSource.<p>
     * JDBCDataSource is a factory for normal connections and can be accessed
     * directly by user applications.<p>
     * JDBCPool is a connection pool accessed directly by user applications.<p>
     *
     * @param obj The reference information used in creating a
     *      Datasource object.
     * @param name ignored
     * @param nameCtx ignored
     * @param environment ignored
     * @return A newly created JDBCDataSource object; null if an object
     *      cannot be created.
     * @exception Exception is thrown if database or user is null or invalid
     */
    public Object getObjectInstance(Object obj, Name name, Context nameCtx,
                                    Hashtable environment) throws Exception {

        if (!(obj instanceof Reference)) {
            return null;
        }

        Reference ref       = (Reference) obj;
        String    className = ref.getClassName();

        if (bdsClassName.equals(className) || poolClassName.equals(className)
                || pdsClassName.equals(className)
                || xdsClassName.equals(className)) {
            RefAddr refAddr;
            Object  value;
            JDBCCommonDataSource ds =
                (JDBCCommonDataSource) Class.forName(className).newInstance();

            refAddr = ref.get("database");

            if (refAddr == null) {
                throw new Exception(className + ": RefAddr not set: database");
            }

            value = refAddr.getContent();

            if (!(value instanceof String)) {
                throw new Exception(className + ": invalid RefAddr: database");
            }

            ds.setDatabase((String) value);

            refAddr = ref.get("user");

            if (refAddr == null) {
                throw new Exception(className + ": RefAddr not set: user");
            }

            value = ref.get("user").getContent();

            if (!(value instanceof String)) {
                throw new Exception(className + ": invalid RefAddr: user");
            }

            ds.setUser((String) value);

            refAddr = ref.get("password");

            if (refAddr == null) {
                value = "";
            } else {
                value = ref.get("password").getContent();

                if (!(value instanceof String)) {
                    throw new Exception(className
                                        + ": invalid RefAddr: password");
                }
            }

            ds.setPassword((String) value);

            refAddr = ref.get("loginTimeout");

            if (refAddr != null) {
                value = refAddr.getContent();

                if (value instanceof String) {
                    String loginTimeoutContent = ((String) value).trim();

                    if (loginTimeoutContent.length() > 0) {
                        try {
                            ds.setLoginTimeout(
                                Integer.parseInt(loginTimeoutContent));
                        } catch (NumberFormatException nfe) {}
                    }
                }
            }

            return ds;
        } else {
            return null;
        }
    }

    /**
     * supported properties
     */
    private static final String urlName          = "url";
    private static final String databaseName     = "database";
    private static final String userName         = "user";
    private static final String userNameName     = "username";
    private static final String passwordName     = "password";
    private static final String loginTimeoutName = "loginTimeout";

    /**
     * class names
     */
    private static final String bdsClassName = "org.hsqldb_voltpatches.jdbc.JDBCDataSource";
    private static final String poolClassName = "org.hsqldb_voltpatches.jdbc.JDBCPool";
    private static final String pdsClassName = "org.hsqldb_voltpatches.jdbc.pool.JDBCPooledDataSource";
    private static final String xdsClassName = "org.hsqldb_voltpatches.jdbc.pool.JDBCXADataSource";

    public JDBCDataSourceFactory() {}
}
