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


package org.hsqldb_voltpatches.jdbc;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

// boucherb@users 20040411 - doc 1.7.2 - javadoc updates toward 1.7.2 final

/**
 * A JNDI ObjectFactory for creating {@link JDBCDataSource JDBCDataSource}
 * object instances.
 *
 * @author deforest@users
 * @version 1.7.2
 */
public class JDBCDataSourceFactory implements ObjectFactory {

    /**
     * Creates a JDBCDataSource object using the location or reference
     * information specified.<p>
     *
     * The Reference object should support the properties, database, user,
     * password.
     *
     * @param obj The reference information used in creating a
     *      JDBCDatasource object.
     * @param name ignored
     * @param nameCtx ignored
     * @param environment ignored
     * @return A newly created JDBCDataSource object; null if an object
     *      cannot be created.
     * @exception Exception never
     */
    public Object getObjectInstance(Object obj, Name name, Context nameCtx,
                                    Hashtable environment) throws Exception {

        String    dsClass = "org.hsqldb_voltpatches.jdbc.JDBCDataSource";
        Reference ref     = (Reference) obj;

        if (ref.getClassName().equals(dsClass)) {
            JDBCDataSource ds = new JDBCDataSource();

            ds.setDatabase((String) ref.get("database").getContent());
            ds.setUser((String) ref.get("user").getContent());
            ds.setPassword((String) ref.get("password").getContent());

            return ds;
        } else {
            return null;
        }
    }
}
