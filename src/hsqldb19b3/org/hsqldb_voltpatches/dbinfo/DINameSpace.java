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


package org.hsqldb_voltpatches.dbinfo;

import java.lang.reflect.Method;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.WrapperIterator;

/* $Id: DINameSpace.java 3001 2009-06-04 12:31:11Z fredt $ */

// boucherb@users - 2004xxxx - patch 1.7.2
// -- canonical database uri for catalog name reporting
// -- enumXXX methods to iterateXXX
// -- simple support for SEQUENCE schema reporting
// -- report built-in procedures/procedure columns without dependency on user grants;
// Revision 1.8  2006/07/12 11:24:05  boucherb
// - merging back remaining material overritten by Fred's type-system upgrades
// - built-in calc uses more efficient set signature
// - rework to use grantee (versus user) orientation for certain system table content

/**
 * Provides catalog and schema related definitions and functionality. <p>
 *
 * Additional features include accessibility tests, class loading, filtered
 * iteration and inverted alias mapping functionality regarding Java Classes
 * and Methods defined within the context of this database name space support
 * object. <p>
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @version 1.8.0
 * @since 1.7.2
 */
final class DINameSpace {

    /** The Database for which the name space functionality is provided */
    private Database database;

    /** The catalog name reported by this namespace */
    private String catalogName;

    /**
     * Set { <code>Class</code> FQN <code>String</code> objects }. <p>
     *
     * The Set contains the names of the classes providing the public static
     * methods that are automatically made accessible to the PUBLIC user in
     * support of the expected SQL CLI scalar functions and other core
     * HSQLDB SQL functions and stored procedures. <p>
     */
    private static HashSet builtin = new HashSet();

    // procedure columns
    // make temporary ad-hoc spec a little more "official"
    // until better system in place
    static {
        builtin.add("org.hsqldb_voltpatches.Library");
        builtin.add("java.lang.Math");
    }

    /**
     * Constructs a new name space support object for the specified Database
     * object.
     *
     * <p>
     *
     * @param database The Database object for which to provide name space
     *   support
     */
    public DINameSpace(Database database) {
        this.database    = database;
        this.catalogName = database.getCatalogName().name;
    }

    /**
     * Retrieves the declaring <code>Class</code> object for the specified
     * fully qualified method name, using (if possible) the classLoader
     * attribute of this object's database. <p>
     *
     * @param fqn the fully qualified name of the method for which to
     *        retrieve the declaring <code>Class</code> object.
     * @return the declaring <code>Class</code> object for the
     *        specified fully qualified method name
     */
    Class classForMethodFQN(String fqn) {

        try {
            return classForName(fqn.substring(0, fqn.lastIndexOf('.')));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Retrieves the <code>Class</code> object specified by the
     * <code>name</code> argument, using, if possible, the
     * classLoader attribute of the database. <p>
     *
     * @param name the fully qualified name of the <code>Class</code>
     *      object to retrieve.
     * @throws ClassNotFoundException if the specified class object
     *      cannot be found in the context of this name space
     * @return the <code>Class</code> object specified by the
     *      <code>name</code> argument
     */
    Class classForName(String name) throws ClassNotFoundException {

        if (name == null) {
            return Class.forName(name);
        }

        return Class.forName(name);
    }

    /**
     * Retrieves an <code>Iterator</code> whose elements form the set of
     * distinct names of all visible catalogs, relative to this object's
     * database. <p>
     *
     * If catalog reporting is turned off, then the empty Iterator is
     * returned. <p>
     *
     * <b>Note:</b> in the present implementation, if catalog reporting is
     * turned on, then the iteration consists of a single element that is the
     * uri of this object's database; HSQLDB  currently does not support the
     * concept a single engine hosting multiple catalogs. <p>
     *
     * @return An Iterator whose elements are <code>String</code> objects
     *      naming all visible catalogs, relative to this object's database.
     */
    Iterator iterateCatalogNames() {
        return catalogName == null ? new WrapperIterator()
                                   : new WrapperIterator(catalogName);
    }

    /**
     * Retrieves the fully qualified name of the given Method object. <p>
     *
     * @param m The Method object for which to retreive the fully
     *      qualified name
     * @return the fully qualified name of the specified Method object.
     */
    static String getMethodFQN(Method m) {
        return m == null ? null
                         : m.getDeclaringClass().getName() + '.' + m.getName();
    }

    /**
     * Retrieves the specific name of the given Method object. <p>
     *
     * @param m The Method object for which to retreive the specific name
     * @return the specific name of the specified Method object.
     */
    static String getMethodSpecificName(Method m) {

        return m == null ? null
                         : m.getDeclaringClass().getName() + '.'
                           + getSignature(m);
    }

    static String getSignature(Method method) {

        StringBuffer sb;
        String       signature;
        Class[]      parmTypes;
        int          len;
        int          last;

        sb        = new StringBuffer();
        parmTypes = method.getParameterTypes();
        len       = parmTypes.length;
        last      = len - 1;

        sb.append(method.getName()).append('(');

        for (int i = 0; i < len; i++) {
            sb.append(parmTypes[i].getName());

            if (i < last) {
                sb.append(',');
            }
        }

        sb.append(')');

        signature = sb.toString();

        return signature;
    }

    /**
     * Adds to the given Set the fully qualified names of the Class objects
     * internally granted to PUBLIC in support of core operation.
     *
     * @param set HashSet to which to add the fully qualified names of the
     *   Class objects internally granted to PUBLIC in support of core
     *   operation.
     */
    void addBuiltinToSet(HashSet set) {
        set.addAll(builtin);
    }

    /**
     * Retrieves whether the indicated Class object is systematically
     * granted to PUBLIC in support of core operation. <p>
     *
     * @return whether the indicated Class object is systematically
     * granted to PUBLIC in support of core operation
     * @param clazz The Class object for which to make the determination
     */
    boolean isBuiltin(Class clazz) {
        return clazz == null ? false
                             : builtin.contains(clazz.getName());
    }

    /**
     * Retrieves whether the Class object indicated by the fully qualified
     * class name is systematically granted to PUBLIC in support of
     * core operation. <p>
     *
     * @return true if system makes grant, else false
     * @param name fully qualified name of a Class
     */
    boolean isBuiltin(String name) {
        return (name == null) ? false
                              : builtin.contains(name);
    }

/** @todo - fredt - there are no class grants in 1.9 */

    /**
     * @return a composite <code>Iterator</code>
     * @param session The context in which to produce the iterator
     * @param andAliases true if the alias lists for the "ROUTINE" type method
     *      elements are to be generated.
     */
    Iterator iterateAllAccessibleMethods(Session session, boolean andAliases) {

        Iterator out;
        HashSet  classNameSet;
        Iterator classNames;
        Iterator methods;
        String   className;

        out = new WrapperIterator();

//        classNameSet = session.getGrantee().getGrantedClassNames(true);
//        addBuiltinToSet(classNameSet);
//        classNames = classNameSet.iterator();
/*
        while (classNames.hasNext()) {
            className = (String) classNames.next();
            methods   = iterateRoutineMethods(className, andAliases);
            out       = new WrapperIterator(out, methods);
        }
*/
        return out;
    }

    /**
     * Retrieves the set of distinct, visible sessions connected to this
     * object's database, as a list. <p>
     *
     * @param session The context in which to produce the list
     * @return the set of distinct, visible sessions connected
     *        to this object's database, as a list.
     */
    Session[] listVisibleSessions(Session session) {
        return database.sessionManager.getVisibleSessions(session);
    }
}
