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


package org.hsqldb_voltpatches.jdbc.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.hsqldb_voltpatches.lib.IntValueHashMap;

/**
 * An <tt>InvocationHandler</tt> that facilitates <tt>Connection</tt> and
 * <tt>Statement</tt> pooling.
 *
 * The primary function is to avoid directly exposing close() and isClosed()
 * methods on physical <tt>Connection</tt> and <tt>Statement</tt> objects that
 * are participating in a pooling implementation.
 *
 * The secondary function is to assist the pooling mechanism by providing
 * check in notification for <tt>Connection</tt> objects and
 * check in / check out notification for derived <tt>Statement</tt> objects.
 *
 * @author boucherb@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class WrapperInvocationHandler implements InvocationHandler {

    // First, some helper definitions

    /**
     * Uniquely identifies, by the generating invocation signature, a
     * physical <tt>Statement</tt> object or like collection thereof. <p>
     */
    public final class StatementKey {

        // assigned in constructor
        private final Method   method;
        private final Object[] args;

        // derived
        private int hashCode;

        /**
         * Constructs a new <tt>StatementKey</tt> from the given invocation
         * signature.
         *
         * @param method the invocation's method
         * @param args the invocation's arguments
         */
        StatementKey(Method method, Object[] args) {

            this.method = method;
            this.args   = (args == null) ? null
                                         : (Object[]) args.clone();
        }

        /**
         * Returns the hash code value for this object. <p>
         *
         * This method is supported to allow semantically correct participation
         * as a key in a keyed <tt>Collection</tt> (e.g. a <tt>Map</tt>),
         * <tt>Hashtable</tt> or as an element in a <tt>HashSet</tt>.
         *
         * @return The hash code value for this object
         * @see #equals(java.lang.Object)
         * @see java.lang.Object#hashCode()
         */
        public int hashCode() {

            if (hashCode == 0) {
                int h = method.hashCode();

                if (args != null) {
                    for (int i = args.length - 1; i >= 0; i--) {
                        if (args[i] != null) {
                            h = 31 * h + args[i].hashCode();
                        }
                    }
                }

                hashCode = h;
            }

            return hashCode;
        }

        /**
         * Indicates whether some other object is "equal to" this one. <p>
         *
         * An object is equal to a <tt>StatementKey</tt> if it is the same
         * object or it is a different StatementKey having an equivalent
         * method and argument array.
         *
         * @param obj the reference object with which to compare.
         * @return  <code>true</code> if this object is the same as the obj
         *          argument; <code>false</code> otherwise.
         * @see     #hashCode()
         * @see     java.lang.Object#equals(java.lang.Object)
         */
        public boolean equals(Object obj) {

            if (this == obj) {
                return true;
            } else if (obj instanceof StatementKey) {
                StatementKey other = (StatementKey) obj;

                return (this.method.equals(other.method)
                        && ((this.args == other.args)
                            || Arrays.equals(this.args, other.args)));
            } else {
                return false;
            }
        }

        /**
         * Retrieves the <tt>Method</tt> object with which this key was
         * constructed. <p>
         *
         * @return the <tt>Method</tt> object with which this key was
         *      constructed.
         */
        public Method getMethod() {
            return this.method;
        }

        /**
         * Retrieves a copy of the argument array which this key was
         * constructed. <p>
         *
         * @return a copy of the argument array which this key was
         *      constructed
         */
        public Object[] getArgs() {
            return (args == null) ? null
                                  : (Object[]) args.clone();
        }
    }


    /**
     * Interface required to cooperate with a <tt>Statement</tt> pool
     * implementation. <p>
     */
    public interface StatementPool {

        /**
         * Indicates to an underlying pool that the given physical
         * <tt>Statement</tt> object is no longer in use by a
         * surrogate. <p>
         *
         * @param key an object representing the invocation signature with
         *      which the surrogate's invocation handler originally generated
         *      the statement.
         * @param stmt the physical <tt>Statement</tt> object
         */
        public void checkIn(StatementKey key, Statement stmt);

        /**
         * Asks an underlying pool for a physical <tt>Statement</tt> object
         * compatible with the invocation signature represented by the given
         * key. <p>
         *
         * The pool may respond with a <tt>null</tt> value, in which case it is
         * the job of the invocation handler to delegate to the underlying
         * physical <tt>Connection</tt>.
         *
         * @param key an object representing the invocation signature used
         *      to request a statement.
         */
        public Statement checkOut(StatementKey key);

        /**
         * Retrieves whether the given physical <tt>Statement</tt> object is
         * poolable. <p>
         *
         * If it is not, then the invocation handler is free to skip checking
         * the <tt>Statement</tt> back in when its surrogate is closed,
         * closing the <tt>Statement</tt> directly, instead.  On the other
         * hand, a well-written statement pool implementation should correctly
         * handle checkin of non-poolable statements by closing them.
         */
        public boolean isPoolable(Statement stmt);
    }

    /**
     * Interface required to cooperate with a <tt>Connection</tt> pool
     * implementation. <p>
     *
     * A <tt>DataSource</tt> handles checkout of physical connections from an
     * underlying pool and exposes a <tt>WrapperInvocationHandler.ConnectionPool</tt>
     * interface to allow each physical <tt>Connection</tt> object's
     * corresponding <tt>WrapperInvocationHandler</tt> to check it back in to
     * the underlying pool when the surrogate is closed.
     */
    public interface ConnectionPool {

        /**
         * Returns a physical <tt>Connection</tt> to an underlying pool. <p>
         *
         * @param connection The physical connection object being returned to
         *      the pool.
         * @param statementPool The implementation originally provided
         *      by the pooling implementation to facilitate statement reuse
         *      against the given <tt>Connection</tt> object.
         */
        void checkIn(Connection connection, StatementPool statementPool);
    }

    // ----- Static computation of Methods that are sensitive to pooling -------
    public static final int                WIH_NO_SURROGATE            = 0;
    public static final int                WIH_CLOSE_SURROGATE         = 1;
    public static final int                WIH_IS_CLOSED_SURROGATE     = 2;
    public static final int                WIH_GET_PARENT_SURROGATE    = 3;
    public static final int WIH_GET_DATABASEMETADATA_SURROGATE         = 4;
    public static final int WIH_CREATE_OR_PREPARE_STATEMENT_SURROGATE  = 5;
    public static final int                WIH_GET_RESULTSET_SURROGATE = 6;
    public static final int                WIH_UNWRAP_SURROGATE        = 7;
    public static final int                WIH_GET_ARRAY_SURROGATE     = 8;
    protected static final IntValueHashMap methodMap = new IntValueHashMap();

    // ------- Interfaces having methods that are sensitive to pooling ---------
    protected static final Class[] arrayInterface = new Class[]{ Array.class };
    protected static final Class[] connectionInterface = new Class[]{
        Connection.class };
    protected static final Class[] callableStatementInterface = new Class[]{
        CallableStatement.class };
    protected static final Class[] databaseMetaDataInterface = new Class[]{
        DatabaseMetaData.class };

    //protected static final Class[] parameterMetaDataInterface
    //        = new Class[]{ParameterMetaData.class};
    protected static final Class[] preparedStatementInterface = new Class[]{
        PreparedStatement.class };

    //protected static final Class[] resultSetMetaDataInterface
    //        = new Class[]{ResultSetMetaData.class};
    protected static final Class[] resultSetInterface = new Class[]{
        ResultSet.class };
    protected static final Class[] statementInterface = new Class[]{
        Statement.class };

    // ------------------ Static Initialization Helper Methods -----------------

    /**
     * Simple test used only during static initialization.
     *
     * @param clazz reflecting the given public member method
     * @param method to test
     * @return true if close() method of poolable class
     */
    protected static boolean _isCloseSurrogateMethod(final Class clazz,
            final Method method) {

        return ((Connection.class.isAssignableFrom(
            clazz) || Statement.class.isAssignableFrom(
            clazz)) && "close".equals(method.getName()));
    }

    /**
     * Simple test used only during static initialization.
     *
     * @param clazz reflecting the given public member method
     * @param method to test
     * @return true if isClosed() method of poolable class
     */
    protected static boolean _isIsClosedSurrogateMethod(final Class clazz,
            final Method method) {

        return ((Connection.class.isAssignableFrom(
            clazz) || Statement.class.isAssignableFrom(
            clazz)) && "isClosed".equals(method.getName()));
    }

//    /**
//     *
//     * Simple test used only during static initialization.
//     *
//     * @param clazz reflecting the given public member method
//     * @param method to test
//     * @return true if isWrapperFor() method of class exposes pooling-senstive
//     *      behavior
//     */
//    protected static  boolean isIsWrapperForMethod(final Method method) {
//        return "isWrapperFor".equals(method.getName());
//    }

    /**
     * Simple test used only during static initialization.
     *
     * @param method to test
     * @return true if method is an unwrap() method
     */
    protected static boolean _isUnwrapMethod(final Method method) {
        return "unwrap".equals(method.getName());
    }

    // ------------------------ Static Initialization --------------------------
    static {
        Class[] poolingSensitiveInterfaces = new Class[] {
            java.sql.Array.class, java.sql.CallableStatement.class,
            java.sql.Connection.class, java.sql.DatabaseMetaData.class,

            // unlikely to expose raw delegate of interest
            //java.sql.ParameterMetaData.class,
            java.sql.PreparedStatement.class, java.sql.ResultSet.class,

            // unlikely to expose raw delegate of interest
            //java.sql.ResultSetMetaData.class,
            java.sql.Statement.class
        };

        for (int i = 0; i < poolingSensitiveInterfaces.length; i++) {
            Class    clazz   = poolingSensitiveInterfaces[i];
            Method[] methods = clazz.getMethods();

            for (int j = 0; j < methods.length; j++) {
                Method method     = methods[j];
                Class  returnType = method.getReturnType();

                if (_isCloseSurrogateMethod(clazz, method)) {
                    methodMap.put(method, WIH_CLOSE_SURROGATE);
                } else if (_isIsClosedSurrogateMethod(clazz, method)) {
                    methodMap.put(method, WIH_IS_CLOSED_SURROGATE);
                } else if (Array.class.isAssignableFrom(returnType)) {
                    methodMap.put(method, WIH_GET_ARRAY_SURROGATE);
                } else if (Connection.class.isAssignableFrom(returnType)) {
                    methodMap.put(method, WIH_GET_PARENT_SURROGATE);
                } else if (Statement.class.isAssignableFrom(returnType)) {
                    String methodName = method.getName();

                    if (methodName.startsWith("create")
                            || methodName.startsWith("prepare")) {
                        methodMap.put(
                            method, WIH_CREATE_OR_PREPARE_STATEMENT_SURROGATE);
                    } else {
                        methodMap.put(method, WIH_GET_PARENT_SURROGATE);
                    }
                } else if (ResultSet.class.isAssignableFrom(returnType)) {
                    methodMap.put(method, WIH_GET_RESULTSET_SURROGATE);
                } else if (DatabaseMetaData.class.isAssignableFrom(
                        returnType)) {
                    methodMap.put(method, WIH_GET_DATABASEMETADATA_SURROGATE);

                    //} else if (ParameterMetaData.class.
                    //        isAssignableFrom(returnType)) {
                    // *************************************************************
                    //} else if (ResultSetMetaData.class.
                    //        isAssignableFrom(returnType)) {
                    // *************************************************************
                    //} else if (isIsWrapperForMethod(method)) {
                    // *************************************************************
                } else if (_isUnwrapMethod(method)) {
                    methodMap.put(method, WIH_UNWRAP_SURROGATE);
                }
            }
        }
    }

    // ----------------------- Construction Utility Method ---------------------

    /**
     * Given a delegate, retrieves the interface that must be implemented by a
     * surrogate dynamic proxy to ensure pooling sensitive methods
     * of the delegate are not exposed directly to clients.
     *
     * @param delegate the target delegate of interest
     * @return the interface that must be implemented by a surrogate dynamic
     *         proxy to ensure pooling sensitive methods of the delegate are
     *         not exposed directly to clients
     */
    protected static Class[] _computeProxiedInterface(Object delegate) {

        // NOTE:  Order is important for XXXStatement.
        if (delegate instanceof Array) {
            return arrayInterface;
        } else if (delegate instanceof Connection) {
            return connectionInterface;
        } else if (delegate instanceof CallableStatement) {
            return callableStatementInterface;
        } else if (delegate instanceof DatabaseMetaData) {
            return databaseMetaDataInterface;
        } else if (delegate instanceof PreparedStatement) {
            return preparedStatementInterface;
        } else if (delegate instanceof ResultSet) {
            return resultSetInterface;
        } else if (delegate instanceof Statement) {
            return statementInterface;
        } else {
            return null;
        }
    }

    /**
     * Retrieves a numeric classification of the surrogate method type
     * corresponding to the given delegate method.
     *
     * @param method the method to test
     * @return the numeric classification
     */
    protected static int _computeSurrogateType(Method method) {
        return methodMap.get(method, WIH_NO_SURROGATE);
    }

    // ---------------------------- Instance Fields ----------------------------
    // set in constructor
    private Object                   delegate;
    private Object                   surrogate;
    private WrapperInvocationHandler parentHandler;
    private ConnectionPool           connectionPool;
    private StatementPool            statementPool;

    // derivied
    private WrapperInvocationHandler dbmdHandler;
    private boolean                  surrogateClosed;
    private StatementKey             statementKey;
    private Set                      resultSets;    //ResultSet invocation handlers
    private Set                      statements;    //Statement invocation handlers

// ------------------------------ Constructors ---------------------------------

    /**
     * Constructs a new invocation handler for the given <tt>Connection</tt>.
     *
     * @param connection the <tt>Connection</tt> for which to construct an
     *      invocation handler
     * @param connectionPool interface to an external connection pool; may be null
     * @param statementPool interface to an external statement pool; may be null
     * @throws java.lang.IllegalArgumentException if connection is null
     */
    public WrapperInvocationHandler(Connection connection,
                                    ConnectionPool connectionPool,
                                    StatementPool statementPool)
                                    throws IllegalArgumentException {

        this(connection, null);

        this.connectionPool = connectionPool;
        this.statementPool  = statementPool;
    }

    /**
     * Constructs a new invocation handler for the given delegate and
     * parent invocation handler. <p>
     *
     * @param delegate the delegate for which to construct the invocation handler
     * @param parent the invocation handler of the delegate's parent; may be null.
     * @throws IllegalArgumentException if delegate is null; or if its proxied
     *      interface cannot be determined; or if any of the restrictions on the
     *      parameters that may be passed to <code>Proxy.newProxyInstance</code>
     *      are violated
     */
    public WrapperInvocationHandler(Object delegate,
                                    WrapperInvocationHandler parent)
                                    throws IllegalArgumentException {

        if (delegate == null) {
            throw new IllegalArgumentException("delegate: null");
        }

        Class[] proxiedInterface = _computeProxiedInterface(delegate);

        if (proxiedInterface == null) {
            throw new IllegalArgumentException("delegate: " + delegate);
        }

        this.delegate      = delegate;
        this.parentHandler = parent;
        this.surrogate =
            Proxy.newProxyInstance(proxiedInterface[0].getClassLoader(),
                                   proxiedInterface, this);
    }

    // ----------------------- Interface Implementation ------------------------

    /**
     * Processes a method invocation on a proxy instance and returns
     * the result.  This method will be invoked on an invocation handler
     * when a method is invoked on a proxy instance that it is
     * associated with.
     *
     * @param   proxy the proxy instance that the method was invoked on
     *
     * @param   method the <code>Method</code> instance corresponding to
     * the interface method invoked on the proxy instance.  The declaring
     * class of the <code>Method</code> object will be the interface that
     * the method was declared in, which may be a superinterface of the
     * proxy interface that the proxy class inherits the method through.
     *
     * @param   args an array of objects containing the values of the
     * arguments passed in the method invocation on the proxy instance,
     * or <code>null</code> if interface method takes no arguments.
     * Arguments of primitive types are wrapped in instances of the
     * appropriate primitive wrapper class, such as
     * <code>java.lang.Integer</code> or <code>java.lang.Boolean</code>.
     *
     * @return  the value to return from the method invocation on the
     * proxy instance.  If the declared return type of the interface
     * method is a primitive type, then the value returned by
     * this method must be an instance of the corresponding primitive
     * wrapper class; otherwise, it must be a type assignable to the
     * declared return type.  If the value returned by this method is
     * <code>null</code> and the interface method's return type is
     * primitive, then a <code>NullPointerException</code> will be
     * thrown by the method invocation on the proxy instance.  If the
     * value returned by this method is otherwise not compatible with
     * the interface method's declared return type as described above,
     * a <code>ClassCastException</code> will be thrown by the method
     * invocation on the proxy instance.
     *
     * @throws  Throwable the exception to throw from the method
     * invocation on the proxy instance.  The exception's type must be
     * assignable either to any of the exception types declared in the
     * <code>throws</code> clause of the interface method or to the
     * unchecked exception types <code>java.lang.RuntimeException</code>
     * or <code>java.lang.Error</code>.  If a checked exception is
     * thrown by this method that is not assignable to any of the
     * exception types declared in the <code>throws</code> clause of
     * the interface method, then undeclared {@link Throwable} containing
     * the exception that was thrown by this method will be thrown by the
     * method invocation on the proxy instance.
     *
     * @see Throwable
     */

    /**
     * @todo: - Synchronization can be made more granular if performance suffers.
     *        - Requires some private lock objects and synchronized blocks in
     *          certain methods.
     *        - This was the obvious and easy synchronization point to pick
     *          initially for prototyping purposes
     */
    public synchronized Object invoke(final Object proxy, final Method method,
                                      final Object[] args) throws Throwable {

        Object result;

        switch (_computeSurrogateType(method)) {

            case WIH_CLOSE_SURROGATE : {
                closeSurrogate();

                result = null;

                break;
            }
            case WIH_IS_CLOSED_SURROGATE : {
                result = isClosedSurrogate() ? Boolean.TRUE
                                             : Boolean.FALSE;

                break;
            }
            case WIH_GET_PARENT_SURROGATE : {
                checkSurrogateClosed();

                result = getParentSurrogate(method, args);

                break;
            }
            case WIH_GET_DATABASEMETADATA_SURROGATE : {
                checkSurrogateClosed();

                result = getDatabaseMetaDataSurrogate(method, args);

                break;
            }
            case WIH_CREATE_OR_PREPARE_STATEMENT_SURROGATE : {
                checkSurrogateClosed();

                result = getCreatedOrPreparedStatementSurrogate(method, args);

                break;
            }
            case WIH_GET_RESULTSET_SURROGATE : {
                checkSurrogateClosed();

                result = getResultSetSurrogate(method, args);

                break;
            }
            case WIH_UNWRAP_SURROGATE : {
                checkSurrogateClosed();

                result = unwrapSurrogate(method, args);

                break;
            }
            case WIH_GET_ARRAY_SURROGATE : {
                checkSurrogateClosed();

                result = getArraySurrogate(method, args);

                break;
            }
            case WIH_NO_SURROGATE :
            default : {
                checkSurrogateClosed();

                result = method.invoke(delegate, args);

                break;
            }
        }

        return result;
    }

    // --------------------- java.lang.Object overrides ------------------------

    /**
     * Ensures the identity hash code is returned for all descendents
     *
     * @return the identity hashCode for this object.
     */
    public final int hashCode() {
        return System.identityHashCode(this);
    }

    /**
     * Ensures identity equality semantics are preserved for all descendents.
     *
     * @param o the object with which to compare
     * @return  true if (this == o); else false
     */
    public final boolean equals(Object o) {
        return (this == o);
    }

    // -------------------------- Internal Implementation ----------------------

    /**
     * Checks if the surrogate is closed.
     *
     * @throws java.lang.Throwable if the surrogate is closed.
     */
    protected void checkSurrogateClosed() throws Throwable {

        if (isClosedSurrogate()) {
            throw new SQLException("Surrogate Closed.");    // TODO: better msg
        }
    }

    /**
     * Effectively closes the surrogate, possibly doing work toward
     * enabling reuse of the delegate. <p>
     *
     * @throws java.lang.Throwable if an access error occurs during work to
     *      enable reuse of the delegate
     */
    protected void closeSurrogate() throws Throwable {

        if (this.surrogateClosed) {
            return;
        }

        if (this.resultSets != null) {
            Iterator it = this.resultSets.iterator();

            // Changed to set of ResultSet invocation handlers so
            // that handler resources can be cleaned up too.
            while (it.hasNext()) {
                WrapperInvocationHandler handler =
                    (WrapperInvocationHandler) it.next();

                try {
                    ((ResultSet) handler.delegate).close();
                } catch (Exception ex) {}

                try {
                    handler.closeSurrogate();
                } catch (Exception e) {}
            }
        }

        if (this.statements != null) {
            Iterator it = this.statements.iterator();

            while (it.hasNext()) {
                WrapperInvocationHandler handler =
                    (WrapperInvocationHandler) it.next();

                try {
                    handler.closeSurrogate();
                } catch (Exception e) {}
            }
        }

        if (this.dbmdHandler != null) {
            try {
                this.dbmdHandler.closeSurrogate();
            } catch (Throwable ex) {}
        }

        Object delegate = this.delegate;

        try {
            if (delegate instanceof Connection) {
                closeConnectionSurrogate();
            } else if (delegate instanceof Statement) {
                closeStatementSurrogate();
            }
        } finally {
            this.delegate        = null;
            this.surrogate       = null;
            this.dbmdHandler     = null;
            this.parentHandler   = null;
            this.statementKey    = null;
            this.statementPool   = null;
            this.connectionPool  = null;
            this.surrogateClosed = true;
        }
    }

    /**
     * Does work toward enabling reuse of the delegate,
     * when it is a Connection.
     *
     * @throws java.lang.Throwable the exception, if any, thrown by
     *      returning the delegate connection to the ConnectionPool
     *      designated at construction of the connection's
     *      invocation handler.
     */
    protected void closeConnectionSurrogate() throws Throwable {

        ConnectionPool connectionPool = this.connectionPool;

        if (connectionPool == null) {

            // CHECKME: policy?
            // pool has "disapeared" or was never provided (why?): should
            // "really" close the connection since it will no be reused.
            Connection connection = (Connection) this.delegate;

            try {
                connection.close();
            } catch (SQLException ex) {}
        } else {
            Connection    connection    = (Connection) this.delegate;
            StatementPool statementPool = this.statementPool;

            connectionPool.checkIn(connection, statementPool);
        }
    }

    /**
     * Does work toward enabling reuse of the delegate,
     * when it is an instance of <tt>Statement</tt>.
     *
     * @throws java.lang.Throwable the exception, if any, thrown by
     *      returning the delegate statement to the StatementPool
     *      designated at construction of the statement's parent connection
     *      invocation handler.
     */
    protected void closeStatementSurrogate() throws Throwable {

        Statement     stmt          = (Statement) this.delegate;
        StatementKey  key           = this.statementKey;
        StatementPool statementPool = (this.parentHandler == null) ? null
                                                                   : this.parentHandler
                                                                       .statementPool;

        if (key == null || statementPool == null
                || !statementPool.isPoolable(stmt)) {
            try {
                stmt.close();
            } catch (Exception ex) {}
        } else {
            statementPool.checkIn(key, stmt);
        }
    }

    /**
     * Retrieves whether the surrogate is closed. <p>
     *
     * @return true if surrogate is closed; else false
     * @throws java.lang.Throwable
     */
    protected boolean isClosedSurrogate() throws Throwable {

        if (this.surrogateClosed) {
            return true;
        }

        // This part is overkill now, but does not introduce
        // incorrect operation.
        //

        /**
         * @todo:  Special handling to check the parent is still desirable
         *       for Array surrogates (and any other proxied delegate
         *       that has a parent whose valid lifetime is at most the
         *       life of the parent and does not expose a public close()
         *       method through the JDBC API
         */
        WrapperInvocationHandler parent = this.parentHandler;

        if (parent != null && parent.isClosedSurrogate()) {
            closeSurrogate();
        }

        return this.surrogateClosed;
    }

    /**
     * Retrieves a surrogate for the parent <tt>Statement</tt> or
     * <tt>Connection</tt> object of this handler's delegate.
     *
     * @param method that retrieves the delegate's parent object
     * @param args required for method invocation
     * @throws java.lang.Throwable the exception, if any, thrown by invoking
     * the given method with the given arguments upon the delegate
     * @return surrogate for the underlying parent object
     */
    protected Object getParentSurrogate(final Method method,
                                        final Object[] args) throws Throwable {

        WrapperInvocationHandler parent = this.parentHandler;

        return (parent == null) ? null
                                : parent.surrogate;
    }

    /**
     * Surrogate for any method of the delegate that returns
     * an instance of <tt>DatabaseMetaData</tt> object. <p>
     *
     * @param method returning <tt>DatabaseMetaData</tt>
     * @param args to the method
     * @throws java.lang.Throwable the exception, if any, thrown by invoking
     * the given method with the given arguments upon the delegate
     * @return surrogate for the underlying DatabaseMetaData object
     */
    protected Object getDatabaseMetaDataSurrogate(final Method method,
            final Object[] args) throws Throwable {

        if (this.dbmdHandler == null) {
            Object dbmd = method.invoke(this.delegate, args);

            this.dbmdHandler = new WrapperInvocationHandler(dbmd, this);
        }

        return this.dbmdHandler.surrogate;
    }

    /**
     * Surrogate for any method of the delegate that returns an instance of
     * <tt>Statement</tt>. <p>
     *
     * @param method returning instance of <tt>Statement</tt>
     * @param args to the method
     * @throws java.lang.Throwable the exception, if any, thrown by invoking
     * the given method with the given arguments upon the delegate
     * @return surrogate for the delegate Statement object
     */
    protected Object getCreatedOrPreparedStatementSurrogate(
            final Method method, final Object[] args) throws Throwable {

        WrapperInvocationHandler handler;
        Object                   stmt = null;
        StatementKey             key  = new StatementKey(method, args);
        StatementPool            pool = this.statementPool;

        if (pool != null) {
            stmt = pool.checkOut(key);
        }

        if (stmt == null) {
            stmt = method.invoke(this.delegate, args);
        }

        handler              = new WrapperInvocationHandler(stmt, this);
        handler.statementKey = key;

        if (this.statements == null) {
            this.statements = new HashSet();
        }

        statements.add(handler);

        return handler.surrogate;
    }

    /**
     * Surrogate for any method of the delegate that returns an
     * instance of <tt>ResultSet</tt>. <p>
     *
     * @param method returning a <tt>ResultSet</tt>
     * @param args to the method
     * @throws java.lang.Throwable the exception, if any, thrown by invoking
     * the given method with the given arguments upon the delegate
     * @return surrogate for the underlying ResultSet object
     */
    protected Object getResultSetSurrogate(final Method method,
                                           final Object[] args)
                                           throws Throwable {

        Object rs = method.invoke(this.delegate, args);
        WrapperInvocationHandler handler = new WrapperInvocationHandler(rs,
            this);

        if (resultSets == null) {
            resultSets = new HashSet();
        }

        // Changed to set of ResultSet invocation handlers so
        // that handler resources can be cleaned up too.
        resultSets.add(handler);

        return handler.surrogate;
    }

    /**
     * Surrogate for the delegate's unwrap(...) method. <p>
     *
     * If invocation of the method returns the delegate itself, then
     * the delegate's surrogate is returned instead.
     *
     * @param method the unwrap method
     * @param args the argument(s) to the unwrap method
     * @throws java.lang.Throwable the exception, if any, thrown by invoking
     * the given method with the given arguments upon the delegate
     * @return proxy if the method returns the delegate itself; else the actual
     *      result of invoking the method upon the delegate.
     */
    protected Object unwrapSurrogate(final Method method,
                                     final Object[] args) throws Throwable {

        Object result = method.invoke(this.delegate, args);

        return (result == this.delegate) ? this.surrogate
                                         : result;
    }

    /**
     * Surrogate for any method of the delegate that returns an
     * instance of <tt>Array</tt>. <p>
     *
     * @param method returning an <tt>Array</tt>
     * @param args to the method
     * @throws java.lang.Throwable the exception, if any, thrown by invoking
     * the given method with the given arguments upon the delegate
     * @return surrogate for the underlying Array object
     */
    protected Object getArraySurrogate(final Method method,
                                       final Object[] args)
                                       throws java.lang.Throwable {

        Object array = method.invoke(this.delegate, args);
        WrapperInvocationHandler handler = new WrapperInvocationHandler(array,
            this);

        return handler.surrogate;
    }
}
