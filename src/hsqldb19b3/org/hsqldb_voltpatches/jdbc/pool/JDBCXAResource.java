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


package org.hsqldb_voltpatches.jdbc.pool;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

// These require a global transaction API:
//import javax.transaction.HeuristicMixedException;
//import javax.transaction.HeuristicCommitException;
//import javax.transaction.HeuristicRollbackException;
import org.hsqldb_voltpatches.jdbc.JDBCConnection;

import java.sql.SQLException;

import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.HsqlException;

// @(#)$Id: JDBCXAResource.java 5198 2013-03-10 21:54:46Z fredt $

/**
 * Used by a global transaction service to control HSQLDB transactions.
 * Not for use by end-users.
 * End manage global transactions using transaction APIs such as JTA.
 * <P>
 * According to section 12.3 of the JDBC 3.0 spec, there is a
 * 1:1 correspondence between XAConnection and XAResource, and
 * <I>A given XAConnection object may be associated with at most one
 * transaction at a time</I>.
 * Therefore, there may be at any time at most one transaction
 * managed by a XAResource object.
 * One implication is, the XAResource can track the current transaction
 * state with a scaler.
 * Another implication is, the Xids for most of the XAResource interface
 * methods just introduce unnecessary complexity and an unnecessary point
 * of failure-- there can be only one transaction for this object, so
 * why track another identifier for it.
 * My strategy is to just "validate" that the Xid does not change
 * within a transaction.
 * Exceptions to this are the commit and rollback methods, which the
 * JDBC spec says can operate against any XAResource instance from
 * the same XADataSource.
 * N.b. The JDBC Spec does not state whether the prepare and forget
 * methods are XAResource-specific or XADataSource-specific.
 *
 * @version 2.3.0
 * @since 2.0.0
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @see javax.transaction.xa.XAResource
 */
public class JDBCXAResource implements XAResource {

    /**
     * @todo:
     * Make thread safe.
     * Figure out how to ensure that orphaned transactions to do not make
     * a memory leak in JDBCXADataSource.resources.  I.e.,
     * JDBCXADataSource.removeResource() must be called even for all
     * transactions, even aborted ones.  Maybe tx managers are
     * already obligated to call one of commit/forget/rollback for
     * even transactions for which they have called start???... TEST THIS.
     * (They may only need to commit/forget/rollback if prepare has been
     * called?).
     * The answer may be to implement Timeouts.
     */
    private JDBCConnection   connection;
    private boolean          originalAutoCommitMode;
    static int               XA_STATE_INITIAL  = 0;
    static int               XA_STATE_STARTED  = 1;
    static int               XA_STATE_ENDED    = 2;
    static int               XA_STATE_PREPARED = 3;
    static int               XA_STATE_DISPOSED = 4;
    int                      state             = XA_STATE_INITIAL;
    private JDBCXADataSource xaDataSource;
    Xid                      xid = null;

    public boolean withinGlobalTransaction() {
        return state == XA_STATE_STARTED;
    }

    /**
     *
     * @throws XAException if the given Xid is the not the Xid of the current
     *   transaction for this XAResource object.
     * @param xid Xid
     */
    private void validateXid(Xid xid) throws XAException {

        if (xid == null) {
            throw new XAException("Null Xid");
        }

        if (this.xid == null) {
            throw new XAException(
                "There is no live transaction for this XAResource");
        }

        if (!xid.equals(this.xid)) {
            throw new XAException(
                "Given Xid is not that associated with this XAResource object");
        }
    }

    /**
     * Constructs a resource using the given data source and connection.
     *
     * @param xaDataSource JDBCXADataSource
     * @param connection A non-wrapped JDBCConnection which we need in order to
     *   do real (non-wrapped) commits, rollbacks, etc. This is not for the end
     *   user. We need the real thing.
     */
    public JDBCXAResource(JDBCXADataSource xaDataSource,
                          JDBCConnection connection) {
        this.connection   = connection;
        this.xaDataSource = xaDataSource;
    }

    JDBCXADataSource getXADataSource() {
        return xaDataSource;
    }

    /**
     * Per the JDBC 3.0 spec, this commits the transaction for the specified
     * Xid, not necessarily for the transaction associated with this XAResource
     * object.
     *
     * @param xid Xid
     * @param onePhase boolean
     * @throws XAException
     */
    public void commit(Xid xid, boolean onePhase) throws XAException {

        // Comment out following debug statement before public release:
/*
        System.err.println("Performing a " + (onePhase ? "1-phase"
                                                       : "2-phase") + " commit on "
                                                       + xid);
*/
        JDBCXAResource resource = xaDataSource.getResource(xid);

        if (resource == null) {
            throw new XAException("The XADataSource has no such Xid:  " + xid);
        }

        resource.commitThis(onePhase);
    }

    /**
     * This commits the connection associated with <i>this</i> XAResource.
     *
     * @throws XAException generically, since the more specific exceptions
     *   require a JTA API to compile.
      * @param onePhase boolean
      */
     public void commitThis(boolean onePhase) throws XAException {

        if (onePhase && state == XA_STATE_PREPARED) {
            throw new XAException(
                "Transaction is in a 2-phase state when 1-phase is requested");
        }

        if ((!onePhase) && state != XA_STATE_PREPARED) {
            throw new XAException("Attempt to do a 2-phase commit when "
                                  + "transaction is not prepared");
        }

        //if (!onePhase) {
        //  throw new XAException(
        //   "Sorry.  HSQLDB has not implemented 2-phase commits yet");
        //}
        try {

            /**
             * @todo:  Determine if work was committed, rolled back, or both,
             * and return appropriate Heuristic*Exception.
             * connection.commit();
             *  Commits the real, physical conn.
             */
            connection.commit();
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }

        dispose();
    }

    private void dispose() throws XAException {

        state = XA_STATE_DISPOSED;

        xaDataSource.removeResource(xid);

        xid = null;

        try {
            connection.setAutoCommit(originalAutoCommitMode);    // real/phys.
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }
    }

    public void end(Xid xid, int flags) throws XAException {

        validateXid(xid);

        if (state != XA_STATE_STARTED) {
            throw new XAException("Invalid XAResource state");
        }

        /** @todo - probably all flags can be ignored */
        if (flags == XAResource.TMSUCCESS) {}

        state = XA_STATE_ENDED;
    }

    /**
     * The XAResource API spec indicates implies that this is only for 2-phase
     * transactions. I guess that one-phase transactions need to call rollback()
     * to abort. I think we want this JDBCXAResource instance to be
     * garbage-collectable after (a) this method is called, and (b) the tx
     * manager releases its handle to it.
     *
     * @see javax.transaction.xa.XAResource#forget(Xid)
     * @param xid Xid
     * @throws XAException
     */
    public void forget(Xid xid) throws XAException {

        /**
         * Should this method not attempt to clean up the aborted
         * transaction by rolling back or something?  Maybe the
         * tx manager will already have called rollback() if
         * it were necessasry?
         */
        validateXid(xid);

        if (state != XA_STATE_PREPARED) {
            throw new XAException(
                "Attempted to forget a XAResource that "
                + "is not in a heuristically completed state");
        }

        dispose();

        state = XA_STATE_INITIAL;
    }

    /**
     *
     * @todo: Implement
     * @throws XAException
     * @return int
     */
    public int getTransactionTimeout() throws XAException {
        throw new XAException("Transaction timeouts not implemented yet");
    }

    /**
     * Stub. See implementation comment in the method for why this is not
     * implemented yet.
     *
     * @return false.
     * @param xares XAResource
     * @throws XAException
     */
    public boolean isSameRM(XAResource xares) throws XAException {

        if (!(xares instanceof JDBCXAResource)) {
            return false;
        }

        return xaDataSource == ((JDBCXAResource) xares).getXADataSource();
    }

    /**
     * Vote on whether to commit the global transaction. We assume Xid may be
     * different from this, as in commit() method.
     *
     * @throws XAException to vote negative.
     * @return commitType of XA_RDONLY or XA_OK. (Actually only XA_OK now).
     * @param xid Xid
     */
    public int prepare(Xid xid) throws XAException {

        JDBCXAResource resource = xaDataSource.getResource(xid);

        if (resource == null) {
            throw new XAException("The XADataSource has no such Xid:  " + xid);
        }

        return resource.prepareThis();
    }

    public int prepareThis() throws XAException {

        /**
         * @todo:  This is where the real 2-phase work should be done to
         * determine if a commit done here would succeed or not.
         */

        /**
         * @todo:  May improve performance to return XA_RDONLY whenever
         * possible, but I don't know.
         * Could determine this by checking if DB instance is in RO mode,
         * or perhaps (with much difficulty) to determine if there have
         * been any modifications performed.
         */
        if (state != XA_STATE_ENDED) {
            throw new XAException("Invalid XAResource state");
        }

        try {
            connection.getSession().prepareCommit();
        } catch (HsqlException e) {
            state = XA_STATE_PREPARED;  // ??? didn't prepare

            throw new XAException(e.getMessage());
        }

        state = XA_STATE_PREPARED;

        return XA_OK;    // As noted above, should check non-committed work.
    }

    /**
     * Obtain a list of Xids of the current <i>resource manager</i> for
     * XAResources currently in the 'prepared' * state. According to the JDBC
     * 3.0 spec, the Xids of a specific resource manager are those of the same
     * XADataSource.
     *
     * @param flag int
     * @throws XAException
     * @return Xid[]
     */
    public Xid[] recover(int flag) throws XAException {
        return xaDataSource.getPreparedXids();
    }

    /**
     * Per the JDBC 3.0 spec, this rolls back the transaction for the specified
     * Xid, not necessarily for the transaction associated with this XAResource
     * object.
     *
     * @param xid Xid
     * @throws XAException
     */
    public void rollback(Xid xid) throws XAException {

        JDBCXAResource resource = xaDataSource.getResource(xid);

        if (resource == null) {
            throw new XAException(
                "The XADataSource has no such Xid in prepared state:  " + xid);
        }

        resource.rollbackThis();
    }

    /**
     * This rolls back the connection associated with <i>this</i> XAResource.
     *
     * @throws javax.transaction.xa.XAException generically, since the more
     * specific exceptions require a JTA API to compile.
     */
    /* @throws javax.transaction.HeuristicCommitException
     *         if work was committed.
     * @throws javax.transaction.HeuristicMixedException
     *         if some work was committed and some work was rolled back
     */
    public void rollbackThis() throws XAException {

        if (state != XA_STATE_PREPARED && state != XA_STATE_ENDED) {
            throw new XAException("Invalid XAResource state");
        }

        try {

            /**
             * @todo:  Determine if work was committed, rolled back, or both,
             * and return appropriate Heuristic Exception.
             */
            connection.rollback();    // real/phys.
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }

        dispose();
    }

    /**
     *
     * @todo: Implement
     * @param seconds int
     * @throws XAException
     * @return boolean
     */
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }

    public void start(Xid xid, int flags) throws XAException {

        // Comment out following debug statement before public release:
/*
        System.err.println("STARTING NEW Xid: " + xid);
*/
        if (state != XA_STATE_INITIAL && state != XA_STATE_DISPOSED) {
            throw new XAException("Invalid XAResource state");
        }

        if (xaDataSource == null) {
            throw new XAException(
                "JDBCXAResource has not been associated with a XADataSource");
        }

        if (xid == null) {

            // This block asserts that all JDBCXAResources with state
            // >= XA_STATE_STARTED have a non-null xid.
            throw new XAException("Null Xid");
        }

        try {
            originalAutoCommitMode = connection.getAutoCommit();    // real/phys.

            connection.setAutoCommit(false);                        // real/phys.
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }

        this.xid = xid;
        state    = XA_STATE_STARTED;

        xaDataSource.addResource(this.xid, this);

        // N.b.  The DataSource does not have this XAResource in its list
        // until right here.  We can't tell DataSource before our start()
        // method, because we don't know our Xid before now.
    }

    JDBCConnection getConnection() {
        return this.connection;
    }

    void setConnection(JDBCConnection userConnection) {
        connection = userConnection;
    }
}
