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

package org.voltdb;

import java.util.Date;
import java.util.Random;

import org.voltdb.Expectation.Type;
import org.voltdb.client.ClientResponse;

/**
 * Wraps the stored procedure object created by the user
 * with metadata available at runtime. This is used to call
 * the procedure.
 *
 * VoltProcedure is extended by all running stored procedures.
 * Consider this when specifying access privileges.
 *
 */
public abstract class VoltProcedure {

    final static Double DOUBLE_NULL = new Double(-1.7976931348623157E+308);

    /**
     * Expect an empty result set (0 rows)
     */
    public static final Expectation EXPECT_EMPTY = new Expectation(Type.EXPECT_EMPTY);

    /**
     * Expect a result set with exactly one row
     */
    public static final Expectation EXPECT_ONE_ROW = new Expectation(Type.EXPECT_ONE_ROW);

    /**
     * Expect a result set with one or no rows
     */
    public static final Expectation EXPECT_ZERO_OR_ONE_ROW = new Expectation(Type.EXPECT_ZERO_OR_ONE_ROW);

    /**
     * Expect a result set with one or more rows
     */
    public static final Expectation EXPECT_NON_EMPTY = new Expectation(Type.EXPECT_NON_EMPTY);

    /**
     * Expect a result set with a single row and a single column (scalar value)
     */
    public static final Expectation EXPECT_SCALAR = new Expectation(Type.EXPECT_SCALAR);

    /**
     * Expect a result with a single row and a single BIGINT column
     */
    public static final Expectation EXPECT_SCALAR_LONG = new Expectation(Type.EXPECT_SCALAR_LONG);

    /**
     * Expect a result with a single row and a single BIGINT column containing
     * the specified value. This factory method constructs an Expectation for the specified
     * value.
     * @param scalar The expected value the single row/column should contain
     * @return An Expectation that will cause an exception to be thrown if the value or schema doesn't match
     */
    public static final Expectation EXPECT_SCALAR_MATCH(long scalar) {
        return new Expectation(Type.EXPECT_SCALAR_MATCH, scalar);
    }

    ProcedureRunner m_runner;
    private boolean m_initialized;

    /**
     * <p>Allow VoltProcedures access to a unique ID generated for each transaction.</p>
     *
     * <p>Note that calling it repeatedly within a single transaction will return the same
     * number.</p>
     *
     * <p>The id consists of a time based component in the most significant bits followed
     * by a counter, and then a partition id to allow parallel unique number generation.</p>
     *
     * <p>Currently, if the host clock moves backwards this call may block while the clock
     * catches up to its previous max. Running NTP with the -x option will prevent this in
     * most cases. It may not be sufficient to 100% avoid this, especially in managed and/or
     * virtualized environments (like AWS). The value will always be unique, but clocks moving
     * backwards may affect system throughput.</p>
     *
     * <p>Since this number is initially based on the wall clock time (UTC), it is not
     * guaranteed to be unique across different clusters of VoltDB. If you snapshot, then
     * move time back by 1HR, then load that snapshot into a new VoltDB cluster, you might
     * see duplicate ids for up to 1HR. Since the wall clocks VoltDB uses are UTC, and it
     * takes some amount of time to snapshot and restore, this is a pretty easy corner case
     * to avoid. Note that failure and recovery from disk using command logs will preserve
     * unique id continuity even if the clock shifts.</p>
     *
     * <p>In many cases it may make sense to generate unique ids on the client-side and pass
     * them to the server. Please reach out to VoltDB if you have questions about when using
     * this method is appropriate.</p>
     *
     * @return An ID that is unique to this transaction
     */
    public long getUniqueId() {
        return m_runner.getUniqueId();
    }

    /**
     * Get the ID of cluster that the client connects to.
     * @return An ID that identifies the VoltDB cluster
     */
    public int getClusterId() {
        return m_runner.getClusterId();
    }


    /**
     * End users should not instantiate VoltProcedure instances.
     * Constructor does nothing. All actual initialization is done in the
     * {@link VoltProcedure init} method.
     */
    public VoltProcedure() {}

    /**
     * End users should not call this method.
     * Used by the VoltDB runtime to initialize stored procedures for execution.
     */
    void init(ProcedureRunner procRunner)
    {
        if (m_initialized) {
            throw new IllegalStateException("VoltProcedure has already been initialized");
        }

        m_initialized = true;
        m_runner = procRunner;
    }

    /**
     * Thrown from a stored procedure to indicate to VoltDB
     * that the procedure should be aborted and rolled back.
     */
    public static class VoltAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;
        private String message = "No message specified.";

        /**
         * Constructs a new AbortException
         */
        public VoltAbortException() {}

        /**
         * Constructs a new AbortException from an existing <code>Throwable</code>.
         *
         * @param t Throwable to embed.
         */
        public VoltAbortException(Throwable t) {
            super(t);
            if (t.getMessage() != null) {
                message = t.getMessage();
            }
            else if (t.getCause() != null) {
                message = t.getCause().getMessage();
            }
        }

        /**
         * Constructs a new AbortException with the specified detail message.
         *
         * @param msg Exception specific message.
         */
        public VoltAbortException(String msg) {
            message = msg;
        }
        /**
         * Returns the detail message string of this <tt>AbortException</tt>
         *
         * @return The detail message.
         */
        @Override
        public String getMessage() {
            return message;
        }

        public byte getClientResponseStatus() {
            return ClientResponse.USER_ABORT;
        }

        public String getShortStatusString() {
            return "USER ABORT";
        }
    }

    /**
     * Get a Java RNG seeded with the current transaction id. This will ensure that
     * two procedures for the same transaction, but running on different replicas,
     * can generate an identical stream of random numbers. This is required to endure
     * procedures have deterministic behavior. The RNG is memoized so you can invoke this
     * multiple times within a single procedure.
     *
     * @return A deterministically-seeded java.util.Random instance.
     */
    public Random getSeededRandomNumberGenerator() {
        return m_runner.getSeededRandomNumberGenerator();
    }

    /**
     * YOU MUST BE RUNNING NTP AND START NTP WITH THE -x OPTION
     * TO GET GOOD BEHAVIOR FROM THIS METHOD - e.g. time always goes forward
     *
     * Get the time that this procedure was accepted into the VoltDB cluster. This is the
     * effective, but not always actual, moment in time this procedure executes. Use this
     * method to get the current time instead of non-deterministic methods. Note that the
     * value will not be unique across transactions as it is only millisecond granularity.
     *
     * @return A java.util.Date instance with deterministic time for all replicas using
     * UTC (Universal Coordinated Time is like GMT).
     */
    public Date getTransactionTime() {
        return m_runner.getTransactionTime();
    }

    /**
     * <p>Queue the SQL {@link org.voltdb.SQLStmt statement} for execution with the specified argument list,
     * and an Expectation describing the expected results. If the Expectation is not met then VoltAbortException
     * will be thrown with a description of the expectation that was not met. This exception must not be
     * caught from within the procedure.</p>
     *
     * @param stmt {@link org.voltdb.SQLStmt Statement} to queue for execution.
     * @param expectation Expectation describing the expected result of executing this SQL statement.
     * @param args List of arguments to be bound as parameters for the {@link org.voltdb.SQLStmt statement}
     * @see <a href="#allowable_params">List of allowable parameter types</a>
     */
    public void voltQueueSQL(final SQLStmt stmt, Expectation expectation, Object... args) {
        m_runner.voltQueueSQL(stmt, expectation, args);
    }

    /**
     * Queue the SQL {@link org.voltdb.SQLStmt statement} for execution with the specified argument list.
     *
     * @param stmt {@link org.voltdb.SQLStmt Statement} to queue for execution.
     * @param args List of arguments to be bound as parameters for the {@link org.voltdb.SQLStmt statement}
     * @see <a href="#allowable_params">List of allowable parameter types</a>
     */
    public void voltQueueSQL(final SQLStmt stmt, Object... args) {
        m_runner.voltQueueSQL(stmt, (Expectation) null, args);
    }

    /**
     * Execute the currently queued SQL {@link org.voltdb.SQLStmt statements} and return
     * the result tables.
     *
     * @return Result {@link org.voltdb.VoltTable tables} generated by executing the queued
     * query {@link org.voltdb.SQLStmt statements}
     */
    public VoltTable[] voltExecuteSQL() {
        return voltExecuteSQL(false);
    }

    /**
     * Execute the currently queued SQL {@link org.voltdb.SQLStmt statements} and return
     * the result tables. Boolean option allows caller to indicate if this is the final
     * batch for a procedure. If it's final, then additional optimizations can be enabled.
     * Any call to voltExecuteSQL() after calling this with the argument set to true
     * will cause the entire procedure to roll back.
     *
     * @param isFinalSQL Is this the final batch for a procedure?
     * @return Result {@link org.voltdb.VoltTable tables} generated by executing the queued
     * query {@link org.voltdb.SQLStmt statements}
     */
    public VoltTable[] voltExecuteSQL(boolean isFinalSQL) {
        return m_runner.voltExecuteSQL(isFinalSQL);
    }

    /**
     * Set the status code that will be returned to the client. This is not the same as the status
     * code returned by the server. If a procedure sets the status code and then rolls back or causes an error
     * the status code will still be propagated back to the client so it is always necessary to check
     * the server status code first.
     *
     * @param statusCode Byte-long application-specific status code.
     */
    public void setAppStatusCode(byte statusCode) {
        m_runner.setAppStatusCode(statusCode);
    }

    /**
     * Set the string that will be turned to the client. This is not the same as the status string
     * returned by the server. If a procedure sets the status string and then rolls back or causes an error
     * the status string will still be propagated back to the client so it is always necessary to check
     * the server status code first.
     *
     * @param statusString Application specific status string.
     */
    public void setAppStatusString(String statusString) {
        m_runner.setAppStatusString(statusString);
    }

    public int getPartitionId() {
        return m_runner.getPartitionId();
    }
}
