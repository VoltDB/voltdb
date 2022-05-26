/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.voltdb.client.ClientResponse;

/**
 * Base class for any user-provided compound procedures. A compound
 * procedure is one that invokes other procedures, perhaps in
 * order to combine results from those procedures into some
 * table update.
 * <p>
 * It is important to understand that a compound procedure does not
 * provide transactional guarantees. If there is a failure during
 * execution, in-progress transactions executed by the compound proc
 * may be rolled back as usual, but completed transactions will
 * not be.
 * <p>
 * Operation of a compound procedure is essentially asynchronous.
 * In a typical pattern, calls are issued to other procedures,
 * and the results of those calls are processed as they arrive.
 * To facilitate this, the execution of the compound procedure
 * is separated into 'stages'. A stage issues one or more
 * procedure calls, and then exits. The next stage will be
 * executed with the responses from the calls issued by the
 * previous stage. Intermediate results and working values must
 * be saved in member variables.
 * <p>
 * The last stage must complete the compound procedure, and return
 * a result, by calling <code>completeProcedure</code>. The result
 * must be one of the following types
 * <ul>
 * <li><code>Long</code></li>
 * <li><code>VoltTable</code></li>
 * <li><code>VoltTable[]</code></li>
 * </ul>
 * <p>
 * The compound procedure starts its execution in its <code>run(...)</code>
 * method, which must accept parameters compatible with the parameters provided
 * by the client. The <code>run</code> method must set up a list of
 * execution stages; the auxiliary class <code>StageListBuilder</code>
 * is provided to help with this. Any return value from <code>run</code>
 * is ignored, but must have a type acceptable to the Volt compiler.
 * The primitive type 'long' is recommended.
 * <p>
 * Execution of the compound procedure can be terminated prematurely
 * by calling <code>abortProcedure</code> or, equivalently, by throwing
 * a <code>CompoundProcAbortException</code>. This will be reported
 * to the client via the usual <code>ClientResponse</code> with
 * a status of <code>COMPOUND_PROC_USER_ABORT</code>.
 *
 * @since 11.4.0
 */
public class VoltCompoundProcedure extends VoltNonTransactionalProcedure {

    private CompoundProcedureRunner compoundProcRunner;

    final void setRunner(CompoundProcedureRunner r) {
        compoundProcRunner = r;
    }

    /**
     * Returns a {@link StageListBuilder} for use in building
     * the list of execution stages for this compound
     * procedure. The initial stage is given as an argument;
     * other stages can be added according to the needs of
     * the application.
     * <p>
     * Usage:
     * <pre><code>newStageList(STAGE)
     *    .then(STAGE)
     *    .then(STAGE)
     *    .build();
     * </code></pre>
     * The stage list must be built during execution of
     * your procedure's <code>run</code> method.
     *
     * @param consumer first stage handler
     * @return StageListBuilder instance of builder class
     * @see StageListBuilder
     */
    public final StageListBuilder newStageList(Consumer<ClientResponse[]> consumer) {
        return new StageListBuilder().then(consumer);
    }

    /**
     * Queues an asynchronous procedure call, to be executed
     * when the current stage returns.
     * <p>
     * The system supports parallel initiation. You can code
     * several calls to <code>queueProcedureCall</code> in
     * a single stage. There is a system limit on the number
     * of calls that can be queued in any one stage; this
     * limit defaults to 10, but can be changed in the VoltDB
     * deployment file.
     * <p>
     * When all calls have completed, the next stage will be
     * entered, with the results of all calls. The results
     * will have the same order as calls issued to
     * <code>queueProcedureCall</code>.
     * <p>
     * <code>queueProcedureCall</code> must only be called
     * in the context of a thread that is executing either
     * the initial <code>run</code> call, or one of the
     * stages set up for the procedure.
     *
     * @param procName name of procedure to call
     * @param params arguments as required by called procedure
     */
    public final void queueProcedureCall(String procName, Object... params) {
        compoundProcRunner.queueProcedureCall(procName, params);
    }

    /**
     * All processing of this compound procedure is now complete.
     * A response will be sent to the client. No more stages
     * will be executed.
     * <p>
     * If the current stage has queued any procedure calls
     * before calling <code>completeProcedure</code>, those
     * calls will not be issued: execution is complete.
     *
     * @param result the result of the compound procedure
     */
    public final void completeProcedure(long result) {
        compoundProcRunner.completeProcedure(Long.class, result);
    }

    /**
     * All processing of this compound procedure is now complete.
     * A response will be sent to the client. No more stages
     * will be executed.
     * <p>
     * If the current stage has queued any procedure calls
     * before calling <code>completeProcedure</code>, those
     * calls will not be issued: execution is complete.
     *
     * @param result the result of the compound procedure
     */
    public final void completeProcedure(VoltTable result) {
        compoundProcRunner.completeProcedure(VoltTable.class, result);
    }

    /**
     * All processing of this compound procedure is now complete.
     * A response will be sent to the client. No more stages
     * will be executed.
     * <p>
     * If the current stage has queued any procedure calls
     * before calling <code>completeProcedure</code>, those
     * calls will not be issued: execution is complete.
     *
     * @param result the result of the compound procedure
     */
    public final void completeProcedure(VoltTable[] result) {
        compoundProcRunner.completeProcedure(VoltTable[].class, result);
    }

    /**
     * Indicates that processing of this compound procedure
     * is being abandoned. A response will be sent to the client
     * with status set to <code>COMPOUND_PROC_USER_ABORT</code>.
     * <p>
     * Completed calls previously issued by the compound procedure
     * will not be affected; there is no rollback of transactions.
     * <p>
     * This should be the last action taken by the compound
     * procedure.
     * <p>
     * If the current stage has queued any procedure calls
     * before calling <code>abortProcedure</code>, those
     * calls will not be issued: execution has concluded.
     * <p>
     * Throwing a <code>CompoundProcAbortException</code> can be
     * used instead of calling <code>abortProcedure</code>, with
     * the same effect.
     *
     * @param reason to be included in client response
     */
    public final void abortProcedure(String reason) {
        compoundProcRunner.abortProcedure(reason);
    }

    /**
     * Set the application status code that will be returned to the
     * client as part of the <code>ClientResponse</code>. This is
     * distinct from the VoltDB-provided status code. The meaning
     * is determined by your procedure and the application.
     *
     * @param status application-specific status code
     */
    public void setAppStatusCode(byte status) {
        compoundProcRunner.setResponseAppStatusCode(status);
    }

    /**
     * Set the application status string that will be returned to the
     * client as part of the <code>ClientResponse</code>. This is
     * distinct from the VoltDB-provided status string. The meaning
     * is determined by your procedure and the application.
     *
     * @param message application-specific status string
     */
    public void setAppStatusString(String message) {
        compoundProcRunner.setResponseAppStatusString(message);
    }

    /**
     * Represents the stages of processing of a <code>VoltCompoundProcedure</code>.
     * <p>
     * A <code>Stage</code> can only be constructed using one of the methods
     * of {@link StageListBuilder}. The compound procedure does not interact
     * directly with <code>Stage</code> objects; they provide a description
     * of how the compound procedure is to be executed.
     * <p>
     * Each stage is a method of the procedure. The stages are declared
     * in the initial <code>run</code> method of procedure execution.
     * <p>
     * A stage may execute any desired computation, and optionally issue one
     * or more procedure calls, prior to returning. The next stage will
     * be called with an array of the responses from the calls issued by
     * the previous stage, when they have been received. The first stage will
     * have an empty response array.
     * <p>
     * Stages execute sequentially. Each stage executes on an arbitrary thread
     * from a dedicated thread pool, potentially a different thread for each stage.
     * <p>
     * Intermediate results and working values must be saved in member variables.
     *
     * @see #newStageList
     * @see StageListBuilder
     */
    public final static class Stage {
        private final Consumer<ClientResponse[]> consumer;
        private Stage(Consumer<ClientResponse[]> c) { // use StageListBuilder
            consumer = c;
        }
        void execute(ClientResponse[] r) {
            consumer.accept(r);
        }
    }

    /**
     * Builds a list of execution stages for this <code>VoltCompoundProcedure</code>.
     * <p>
     * The stage list is built at the start of execution, in your procedure's
     * <code>run</code> method, like this:
     * <pre><code>newStageList(STAGE)
     *    .then(STAGE)
     *    .then(STAGE)
     *    .build();
     * </code></pre>
     * A stage list is created by the call to {@link #newStageList}, with
     * the first stage to be executed. Additional stages can be added
     * one-by-one as required by the application logic. Once built, the
     * stage list cannot be changed.
     *
     * @see Stage
     */
    public final class StageListBuilder {
        private final List<Stage> stages = new ArrayList<>();
        private StageListBuilder() { } // use VoltCompoundProcedure.newStageList()

        /**
         * Adds the next sequential state to the list of stages
         *
         * @param consumer next stage handler
         * @return this StageListBuilder
         */
        public StageListBuilder then(Consumer<ClientResponse[]> consumer) {
            stages.add(new Stage(consumer));
            return this;
        }

        /**
         * Finalizes the list of stages, and sets it up ready
         * for execution.
         */
        public void build() {
            compoundProcRunner.setStageList(stages);
        }
    }

    /**
     * Thrown from a stored procedure to abort a VoltCompoundProcedure.
     * There is no rollback. The client will get a response with a status
     * code of ClientResponse.COMPOUND_PROC_USER_ABORT.
     */
    public static class CompoundProcAbortException extends RuntimeException {
        private static final long serialVersionUID = 20191118L;

        public CompoundProcAbortException(String msg) {
            super(msg);
        }

        public CompoundProcAbortException(Throwable cause) {
            super(cause);
        }

        public CompoundProcAbortException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
