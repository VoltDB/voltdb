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

package org.voltdb.importer;

import java.util.function.Predicate;

import org.voltdb.client.ProcedureCallback;


/**
 * Adapter that is used by importers to access the server. For example, to execute procedures
 * and report information for statistics collection.
 * <p> This separates out server specific classes from importer specific bundle classes and thus allows
 * importer bundles to run without depending on VoltDB internal classes.
 */
public interface ImporterServerAdapter {

    /**
     * This is used by importers to execute procedures in the server.
     *
     * @param importer the calling importer instance. This may be used by the importer framework
     * to report back pressure.
     * @param backPressurePredicate the predicate to check when the partition is
     *                              on back pressure for over a certain amount
     *                              of time. The partition ID will be passed to
     *                              the predicate. If the predicate evaluates to
     *                              true, it keeps waiting for back pressure to
     *                              be relieved. Otherwise, it ignores back
     *                              pressure and initiates the transaction.
     * @param callback the callback object that will receive procedure execution status
     * @param proc the name of the procedure that is to be executed
     * @param fieldList the parameters to be passed in to the procedure
     * @return returns true if the procedure execution was queued successfully; false otherwise.
     */
    public boolean callProcedure(AbstractImporter importer, Predicate<Integer> backPressurePredicate,
            ProcedureCallback callback, String proc, Object... fieldList);

    /**
     * This should be used by importers to report failure while trying to execute a procedure.
     *
     * @param importerName the name of the importer
     * @param procName the name of the procedure that the importer was trying to execute
     * @param decrementPending indicates if this failed after the importer reported that the
     * procedure execution was queued or not. True indicates that reporter already reported queuing
     * and hence must decrement that count. False indicates that failure occurred before the importer
     * reported queuing.
     */
    public void reportFailure(String importerName, String procName, boolean decrementPending);

    /**
     * This should be used by importers to report that a procedure executed was queued successfully.
     *
     * @param importerName the name of the importer
     * @param procName the name of the procedure that the importer was trying to execute
     */
    public void reportQueued(String importerName, String procName);

    /** This should be used by importer to report that the importer was successfully initialized.
     *
     * @param importerName the name of the importer
     * @param procName the name of the procedure that the importer was trying to execute
     */
    public void reportInitialized(String importerName, String procName);
}
