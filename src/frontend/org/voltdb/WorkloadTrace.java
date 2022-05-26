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

import org.voltdb.catalog.*;

/**
 * Workload Manager Interface
 */
public interface WorkloadTrace {

    // ---------------------------------------------------------------------
    // WORKLOAD TRACER INTERFACE
    // ---------------------------------------------------------------------

    /**
     * Starts a new transaction trace record in the trace manager.
     *
     * @param caller - the object that is invoking this new stored procedure (used as the transaction id)
     * @param catalog_proc - the Procedure being invoked in this transaction
     * @param args - the input parameters for this stored procedure
     * @return a unique handle to use in further tracing operations for this xact
     */
    public Object startTransaction(Object caller, Procedure catalog_proc, Object args[]);

    /**
     * For the given transaction trace handle, the trace manager will close the tracing record
     * and perform any final operations.
     *
     * @param xact_handle - the transaction handle created by startTransaction()
     */
    public void stopTransaction(Object xact_handle);

    /**
     * Start a new query trace record
     *
     * @param xact_handle - the transaction handle created by startTransaction()
     * @param catalog_statement - the catalog Statement object representing this query
     * @param args - the input parameters for this query
     * @param batch_id - a identifier for which batch of queries this query is in.
     * @return a unique handle to use in further tracing operations for this query
     */
    public Object startQuery(Object xact_handle, Statement catalog_statement, Object args[], int batch_id);

    /**
     * For the given query trace handle, the trace manager will close the tracing record.
     *
     * @param query_handle - the query trace handle created by startQuery()
     */
    public void stopQuery(Object query_handle);

    // ---------------------------------------------------------------------
    // UTILITY METHODS
    // ---------------------------------------------------------------------

    /**
     * For the given transaction handle, get the next query batch id (starting at zero)
     *
     * @param xact_handle - the transaction handle created by startTransaction()
     * @return the next query batch id for a transaction
     */
    public int getNextBatchId(Object xact_handle);

    /**
     * Set the catalog for monitoring
     * This must be set before starting any trace operations
     *
     * @param catalog - the catalog that the trace manager will be working with
     */
    public void setCatalog(Catalog catalog);

    /**
     * Set the output path of trace files. Depends on implementation
     *
     * @param path - the path to a file for where the trace manager should store traces
     */
    public void setOutputPath(String path);

    /**
     * Adds a stored procedure to a list of procedures to ignore.
     * If a procedure is ignored, then startTransaction() will return a null handle
     *
     * @param name - the name of the stored procedure to ignore
     */
    public void addIgnoredProcedure(String name);
}
