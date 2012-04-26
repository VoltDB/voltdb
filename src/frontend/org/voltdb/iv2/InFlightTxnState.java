/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.iv2;

import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

public class InFlightTxnState {
    private volatile InitiateResponseMessage response = null;
    private AtomicInteger outstandingResponse;
    private final InitiateTaskMessage task;

    /**
     * Constructor
     *
     * @param task
     *            The initiate task
     * @param expectedResponses
     *            Expected number of responses before it is considered finished
     */
    public InFlightTxnState(InitiateTaskMessage task, int expectedResponses)
    {
        outstandingResponse = new AtomicInteger(expectedResponses);
        this.task = task;
    }

    /**
     * Adds a new response. It checks if the response is the same as the
     * previous responses.
     *
     * @param newResponse new response
     * @return true if all expected responses are received, false otherwise
     */
    public boolean addResponse(InitiateResponseMessage newResponse)
    {
        int remainingResponses = outstandingResponse.decrementAndGet();
        if (response == null) {
            response = newResponse;
        }
        else {
            ClientResponseImpl newResponseData = newResponse.getClientResponseData();
            ClientResponseImpl existingResponseData = response.getClientResponseData();
            VoltTable[] newResults = newResponseData.getResults();
            VoltTable[] existingResults = existingResponseData.getResults();
            if (existingResults.length != newResults.length) {
                String msg = "Mismatched result count received for transaction ID: " + response.getTxnId();
                msg += "\n  while executing stored procedure: " + task.getStoredProcedureName();
                msg += "\n  from execution site: " + newResponse.m_sourceHSId;
                msg += "\n  Expected number of results: " + existingResults.length;
                msg += "\n  Mismatched number of results: " + newResults.length;
                // die die die
                VoltDB.crashGlobalVoltDB(msg, false, null); // kills process
            }
            for (int i = 0; i < existingResults.length; ++i) {
                if (!existingResults[i].hasSameContents(newResults[i])) {
                    String msg = "Mismatched result count received for transaction ID: " + response.getTxnId();
                    msg += "\n  while executing stored procedure: " + task.getStoredProcedureName();
                    msg += "\n  from execution site: " + newResponse.m_sourceHSId;
                    msg += "\n  Expected results: " + existingResults[i].toString();
                    msg += "\n  Mismatched results: " + newResults[i].toString();
                    // die die die
                    VoltDB.crashGlobalVoltDB(msg, false, null); // kills process
                }
            }
        }

        return (remainingResponses == 0);
    }

    public InitiateTaskMessage getTask()
    {
        return task;
    }

    public InitiateResponseMessage getResponse()
    {
        return response;
    }
}
