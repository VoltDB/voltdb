/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package server;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltCompoundProcedure;
import org.voltdb.client.ClientResponse;

public class HandleHit extends VoltCompoundProcedure {
    static VoltLogger LOG = new VoltLogger("HandleHit");

    long cookieId;
    String urlStr;
    boolean fakeFailures;
    String domain;
    String userName;
    Long accountId;
    boolean errorsReported;

    static AtomicInteger failCount = new AtomicInteger(0);

    public long run(long id, String str, int fails) {

        // Save inputs
        cookieId = id;
        urlStr = str;
        fakeFailures = fails != 0;

        // Build stages
        newStageList(this::doLookups)
        .then(this::doUpdates)
        .then(this::doUpdateResults)
        .build();
        return 0L;
    }

    // Invoke first stage procedures, lookups on different partitioning keys
    private void doLookups(ClientResponse[] unused) {

        // Extract domain from url
        URL url = null;
        try {
            url = new URL(urlStr);
            domain = url.getHost();
        }
        catch (Exception e) {
            reportError(e.getMessage());
            return;
        }

        queueProcedureCall("GetUsernameFromCookie", cookieId);
        queueProcedureCall("GetAccountIdFromDomain", domain);
    }

    // Process results of first stage, i.e. lookups, and perform updates
    private void doUpdates(ClientResponse[] resp) {
        boolean allGood = true;

        // Process response 0 = username
        ClientResponse resp0 = resp[0];
        if (resp0.getStatus() != ClientResponse.SUCCESS) {
            // TBD - should all non success ClientResponse be returned as errors to the producer
            abortProcedure(String.format("GetUsernameFromCookie returned: %d", resp0.getStatus()));
        }
        else if (resp0.getResults().length > 0 && resp0.getResults()[0].advanceRow()) {
            userName = resp0.getResults()[0].getString("username");
            if (resp0.getResults()[0].wasNull()) {
                userName = null;
            }
        }
        else {
            reportError(String.format("No user match found for cookie %d", cookieId));
            allGood = false;
        }

        // Process response 1 = account id
        ClientResponse resp1 = resp[1];
        if (resp1.getStatus() != ClientResponse.SUCCESS) {
            abortProcedure(String.format("GetAccountIdFromDomain returned: %d", resp1.getStatus()));
        }
        else if (resp1.getResults().length > 0 && resp1.getResults()[0].advanceRow()) {
            accountId = resp1.getResults()[0].getLong("accountid");
            if (resp1.getResults()[0].wasNull()) {
                accountId = null;
            }
        }
        else {
            reportError(String.format("No account match found for domain %s", domain));
            allGood = false;
        }

        // GENERATE FAILURES
        if (fakeFailures && (cookieId % 4 == 0 || cookieId % 7 == 0)) {
            if (failCount.incrementAndGet() % 2 == 0) {
                // Should be counted as an abort
                abortProcedure(String.format("ABORT for cookieId %d", cookieId));
            }
            else {
                // Should be counted as a fail
                throw new RuntimeException(String.format("THROW for cookieId %d", cookieId));
            }
        }

        // Did we get all we need?
        if (allGood) {
            queueProcedureCall("UpdateAccount", userName, domain);
            queueProcedureCall("GetNextBestAction",userName, cookieId, accountId, urlStr);
        }
    }

    // Last stage if all went well: just check the update results
    private void doUpdateResults(ClientResponse[] resp) {
        if (resp[0].getStatus() != ClientResponse.SUCCESS) {
            abortProcedure(String.format("UpdateAccount returned: %d", resp[0].getStatus()));
        }
        else if (resp[0].getResults().length > 0 && resp[0].getResults()[0].advanceRow()) {
            long rowCount = resp[0].getResults()[0].getLong(0);
            LOG.infoFmt("%d rows updated for account %d, domain %s, url %s", rowCount, accountId, domain, urlStr);
        }
        else {
            LOG.errorFmt("No rows updated for account %d, domain %d, url %s", accountId, domain, urlStr);
        }
        if (resp[1].getStatus() != ClientResponse.SUCCESS) {
            abortProcedure(String.format("GetNextBestAction returned: %d", resp[1].getStatus()));
        }

        // We're done (this is ignored if we already called abort)
        completeProcedure(0L);
    }

    // Complete the procedure after reporting errors: check if we succeeded logging them
    private void completeWithErrors(ClientResponse[] resp) {
        for (ClientResponse r : resp) {
            if (r.getStatus() != ClientResponse.SUCCESS) {
                abortProcedure(String.format("Failed reporting errors: %s", r.getStatusString()));
            }
        }
        completeProcedure(-1L);
    }

    // Report execution errors to special topic. We:
    // 1.  Change the stage list so as to abandon all incomplete stages
    //     and set up a new final stage
    // 2.  Queue up a request, to be executed after the
    //     current stage, to update the special topic
    private void reportError(String message) {
        if (!errorsReported) {
            newStageList(this::completeWithErrors)
                          .build();
            errorsReported = true;
        }
        queueProcedureCall("COOKIE_ERRORS.insert", cookieId, urlStr, message);
    }
}
