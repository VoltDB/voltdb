/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import java.util.ArrayList;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltCompoundProcedure;
import org.voltdb.client.ClientResponse;

public class HandleHit extends VoltCompoundProcedure {
    static VoltLogger LOG = new VoltLogger("HandleHit");

    long cookieId;
    String urlStr;
    String domain;
    String userName;
    Long accountId;

    // Errors are saved and posted to a special
    // topic, using a dedicated stage.
    boolean errorsReported;
    List<String> errorMessages;

    public long run(long id, String str) {

        // Save inputs
        cookieId = id;
        urlStr = str;

        // Build stages
        newStageList(this::doLookups)
        .then(this::doUpdates)
        .then(this::doUpdateResults)
        // following stages on error only
        .then(this::saveReportedErrors)
        .then(this::completeWithErrors)
         .build();
        return 0;
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

        // Do nothing if previous stage failed
        if (errorsReported) {
            return;
        }

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
        }

        // Quit if we did not get all we need
        if (errorsReported) {
            return;
        }

        // Otherwise proceed to update
        queueProcedureCall("UpdateAccount", userName, domain);
        queueProcedureCall("GetNextBestAction",userName, cookieId, accountId, urlStr);
    }

    // Last stage if all went well: just check the update results
    private void doUpdateResults(ClientResponse[] resp) {

        // Do nothing if previous stage failed
        if (errorsReported) {
            return;
        }

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
        completeProcedure(0);
    }

    // We only execute this stage if lookup errors were previously reported
    // (otherwise we have executed completeProcedure or abortProcedure)
    private void saveReportedErrors(ClientResponse[] nil) {
        for (String message : errorMessages) {
            queueProcedureCall("COOKIE_ERRORS.insert", cookieId, urlStr, message);
        }
    }

    // Complete the procedure after reporting errors: check if we succeeded logging them
    private void completeWithErrors(ClientResponse[] resp) {
        for (ClientResponse r : resp) {
            if (r.getStatus() != ClientResponse.SUCCESS) {
                abortProcedure(String.format("Failed reporting errors: %s", r.getStatusString()));
            }
        }
        completeProcedure(-1);
    }

    // Report execution errors. We save up error messages
    // for later reporting to a special topic in the
    // saveReportedErrors stage.
    private void reportError(String message) {
        errorsReported = true;
        if (errorMessages == null) {
            errorMessages = new ArrayList<>();
        }
        errorMessages.add(message);
    }
}
