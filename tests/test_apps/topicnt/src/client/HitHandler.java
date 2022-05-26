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
package client;

import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.client.Client2;
import org.voltdb.client.ClientResponse;

/**
 * Class that mimics server's HandleHit compound procedure, but from a client application.
 * The logic is too complicated for a standard client application but it reproduces
 * the parallel invocations of server procedures done by HandleHit.
 */
public class HitHandler {
    final Client2 m_client;
    final MyLogger m_log;
    final long m_cookieId;
    final String m_url;

    final CompletableFuture<Boolean> m_fut = new CompletableFuture<>();

    volatile String m_domain;
    volatile String m_userName;
    volatile Long m_accountId;

    public HitHandler(Client2 client, MyLogger log, long cookieId, String url) {
        m_client = client;
        m_log = log;
        m_cookieId = cookieId;
        m_url = url;
    }

    public CompletableFuture<Boolean> run() {
        doLookups();
        return m_fut;
    }

    // Stage 1: Call the 2 lookup procedures in parallel
    private void doLookups() {
        try {
            // Extract domain from url
            URL url = new URL(m_url);
            m_domain = url.getHost();

            CompletableFuture<?> fut1 = m_client.callProcedureAsync("GetUsernameFromCookie", m_cookieId)
            .exceptionally(th -> {
                m_log.errorFmt("Failed GetUsernameFromCookie for %d, %s: %s", m_cookieId, m_url, th.getMessage());
                m_fut.completeExceptionally(th);
                return null;
            })
            .thenAcceptAsync(resp -> {
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    m_log.errorFmt("GetUsernameFromCookie returned %d for %d, %s",
                            resp.getStatus(), m_cookieId, m_url);
                }
                else if (resp.getResults()[0].advanceRow()) {
                    m_userName = resp.getResults()[0].getString("username");
                    if (resp.getResults()[0].wasNull()) {
                        m_userName = null;
                    }
                }
                else {
                    m_log.infoFmt("No user match for %d, %s", m_cookieId, m_url);
                }
            });

            CompletableFuture<?> fut2 = m_client.callProcedureAsync("GetAccountIdFromDomain", m_domain)
            .exceptionally(th -> {
                m_log.errorFmt("Failed GetAccountIdFromDomain for %d, %s: %s", m_cookieId, m_url, th.getMessage());
                m_fut.completeExceptionally(th);
                return null;
            })
            .thenAcceptAsync(resp -> {
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    m_log.errorFmt("GetAccountIdFromDomain returned %d for %d, %s",
                            resp.getStatus(), m_cookieId, m_url);
                }
                else if (resp.getResults()[0].advanceRow()) {
                    m_accountId = resp.getResults()[0].getLong("accountid");
                    if (resp.getResults()[0].wasNull()) {
                        m_accountId = null;
                    }
                }
                else {
                    m_log.infoFmt("No account match for %d, %s", m_cookieId, m_url);
                }
            });

            CompletableFuture.allOf(fut1, fut2).whenComplete((nil, th) -> {
                if (th != null) {
                    m_fut.completeExceptionally(th);
                }
                else if (m_userName != null && m_accountId != null) {
                    doUpdates();
                }
                else {
                    m_fut.complete(false);
                }
            });
        }
        catch (Exception ex) {
            m_log.errorFmt("Failed doLookups for %d, %s: %s", m_cookieId, m_url, ex.getMessage());
            m_fut.completeExceptionally(ex);
        }
    }

    // Call the 2 update procedures in parallel
    private void doUpdates() {
        AtomicBoolean allGood = new AtomicBoolean(true);
        try {
            CompletableFuture<?> fut1 = m_client.callProcedureAsync("UpdateAccount", m_userName, m_domain)
            .exceptionally(th -> {
                m_log.errorFmt("Failed UpdateAccount for %d, %s: %s", m_cookieId, m_url, th.getMessage());
                m_fut.completeExceptionally(th);
                return null;
            })
            .thenAcceptAsync(resp -> {
                long rowCount = 0;
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    m_log.errorFmt("UpdateAccount returned %d for %d, %s",
                            resp.getStatus(), m_cookieId, m_url);
                }
                else if (resp.getResults()[0].advanceRow()) {
                    rowCount = resp.getResults()[0].getLong(0);
                }
                if (rowCount != 1) {
                    m_log.errorFmt("UpdateAccount updated %d rows for %d, %s",
                            rowCount, m_cookieId, m_url);
                    allGood.compareAndSet(true, false);
                }
            });

            CompletableFuture<?> fut2 = m_client.callProcedureAsync("GetNextBestAction", m_userName, m_cookieId, m_accountId, m_url)
            .exceptionally(th -> {
                m_log.errorFmt("Failed GetNextBestAction for %d, %s: %s", m_cookieId, m_url, th.getMessage());
                m_fut.completeExceptionally(th);
                return null;
            })
            .thenAcceptAsync(resp -> {
                if (resp.getStatus() != ClientResponse.SUCCESS) {
                    m_log.errorFmt("GetNextBestAction returned %d for %d, %s",
                            resp.getStatus(), m_cookieId, m_url);
                    allGood.compareAndSet(true, false);
                }
            });

            CompletableFuture.allOf(fut1, fut2).whenComplete((nil, th) -> {
                if (th != null) {
                    m_fut.completeExceptionally(th);
                }
                else {
                    m_fut.complete(allGood.get());
                }
            });
        }
        catch (Exception ex) {
            m_log.errorFmt("Failed doUpdates for %d, %s: %s", m_cookieId, m_url, ex.getMessage());
            m_fut.completeExceptionally(ex);
        }
    }
}
