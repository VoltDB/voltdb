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

package org.voltdb.client;

/**
 * Container for configuration settings for a Client
 */
public class ClientConfig {

    static final long DEFAULT_PROCEDURE_TIMOUT_MS = 2 * 60 * 1000; // default timeout is 2 minutes;
    static final long DEFAULT_CONNECTION_TIMOUT_MS = 2 * 60 * 1000; // default timeout is 2 minutes;

    final String m_username;
    final String m_password;
    final ClientStatusListenerExt m_listener;
    boolean m_heavyweight = false;
    int m_maxOutstandingTxns = 3000;
    int m_maxTransactionsPerSecond = Integer.MAX_VALUE;
    boolean m_autoTune = false;
    int m_autoTuneTargetInternalLatency = 5;
    long m_procedureCallTimeoutMS = DEFAULT_PROCEDURE_TIMOUT_MS;
    long m_connectionResponseTimeoutMS = DEFAULT_CONNECTION_TIMOUT_MS;

    /**
     * Configuration for a client with no authentication credentials that will
     * work with a server with security disabled. Also specifies no status listener.
     */
    public ClientConfig() {
        m_username = "";
        m_password = "";
        m_listener = null;
    }

    /**
     * Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string.
     * @param username
     * @param password
     */
    public ClientConfig(String username, String password) {
        this(username, password, (ClientStatusListenerExt) null);
    }

    /**
     * Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string. Also specifies a status listener.
     * @param username
     * @param password
     * @param listener
     */
    @Deprecated
    public ClientConfig(String username, String password, ClientStatusListener listener) {
        this(username, password, new ClientStatusListenerWrapper(listener));
    }

    /**
     * Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string. Also specifies a status listener.
     * @param username
     * @param password
     * @param listener
     */
    public ClientConfig(String username, String password, ClientStatusListenerExt listener) {
        if (username == null) {
            m_username = "";
        } else {
            m_username = username;
        }
        if (password == null) {
            m_password = "";
        } else {
            m_password = password;
        }
        m_listener = listener;
    }

    /**
     * Set the timeout for procedure call. If the timeout expires before the call returns,
     * the procedure callback will be called with status {@link ClientResponse#CONNECTION_TIMEOUT}.
     * Synchronous procedures will throw an exception. If a response comes back after the
     * expiration has triggered, then a callback method
     * {@link ClientStatusListenerExt#lateProcedureResponse(ClientResponse, String, int)}
     * will be called.
     *
     * Default value is 2 minutes if not set. Value of 0 means forever.
     *
     * Note that while specified in MS, this timeout is only accurate to within a second or so.
     *
     * @param ms Timeout value in milliseconds.
     */
    public void setProcedureCallTimeout(long ms) {
        assert(ms >= 0);
        if (ms < 0) ms = 0;
        // 0 implies infinite, but use LONG_MAX to reduce branches to test
        if (ms == 0) ms = Long.MAX_VALUE;
        m_procedureCallTimeoutMS = ms;
    }

    /**
     * Set the timeout for reading from a connection. If a connection receives no responses,
     * either from procedure calls or &amp;Pings, for the timeout time in milliseconds,
     * then the connection will be assumed dead and the closed connection callback will
     * be called.
     *
     * Default value is 2 minutes if not set. Value of 0 means forever.
     *
     * Note that while specified in MS, this timeout is only accurate to within a second or so.
     *
     * @param ms Timeout value in milliseconds.
     */
    public void setConnectionResponseTimeout(long ms) {
        assert(ms >= 0);
        if (ms < 0) ms = 0;
        // 0 implies infinite, but use LONG_MAX to reduce branches to test
        if (ms == 0) ms = Long.MAX_VALUE;
        m_connectionResponseTimeoutMS = ms;
    }

    /**
     * Deprecated because memory pooling no longer uses arenas. Has no effect
     * Set the maximum size of memory pool arenas before falling back to using heap byte buffers.
     * @param maxArenaSizes
     */
    @Deprecated
    public void setMaxArenaSizes(int maxArenaSizes[]) {
    }

    /**
     * By default a single network thread is created and used to do IO and invoke callbacks.
     * When set to true, Runtime.getRuntime().availableProcessors() / 2 threads are created.
     * Multiple server connections are required for more threads to be involved, a connection
     * is assigned exclusively to a connection.
     */
    public void setHeavyweight(boolean heavyweight) {
        m_heavyweight = heavyweight;
    }

    /**
     * Deprecated and has no effect
     * Provide a hint indicating how large messages will be once serialized. Ensures
     * efficient message buffer allocation.
     * @param size
     */
    @Deprecated
    public void setExpectedOutgoingMessageSize(int size) {
    }

    /**
     * Set the maximum number of outstanding requests that will be submitted before
     * blocking. Similar to the number of concurrent connections in a traditional synchronous API.
     * Defaults to 2k.
     * @param maxOutstanding
     */
    public void setMaxOutstandingTxns(int maxOutstanding) {
        if (maxOutstanding < 1) {
            throw new IllegalArgumentException(
                    "Max outstanding must be greater than 0, " + maxOutstanding + " was specified");
        }
        m_maxOutstandingTxns = maxOutstanding;
    }

    /**
     * Set the maximum number of transactions that can be run in 1 second. Note this
     * specified a rate, not a ceiling. If the limit is set to 10, you can't send 10 in
     * the first half of the second and 5 in the later half; the client will let you send
     * about 1 transaction every 100ms. Default is {@see Integer#MAX_VALUE}.
     * @param maxTxnsPerSecond Requested ceiling on rate of call in transaction per second.
     */
    public void setMaxTransactionsPerSecond(int maxTxnsPerSecond) {
        if (maxTxnsPerSecond < 1) {
            throw new IllegalArgumentException(
                    "Max TPS must be greater than 0, " + maxTxnsPerSecond + " was specified");
        }
    }

    /**
     * Enable the Auto Tuning feature, which dynamically adjusts the maximum
     * allowable transaction number with the goal of maintaining a target latency.
     * The latency value used is the internal latency as reported by the servers.
     * The internal latency is a good measure of system saturation.
     * {@see ClientConfig#setAutoTuneTargetInternalLatency(int) setAutoTuneTargetInternalLatency}
     */
    public void enableAutoTune() {
        m_autoTune = true;
    }

    /**
     * Set the target latency for the Auto Tune feature. Note this represents internal
     * latency as reported by the server(s), not round-trip latency measured by the
     * client. Default value is 5 if this is not called.
     * @param targetLatency New target latency in milliseconds.
     */
    public void setAutoTuneTargetInternalLatency(int targetLatency) {
        if (targetLatency < 1) {
            throw new IllegalArgumentException(
                    "Max auto tune latency must be greater than 0, " + targetLatency + " was specified");
        }
    }
}
