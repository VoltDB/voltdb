/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.client;

import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Container for configuration settings for a Client
 */
public class ClientConfig {

    static final long DEFAULT_PROCEDURE_TIMOUT_NANOS = TimeUnit.MINUTES.toNanos(2);// default timeout is 2 minutes;
    static final long DEFAULT_CONNECTION_TIMOUT_MS = 2 * 60 * 1000; // default timeout is 2 minutes;
    static final long DEFAULT_INITIAL_CONNECTION_RETRY_INTERVAL_MS = 1000; // default initial connection retry interval is 1 second
    static final long DEFAULT_MAX_CONNECTION_RETRY_INTERVAL_MS = 8000; // default max connection retry interval is 8 seconds

    final String m_username;
    final String m_password;
    final boolean m_cleartext;
    final ClientStatusListenerExt m_listener;
    boolean m_heavyweight = false;
    int m_maxOutstandingTxns = 3000;
    int m_maxTransactionsPerSecond = Integer.MAX_VALUE;
    boolean m_autoTune = false;
    int m_autoTuneTargetInternalLatency = 5;
    long m_procedureCallTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(DEFAULT_PROCEDURE_TIMOUT_NANOS);
    long m_connectionResponseTimeoutMS = DEFAULT_CONNECTION_TIMOUT_MS;
    boolean m_useClientAffinity = true;
    Subject m_subject = null;
    boolean m_reconnectOnConnectionLoss;
    long m_initialConnectionRetryIntervalMS = DEFAULT_INITIAL_CONNECTION_RETRY_INTERVAL_MS;
    long m_maxConnectionRetryIntervalMS = DEFAULT_MAX_CONNECTION_RETRY_INTERVAL_MS;

    /**
     * <p>Configuration for a client with no authentication credentials that will
     * work with a server with security disabled. Also specifies no status listener.</p>
     */
    public ClientConfig() {
        m_username = "";
        m_password = "";
        m_listener = null;
        m_cleartext = true;
    }

    /**
     * <p>Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string.</p>
     *
     * @param username Cleartext username.
     * @param password Cleartext password.
     */
    public ClientConfig(String username, String password) {
        this(username, password, true, (ClientStatusListenerExt) null);
    }

    /**
     * <p>Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string. Also specifies a status listener.</p>
     *
     * @deprecated {@link ClientStatusListener} deprecated in favor of {@link ClientStatusListenerExt}
     * in
     * @param username Cleartext username.
     * @param password Cleartext password.
     * @param listener {@link ClientStatusListener} implementation to receive callbacks.
     */
    @Deprecated
    public ClientConfig(String username, String password, ClientStatusListener listener) {
        this(username, password, true, new ClientStatusListenerWrapper(listener));
    }

    /**
     * <p>Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string. Also specifies a status listener.</p>
     *
     * @param username Cleartext username.
     * @param password Cleartext password.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     */
    public ClientConfig(String username, String password, ClientStatusListenerExt listener) {
        this(username,password,true,listener);
    }

    /**
     * <p>Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string. Also specifies a status listener.</p>
     *
     * @param username Cleartext username.
     * @param password A cleartext or hashed passowrd.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     * @param cleartext Whether the password is hashed.
     */
    public ClientConfig(String username, String password, boolean cleartext, ClientStatusListenerExt listener) {
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
        m_cleartext = cleartext;
    }

    /**
     * <p>Set the timeout for procedure call. If the timeout expires before the call returns,
     * the procedure callback will be called with status {@link ClientResponse#CONNECTION_TIMEOUT}.
     * Synchronous procedures will throw an exception. If a response comes back after the
     * expiration has triggered, then a callback method
     * {@link ClientStatusListenerExt#lateProcedureResponse(ClientResponse, String, int)}
     * will be called.</p>
     *
     * <p>Default value is 2 minutes if not set. Value of 0 means forever.</p>
     *
     * <p>Note that while specified in MS, this timeout is only accurate to within a second or so.</p>
     *
     * @param ms Timeout value in milliseconds.
     */
    public void setProcedureCallTimeout(long ms) {
        assert(ms >= 0);
        if (ms < 0) ms = 0;
        // 0 implies infinite, but use LONG_MAX to reduce branches to test
        if (ms == 0) ms = Long.MAX_VALUE;
        m_procedureCallTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(ms);
    }

    /**
     * <p>Set the timeout for reading from a connection. If a connection receives no responses,
     * either from procedure calls or &amp;Pings, for the timeout time in milliseconds,
     * then the connection will be assumed dead and the closed connection callback will
     * be called.</p>
     *
     * <p>Default value is 2 minutes if not set. Value of 0 means forever.</p>
     *
     * <p>Note that while specified in MS, this timeout is only accurate to within a second or so.</p>
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
     * <p>Set the maximum size of memory pool arenas before falling back to using heap byte buffers.</p>
     *
     * @deprecated Deprecated because memory pooling no longer uses arenas. Has no effect.
     * @param maxArenaSizes Maximum size of each arena.
     */
    @Deprecated
    public void setMaxArenaSizes(int maxArenaSizes[]) {
    }

    /**
     * <p>By default a single network thread is created and used to do IO and invoke callbacks.
     * When set to true, Runtime.getRuntime().availableProcessors() / 2 threads are created.
     * Multiple server connections are required for more threads to be involved, a connection
     * is assigned exclusively to a connection.</p>
     *
     * @param heavyweight Whether to create additional threads for high IO or
     * high processing workloads.
     */
    public void setHeavyweight(boolean heavyweight) {
        m_heavyweight = heavyweight;
    }

    /**
     * <p>Provide a hint indicating how large messages will be once serialized. Ensures
     * efficient message buffer allocation.</p>
     *
     * @deprecated Has no effect.
     * @param size The expected size of the outgoing message.
     */
    @Deprecated
    public void setExpectedOutgoingMessageSize(int size) {
    }

    /**
     * <p>Set the maximum number of outstanding requests that will be submitted before
     * blocking. Similar to the number of concurrent connections in a traditional synchronous API.
     * Defaults to 2k.</p>
     *
     * @param maxOutstanding The maximum outstanding transactions before calls to
     * {@link Client#callProcedure(ProcedureCallback, String, Object...)} will block
     * or return false (depending on settings).
     */
    public void setMaxOutstandingTxns(int maxOutstanding) {
        if (maxOutstanding < 1) {
            throw new IllegalArgumentException(
                    "Max outstanding must be greater than 0, " + maxOutstanding + " was specified");
        }
        m_maxOutstandingTxns = maxOutstanding;
    }

    /**
     * <p>Set the maximum number of transactions that can be run in 1 second. Note this
     * specifies a rate, not a ceiling. If the limit is set to 10, you can't send 10 in
     * the first half of the second and 5 in the later half; the client will let you send
     * about 1 transaction every 100ms. Default is {link Integer#MAX_VALUE}.</p>
     *
     * @param maxTxnsPerSecond Requested ceiling on rate of call in transaction per second.
     */
    public void setMaxTransactionsPerSecond(int maxTxnsPerSecond) {
        if (maxTxnsPerSecond < 1) {
            throw new IllegalArgumentException(
                    "Max TPS must be greater than 0, " + maxTxnsPerSecond + " was specified");
        }
        m_maxTransactionsPerSecond = maxTxnsPerSecond;
    }

    /**
     * <p>Enable the Auto Tuning feature, which dynamically adjusts the maximum
     * allowable transaction number with the goal of maintaining a target latency.
     * The latency value used is the internal latency as reported by the servers.
     * The internal latency is a good measure of system saturation.</p>
     *
     * <p>See {@link #setAutoTuneTargetInternalLatency(int)}.</p>
     */
    public void enableAutoTune() {
        m_autoTune = true;
    }

    /**
     * <p>Attempts to route transactions to the correct master partition improving latency
     * and throughput</p>
     *
     * <p>If you are using persistent connections you definitely want this.</p>
     *
     * @param on Enable or disable the affinity feature.
     */
    public void setClientAffinity(boolean on) {
        m_useClientAffinity = on;
    }

    /**
     * <p>Experimental: Attempts to reconnect to a node with retry after connection loss. See the {@link ReconnectStatusListener}.</p>
     *
     * @param on Enable or disable the reconnection feature. Default is off.
     */
    public void setReconnectOnConnectionLoss(boolean on) {
        this.m_reconnectOnConnectionLoss = on;
    }

    /**
     * <p>Set the initial connection retry interval. Only takes effect if {@link #m_reconnectOnConnectionLoss} is turned on.</p>
     *
     * @param ms initial connection retry interval in milliseconds.
     */
    public void setInitialConnectionRetryInterval(long ms) {
        this.m_initialConnectionRetryIntervalMS = ms;
    }

    /**
     * <p>Set the max connection retry interval. Only takes effect if {@link #m_reconnectOnConnectionLoss} is turned on.</p>
     *
     * @param ms max connection retry interval in milliseconds.
     */
    public void setMaxConnectionRetryInterval(long ms) {
        this.m_maxConnectionRetryIntervalMS = ms;
    }

    /**
     * <p>Set the target latency for the Auto Tune feature. Note this represents internal
     * latency as reported by the server(s), not round-trip latency measured by the
     * client. Default value is 5 if this is not called.</p>
     *
     * @param targetLatency New target latency in milliseconds.
     */
    public void setAutoTuneTargetInternalLatency(int targetLatency) {
        if (targetLatency < 1) {
            throw new IllegalArgumentException(
                    "Max auto tune latency must be greater than 0, " + targetLatency + " was specified");
        }
        m_autoTuneTargetInternalLatency = targetLatency;
    }

    /**
     * <p>Enable Kerberos authentication with the provided subject credentials<p>
     * @param subject
     */
    public void enableKerberosAuthentication(final Subject subject) {
        m_subject = subject;
    }

    /**
     * <p>Use the provided JAAS login context entry key to get the authentication
     * credentials held by the caller<p>
     *
     * @param loginContextEntryKey JAAS login context config entry designation
     */
    public void enableKerberosAuthentication(final String loginContextEntryKey) {
       try {
           LoginContext lc = new LoginContext(loginContextEntryKey);
           lc.login();
           m_subject = lc.getSubject();
       } catch (SecurityException ex) {
           throw new IllegalArgumentException("Cannot determine client consumer's credentials", ex);
       } catch (LoginException ex) {
           throw new IllegalArgumentException("Cannot determine client consumer's credentials", ex);
       }
    }
}
