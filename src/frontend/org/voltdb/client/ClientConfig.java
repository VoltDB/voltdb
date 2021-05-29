/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.RoundingMode;
import java.security.Principal;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltcore.utils.ssl.SSLConfiguration.SslConfig;
import org.voltdb.common.Constants;
import org.voltdb.types.VoltDecimalHelper;

/**
 * Container for configuration settings for a Client
 */
public class ClientConfig {

    private static final String DEFAULT_SSL_PROPS_FILE = "ssl-config";

    static final long DEFAULT_PROCEDURE_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(2);// default timeout is 2 minutes
    static final long DEFAULT_CONNECTION_TIMEOUT_MS = 2 * 60 * 1000; // default timeout is 2 minutes
    static final long DEFAULT_INITIAL_CONNECTION_RETRY_INTERVAL_MS = 1000; // default initial connection retry interval is 1 second
    static final long DEFAULT_MAX_CONNECTION_RETRY_INTERVAL_MS = 8000; // default max connection retry interval is 8 seconds
    static final long DEFAULT_NONBLOCKING_ASYNC_TIMEOUT_NANOS = 500_000; // default timeout, 500 microseconds
    static final int DEFAULT_MAX_OUTSTANDING_TRANSACTIONS = 3000; // maximum outstanding transaction count
    static final int DEFAULT_BACKPRESSURE_QUEUE_REQUEST_LIMIT = 100; // default request limit
    static final int DEFAULT_BACKPRESSURE_QUEUE_BYTE_LIMIT = 256 * 1024; // default byte limit, 256K

    final ClientAuthScheme m_hashScheme;
    final String m_username;
    final String m_password;
    final boolean m_cleartext;
    final ClientStatusListenerExt m_listener;
    boolean m_heavyweight = false;
    int m_maxOutstandingTxns = DEFAULT_MAX_OUTSTANDING_TRANSACTIONS;
    int m_maxTransactionsPerSecond = Integer.MAX_VALUE;
    long m_procedureCallTimeoutNanos = DEFAULT_PROCEDURE_TIMEOUT_NANOS;
    long m_connectionResponseTimeoutMS = DEFAULT_CONNECTION_TIMEOUT_MS;
    Subject m_subject = null;
    boolean m_reconnectOnConnectionLoss;
    long m_initialConnectionRetryIntervalMS = DEFAULT_INITIAL_CONNECTION_RETRY_INTERVAL_MS;
    long m_maxConnectionRetryIntervalMS = DEFAULT_MAX_CONNECTION_RETRY_INTERVAL_MS;
    SslConfig m_sslConfig;
    boolean m_topologyChangeAware = false;
    boolean m_enableSSL = false;
    String m_sslPropsFile = null;
    boolean m_nonblocking = false;
    long m_asyncBlockingTimeout = DEFAULT_NONBLOCKING_ASYNC_TIMEOUT_NANOS;
    int m_backpressureQueueRequestLimit = DEFAULT_BACKPRESSURE_QUEUE_REQUEST_LIMIT;
    int m_backpressureQueueByteLimit = DEFAULT_BACKPRESSURE_QUEUE_BYTE_LIMIT;

    //For unit testing. This should really be in Environment class we should assemble all such there.
    public static final boolean ENABLE_SSL_FOR_TEST = Boolean.valueOf(
            System.getenv("ENABLE_SSL") == null ?
                    Boolean.toString(Boolean.getBoolean("ENABLE_SSL"))
                  : System.getenv("ENABLE_SSL"));

    final static String getUserNameFromSubject(Subject subject) {
        if (subject == null || subject.getPrincipals() == null || subject.getPrincipals().isEmpty()) {
            throw new IllegalArgumentException("Subject is null or does not contain principals");
        }
        Iterator<Principal> piter = subject.getPrincipals().iterator();
        Principal principal = piter.next();
        String username = principal.getName();
        while (piter.hasNext()) {
            principal = piter.next();
            if (principal instanceof DelegatePrincipal) {
                username = principal.getName();
                break;
            }
        }
        return username;
    }

    /**
     * <p>Configuration for a client with no authentication credentials that will
     * work with a server with security disabled. Also specifies no status listener.</p>
     */
    public ClientConfig() {
        this("", "", true, (ClientStatusListenerExt) null, ClientAuthScheme.HASH_SHA256);
    }


    /**
     * <p>Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string.</p>
     *
     * @param username Cleartext username.
     * @param password Cleartext password.
     */
    public ClientConfig(String username, String password) {
        this(username, password, true, (ClientStatusListenerExt) null, ClientAuthScheme.HASH_SHA256);
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
        this(username,password,true,listener, ClientAuthScheme.HASH_SHA256);
    }

    /**
     * <p>Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string. Also specifies a status listener.</p>
     *
     * @param username Cleartext username.
     * @param password Cleartext password.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     * @param scheme Client password hash scheme
     */
    public ClientConfig(String username, String password, ClientStatusListenerExt listener, ClientAuthScheme scheme) {
        this(username,password,true,listener, scheme);
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
        this(username, password, cleartext, listener, ClientAuthScheme.HASH_SHA256);
    }

    /**
     * <p>Configuration for a client that specifies an already authenticated {@link Subject}.
     * Also specifies a status listener.</p>
     *
     * @param subject an authenticated {@link Subject}
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     */
    public ClientConfig(Subject subject, ClientStatusListenerExt listener) {
        this(getUserNameFromSubject(subject), "", true, listener, ClientAuthScheme.HASH_SHA256);
        m_subject = subject;
    }

    /**
     * <p>Configuration for a client that specifies authentication credentials. The username and
     * password can be null or the empty string. Also specifies a status listener.</p>
     *
     * @param username Cleartext username.
     * @param password A cleartext or hashed passowrd.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     * @param cleartext Whether the password is hashed.
     * @param scheme Client password hash scheme
     */
    public ClientConfig(String username, String password, boolean cleartext, ClientStatusListenerExt listener, ClientAuthScheme scheme) {

        if (ClientConfig.ENABLE_SSL_FOR_TEST) {
            try (InputStream is = ClientConfig.class.getResourceAsStream(DEFAULT_SSL_PROPS_FILE)) {
                Properties sslProperties = new Properties();
                sslProperties.load(is);
                String trustStorePath = sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_CONFIG_PROP);
                String trustStorePassword = sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_PASSWORD_CONFIG_PROP);
                setTrustStore(trustStorePath, trustStorePassword);
                enableSSL();
            } catch (IOException e) {
                throw new IllegalArgumentException("Unable to access SSL configuration.", e);
            }
        }

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
        m_hashScheme = scheme;
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
     * <p>Set the maximum number of outstanding requests that will be submitted before
     * blocking. Similar to the number of concurrent connections in a traditional synchronous
     * API. The default value is 3000.</p>
     *
     * @param maxOutstanding The maximum outstanding transactions before calls to
     * {@link Client#callProcedure(ProcedureCallback, String, Object...)} will block
     * or return false (depending on settings). Use 0 to reset to default.
     */
    public void setMaxOutstandingTxns(int maxOutstanding) {
        m_maxOutstandingTxns = maxOutstanding > 0 ? maxOutstanding : DEFAULT_MAX_OUTSTANDING_TRANSACTIONS;
    }

    /**
     * <p>Returns the maximum number of outstanding requests as settable by
     * {@link setMaxOutstandingTxns}.</p>
     *
     * @return max outstanding transaction count
     */
    public int getMaxOutstandingTxns() {
        return m_maxOutstandingTxns;
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
        if (m_nonblocking) {
            throw new IllegalStateException("Cannot set limit on TPS with non-blocking async");
        }
        m_maxTransactionsPerSecond = maxTxnsPerSecond;
    }

    /**
     * <p>The default behavior for queueing of asynchronous procedure invocations is to block until
     * it is possible to queue the invocation. If non-blocking async is configured, then an async
     * callProcedure will return immediately if it is not possible to queue the procedure
     * invocation due to backpressure. There is no effect on the synchronous variants of
     * callProcedure.</p>
     *
     * <p>Performance is sometimes improved if the callProcedure is permitted to block
     * for a short while, say a few hundred microseconds, to ride out a short blip in
     * backpressure. By default, this timeout is set to 500 microseconds.</p>
     *
     * <p>Not supported if rate-limiting has been configured by setMaxTransactionsPerSecond.</p>
     */
    public void setNonblockingAsync() {
        setNonblockingAsync(DEFAULT_NONBLOCKING_ASYNC_TIMEOUT_NANOS);
    }

    /**
     * <p>Variation on {@link setNonblockingAsync() setNonblockingAsync} with provision
     * for user-supplied blocking time limit.</p>
     *
     * @param blockingTimeout limit on blocking time, in nanoseconds; zero
     *        if immediate return is desired.
     */
    public void setNonblockingAsync(long blockingTimeout) {
        if (m_maxTransactionsPerSecond != Integer.MAX_VALUE) {
            throw new IllegalStateException("Cannot set non-blocking with limit on TPS");
        }
        m_nonblocking = true;
        m_asyncBlockingTimeout = Math.max(0, blockingTimeout);
    }

    /**
     * <p>Returns non-blocking async setting, as established by
     * a prior {@link setNonblockingAsync}.
     *
     * @return negative if non-blocking async is not enabled,
     *         otherwise the blocking timeout in nanoseconds.
     */
    public long getNonblockingAsync() {
        return m_nonblocking ? m_asyncBlockingTimeout : -1;
    }

    /**
     * <p>Set thresholds for backpressure reporting based on pending
     * request count and pending byte count.</p>
     *
     * <p>Reducing limit below current queue length will not cause
     * backpressure indication until next callProcedure.</p>
     *
     * @param reqLimit:  request limit, greater than 0 for actual
     *                   limit, 0 to reset to default
     * @param byteLimit: byte limit, greater than 0 for actual
     *                   limit, 0 to reset to default
     */
    public void setBackpressureQueueThresholds(int reqLimit, int byteLimit) {
        m_backpressureQueueRequestLimit = reqLimit > 0 ? reqLimit : DEFAULT_BACKPRESSURE_QUEUE_REQUEST_LIMIT;
        m_backpressureQueueByteLimit = byteLimit > 0 ? byteLimit : DEFAULT_BACKPRESSURE_QUEUE_BYTE_LIMIT;
    }

    /**
     * <p>Get thresholds for backpressure reporting, as set by
     * {@link setBackpressureQueueThresholds}.</p>
     *
     * @return integer array: reqLimit, byteLimit, in that order.
     */
    public int[] getBackpressureQueueThresholds() {
        return new int[] { m_backpressureQueueRequestLimit, m_backpressureQueueByteLimit };
    }

    /**
     * @deprecated client affinity is always {@code true}: transactions are always
     * routed to the correct master partition improving latency and throughput.
     *
     * (Deprecated in v11.0, 2021-03-23)
     *
     * @param on unused
     */
    @Deprecated
    public void setClientAffinity(boolean on) {
    }

    /**
     * <p>Configures the client so that it attempts to connect to all nodes in
     * the cluster as they are discovered, and will reconnect if those connections fail.</p>
     *
     * If the first connection attempt fails, then retries are made with
     * a fixed 10-second interval.
     *
     * <p>Defaults to false.</p>
     *
     * @param enabled Enable or disable the topology awareness feature.
     */
    public void setTopologyChangeAware(boolean enabled) {
        m_topologyChangeAware = enabled;
        m_reconnectOnConnectionLoss |= enabled;
    }

    /**
     * @deprecated no longer meaningful: reads are always sent
     * to the leader; sending to a replica would not have resulted
     * in better performance.
     *
     * (Deprecated in v11.0, 2021-04-16)
     *
     * @param on unused
     */
    @Deprecated
    public void setSendReadsToReplicasByDefault(boolean on) {
    }

    /**
     * <p>Attempts to automatically reconnect to a node after connection loss,
     * with retry until successful. The interval between retries is subject
     * to exponential backoff between user-supplied limits.
     * See {@link #setInitialConnectionRetryInterval}
     * and {@link #setMaxConnectionRetryInterval}.
     * </p>
     *
     * <p>Topology-change-aware clients automatically attempt to reconnect
     * failed connections, regardless of whether enabled by this method.
     * See {@link #setTopologyChangeAware}.</p>
     *
     * @param on Enable or disable the reconnection feature. Default is off.
     */
    public void setReconnectOnConnectionLoss(boolean on) {
        this.m_reconnectOnConnectionLoss = on;
    }

    /**
     * <p>Set the initial connection retry interval for automatic reconnection.
     * This is the delay between the first and second reconnect attempts.
     * Only has an effect if reconnection on connection loss is enabled.
     * </p>
     *
     * @param ms initial connection retry interval in milliseconds.
     */
    public void setInitialConnectionRetryInterval(long ms) {
        this.m_initialConnectionRetryIntervalMS = ms;
    }

    /**
     * <p>Set the maximum connection retry interval. After each reconnection
     * failure, the interval before the next retry is doubled, but will never
     * exceed this maximum.
     * Only has an effect if reconnection on connection loss is enabled.
     * </p>
     *
     * @param ms max connection retry interval in milliseconds.
     */
    public void setMaxConnectionRetryInterval(long ms) {
        this.m_maxConnectionRetryIntervalMS = ms;
    }

    /**
     * <p>Enable Kerberos authentication with the provided subject credentials</p>
     * @param subject Identity of the authenticated user.
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
       } catch (SecurityException | LoginException ex) {
           throw new IllegalArgumentException("Cannot determine client consumer's credentials", ex);
       }
    }

    /**
     * Enable or disable the rounding mode in the client.  This must match the
     * rounding mode set in the server, which is set using system properties.
     *
     * @param isEnabled True iff rounding is enabled.
     * @param mode The rounding mode, with values taken from java.math.RoundingMode.
     */
    public static void setRoundingConfig(boolean isEnabled, RoundingMode mode) {
        VoltDecimalHelper.setRoundingConfig(isEnabled, mode);
    }

    /**
     * Configure trust store
     *
     * @param pathToTrustStore file specification for the trust store
     * @param trustStorePassword trust store key file password
     */
    public void setTrustStore(String pathToTrustStore, String trustStorePassword) {
        File tsFD = new File(pathToTrustStore != null && !pathToTrustStore.trim().isEmpty() ? pathToTrustStore : "");
        if (!tsFD.exists() || !tsFD.isFile() || !tsFD.canRead()) {
            throw new IllegalArgumentException("Trust store " + pathToTrustStore + " is not a read accessible file");
        }
        m_sslConfig = new SSLConfiguration.SslConfig(null, null, pathToTrustStore, trustStorePassword);
    }

    /**
     * Configure trust store
     *
     * @param propFN property file name containing trust store properties:
     * <ul>
     * <li>{@code trustStore} trust store file specification
     * <li>{@code trustStorePassword} trust store password
     * </ul>
     */
    public void setTrustStoreConfigFromPropertyFile(String propFN) {
        File propFD = new File(propFN != null && !propFN.trim().isEmpty() ? propFN : "");
        if (!propFD.exists() || !propFD.isFile() || !propFD.canRead()) {
            throw new IllegalArgumentException("Properties file " + propFN + " is not a read accessible file");
        }
        Properties props = new Properties();
        try (FileReader fr = new FileReader(propFD)) {
            props.load(fr);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read properties file " + propFN, e);
        }
        String trustStore = props.getProperty(SSLConfiguration.TRUSTSTORE_CONFIG_PROP);
        String trustStorePassword = props.getProperty(SSLConfiguration.TRUSTSTORE_PASSWORD_CONFIG_PROP);

        m_sslConfig = new SSLConfiguration.SslConfig(null, null, trustStore, trustStorePassword);
    }

    /**
     * Configure ssl from the provided properties file. if file is not provided we configure without keystore and truststore manager.
     */
    public void enableSSL() {
        m_enableSSL = true;
        if (m_sslConfig == null) {
            m_sslConfig = new SSLConfiguration.SslConfig();
        }
    }

    public void setTrustStoreConfigFromDefault() {
        String trustStore = Constants.DEFAULT_TRUSTSTORE_RESOURCE;
        String trustStorePassword = Constants.DEFAULT_TRUSTSTORE_PASSWD;

        m_sslConfig = new SSLConfiguration.SslConfig(null, null, trustStore, trustStorePassword);
    }
}
