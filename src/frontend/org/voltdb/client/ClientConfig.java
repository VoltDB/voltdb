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

    static final int HIGHEST_PRIORITY = Priority.HIGHEST_PRIORITY;
    static final int LOWEST_PRIORITY = Priority.LOWEST_PRIORITY;

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
    int m_requestPriority = -1;

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
     * Configuration for a client with no authentication credentials.
     */
    public ClientConfig() {
        this("", "", true, (ClientStatusListenerExt) null, ClientAuthScheme.HASH_SHA256);
    }


    /**
     * Configuration for a client that specifies cleartext authentication credentials.
     * The username and password can be null or the empty string.
     *
     * @param username Cleartext username.
     * @param password Cleartext password.
     */
    public ClientConfig(String username, String password) {
        this(username, password, true, (ClientStatusListenerExt) null, ClientAuthScheme.HASH_SHA256);
    }

    /**
     * Configuration for a client that specifies cleartext authentication credentials.
     * The username and password can be null or the empty string.
     *
     * @param username Cleartext username.
     * @param password Cleartext password.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     */
    public ClientConfig(String username, String password, ClientStatusListenerExt listener) {
        this(username, password, true, listener, ClientAuthScheme.HASH_SHA256);
    }

    /**
     * Configuration for a client that specifies cleartext authentication credentials.
     * The username and password can be null or the empty string.
     *
     * @param username Cleartext username.
     * @param password Cleartext password.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     * @param scheme Client password hash scheme.
     */
    public ClientConfig(String username, String password, ClientStatusListenerExt listener, ClientAuthScheme scheme) {
        this(username, password, true, listener, scheme);
    }

    /**
     * Configuration for a client that specifies authentication credentials, the password
     * being optionally hashed prior to the call. The username and password can be null
     * or the empty string.
     *
     * @param username Cleartext username.
     * @param password A cleartext or hashed password.
     * @param cleartext Whether the password is hashed.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     */
    public ClientConfig(String username, String password, boolean cleartext, ClientStatusListenerExt listener) {
        this(username, password, cleartext, listener, ClientAuthScheme.HASH_SHA256);
    }

    /**
     * Configuration for a client that specifies an already authenticated {@link Subject}.
     *
     * @param subject an authenticated {@link Subject}.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
     */
    public ClientConfig(Subject subject, ClientStatusListenerExt listener) {
        this(getUserNameFromSubject(subject), "", true, listener, ClientAuthScheme.HASH_SHA256);
        m_subject = subject;
    }

    /**
     * Configuration for a client that specifies authentication credentials, the password
     * being optionally hashed prior to the call. The username and password can be null
     * or the empty string.
     *
     * @param username Cleartext username.
     * @param password A cleartext or hashed password.
     * @param cleartext Whether the password is hashed.
     * @param listener {@link ClientStatusListenerExt} implementation to receive callbacks.
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
     * Set the timeout for procedure calls. The default timeout is 2 minutes. A zero
     * or negative value indicates no timeout.
     * <p>
     * If the timeout expires before the procedure call could even be queued for
     * transmission, because of backpressure:
     * <ol>
     * <li> Synchronous procedures will throw a {@link ProcCallException}. The response
     * status will be {@link ClientResponse#GRACEFUL_FAILURE}.
     * <li> Asynchronous procedures will return <code>false</code>.
     * </ol>
     * <p>
     * If the timeout expires after the call is queued for transmission:
     * <ol>
     * <li> Synchronous procedures will throw a {@link ProcCallException}. The response
     * status will be {@link ClientResponse#CONNECTION_TIMEOUT}.
     * <li> Asynchronous procedures will invoke a callback. The response
     * status will be {@link ClientResponse#CONNECTION_TIMEOUT}.
     * </ol>
     * <p>
     * Note that while specified in mSec, this timeout is only accurate to within a second or so.
     *
     * @param ms Timeout value in milliseconds.
     */
    public void setProcedureCallTimeout(long ms) {
        m_procedureCallTimeoutNanos = ms > 0 ? TimeUnit.MILLISECONDS.toNanos(ms) : Long.MAX_VALUE;
    }

    /**
     * Set the timeout for reading from a connection. If a connection receives no responses
     * for the specified time interval, either from procedure calls or pings, then the connection
     * will be assumed dead. Lost-connection callbacks will be executed, and in-progress
     * requests will be completed.
     * <p>
     * The default timeout is 2 minutes. A zero or negative value indicates no timeout.
     * <p>
     * Note that while specified in mSec, this timeout is only accurate to within a second or so.
     *
     * @param ms Timeout value in milliseconds.
     */
    public void setConnectionResponseTimeout(long ms) {
        m_connectionResponseTimeoutMS = ms > 0 ? ms : Long.MAX_VALUE;
    }

    /**
     * Specifies that the client wants additional network threads.
     * <p>
     * By default a single network thread is created to do IO and invoke callbacks.
     * When <code>heavyweight</code> is set to true, additional threads are
     * created. This results in multiple connections to each server.
     * <p>
     * The number of network threads depends on the number of processors available.
     * Specifically, there will be <code>Runtime.getRuntime().availableProcessors() / 2</code> threads.
     *
     * @param heavyweight Whether to create additional threads for high IO or
     * high processing workloads.
     */
    public void setHeavyweight(boolean heavyweight) {
        m_heavyweight = heavyweight;
    }

    /**
     * Set the maximum number of outstanding requests that will be submitted before
     * blocking. The default value is 3000.
     *
     * @param maxOutstanding The maximum outstanding transactions before calls to
     * {@link Client#callProcedure(ProcedureCallback, String, Object...)} will block
     * or return false (depending on settings). Use 0 to reset to default.
     */
    public void setMaxOutstandingTxns(int maxOutstanding) {
        m_maxOutstandingTxns = maxOutstanding > 0 ? maxOutstanding : DEFAULT_MAX_OUTSTANDING_TRANSACTIONS;
    }

    /**
     * Returns the maximum number of outstanding requests as set by
     * {@link #setMaxOutstandingTxns}.
     *
     * @return max outstanding transaction count
     */
    public int getMaxOutstandingTxns() {
        return m_maxOutstandingTxns;
    }

    /**
     * Set a limit on the number of transactions that can be executed per second.
     * This operates by stalling the client so as to keep the rate from exceeding
     * the target limit.
     * <p>
     * Usage notes:
     * <p>
     * The limit should be less than 1,073,741,823 (half of <code>Integer.MAX_VALUE</code>);
     * larger values disable rate-limiting. The default is <code>Integer.MAX_VALUE</code>.
     * <p>
     * Transactions will not be timed out while they are stalled waiting for
     * the average rate to permit transmission.
     * <p>
     * You cannot use <code>setMaxTransactionsPerSecond</code> when
     * <code>setNonblockingAsync</code> is in effect, since rate-limiting
     * potentially needs to block.
     *
     * @param maxTxnsPerSecond Requested limit in transaction per second.
     */
    public void setMaxTransactionsPerSecond(int maxTxnsPerSecond) {
        if (m_nonblocking) {
            throw new IllegalStateException("Cannot set limit on TPS with non-blocking async");
        }
        m_maxTransactionsPerSecond = Math.max(1, maxTxnsPerSecond);
    }

    /**
     * Sets nonblocking mode for asynchronous procedure invocations.
     * <p>
     * The default behavior for queueing of asynchronous procedure invocations is to block until
     * it is possible to queue the invocation. If non-blocking async is configured, then an async
     * callProcedure will return immediately if it is not possible to queue the procedure
     * invocation due to backpressure. There is no effect on the synchronous variants of
     * callProcedure.
     * <p>
     * Performance is sometimes improved if the callProcedure is permitted to block
     * for a short while, say a few hundred microseconds, to ride out a short blip in
     * backpressure. By default, this timeout is set to 500 microseconds.
     * <p>
     * Not supported if rate-limiting has been configured by setMaxTransactionsPerSecond.
     */
    public void setNonblockingAsync() {
        setNonblockingAsync(DEFAULT_NONBLOCKING_ASYNC_TIMEOUT_NANOS);
    }

    /**
     * Sets nonblocking mode for asynchronous procedure invocations, with provision
     * for user-supplied blocking time limit. See also {@link #setNonblockingAsync()}.
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
     * Returns non-blocking async setting, as established by
     * a prior {@link #setNonblockingAsync}.
     *
     * @return negative if non-blocking async is not enabled,
     *         otherwise the blocking timeout in nanoseconds.
     */
    public long getNonblockingAsync() {
        return m_nonblocking ? m_asyncBlockingTimeout : -1;
    }

    /**
     * Set thresholds for backpressure reporting based on pending
     * request count and pending byte count.
     * <p>
     * Reducing limit below current queue length will not cause
     * backpressure indication until next callProcedure.
     *
     * @param reqLimit   request limit, greater than 0 for actual
     *                   limit, 0 to reset to default
     * @param byteLimit  byte limit, greater than 0 for actual
     *                   limit, 0 to reset to default
     */
    public void setBackpressureQueueThresholds(int reqLimit, int byteLimit) {
        m_backpressureQueueRequestLimit = reqLimit > 0 ? reqLimit : DEFAULT_BACKPRESSURE_QUEUE_REQUEST_LIMIT;
        m_backpressureQueueByteLimit = byteLimit > 0 ? byteLimit : DEFAULT_BACKPRESSURE_QUEUE_BYTE_LIMIT;
    }

    /**
     * Get thresholds for backpressure reporting, as set by
     * {@link #setBackpressureQueueThresholds}.
     *
     * @return integer array: reqLimit, byteLimit, in that order.
     */
    public int[] getBackpressureQueueThresholds() {
        return new int[] { m_backpressureQueueRequestLimit, m_backpressureQueueByteLimit };
    }

    /**
     * Configures the client so that it attempts to connect to all nodes in
     * the cluster as they are discovered, and will reconnect if those connections fail.
     * Defaults to false.
     * <p>
     * The interval between retries is subject to exponential backoff
     * between user-supplied limits.
     * See {@link #setInitialConnectionRetryInterval}
     * and {@link #setMaxConnectionRetryInterval}.
     *
     * @param enabled Enable or disable the topology awareness feature.
     */
    public void setTopologyChangeAware(boolean enabled) {
        m_topologyChangeAware = enabled;
    }

    /**
     * Attempts to automatically reconnect to a node after connection loss,
     * with retry until successful. The interval between retries is subject
     * to exponential backoff between user-supplied limits.
     * See {@link #setInitialConnectionRetryInterval}
     * and {@link #setMaxConnectionRetryInterval}.
     * <p>
     * @deprecated prefer using a topology-change-aware client, which
     * provides more automatic handling of connections to VoltDB cluster
     * nodes.
     * <p>
     * (Deprecated in v11.1, 2021-07-30)
      *
     * @param on Enable or disable the reconnection feature. Default is off.
     * @see #setTopologyChangeAware(boolean)
     */
    @Deprecated
    public void setReconnectOnConnectionLoss(boolean on) {
        this.m_reconnectOnConnectionLoss = on;
    }

    /**
     * Set the initial connection retry interval for automatic reconnection.
     * This is the delay between the first and second reconnect attempts.
     *
     * @param ms initial connection retry interval in milliseconds.
     */
    public void setInitialConnectionRetryInterval(long ms) {
        this.m_initialConnectionRetryIntervalMS = ms;
    }

    /**
     * Set the maximum connection retry interval. After each reconnection
     * failure, the interval before the next retry is doubled, but will never
     * exceed this maximum.
     *
     * @param ms max connection retry interval in milliseconds.
     */
    public void setMaxConnectionRetryInterval(long ms) {
        this.m_maxConnectionRetryIntervalMS = ms;
    }

    /**
     * Enable Kerberos authentication with the provided subject credentials.
     *
     * @param subject Identity of the authenticated user.
     */
    public void enableKerberosAuthentication(final Subject subject) {
        m_subject = subject;
    }

    /**
     * Enable Kerberos authentication, using the provided JAAS login context
     * entry key to get the authentication credentials held by the caller.
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
     * Configure trust store with specified path and password.
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
     * Configure trust store via a property file.
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
     * Configure trust store from default file with default password.
     */
    public void setTrustStoreConfigFromDefault() {
        String trustStore = Constants.DEFAULT_TRUSTSTORE_RESOURCE;
        String trustStorePassword = Constants.DEFAULT_TRUSTSTORE_PASSWD;

        m_sslConfig = new SSLConfiguration.SslConfig(null, null, trustStore, trustStorePassword);
    }

    /**
     * Enables SSL with previously-configured trust store. If no call
     * has been made to configure a trust store, we process without one.
     */
    public void enableSSL() {
        m_enableSSL = true;
        if (m_sslConfig == null) {
            m_sslConfig = new SSLConfiguration.SslConfig();
        }
    }

   /**
     * Sets the request priority for all procedure calls from
     * a <code>Client</code> created using this configuration.
     * <p>
     * This will be used only if priorities are enabled by the
     * VoltDB cluster, and then it affects the order in which
     * requests are dispatched.
     * <p>
     * The valid priority range is from 1 to 8, inclusive.
     * Higher priorities have lower numerical values.
     *
     * @param prio priority
     */
    public void setRequestPriority(int prio) {
        if (prio < HIGHEST_PRIORITY || prio > LOWEST_PRIORITY) {
            String err = String.format("Invalid request priority %d; range is %d to %d",
                                       prio, HIGHEST_PRIORITY, LOWEST_PRIORITY);
            throw new IllegalArgumentException(err);
        }
        m_requestPriority = prio;
    }
}
