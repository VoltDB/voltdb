/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import java.math.RoundingMode;
import java.security.Principal;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltcore.utils.ssl.SSLConfiguration.SslConfig;
import org.voltdb.common.Constants;
import org.voltdb.types.VoltDecimalHelper;

/**
 * Container for configuration settings for a <code>Client2</code>
 * VoltDB client. A <code>Client2Config</code> object is passed
 * as input to <code>ClientFactory.createClient()</code>, which
 * generates a <code>Client2</code> with the desired characteristics.
 *
 * @see Client2
 * @see Client2Notification
 * @see ClientFactory
 */
public class Client2Config {

    // Defaults are defined in the Client2Impl source as the single point of truth
    // but are exposed to the public through the Client2Config class.

    public static final long DEFAULT_CONNECTION_SETUP_TIMEOUT = Client2Impl.DEFAULT_CONNECTION_SETUP_TIMEOUT;
    public static final long DEFAULT_CONNECTION_RESPONSE_TIMEOUT = Client2Impl.DEFAULT_CONNECTION_RESPONSE_TIMEOUT;
    public static final long DEFAULT_PROCEDURE_TIMEOUT = Client2Impl.DEFAULT_PROCEDURE_TIMEOUT;
    public static final int DEFAULT_CLIENT_REQUEST_HARD_LIMIT = Client2Impl.DEFAULT_REQUEST_HARD_LIMIT;
    public static final int DEFAULT_CLIENT_REQUEST_WARNING_LEVEL = Client2Impl.DEFAULT_REQUEST_WARNING_LEVEL;
    public static final int DEFAULT_CLIENT_REQUEST_RESUME_LEVEL = Client2Impl.DEFAULT_REQUEST_RESUME_LEVEL;
    public static final int DEFAULT_OUTSTANDING_TRANSACTION_LIMIT = Client2Impl.DEFAULT_TXN_OUT_LIMIT;
    public static final int DEFAULT_NETWORK_BACKPRESSURE_LEVEL = Client2Impl.DEFAULT_BACKPRESSURE_QUEUE_LIMIT;
    public static final long DEFAULT_RECONNECT_DELAY = Client2Impl.DEFAULT_RECONNECT_DELAY;
    public static final long DEFAULT_RECONNECT_RETRY_DELAY = Client2Impl.DEFAULT_RECONNECT_RETRY_DELAY;
    public static final int DEFAULT_REQUEST_PRIORITY = Client2Impl.DEFAULT_REQUEST_PRIORITY;
    public static final int DEFAULT_RESPONSE_THREADS = Client2Impl.DEFAULT_RESPONSE_THREADS;

    // Similarly for the valid priority range (from ProcedureInvocation)

    public static final int HIGHEST_PRIORITY = Priority.HIGHEST_PRIORITY;
    public static final int LOWEST_PRIORITY = Priority.LOWEST_PRIORITY;

    // All these have package access for use by Client2Impl
    // All times are in nanoseconds

    String username = "";
    String password = "";
    boolean cleartext = true;
    ClientAuthScheme hashScheme = ClientAuthScheme.HASH_SHA256;
    Subject subject = null;
    SslConfig sslConfig = null;
    boolean enableSsl = false;
    long connectionSetupTimeout = DEFAULT_CONNECTION_SETUP_TIMEOUT;
    long connectionResponseTimeout = DEFAULT_CONNECTION_RESPONSE_TIMEOUT;
    long procedureCallTimeout = DEFAULT_PROCEDURE_TIMEOUT;
    int requestHardLimit = DEFAULT_CLIENT_REQUEST_HARD_LIMIT;
    int requestWarningLevel = DEFAULT_CLIENT_REQUEST_WARNING_LEVEL;
    int requestResumeLevel = DEFAULT_CLIENT_REQUEST_RESUME_LEVEL;
    int outstandingTxnLimit = DEFAULT_OUTSTANDING_TRANSACTION_LIMIT;
    int txnPerSecRateLimit = 0;
    int networkBackpressureLevel = DEFAULT_NETWORK_BACKPRESSURE_LEVEL;
    long reconnectDelay = DEFAULT_RECONNECT_DELAY;
    long reconnectRetryDelay = DEFAULT_RECONNECT_RETRY_DELAY;
    boolean disableConnectionMgmt = false;
    int requestPriority = DEFAULT_REQUEST_PRIORITY;
    Client2Notification.ConnectionStatus notificationConnectFailure = null;
    Client2Notification.ConnectionStatus notificationConnectionUp = null;
    Client2Notification.ConnectionStatus notificationConnectionDown = null;
    Client2Notification.LateResponse notificationLateResponse = null;
    Client2Notification.RequestBackpressure notificationRequestBackpressure = null;
    Client2Notification.ErrorLog notificationErrorLog = null;
    int responseThreadCount = DEFAULT_RESPONSE_THREADS;
    ExecutorService responseExecutorService = null;
    boolean stopResponseServiceOnClose = true;

    /**
     * Constructs a configuration with default values.
     * Various methods can be used to override the defaults. You'll likely
     * need authentication information (username and password) at a minimum.
     * <p>
     * All configuration methods in {@code Client2Config} return {@code this},
     * so calls can be chained.
     */
    public Client2Config() {
    }

    /**
     * Set username for connections to VoltDB.
     *
     * @param username username
     * @return this
     */
    public Client2Config username(String username) {
        this.subject = null;
        this.username = notNull(username);
        return this;
    }

    /**
     * Set cleartext password for connections to VoltDB.
     *
     * @param password password
     * @return this
     */
     public Client2Config password(String password) {
        this.password = notNull(password);
        this.cleartext = true;
        this.hashScheme = ClientAuthScheme.HASH_SHA256;
        return this;
    }

    /**
     * Set hashed password for connections to VoltDB.
     *
     * @param password hashed password
     * @return this
     */
    public Client2Config hashedPassword(String password) {
        this.password = notNull(password);
        this.cleartext = false;
        this.hashScheme = ClientAuthScheme.HASH_SHA256;
        return this;
    }

    /**
     * Set hashed password for connections to VoltDB. The password was
     * hashed using a specified hash scheme.
     *
     * @param password  hashed password
     * @param hashScheme hash scheme used to hash the password
     * @return this
     */
    public Client2Config hashedPassword(String password, ClientAuthScheme hashScheme) {
        this.password = notNull(password);
        this.cleartext = false;
        this.hashScheme = hashScheme;
        return this;
    }

    /**
     * Sets the authenticated subject to be used for connections
     * to VoltDB. This can be used as an alternative to a username
     * and password combination.
     *
     * @param subject subject (<code>javax.security.auth.Subject</code>)
     * @return this
      */
    public Client2Config authenticatedSubject(Subject subject) {
        this.subject = subject;
        this.username = getUserNameFromSubject(subject);
        this.password = "";
        this.cleartext = true;
        this.hashScheme = ClientAuthScheme.HASH_SHA256;
        return this;
    }

    /**
     * Creates a new login context and authenticates the user, then
     * adds the authenticated subject to this client configuration.
     * <p>
     * See package <code>javax.security.auth.login</code> for details.
     *
     * @param name as used as key to locate the login context entry
     * @return this
     * @see #authenticatedSubject
      */
    public Client2Config loginContext(String name) {
       try {
           LoginContext lc = new LoginContext(name);
           lc.login();
           return authenticatedSubject(lc.getSubject());
       }
       catch (SecurityException | LoginException ex) {
           throw new IllegalArgumentException("Cannot determine client consumer's credentials", ex);
       }
    }

    /**
     * Sets the timeout for connection setup, including authentication
     * to the server. A zero or negative value means there is no limit.
     *
     * @param timeout the timeout interval
     * @param unit the units in which the timeout was expressed
     * @return this
     */
    public Client2Config connectionSetupTimeout(long timeout, TimeUnit unit) {
        connectionSetupTimeout = timeout > 0 ? unit.toNanos(timeout) : Long.MAX_VALUE;
        return this;
    }

    /**
     * Sets the connection response timeout.
     * A zero or negative value means there is no limit.
     * <p>
     * If no response has been received on a connection within the
     * specified time, the connection will be considered lost. A 'ping'
     * request will be periodically executed on an otherwise-idle
     * connection in order to ensure keep the connection alive.
     *
     * @param timeout the timeout interval
     * @param unit the units in which the timeout was expressed
     * @return this
     */
    public Client2Config connectionResponseTimeout(long timeout, TimeUnit unit) {
        connectionResponseTimeout = timeout > 0 ? unit.toNanos(timeout) : Long.MAX_VALUE;
        return this;
    }

    /**
     * Sets the timeout for procedure calls.
     * A zero or negative value means there is no limit.
     * <p>
     * If a call has received no response from VoltDB in the specified
     * time, it will be completed with a timeout error.
     *
     * @param timeout the timeout interval
     * @param unit the units in which the timeout was expressed
     * @return this
     */
    public Client2Config procedureCallTimeout(long timeout, TimeUnit unit) {
        procedureCallTimeout = timeout > 0 ? unit.toNanos(timeout) : Long.MAX_VALUE;
        return this;
    }

    /**
     * Sets the limit on the number of requests that can be pending in
     * a client at any one time. Requests are pending from the time
     * that {@code callProcedure()} is called, up to when the response
     * has been delivered to the application or the call has failed.
     * <p>
     * Calls which exceed this limit will be rejected by the API.
     * This is an absolute limit on resource consumption. It should
     * be set somewhat larger than the warning level established by
     * a call to {@link #clientRequestBackpressureLevel} in order to
     * give the application headroom to react to the warning.
     *
     * @param limit the desired request limit
     * @return this
     * @see #clientRequestBackpressureLevel
     */
    public Client2Config clientRequestLimit(int limit) {
        requestHardLimit = Math.max(1, limit);
        return this;
    }

    /**
     * Sets levels for controlling backpressure notifications.
     * <p>
     * When the pending count reaches the warning level or greater,
     * the application is warned to slow down: backpressure starts.
     * When the pending count subsequently drops to the resume level
     * (or lower), the application is informed that it no longer needs
     * to slow down: backpressure ends.
     * <p>
     * For predictable operation, the implementation will adjust
     * values such that:
     * <ul>
     * <li>resume level &le; warning level
     * <li>warning level &le; hard limit
     * </ul>
     * @param warning the level at which the application is warned to slow down
     * @param resume the level at which application slow-down can end
     * @return this
     * @see #clientRequestLimit
     * @see #requestBackpressureHandler
     */
    public Client2Config clientRequestBackpressureLevel(int warning, int resume) {
        requestWarningLevel = Math.max(1, warning);
        requestResumeLevel = Math.max(requestWarningLevel, resume);
        return this;
    }

    /**
     * Sets the limit on the number of transactions that can be
     * outstanding at the VoltDB server at any one time. Requests are
     * outstanding from the time they are handed over to the
     * networking code, up to when the response is received.
     * <p>
     * Requests will be queued in the Client2 API after this
     * limit is reached, to avoid overwhelming the VoltDB cluster.
     *
     * @param limit the desired request limit
     * @return this
     */
    public Client2Config outstandingTransactionLimit(int limit) {
        outstandingTxnLimit = Math.max(1, limit);
        return this;
    }

    /**
     * Limits the rate at which transactions can be sent to the
     * VoltDB server. This is intended for performance modelling
     * and is not recommended for production use.
     * <p>
     * If the application is making requests to {@code callProcedure}
     * in excess of the target rate, then the client queue length
     * will likely grow to the point at which request backpressure
     * is signaled. Queued requests may be timed out in the usual manner.
     * <p>
     * Any throttling of sends imposed by setting this configuration
     * parameter is applied before, and independently of, that implied
     * by {@code outstandingTransactionLimit}.
     * <p>
     * By default there is no rate limiting.
     *
     * @param tpsLimit target rate limit in transactions per second
     * @return this
     */
    public Client2Config transactionRateLimit(int tpsLimit) {
        txnPerSecRateLimit = Math.max(1, tpsLimit);
        return this;
    }

    /**
     * This setting controls the maximum number of requests that
     * the Client2 API can have queued at the VoltDB network
     * layer for a single connection. At that point, the network
     * interface will backpressure the API; this backpressure
     * is not directly visible to the application using the API,
     * since the API has internal queueing.
     * <p>
     * Transmission order is fixed before a request is queued to
     * the network layer. Thus, setting this value too high can
     * adversely affect the ability of high-priority requests
     * to overtake lower-priority requests.
     * <p>
     * VoltDB recommends changing the default only after careful
     * measurement in a realistic scenario.
     *
     * @param level the desired backpressure level
     * @return this
     */
    public Client2Config networkBackpressureLevel(int level) {
        networkBackpressureLevel = Math.max(1, level);
        return this;
    }

    /**
     * Sets delay times for attempts to reconnect failed connections.
     * There are two settings: one for the time to wait before the first
     * reconnect attempt, and the other for the time to wait between
     * retries, if the first attempt fails.
     * <p>
     * In typical use, the initial delay will be fairly small, in order
     * to recover from a momentary glitch; the retryDelay will be somewhat
     * longer.
     * <p>
     * VoltDB recommends changing delay times from the defaults only when
     * default settings have been shown to be problematic in your specific
     * use-case.
     *
     * @param initialDelay delay before first reconnect attempt
     * @param retryDelay delay between subsequent retries
     * @param unit time units used for both delay values
     * @return this
     */
    public Client2Config reconnectDelay(long initialDelay, long retryDelay, TimeUnit unit) {
        reconnectDelay = unit.toNanos(Math.max(1, initialDelay));
        reconnectRetryDelay = unit.toNanos(Math.max(1, retryDelay));
        return this;
    }

    /**
     * Disable automatic connection management.
     * <p>
     * Normally, after the application connects to at least one VoltDB
     * server using <code>connectSync</code> or <code>connectAsync</code>,
     * the API will manage connections to the VoltDB cluster. Connections
     * will be created to available cluster nodes as they are discovered,
     * and failed connections will be reconnected when possible.
     * <p>
     * The application can disable this, and assume all responsibility
     * for making initial connections, and for recovery from loss of
     * connection.
     *
     * @return this
     */
    public Client2Config disableConnectionMgmt() {
        disableConnectionMgmt = false;
        return this;
    }

    /**
     * Sets the default priority for procedure calls from
     * a <code>Client2</code> created using this configuration.
     * <p>
     * The value given here can be overridden by individual
     * procedure calls.
     * <p>
     * The valid priority range is from {@link #HIGHEST_PRIORITY}
     * to {@link #LOWEST_PRIORITY}, inclusive. Higher priorities
     * have lower numerical values.
     *
     * @param prio priority
     * @return this
     */
    public Client2Config requestPriority(int prio) {
        requestPriority = checkRequestPriority(prio);
        return this;
    }

    /**
     * Configures trust store for TLS/SSL from a property file.
     *
     * @param path property file containing trust store properties:
     * <ul>
     * <li>{@code trustStore} trust store file specification
     * <li>{@code trustStorePassword} trust store password
     * </ul>
     * @return this
     * @see #enableSSL
     */
    public Client2Config trustStoreFromPropertyFile(String path) {
        File pf = new File(path);
        if (!(pf.canRead() && pf.isFile())) {
            throw new IllegalArgumentException(String.format("Properties file '%s' cannot be read", path));
        }
        Properties props = new Properties();
        try (FileReader fr = new FileReader(pf)) {
            props.load(fr);
        }
        catch (IOException ex) {
            throw new IllegalArgumentException(String.format("Properties file '%s' cannot be read", path), ex);
        }
        String store = props.getProperty(SSLConfiguration.TRUSTSTORE_CONFIG_PROP);
        String pswd = props.getProperty(SSLConfiguration.TRUSTSTORE_PASSWORD_CONFIG_PROP);
        return trustStore(store, pswd);
    }

    /**
     * Configures trust store for TLS/SSL using a default path for the
     * trust store file, based on where Java is installed, and a default
     * password.
     *
     * @return this
     * @see #enableSSL
     */
     public Client2Config defaultTrustStore() {
        String store = Constants.DEFAULT_TRUSTSTORE_RESOURCE;
        String pswd = Constants.DEFAULT_TRUSTSTORE_PASSWD;
        return trustStore(store, pswd);
    }

    /**
     * Configures trust store for TLS/SSL using a specified name for the
     * trust store and a specified password.
     *
     * @param path trust store file specification
     * @param password trust store password
     * @return this
     * @see #enableSSL
     */
    public Client2Config trustStore(String path, String password) {
        File ts = new File(path);
        if (!(ts.canRead() && ts.isFile())) {
            throw new IllegalArgumentException(String.format("Trust store '%s' cannot be read", path));
        }
        sslConfig = new SSLConfiguration.SslConfig(null, null, path, password);
        return this;
    }

    /**
     * Enables TLS/SSL for server connections. If a trust store is needed to
     * authenticate the server, it must be set in the configuration before
     * calling {@code enableSSL}.
     *
     * @return this
     */
    public Client2Config enableSSL() {
        enableSsl = true;
        if (sslConfig == null) {
            sslConfig = new SSLConfiguration.SslConfig();
        }
        return this;
    }

    /**
     * Registers a handler for connection-establishment failures. This
     * will be called when an attempt to connect to a VoltDB server fails.
     *
     * @param handler a {@link Client2Notification.ConnectionStatus}
     * @return this
     * @see Client2Notification
     */
    public Client2Config connectFailureHandler(Client2Notification.ConnectionStatus handler) {
        notificationConnectFailure = handler;
        return this;
    }

    /**
     * Registers a handler for connection-up events. This will be called when an
     * attempt to connect to a VoltDB server has successfully completed, including
     * any necessary authentication.
     *
     * @param handler a {@link Client2Notification.ConnectionStatus}
     * @return this
     * @see Client2Notification
     */
    public Client2Config connectionUpHandler(Client2Notification.ConnectionStatus handler) {
        notificationConnectionUp = handler;
        return this;
    }

    /**
     * Registers a handler for connection-down events. This will be called when a
     * previously-up connection is lost for any reason.
     *
     * @param handler a {@link Client2Notification.ConnectionStatus}
     * @return this
     * @see Client2Notification
     */
    public Client2Config connectionDownHandler(Client2Notification.ConnectionStatus handler) {
        notificationConnectionDown = handler;
        return this;
    }

    /**
     * Registers a handler for late server responses.
     *
     * @param handler a {@link Client2Notification.LateResponse}
     * @return this
     * @see Client2Notification
     */
    public Client2Config lateResponseHandler(Client2Notification.LateResponse handler) {
        notificationLateResponse = handler;
        return this;
    }

    /**
     * Registers a handler to be notified about changes in request backpressure.
     * <p>
     * Backpressure trigger levels are configurable via {@link #clientRequestBackpressureLevel(int, int)}.
     *
     * @param handler a {@link Client2Notification.RequestBackpressure}
     * @return this
     * @see Client2Notification
     */
    public Client2Config requestBackpressureHandler(Client2Notification.RequestBackpressure handler) {
        notificationRequestBackpressure = handler;
        return this;
    }

    /**
     * Registers an error-log handler.
     * <p>
     * The <code>Client2</code> implementation may print messages on
     * its standard error when certain unexpected situations arise.
     * The application can choose to handle the message instead,
     * perhaps writing to its own log.
     * <p>
     * Applications are cautioned against attempting to interpret
     * the text of a log message. The wording is subject to change
     * without notice.
     *
     * @param handler a {@link Client2Notification.ErrorLog}
     * @return this
     * @see Client2Notification
      */
    public Client2Config  errorLogHandler(Client2Notification.ErrorLog handler) {
        notificationErrorLog = handler;
        return this;
    }

    /**
     * Sets the fixed number of 'response' threads to be available
     * in the pool of such threads. Response messages from the
     * VoltDB cluster are handled by these threads.
     * <p>
     * This setting is only meaningful for the internal client
     * response <code>ExecutorService</code>. If you provide a
     * custom service by {@link #responseExecutorService} then
     * the count specified here is not used.
     *
     * @param count number of response threads to create
     * @return this
     */
    public Client2Config responseThreadCount(int count) {
        responseThreadCount = Math.max(1, count);
        return this;
    }

    /**
     * Provides an <code>ExecutorService</code> with which to
     * complete <code>callProcedure</code> requests. This replaces
     * the default provided by the <code>Client2</code> interface.
     * <p>
     * When a response message is received from the VoltDB cluster, it
     * is handed over to a thread from this service for processing.
     * The eventual setting of the call's <code>CompletableFuture</code>
     * into the 'completed' state will occur on this thread.
     * <p>
     * The caller chooses whether or not {@link Client2#close} will
     * automatically execute a <code>shutdown</code> on the executor
     * service.
     *
     * @param execService an <code>ExecutorService</code>
     * @param stopOnClose true to stop the service on client close
     * @return this
     */
    public Client2Config responseExecutorService(ExecutorService execService, boolean stopOnClose) {
        responseExecutorService = execService;
        stopResponseServiceOnClose = stopOnClose;
        return this;
    }

    /**
     * Enables or disables the rounding mode in the client.  This must match the
     * rounding mode set in the server, which is set using system properties.
     *
     * @param enable true iff rounding is enabled
     * @param mode the rounding mode (<code>java.math.RoundingMode</code>)
     * @return this
     */
    public Client2Config roundingMode(boolean enable, RoundingMode mode) {
        VoltDecimalHelper.setRoundingConfig(enable, mode);
        return this;
    }

    // Implementation assistance

    private static String notNull(String str) {
        return str != null ? str : "";
    }

    private static String getUserNameFromSubject(Subject subject) {
        if (subject == null) {
            throw new IllegalArgumentException("Null subject");
        }
        Iterator<Principal> piter = subject.getPrincipals().iterator();
        if (!piter.hasNext()) {
            throw new IllegalArgumentException("Subject does not contain principals");
        }
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

    static int checkRequestPriority(int prio) {
        if (prio < HIGHEST_PRIORITY || prio > LOWEST_PRIORITY) {
            String err = String.format("Invalid request priority %d; range is %d to %d",
                                       prio, HIGHEST_PRIORITY, LOWEST_PRIORITY);
            throw new IllegalArgumentException(err);
        }
        return prio;
    }
}
