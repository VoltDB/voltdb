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

/**
 * The <code>Client2Notification</code> class defines interfaces
 * for 'notifications' from the client API to the application code.
 * <p>
 * The application can optionally register to receive notifications
 * via a {@link Client2Config} object, which is then used in creation
 * of a {@link Client2} client.
 * <p>
 * Each notification declares a single <code>accept</code>
 * method, and offers a functional interface.
 */
public class Client2Notification {

    private Client2Notification() {
    }

    /**
     * Notification of connection status change.
     * <p>
     * This is used for connect failure, connection up, and
     * connection down events.
     * <p>
     * The affected server is identified by host (name or
     * IP address) and TCP port number.
     *
     * @see Client2Config
     */
    @FunctionalInterface
    public interface ConnectionStatus {
        void accept(String host, int port);
    }

    /**
     * Notification of late response from server.
    * <p>
     * The late response is included in the notification.
     * The server sending the response is identified by host
     * (name or IP address) and TCP port number.
     *
     * @see Client2Config
     */
    @FunctionalInterface
    public interface LateResponse {
        void accept(ClientResponse resp, String host, int port);
    }

    /**
     * Notification of approaching limit on pending requests.
     * <p>
     * This handler is called with <code>slowdown</code> set to <code>true</code>
     * when the number of requests pending in the client has risen to the
     * configured warning level or greater. It will subsequently be called
     * with <code>slowdown</code> set to <code>false</code> when the pending
     * count has fallen to the configured resume level or lower.
     *
     * @see Client2Config
     */
    @FunctionalInterface
    public interface RequestBackpressure {
        void accept(boolean slowdown);
    }

    /**
     * Error logging interception.
     * <p>
     * The <code>Client2</code> implementation may print messages on
     * its standard error when certain unexpected situations arise.
     * The application can choose to handle the message instead,
     * perhaps writing to its own log.
     * <p>
     * Users are cautioned against writing code that attempts to
     * interpret the text of any log message. Wording and format
     * are subject to change.
     *
     * @see Client2Config
     */
    @FunctionalInterface
    public interface ErrorLog {
        void accept(String message);
    }
}
