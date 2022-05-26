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

package org.voltcore.network;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

public class VoltPortFactory {

    public static VoltPort createVoltPort(final SocketChannel channel,
                                          final VoltNetwork network,
                                          final InputHandler handler,
                                          final InetSocketAddress remoteAddress,
                                          final NetworkDBBPool pool,
                                          final CipherExecutor cipherExecutor,
                                          final SSLEngine sslEngine
                                          ) {
        if (sslEngine == null) {
            return new VoltPort(
                    network,
                    handler,
                    (InetSocketAddress) channel.socket().getRemoteSocketAddress(),
                    pool);
        } else {
            return new TLSVoltPort(
                    network,
                    handler,
                    (InetSocketAddress) channel.socket().getRemoteSocketAddress(),
                    pool,
                    sslEngine,
                    cipherExecutor);
        }
    }
}
