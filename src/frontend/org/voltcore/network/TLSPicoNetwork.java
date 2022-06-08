/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

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

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.Pair;

/**
 * PicoNetwork with TLS enabled.
 */
public class TLSPicoNetwork extends PicoNetwork
{
    private TLSDecryptionAdapter m_tlsDecryptAdapter;
    private final SSLEngine m_sslEngine;
    private final CipherExecutor m_cipherExecutor;

    public TLSPicoNetwork(SocketChannel sc, SSLEngine sslEngine, CipherExecutor cipherExecutor, String hostDisplayName) {
        super(sc, hostDisplayName);
        m_sslEngine = sslEngine;
        m_cipherExecutor = cipherExecutor;
    }

    protected void startSetup() {
        m_tlsDecryptAdapter = new TLSDecryptionAdapter(this, m_ih, m_sslEngine, m_cipherExecutor);
        m_writeStream = new TLSPicoNIOWriteStream(this, m_sslEngine, m_cipherExecutor);
    }

    protected void dispatchReadStream() throws IOException {
        try {
            Pair<Integer, Integer> readInfo = m_tlsDecryptAdapter.handleInputStreamMessages(readyForRead(), readStream(), m_sc, m_pool);
            if (readInfo.getFirst() > 0) m_hadWork = true;
            m_messagesRead += readInfo.getSecond();
        } catch(EOFException eof) {
            m_interestOps &= ~SelectionKey.OP_READ;
            m_key.interestOps(m_interestOps);

            if (m_sc.socket().isConnected()) {
                try {
                    m_sc.socket().shutdownInput();
                } catch (SocketException e) {
                    //Safe to ignore to these
                }
            }

            m_shouldStop = true;
            safeStopping();

            /*
             * Allow the write queue to drain if possible
             */
            enableWriteSelection();
        }
    }

    protected void safeStopping() {
        // It is OK if this is called multiple times.
        // So we can skip the m_isStopping checks in parent class.
        m_tlsDecryptAdapter.die();
        super.safeStopping();
    }
}
