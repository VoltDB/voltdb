/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.messaging;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.voltdb.utils.DBBPool;

import org.apache.zookeeper_voltpatches.data.Id;
import org.apache.zookeeper_voltpatches.server.Request;
public class AgreementTaskMessage extends VoltMessage {

    public Request m_request;
    public long m_txnId;
    public int m_initiatorId;
    public long m_lastSafeTxnId;

    public AgreementTaskMessage() {}

    public AgreementTaskMessage(Request request, long txnId, int initiatorId, long lastSafeTxnId) {
        m_request = new Request(request);
        m_txnId = txnId;
        m_initiatorId = initiatorId;
        m_lastSafeTxnId = lastSafeTxnId;
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id

        m_txnId = m_buffer.getLong();
        m_initiatorId = m_buffer.getInt();
        m_lastSafeTxnId = m_buffer.getLong();

        long sessionId = m_buffer.getLong();
        int cxid = m_buffer.getInt();
        int type = m_buffer.getInt();
        int requestBytesLength = m_buffer.getInt();
        ByteBuffer requestBuffer = null;
        if (requestBytesLength > -1) {
            int oldlimit = m_buffer.limit();
            int oldposition = m_buffer.position();
            m_buffer.limit(m_buffer.position() + requestBytesLength);
            requestBuffer = m_buffer.slice();
            m_buffer.limit(oldlimit);
            m_buffer.position(oldposition + requestBytesLength);
        }
        ArrayList<Id> ids = new ArrayList<Id>();
        ArrayList<String> schemes = new ArrayList<String>();
        ArrayList<String> names = new ArrayList<String>();
        int numIds = m_buffer.getInt();
        if (numIds > -1) {
            try {
                for (int ii = 0; ii < numIds; ii++) {
                    byte bytes[] = new byte[m_buffer.getInt()];
                    m_buffer.get(bytes);
                    schemes.add(new String(bytes, "UTF-8"));
                }
                for (int ii = 0; ii < numIds; ii++) {
                    byte bytes[] = new byte[m_buffer.getInt()];
                    m_buffer.get(bytes);
                    names.add(new String(bytes, "UTF-8"));
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            for (int ii = 0; ii < numIds; ii++) {
                ids.add(new Id(schemes.get(ii), names.get(ii)));
            }
        } else {
            ids = null;
        }

        m_request = new Request(null, sessionId, cxid, type, requestBuffer, ids);
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) throws IOException {
        int msgsize = 44;
        if (m_request.authInfo != null) {
            msgsize += (8 * m_request.authInfo.size());
        }
        if (m_request.request != null) {
            msgsize += m_request.request.remaining();
        }
        ArrayList<byte[]> schemes = new ArrayList<byte[]>();
        ArrayList<byte[]> ids = new ArrayList<byte[]>();
        if (m_request.authInfo != null) {
            for (Id id : m_request.authInfo) {
                byte bytes[] = id.getScheme().getBytes("UTF-8");
                schemes.add(bytes);
                msgsize += bytes.length;

                bytes = id.getId().getBytes("UTF-8");
                ids.add(id.getId().getBytes());
                msgsize += bytes.length;
            }
        }


        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(AGREEMENT_TASK_ID);

        m_buffer.putLong(m_txnId);
        m_buffer.putInt(m_initiatorId);
        m_buffer.putLong(m_lastSafeTxnId);

        m_buffer.putLong(m_request.sessionId);
        m_buffer.putInt(m_request.cxid);
        m_buffer.putInt(m_request.type);
        if (m_request.request != null) {
            m_buffer.putInt(m_request.request.remaining());
            m_buffer.put(m_request.request.duplicate());
        } else {
            m_buffer.putInt(-1);
        }

        if (m_request.authInfo != null) {
            m_buffer.putInt(schemes.size());
            for (byte bytes[] : schemes) {
                m_buffer.putInt(bytes.length);
                m_buffer.put(bytes);
            }
            for (byte bytes[] : ids) {
                m_buffer.putInt(bytes.length);
                m_buffer.put(bytes);
            }
        } else {
            m_buffer.putInt(-1);
        }
        m_buffer.limit(m_buffer.position());
    }

}
