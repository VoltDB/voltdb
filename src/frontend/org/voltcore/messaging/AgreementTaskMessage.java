/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
package org.voltcore.messaging;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.zookeeper_voltpatches.data.Id;
import org.apache.zookeeper_voltpatches.server.Request;

public class AgreementTaskMessage extends VoltMessage {

    public Request m_request;
    public long m_txnId;
    public long m_initiatorHSId;
    public long m_lastSafeTxnId;

    private LinkedList<byte[]> m_schemes = new LinkedList<byte[]>();
    private LinkedList<byte[]> m_ids = new LinkedList<byte[]>();

    public AgreementTaskMessage() {}

    public AgreementTaskMessage(Request request, long txnId, long initiatorHSId, long lastSafeTxnId) {
        m_request = new Request(request);
        m_txnId = txnId;
        m_initiatorHSId = initiatorHSId;
        m_lastSafeTxnId = lastSafeTxnId;
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        m_txnId = buf.getLong();
        m_initiatorHSId = buf.getLong();
        m_lastSafeTxnId = buf.getLong();

        long sessionId = buf.getLong();
        int cxid = buf.getInt();
        int type = buf.getInt();
        int requestBytesLength = buf.getInt();
        ByteBuffer requestBuffer = null;
        if (requestBytesLength > -1) {
            int oldlimit = buf.limit();
            int oldposition = buf.position();
            buf.limit(buf.position() + requestBytesLength);
            requestBuffer = buf.slice();
            buf.limit(oldlimit);
            buf.position(oldposition + requestBytesLength);
        }
        ArrayList<Id> ids = new ArrayList<Id>();
        ArrayList<String> schemes = new ArrayList<String>();
        ArrayList<String> names = new ArrayList<String>();
        int numIds = buf.getInt();
        if (numIds > -1) {
            try {
                for (int ii = 0; ii < numIds; ii++) {
                    byte bytes[] = new byte[buf.getInt()];
                    buf.get(bytes);
                    schemes.add(new String(bytes, "UTF-8"));
                }
                for (int ii = 0; ii < numIds; ii++) {
                    byte bytes[] = new byte[buf.getInt()];
                    buf.get(bytes);
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
        assert(buf.capacity() == buf.position());
    }

    @Override
    public int getSerializedSize() {
        int msgsize = 48 + super.getSerializedSize();
        if (m_request.authInfo != null) {
            msgsize += (8 * m_request.authInfo.size());
        }
        if (m_request.request != null) {
            msgsize += m_request.request.remaining();
        }
        if (m_request.authInfo != null) {
            for (Id id : m_request.authInfo) {
                try {
                    byte bytes[] = id.getScheme().getBytes("UTF-8");
                    m_schemes.add(bytes);
                    msgsize += bytes.length;

                    bytes = id.getId().getBytes("UTF-8");
                    m_ids.add(bytes);
                    msgsize += bytes.length;
                } catch (UnsupportedEncodingException e) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Shouldn't happen", false, e);
                }
            }
        }
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(VoltMessageFactory.AGREEMENT_TASK_ID);

        buf.putLong(m_txnId);
        buf.putLong(m_initiatorHSId);
        buf.putLong(m_lastSafeTxnId);

        buf.putLong(m_request.sessionId);
        buf.putInt(m_request.cxid);
        buf.putInt(m_request.type);
        if (m_request.request != null) {
            buf.putInt(m_request.request.remaining());
            buf.put(m_request.request.duplicate());
        } else {
            buf.putInt(-1);
        }

        if (m_request.authInfo != null) {
            buf.putInt(m_schemes.size());
            for (byte bytes[] : m_schemes) {
                buf.putInt(bytes.length);
                buf.put(bytes);
            }
            for (byte bytes[] : m_ids) {
                buf.putInt(bytes.length);
                buf.put(bytes);
            }
        } else {
            buf.putInt(-1);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());

        m_ids = null;
        m_schemes = null;
    }

}
