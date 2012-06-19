/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper_voltpatches.server.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jute_voltpatches.InputArchive;
import org.apache.jute_voltpatches.OutputArchive;
import org.apache.jute_voltpatches.Record;
import org.apache.zookeeper_voltpatches.server.util.SerializeUtils;
import org.apache.zookeeper_voltpatches.ZooDefs.OpCode;
import org.apache.zookeeper_voltpatches.server.DataTree;
import org.apache.zookeeper_voltpatches.server.ZooTrace;
import org.apache.zookeeper_voltpatches.txn.CreateSessionTxn;
import org.apache.zookeeper_voltpatches.txn.CreateTxn;
import org.apache.zookeeper_voltpatches.txn.DeleteTxn;
import org.apache.zookeeper_voltpatches.txn.ErrorTxn;
import org.apache.zookeeper_voltpatches.txn.SetACLTxn;
import org.apache.zookeeper_voltpatches.txn.SetDataTxn;
import org.apache.zookeeper_voltpatches.txn.TxnHeader;
import org.voltcore.logging.VoltLogger;

public class SerializeUtils {
    private static final VoltLogger LOG = new VoltLogger(SerializeUtils.class.getSimpleName());

    public static Record deserializeTxn(InputArchive ia, TxnHeader hdr)
            throws IOException {
        hdr.deserialize(ia, "hdr");
        Record txn = null;
        switch (hdr.getType()) {
        case OpCode.createSession:
            // This isn't really an error txn; it just has the same
            // format. The error represents the timeout
            txn = new CreateSessionTxn();
            break;
        case OpCode.closeSession:
            return null;
        case OpCode.create:
            txn = new CreateTxn();
            break;
        case OpCode.delete:
            txn = new DeleteTxn();
            break;
        case OpCode.setData:
            txn = new SetDataTxn();
            break;
        case OpCode.setACL:
            txn = new SetACLTxn();
            break;
        case OpCode.error:
            txn = new ErrorTxn();
            break;
        }
        if (txn != null) {
            txn.deserialize(ia, "txn");
        }
        return txn;
    }

    public static void deserializeSnapshot(DataTree dt,InputArchive ia,
            Map<Long, Long> sessions) throws IOException {
        int count = ia.readInt("count");
        while (count > 0) {
            long id = ia.readLong("id");
            long to = ia.readLong("timeout");
            sessions.put(id, to);
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "loadData --- session in archive: " + id
                        + " with timeout: " + to);
            }
            count--;
        }
        dt.deserialize(ia, "tree");
    }

    public static void serializeSnapshot(DataTree dt,OutputArchive oa,
            Map<Long, Long> sessions) throws IOException {
        HashMap<Long, Long> sessSnap = new HashMap<Long, Long>(sessions);
        oa.writeInt(sessSnap.size(), "count");
        for (Entry<Long, Long> entry : sessSnap.entrySet()) {
            oa.writeLong(entry.getKey().longValue(), "id");
            oa.writeLong(entry.getValue().longValue(), "timeout");
        }
        dt.serialize(oa, "tree");
    }

}
