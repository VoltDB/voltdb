/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
package org.voltdb.dtxn;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltdb.MailboxNodeContent;
import org.voltdb.VoltZK.MailboxType;

public class MailboxPublisher {
    private final JSONObject m_mailboxes = new JSONObject();
    private final String m_publishPath;
    private boolean publishedOnce = false;
    public MailboxPublisher(String publishPath) {
        m_publishPath = publishPath;
    }

    // Mailbox JSON looks like:
    // ['ExecutionSite':[{'HSId':hsid,
    //                    'partitionId':partitionid}, ...]
    //  'ClientInterface':[{'HSId':hsid}, ...],
    //  'Initiator':[{'HSId':hsid}, ...],
    //  'StatsAgent':[{'HSId':hsid}, ...]
    // ]

    public synchronized void publish(ZooKeeper zk) {
        try {
            byte payload[] = m_mailboxes.toString(4).getBytes("UTF-8");
            if (!publishedOnce) {
                zk.create(
                        m_publishPath ,
                        payload,
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                publishedOnce = true;
            } else {
                zk.setData(m_publishPath, payload, -1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void registerMailbox(MailboxType type, MailboxNodeContent mnc) {
        try {
            JSONArray mailboxes = null;
            if (!m_mailboxes.has(type.name())) {
                mailboxes = new JSONArray();
                m_mailboxes.put(type.name(), mailboxes);
            } else {
                mailboxes = m_mailboxes.getJSONArray(type.name());
            }

            for (int ii = 0; ii < mailboxes.length(); ii++) {
                if (mailboxes.getJSONObject(ii).getLong("HSId") == mnc.HSId) {
                    throw new RuntimeException("Mailbox with HSId " + mnc.HSId + " is already registered");
                }
            }

            JSONObject mailbox = new JSONObject();
            mailbox.put("HSId", mnc.HSId);
            if (mnc.partitionId != null) {
                mailbox.put("partitionId", mnc.partitionId);
            }

            mailboxes.put(mailbox);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
