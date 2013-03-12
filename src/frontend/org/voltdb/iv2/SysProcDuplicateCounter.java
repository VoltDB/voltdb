/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;

import org.voltdb.messaging.FragmentResponseMessage;

import org.voltdb.utils.MiscUtils;

import org.voltdb.VoltTable;
import org.voltdb.utils.VoltTableUtil;

/**
 * A special subclass of DuplicateCounter for multi-part
 * system procedures that expect to see results from every
 * site.  It unions the VoltTables in the offered
 * FragmentResponses.
 */
public class SysProcDuplicateCounter extends DuplicateCounter
{
    Map<Integer, List<VoltTable>> m_alldeps =
        new HashMap<Integer, List<VoltTable>>();

    SysProcDuplicateCounter(
            long destinationHSId,
            long realTxnId,
            List<Long> expectedHSIds)
    {
        super(destinationHSId, realTxnId, expectedHSIds);
    }

    @Override
    int offer(FragmentResponseMessage message)
    {
        long hash = 0;
        if (!message.isRecovering()) {
            for (int i = 0; i < message.getTableCount(); i++) {
                hash ^= MiscUtils.cheesyBufferCheckSum(message.getTableAtIndex(i).getBuffer());
                int depId = message.getTableDependencyIdAtIndex(i);
                VoltTable dep = message.getTableAtIndex(i);
                List<VoltTable> tables = m_alldeps.get(depId);
                if (tables == null)
                {
                    tables = new ArrayList<VoltTable>();
                    m_alldeps.put(depId, tables);
                }
                tables.add(dep);
            }
        }
        return checkCommon(hash, message.isRecovering(), message);
    }

    @Override
    FragmentResponseMessage getLastResponse()
    {
        FragmentResponseMessage unioned =
            new FragmentResponseMessage((FragmentResponseMessage)m_lastResponse);
        // union up all the deps we've collected and jam them in
        for (Entry<Integer, List<VoltTable>> dep : m_alldeps.entrySet()) {
            VoltTable grouped = VoltTableUtil.unionTables(dep.getValue());
            unioned.addDependency(dep.getKey(), grouped);
        }
        return unioned;
    }
}
