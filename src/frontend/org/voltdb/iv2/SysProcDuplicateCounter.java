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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.DependencyPair;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.utils.MiscUtils;
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
            List<Long> expectedHSIds,
            TransactionInfoBaseMessage message,
            long leaderHSID)
    {
        super(destinationHSId, realTxnId, expectedHSIds, message, leaderHSID);
    }

    /**
     * It is possible that duplicate counter will get mixed dummy responses and
     * real responses from replicas, think elastic join and rejoin. The
     * requirement here is that the duplicate counter should never mix these two
     * types of responses together in the list of tables for a given
     * dependency. Mixing them will cause problems for union later. In case of
     * mixed responses, real responses are always preferred. As long as there
     * are real responses for a dependency, dummy responses will be dropped for
     * that dependency.
     */
    @Override
    HashResult offer(FragmentResponseMessage message)
    {
        long hash = 0;
        for (int i = 0; i < message.getTableCount(); i++) {
            int depId = message.getTableDependencyIdAtIndex(i);
            VoltTable dep = message.getTableAtIndex(i);
            List<VoltTable> tables = m_alldeps.get(depId);
            if (tables == null)
            {
                tables = new ArrayList<VoltTable>();
                m_alldeps.put(depId, tables);
            }

            if (!message.isRecovering()) {
                /*
                 * If the current table is a real response, check if
                 * any previous responses were dummy, if so, replace
                 * the dummy ones with this legit response.
                 */
                if (!tables.isEmpty() && tables.get(0).getStatusCode() == VoltTableUtil.NULL_DEPENDENCY_STATUS) {
                    tables.clear();
                }

                // Only update the hash with non-dummy responses
                hash ^= MiscUtils.cheesyBufferCheckSum(dep.getBuffer());
            } else {
                /* If it's a dummy response, record it if and only if
                 * it's the first response. If the previous response
                 * is a real response, we don't want the dummy response.
                 * If the previous one is also a dummy, one should be
                 * enough.
                 */
                if (!tables.isEmpty()) {
                    continue;
                }
            }

            tables.add(dep);
        }

        // needs to be a three long array to work
        int[] hashes = new int[] { (int) hash, 0, 0 };

        return checkCommon(hashes, message.isRecovering(), message, ClientResponse.SUCCESS, null);
    }

    @Override
    FragmentResponseMessage getLastResponse()
    {
        FragmentResponseMessage unioned =
            new FragmentResponseMessage((FragmentResponseMessage)m_lastResponse);
        // union up all the deps we've collected and jam them in
        for (Entry<Integer, List<VoltTable>> dep : m_alldeps.entrySet()) {
            List<VoltTable> depTables = dep.getValue().stream().filter(
                    x -> x.getStatusCode() != VoltTableUtil.DUMMY_DEPENDENCY_STATUS).collect(Collectors.toList());

            if (depTables.isEmpty()){
                unioned.addDependency(new DependencyPair.TableDependencyPair(dep.getKey(), TransactionTask.DUMMAY_RESULT_TABLE));
            } else {
                unioned.addDependency(new DependencyPair.TableDependencyPair(dep.getKey(), VoltTableUtil.unionTables(depTables)));
            }
        }
        // we should never rollback DR buffer for MP sysprocs because we don't report the DR buffer size and therefore don't know if it is empty or not.
        unioned.setDrBufferSize(1);
        return unioned;
    }
}
