/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;

import com.google_voltpatches.common.base.Charsets;

/**
 * A helper class to encapsulate the serialization of snapshot predicates.
 */
public class SnapshotPredicates {
    public final int m_tableId;
    private final List<Pair<AbstractExpression, Boolean>> m_predicates =
            new ArrayList<Pair<AbstractExpression, Boolean>>();

    public SnapshotPredicates(int tableId)
    {
        m_tableId = tableId;
    }

    public void addPredicate(AbstractExpression predicate, boolean deleteTuples)
    {
        m_predicates.add(Pair.of(predicate, deleteTuples));
    }

    public byte[] toBytes()
    {
        byte[][] predicates = new byte[m_predicates.size()][];
        int i = 0;
        int size = 0;
        try {
            for (Pair<AbstractExpression, Boolean> p : m_predicates) {
                final AbstractExpression predicate = p.getFirst();
                JSONStringer stringer = new JSONStringer();
                stringer.object();
                stringer.key("triggersDelete").value(p.getSecond());
                // If the predicate is null, EE will serialize all rows to the corresponding data
                // target. It's the same as passing an always-true expression,
                // but without the overhead of the evaluating the expression. This avoids the
                // overhead when there is only one data target that wants all the rows.
                if (predicate != null) {
                    stringer.key("predicateExpression").object();
                    predicate.toJSONString(stringer);
                    stringer.endObject();
                }
                stringer.endObject();
                predicates[i] = stringer.toString().getBytes(Charsets.UTF_8);
                size += predicates[i].length;
                i++;
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to serialize snapshot predicates", true, e);
        }

        ByteBuffer buf = ByteBuffer.allocate(4 + // predicate count
                                             4 * predicates.length + // predicate byte lengths
                                             size); // predicate bytes

        buf.putInt(m_predicates.size());
        for (byte[] predicate : predicates) {
            buf.putInt(predicate.length);
            buf.put(predicate);
        }

        return buf.array();
    }
}
