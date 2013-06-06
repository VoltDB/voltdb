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

package org.voltdb.sysprocs.saverestore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.types.ExpressionType;

import com.google.common.base.Charsets;

/**
 * A helper class to encapsulate the serialization of snapshot predicates.
 */
public class SnapshotPredicates {
    private final List<Pair<AbstractExpression, Boolean>> m_predicates =
            new ArrayList<Pair<AbstractExpression, Boolean>>();

    public void addPredicate(AbstractExpression predicate, boolean deleteTuples)
    {
        m_predicates.add(Pair.of(predicate, deleteTuples));
    }

    public byte[] toBytes()
    {
        // Special case common case where there's only one target with no predicate
        if (m_predicates.isEmpty() || m_predicates.get(0) == null) {
            return serializeEmpty();
        }

        byte[][] predicates = new byte[m_predicates.size()][];
        int i = 0;
        int size = 0;
        try {
            for (Pair<AbstractExpression, Boolean> p : m_predicates) {
                final AbstractExpression predicate = p.getFirst();
                JSONStringer stringer = new JSONStringer();
                stringer.object();
                stringer.key("triggersDelete").value(p.getSecond());
                stringer.key("predicateExpression").object();
                if (predicate == null) {
                    createAcceptAllPredicate().toJSONString(stringer);
                } else {
                    predicate.toJSONString(stringer);
                }
                stringer.endObject().endObject();
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

    private byte[] serializeEmpty()
    {
        assert m_predicates.size() == 0 && m_predicates.get(0) == null;

        ByteBuffer buf = ByteBuffer.allocate(4); // predicate count

        buf.putInt(0);

        return buf.array();
    }

    /**
     * Create a dummy always-true predicate so that EE won't complain.
     */
    private static AbstractExpression createAcceptAllPredicate()
    {
        ConstantValueExpression constant = new ConstantValueExpression();
        constant.setValueType(VoltType.TINYINT);
        constant.setValueSize(VoltType.TINYINT.getLengthInBytesForFixedTypes());
        constant.setValue("1");
        return new ComparisonExpression(ExpressionType.COMPARE_EQUAL, constant, constant);
    }
}
