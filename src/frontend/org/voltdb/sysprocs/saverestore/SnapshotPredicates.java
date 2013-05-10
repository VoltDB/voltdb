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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.voltdb.expressions.AbstractExpression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A helper class to encapsulate the serialization of snapshot predicates.
 */
public class SnapshotPredicates {
    private final List<AbstractExpression> m_predicates = new ArrayList<AbstractExpression>();
    private final boolean m_deleteTuples;

    public SnapshotPredicates()
    {
        m_deleteTuples = false;
    }

    public SnapshotPredicates(boolean deleteTuples)
    {
        m_deleteTuples = deleteTuples;
    }

    public void addPredicate(AbstractExpression predicate)
    {
        m_predicates.add(predicate);
    }

    public byte[] toBytes()
    {
        byte[][] predicates = new byte[m_predicates.size()][];
        int i = 0;
        int size = 0;
        for (AbstractExpression predicate : m_predicates) {
            predicates[i] = predicate.toJSONString().getBytes(Charsets.UTF_8);
            size += predicates[i].length;
            i++;
        }

        ByteBuffer buf = ByteBuffer.allocate(1 + // deleteTuples
                                             4 + // predicate count
                                             4 * predicates.length + // predicate byte lengths
                                             size); // predicate bytes

        buf.put(m_deleteTuples ? 1 : (byte) 0);
        buf.putInt(m_predicates.size());
        for (byte[] predicate : predicates) {
            buf.putInt(predicate.length);
            buf.put(predicate);
        }

        return buf.array();
    }
}
