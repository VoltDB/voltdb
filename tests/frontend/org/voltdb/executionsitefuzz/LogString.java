/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.executionsitefuzz;

import java.util.HashSet;

public class LogString
{
    String m_text;

    LogString(String text)
    {
        m_text = text;
    }

    public boolean equals(Object o)
    {
        if (o == this)
        {
            return true;
        }
        if (!(o instanceof LogString))
        {
            return false;
        }
        LogString other = (LogString) o;
        return m_text.equals(other.m_text);
    }

    public String toString()
    {
        return m_text;
    }

    boolean isFuzz()
    {
        return m_text.contains("FUZZTEST");
    }

    boolean isTxnStart()
    {
        return m_text.contains("beginNewTxn");
    }

    Long getTxnId()
    {
        if (!isTxnStart() && !isTxnEnd() && !isRollback())
        {
            throw new RuntimeException("getTxnId called on inappropriate message");
        }
        return Long.valueOf(m_text.substring(m_text.indexOf("FUZZTEST")).split(" ")[2]);
    }

    boolean isSinglePart()
    {
        return m_text.contains("single");
    }

    boolean isMultiPart()
    {
        return m_text.contains("multi");
    }

    boolean isReadOnly()
    {
        if (!isTxnStart())
            throw new RuntimeException("isReadOnly called on inappropriate message");

        return m_text.contains("readonly");
    }

    boolean isCoordinator()
    {
        if (!isTxnStart())
            throw new RuntimeException("isCoordinator called on inappropriate message");

        return m_text.contains("coord");
    }

    boolean isOtherFault()
    {
        return m_text.contains("handleNodeFault");
    }

    boolean isSelfFault()
    {
        return m_text.contains("selfNodeFailure");
    }

    HashSet<Integer> getFaultNodes()
    {
        if (!isOtherFault())
        {
            throw new RuntimeException("getFaultNode called on non-fault message");
        }
        HashSet<Integer> retval = new HashSet<Integer>();
        String[] failed_nodes =
            m_text.substring(m_text.indexOf("handleNodeFault"),
                             m_text.indexOf("with")).split(" ");
        // index 0 is handleNodeFault, skip it
        for (int i = 1; i < failed_nodes.length; i++)
        {
            retval.add(Integer.valueOf(failed_nodes[i]));
        }
        return retval;
    }

    boolean isRollback()
    {
        return m_text.contains("rollbackTransaction");
    }

    boolean isTxnEnd()
    {
        return m_text.contains("completeTransaction");
    }
}
